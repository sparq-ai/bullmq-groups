--[[
  Shared prelude, prepended to every script in this folder.

  All group state lives under one key base (ARGV[1]) that shares the BullMQ queue's prefix, so a
  hash-tagged prefix keeps every key in one cluster slot. KEYS[1] is the meta key; it is declared
  only so Redis Cluster routes the script to the right slot.

  Key layout (base = "<prefix>:<queue>:gq:"):
    meta        HASH  concurrency (absent: no per-group limit), failurePolicy, grace,
                      waiting (pending count), seq, rel
    w:<gid>     LIST  pending jobs of one group, FIFO, entries "<readyAt>:<jobId>"
    p:<gid>     ZSET  prioritized pending jobs of one group, member "<seq16>:<readyAt>:<jobId>"
    data        HASH  jobId -> payload, kept until BullMQ has the job
    groups      ZSET  gid -> readyAt of the job the group runs next (only groups with pending jobs)
    active      HASH  gid -> jobs in flight (released to BullMQ, not yet finished)
    inflight    HASH  jobId -> {g=gid, s='P'|'A', t=releasedAt, r=nonce[, b=1][, c=checks]}
    turn        HASH  gid -> its released job that has not started yet (groups with no limit only)
    checks      ZSET  jobId -> when the reconciler should next look at this in-flight job
    delayed     ZSET  "<seq16>:<jobId>" -> readyAt, for jobs added with a delay
    dmeta       HASH  jobId -> {g=gid, p=priority, s=its member in delayed} for delayed jobs
    paused      SET   paused group ids
    conc        HASH  gid -> per-group concurrency override
    blocked     HASH  gid -> number of its jobs that failed for good under failurePolicy=block
    b:<gid>     LIST  those jobs, oldest first
    dedup       HASH  deduplication id -> jobId, while that job is pending or in flight
    jdedup      HASH  jobId -> its deduplication id

  Every release gets a nonce ("r"). It travels to BullMQ in the job's opts.group.r, so acks,
  confirmations and the worker's start check can tell this release from any other copy of the
  same job id (a late add, a resent command, an id reused after the job finished).

  The only BullMQ key the scripts read is the job hash "<prefix>:<queue>:<jobId>": whether it
  exists, and the nonce in its opts. Job ids never contain ':' (BullMQ rejects that), so entries
  split on the first ':'.
]]
local rcall = redis.call
local base = ARGV[1]
local timeNow = rcall('TIME')
local now = tonumber(timeNow[1]) * 1000 + math.floor(tonumber(timeNow[2]) / 1000)

local KEY_META = base .. 'meta'
local KEY_DATA = base .. 'data'
local KEY_GROUPS = base .. 'groups'
local KEY_ACTIVE = base .. 'active'
local KEY_INFLIGHT = base .. 'inflight'
local KEY_CHECKS = base .. 'checks'
local KEY_DELAYED = base .. 'delayed'
local KEY_DMETA = base .. 'dmeta'
local KEY_PAUSED = base .. 'paused'
local KEY_CONC = base .. 'conc'
local KEY_BLOCKED = base .. 'blocked'
local KEY_DEDUP = base .. 'dedup'
local KEY_JDEDUP = base .. 'jdedup'
local KEY_TURN = base .. 'turn'
-- BullMQ's job hashes live at "<prefix>:<queue>:<jobId>": our base without its trailing "gq:".
local BULL_JOB = string.sub(base, 1, -4)

-- How long a confirmed in-flight job goes unchecked by the reconciler. Stored in meta so producers
-- and workers agree.
local GRACE_MS = tonumber(rcall('HGET', KEY_META, 'grace') or '30000')
-- Unconfirmed releases are checked much sooner: adding one to BullMQ again is harmless, because the
-- job id is the same and workers skip a copy whose nonce no longer matches.
local PGRACE_MS = math.min(GRACE_MS, 5000)
-- Confirmed jobs are re-checked with back-off (GRACE, 2xGRACE, 4xGRACE ...) up to this.
local MAX_CHECK_MS = 300000

local function waitKey(gid) return base .. 'w:' .. gid end
local function prioKey(gid) return base .. 'p:' .. gid end
local function blockedKey(gid) return base .. 'b:' .. gid end

local function policy()
  return rcall('HGET', KEY_META, 'failurePolicy') or 'continue'
end

-- Zero-padded so members compare as strings in insertion order.
local function nextSeq()
  return string.format('%016.0f', rcall('HINCRBY', KEY_META, 'seq', 1))
end

-- "<readyAt>:<jobId>" -> readyAt, jobId
local function splitEntry(e)
  local i = string.find(e, ':', 1, true)
  return tonumber(string.sub(e, 1, i - 1)), string.sub(e, i + 1)
end

-- "<seq16>:<readyAt>:<jobId>" -> readyAt, jobId
local function splitPrio(m)
  return splitEntry(string.sub(m, 18))
end

-- The nonce BullMQ's job hash carries for this id: nil when BullMQ has no such job, false when
-- it has one that was not released by this package.
local function bullNonce(jobId)
  local opts = rcall('HGET', BULL_JOB .. jobId, 'opts')
  if not opts then return nil end
  local ok, o = pcall(cjson.decode, opts)
  if ok and type(o) == 'table' and type(o.group) == 'table' and o.group.r then
    return tonumber(o.group.r)
  end
  return false
end

-- readyAt of the job this group will run next, or nil when nothing is pending.
-- Jobs without a priority run before prioritized ones, as in BullMQ.
local function headTime(gid)
  local e = rcall('LINDEX', waitKey(gid), 0)
  if e then
    local t = splitEntry(e)
    return t
  end
  local m = rcall('ZRANGE', prioKey(gid), 0, 0)
  if m[1] then
    local t = splitPrio(m[1])
    return t
  end
  return nil
end

local function refreshGroupIndex(gid)
  local t = headTime(gid)
  if t then
    rcall('ZADD', KEY_GROUPS, t, gid)
  else
    rcall('ZREM', KEY_GROUPS, gid)
  end
end

-- How many jobs of the group may run at once, or nil for no limit (the default, as in BullMQ Pro).
local function capacity(gid)
  local c = rcall('HGET', KEY_CONC, gid) or rcall('HGET', KEY_META, 'concurrency')
  return c and tonumber(c) or nil
end

--[[
  A group with no limit keeps one released job that has not started in BullMQ's wait list: its
  turn. When that job starts, the next one is released behind every other group's turn, so jobs
  of a group start in order and take turns with other groups, and any number of them may run at
  once. The start check and finish() clear the marker in the same script that starts or finishes
  its job. Returns true while the group's turn is still waiting; drops a marker whose job is gone
  (a write lost in a failover).
]]
local function turnPending(gid)
  local jobId = rcall('HGET', KEY_TURN, gid)
  if not jobId then return false end
  if rcall('HEXISTS', KEY_INFLIGHT, jobId) == 1 then return true end
  rcall('HDEL', KEY_TURN, gid)
  return false
end

local function pendingCount(gid)
  return rcall('LLEN', waitKey(gid)) + rcall('ZCARD', prioKey(gid))
end

local function pushJob(gid, jobId, priority, readyAt)
  if priority > 0 then
    rcall('ZADD', prioKey(gid), priority, nextSeq() .. ':' .. readyAt .. ':' .. jobId)
  else
    rcall('RPUSH', waitKey(gid), readyAt .. ':' .. jobId)
  end
  rcall('HINCRBY', KEY_META, 'waiting', 1)
  refreshGroupIndex(gid)
end

--[[
  Move head jobs of `gid` into flight while the group has capacity: up to its limit, or for a group
  with no limit one job while it has no turn waiting. Each released job is appended to `out` as
  jobId, nonce, payload. This is the only place jobs leave a group, and it always runs inside the
  same script as the event that freed capacity, so two callers can never release the same job or
  overfill a group.
]]
local function release(gid, out)
  if rcall('SISMEMBER', KEY_PAUSED, gid) == 1 or rcall('HEXISTS', KEY_BLOCKED, gid) == 1 then
    return
  end
  local limit = capacity(gid)
  local active = tonumber(rcall('HGET', KEY_ACTIVE, gid) or '0')
  local budget
  if limit then
    budget = limit - active
  elseif turnPending(gid) then
    budget = 0
  else
    budget = 1
  end
  local popped = 0
  while popped < budget do
    local jobId
    local e = rcall('LPOP', waitKey(gid))
    if e then
      local _, id = splitEntry(e)
      jobId = id
    else
      local m = rcall('ZPOPMIN', prioKey(gid))
      if not m[1] then break end
      local _, id = splitPrio(m[1])
      jobId = id
    end
    popped = popped + 1
    active = active + 1
    local nonce = rcall('HINCRBY', KEY_META, 'rel', 1)
    rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode({g = gid, s = 'P', t = now, r = nonce}))
    rcall('ZADD', KEY_CHECKS, now + PGRACE_MS, jobId)
    if not limit then
      rcall('HSET', KEY_TURN, gid, jobId)
    end
    out[#out + 1] = jobId
    out[#out + 1] = tostring(nonce)
    out[#out + 1] = rcall('HGET', KEY_DATA, jobId) or ''
  end
  if popped > 0 then
    rcall('HSET', KEY_ACTIVE, gid, active)
    rcall('HINCRBY', KEY_META, 'waiting', -popped)
    refreshGroupIndex(gid)
  end
end

-- Released jobs go back to the caller with the failure policy in front, so the job's BullMQ options
-- follow the policy in force now, not the one the producer saw when it started.
local function withPolicy(out)
  if #out > 0 then
    table.insert(out, 1, policy())
  end
  return out
end

local function clearDedup(jobId)
  local dd = rcall('HGET', KEY_JDEDUP, jobId)
  if dd then
    rcall('HDEL', KEY_JDEDUP, jobId)
    if rcall('HGET', KEY_DEDUP, dd) == jobId then
      rcall('HDEL', KEY_DEDUP, dd)
    end
  end
end

local function unblockJob(gid, jobId)
  if rcall('LREM', blockedKey(gid), 0, jobId) > 0 then
    if rcall('HINCRBY', KEY_BLOCKED, gid, -1) <= 0 then
      rcall('HDEL', KEY_BLOCKED, gid)
    end
  end
end

--[[
  An in-flight job is done for good (completed, failed after its last attempt, removed, rejected
  by BullMQ, or skipped by an operator): free its slot and release the group's next job.
  With `nonce`, only that release is finished; with `requireState`, only while the record is
  still in that state. A second call for the same release finds no record and does nothing.
]]
local function finish(jobId, out, nonce, requireState)
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if not raw then return nil end
  local rec = cjson.decode(raw)
  if (nonce and rec.r ~= nonce) or (requireState and rec.s ~= requireState) then
    return nil
  end
  local gid = rec.g
  rcall('HDEL', KEY_INFLIGHT, jobId)
  rcall('ZREM', KEY_CHECKS, jobId)
  rcall('HDEL', KEY_DATA, jobId)
  clearDedup(jobId)
  if rcall('HGET', KEY_TURN, gid) == jobId then
    rcall('HDEL', KEY_TURN, gid)
  end
  if rec.b then
    unblockJob(gid, jobId)
  end
  if rcall('HINCRBY', KEY_ACTIVE, gid, -1) <= 0 then
    rcall('HDEL', KEY_ACTIVE, gid)
  end
  release(gid, out)
  return gid
end

-- Give up on every blocked job of a group (failurePolicy is no longer 'block').
local function unblockAll(gid, out)
  for _, jobId in ipairs(rcall('LRANGE', blockedKey(gid), 0, -1)) do
    finish(jobId, out)
  end
end

-- Forget a pending job that is leaving its group without running (removed or deleted).
local function dropPending(jobId)
  rcall('HDEL', KEY_DATA, jobId)
  clearDedup(jobId)
end

-- "<anything>:<jobId>" ends with this job id (ids never contain ':').
local function endsWithId(entry, jobId)
  return string.sub(entry, -(#jobId + 1)) == ':' .. jobId
end

--[[
  The group's status, as BullMQ Pro reports it: 'paused', 'maxed' (as many jobs running as its
  limit allows), 'waiting', plus 'blocked' (failurePolicy=block). '' when the group has no pending
  jobs and is neither paused nor blocked: Pro has no such group.
]]
local function groupStatus(gid, pending)
  if rcall('SISMEMBER', KEY_PAUSED, gid) == 1 then return 'paused' end
  if rcall('HEXISTS', KEY_BLOCKED, gid) == 1 then return 'blocked' end
  if pending == 0 then return '' end
  local limit = capacity(gid)
  if limit and tonumber(rcall('HGET', KEY_ACTIVE, gid) or '0') >= limit then return 'maxed' end
  return 'waiting'
end
