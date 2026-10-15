--[[
  Shared prelude, prepended to every script in this folder.

  All group state lives under one key base (ARGV[1]) that shares the BullMQ queue's prefix, so a
  hash-tagged prefix keeps every key in one cluster slot. KEYS[1] is the meta key; it is declared
  only so Redis Cluster routes the script to the right slot.

  Key layout (base = "<prefix>:<queue>:gq:"):
    meta        HASH  concurrency, failurePolicy, grace, waiting (pending count), seq, rel
    w:<gid>     LIST  pending jobs of one group, FIFO, entries "<readyAt>:<jobId>"
    p:<gid>     ZSET  prioritized pending jobs of one group, member "<seq16>:<readyAt>:<jobId>"
    data        HASH  jobId -> payload, kept until BullMQ has the job
    groups      ZSET  gid -> readyAt of the job the group runs next (only groups with pending jobs)
    active      HASH  gid -> jobs in flight (released to BullMQ, not yet finished)
    inflight    HASH  jobId -> {g=gid, s='P'|'A', t=releasedAt, r=nonce[, b=1]}
    checks      ZSET  jobId -> when the reconciler should next look at this in-flight job
    delayed     ZSET  jobId -> readyAt, for jobs added with a delay
    dmeta       HASH  jobId -> {g=gid, p=priority} for delayed jobs
    paused      SET   paused group ids
    conc        HASH  gid -> per-group concurrency override
    blocked     HASH  gid -> jobId that failed for good under failurePolicy=block

  Job ids never contain ':' (BullMQ rejects that), so entries split on the first or last ':'.
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

-- How long a released job may stay unconfirmed before the reconciler looks at it. Stored in meta so
-- producers and workers agree.
local GRACE_MS = tonumber(rcall('HGET', KEY_META, 'grace') or '30000')

local function waitKey(gid) return base .. 'w:' .. gid end
local function prioKey(gid) return base .. 'p:' .. gid end

-- "<readyAt>:<jobId>" -> readyAt, jobId
local function splitEntry(e)
  local i = string.find(e, ':', 1, true)
  return tonumber(string.sub(e, 1, i - 1)), string.sub(e, i + 1)
end

-- "<seq16>:<readyAt>:<jobId>" -> readyAt, jobId
local function splitPrio(m)
  return splitEntry(string.sub(m, 18))
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

local function capacity(gid)
  return tonumber(rcall('HGET', KEY_CONC, gid) or rcall('HGET', KEY_META, 'concurrency') or '1')
end

local function pushJob(gid, jobId, priority, readyAt)
  if priority > 0 then
    local seq = rcall('HINCRBY', KEY_META, 'seq', 1)
    rcall('ZADD', prioKey(gid), priority, string.format('%016.0f', seq) .. ':' .. readyAt .. ':' .. jobId)
  else
    rcall('RPUSH', waitKey(gid), readyAt .. ':' .. jobId)
  end
  rcall('HINCRBY', KEY_META, 'waiting', 1)
  refreshGroupIndex(gid)
end

--[[
  Move head jobs of `gid` into flight while the group has capacity. Each released job is appended
  to `out` as jobId, nonce, payload. This is the only place jobs leave a group, and it always runs
  inside the same script as the event that freed capacity, so two callers can never release the
  same job or overfill a group.
]]
local function release(gid, out)
  if rcall('SISMEMBER', KEY_PAUSED, gid) == 1 or rcall('HEXISTS', KEY_BLOCKED, gid) == 1 then
    return
  end
  local limit = capacity(gid)
  local active = tonumber(rcall('HGET', KEY_ACTIVE, gid) or '0')
  local popped = 0
  while active < limit do
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
    rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
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

--[[
  An in-flight job is done for good (completed, failed after its last attempt, removed, or
  skipped by an operator): free its slot and release the group's next job. Idempotent: a second
  call for the same job finds no record and does nothing.
]]
local function finish(jobId, out)
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if not raw then return nil end
  local gid = cjson.decode(raw).g
  rcall('HDEL', KEY_INFLIGHT, jobId)
  rcall('ZREM', KEY_CHECKS, jobId)
  rcall('HDEL', KEY_DATA, jobId)
  if rcall('HGET', KEY_BLOCKED, gid) == jobId then
    rcall('HDEL', KEY_BLOCKED, gid)
  end
  if rcall('HINCRBY', KEY_ACTIVE, gid, -1) <= 0 then
    rcall('HDEL', KEY_ACTIVE, gid)
  end
  release(gid, out)
  return gid
end
