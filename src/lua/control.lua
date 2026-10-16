--[[
  Per-group operations that change what may run.
  ARGV: base, op, groupId, arg
    pause          stop releasing jobs; released jobs that have not started wait for resume
                   (info = '1', or '0' when it was already paused)
    resume         release up to capacity again (info = '1', or '0' when it was not paused)
    concurrency    arg = new limit for this group, or '' to fall back to the queue default
                   (info = fields added or removed, as HSET/HDEL report them)
    delete         drop up to arg pending jobs of the group, unprioritized first (info = how many)
    blocked-head   the oldest job blocking the group (info = jobId, nonce = its release nonce)
    unblock        arg = jobId: the operator retried it in BullMQ; it keeps its slot and runs
    skip-blocked   arg = jobId, or '' for the oldest: give up on it and move on
  Returns { status, info, released[], nonce }.
]]
local op, gid, arg = ARGV[2], ARGV[3], ARGV[4]
local out = {}
local info, nonce = '', ''
if op == 'pause' then
  info = tostring(rcall('SADD', KEY_PAUSED, gid))
elseif op == 'resume' then
  info = tostring(rcall('SREM', KEY_PAUSED, gid))
  release(gid, out)
elseif op == 'concurrency' then
  if arg == '' then
    info = tostring(rcall('HDEL', KEY_CONC, gid))
  else
    info = tostring(rcall('HSET', KEY_CONC, gid, arg))
  end
  release(gid, out)
elseif op == 'delete' then
  local limit = tonumber(arg)
  local n = 0
  while n < limit do
    local e = rcall('LPOP', waitKey(gid))
    if not e then break end
    local _, id = splitEntry(e)
    dropPending(id)
    n = n + 1
  end
  while n < limit do
    local m = rcall('ZPOPMIN', prioKey(gid))
    if not m[1] then break end
    local _, id = splitPrio(m[1])
    dropPending(id)
    n = n + 1
  end
  if n > 0 then
    rcall('HINCRBY', KEY_META, 'waiting', -n)
  end
  refreshGroupIndex(gid)
  info = tostring(n)
elseif op == 'blocked-head' or op == 'unblock' or op == 'skip-blocked' then
  local jobId = arg
  if jobId == '' then
    jobId = rcall('LINDEX', blockedKey(gid), 0)
  end
  local raw = jobId and rcall('HGET', KEY_INFLIGHT, jobId)
  if not raw then
    return {'none', '', out, ''}
  end
  local rec = cjson.decode(raw)
  if not rec.b or rec.g ~= gid then
    return {'none', '', out, ''}
  end
  info, nonce = jobId, tostring(rec.r)
  if op == 'unblock' then
    rec.b = nil
    rec.c = nil
    rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
    unblockJob(gid, jobId)
    rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
    -- the retried job keeps its slot; at concurrency n the group's other slots fill again
    release(gid, out)
  elseif op == 'skip-blocked' then
    finish(jobId, out)
  end
else
  return redis.error_reply('unknown group op ' .. op)
end
return {'ok', info, withPolicy(out), nonce}
