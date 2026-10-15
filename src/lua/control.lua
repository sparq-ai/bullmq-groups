--[[
  Per-group operations that change what may run.
  ARGV: base, op, groupId, arg
    pause          stop releasing jobs; jobs already running finish normally
    resume         release up to capacity again
    concurrency    arg = new limit for this group, or '' to fall back to the queue default
    retry-blocked  unblock and return the failed job id so the caller can retry it in BullMQ
    skip-blocked   give up on the failed job and move on to the group's next job
  Returns { status, info, released[] }.
]]
local op, gid, arg = ARGV[2], ARGV[3], ARGV[4]
local out = {}
local info = ''
if op == 'pause' then
  rcall('SADD', KEY_PAUSED, gid)
elseif op == 'resume' then
  rcall('SREM', KEY_PAUSED, gid)
  release(gid, out)
elseif op == 'concurrency' then
  if arg == '' then
    rcall('HDEL', KEY_CONC, gid)
  else
    rcall('HSET', KEY_CONC, gid, arg)
  end
  release(gid, out)
elseif op == 'retry-blocked' or op == 'skip-blocked' then
  local jobId = rcall('HGET', KEY_BLOCKED, gid)
  if not jobId then
    return {'none', '', out}
  end
  info = jobId
  if op == 'skip-blocked' then
    finish(jobId, out)
  else
    local raw = rcall('HGET', KEY_INFLIGHT, jobId)
    rcall('HDEL', KEY_BLOCKED, gid)
    if raw then
      local rec = cjson.decode(raw)
      rec.b = nil
      rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
      rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
    end
  end
else
  return redis.error_reply('unknown group op ' .. op)
end
return {'ok', info, out}
