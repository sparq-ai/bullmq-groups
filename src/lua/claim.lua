--[[
  A worker is about to run a released job: check that this BullMQ job is the release the group is
  waiting for, before the processor runs.
  ARGV: base, jobId, nonce, groupId, startedBefore ('1' when this BullMQ job ran before)
  Returns { verdict, failurePolicy, released[] }:
    run      the current release: run it (and mark it confirmed if the confirmation was lost)
    paused   the current release, but its group is paused: run it after resume
    stale    another release of the same job id is in flight: this copy must not run
    ghost    no release is in flight: a late or repeated add re-created a job that already
             finished; it must not run again
    adopted  a finished job retried from outside the package (a dashboard, job.retry): it got a
             free slot of its group and may run
    wait     same, but the group has no free slot (or is paused or blocked): try again later
  A job that was its group's turn hands it on as it starts: a group with no limit releases its
  next job here.
]]
local jobId, nonce, gid = ARGV[2], tonumber(ARGV[3]), ARGV[4]
local startedBefore = ARGV[5] == '1'
local pol = policy()
local out = {}
local raw = rcall('HGET', KEY_INFLIGHT, jobId)
if raw then
  local rec = cjson.decode(raw)
  if rec.r ~= nonce then
    return {'stale', pol, out}
  end
  if rcall('SISMEMBER', KEY_PAUSED, gid) == 1 then
    return {'paused', pol, out}
  end
  if rec.s == 'P' then
    rec.s = 'A'
    rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
    rcall('HDEL', KEY_DATA, jobId)
    rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
  end
  if rcall('HGET', KEY_TURN, rec.g) == jobId then
    -- This job was its group's turn: it starts now, so the group's next job takes its place.
    rcall('HDEL', KEY_TURN, rec.g)
    release(rec.g, out)
  end
  return {'run', pol, withPolicy(out)}
end
if not startedBefore then
  return {'ghost', pol, out}
end
if rcall('SISMEMBER', KEY_PAUSED, gid) == 1 or rcall('HEXISTS', KEY_BLOCKED, gid) == 1 then
  return {'wait', pol, out}
end
local active = tonumber(rcall('HGET', KEY_ACTIVE, gid) or '0')
local limit = capacity(gid)
if limit and active >= limit then
  return {'wait', pol, out}
end
rcall('HSET', KEY_ACTIVE, gid, active + 1)
rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode({g = gid, s = 'A', t = now, r = nonce}))
rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
return {'adopted', pol, out}
