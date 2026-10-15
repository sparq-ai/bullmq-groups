--[[
  A released job reached a final state in BullMQ. Frees its group slot and releases the next job.
  ARGV: base, jobId, outcome ('completed' | 'failed' | 'removed')
  Returns { status, groupId, released[] }; status is 'ok', 'blocked' or 'none' (already acked).
  Under failurePolicy=block a final failure keeps the slot, so nothing else in the group runs
  until an operator retries or skips the job.
]]
local jobId, outcome = ARGV[2], ARGV[3]
local raw = rcall('HGET', KEY_INFLIGHT, jobId)
if not raw then
  return {'none', '', {}}
end
local rec = cjson.decode(raw)
if outcome == 'failed' and rcall('HGET', KEY_META, 'failurePolicy') == 'block' then
  rec.b = 1
  rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
  rcall('HSET', KEY_BLOCKED, rec.g, jobId)
  rcall('ZREM', KEY_CHECKS, jobId)
  return {'blocked', rec.g, {}}
end
local out = {}
local gid = finish(jobId, out)
return {'ok', gid, out}
