--[[
  A released job reached a final state in BullMQ. Frees its group slot and releases the next job.
  ARGV: base, jobId, outcome ('completed' | 'failed' | 'removed' | 'rejected'), nonce ('' = any),
        requireState ('' = any, 'P' = only while unconfirmed, 'A' = only once confirmed)
  Returns { status, groupId, released[] }; status is 'ok', 'blocked' or 'none' (no such release:
  already acked, or the ack belongs to another copy of the same job id).
  Under failurePolicy=block a final failure keeps the slot, so nothing else in the group runs
  until an operator retries or skips the job.
]]
local jobId, outcome = ARGV[2], ARGV[3]
local nonce = tonumber(ARGV[4])
local requireState = ARGV[5] ~= '' and ARGV[5] or nil
local raw = rcall('HGET', KEY_INFLIGHT, jobId)
if not raw then
  return {'none', '', {}}
end
local rec = cjson.decode(raw)
if (nonce and rec.r ~= nonce) or (requireState and rec.s ~= requireState) then
  return {'none', rec.g, {}}
end
if outcome == 'failed' and policy() == 'block' then
  if rec.b then
    return {'none', rec.g, {}} -- already blocked (a resent or second ack)
  end
  rec.b = 1
  rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
  rcall('RPUSH', blockedKey(rec.g), jobId)
  rcall('HINCRBY', KEY_BLOCKED, rec.g, 1)
  rcall('ZREM', KEY_CHECKS, jobId)
  return {'blocked', rec.g, {}}
end
local out = {}
local gid = finish(jobId, out)
return {'ok', gid, withPolicy(out)}
