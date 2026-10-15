--[[
  Reconciler, step 1: hand out in-flight jobs that are due for a check, and push their next check
  out by the grace period so concurrent reconcilers do not look at the same job twice.
  ARGV: base, limit
  Returns { serverNow, flat jobId, record list }.
]]
local limit = tonumber(ARGV[2])
local due = rcall('ZRANGEBYSCORE', KEY_CHECKS, '-inf', now, 'LIMIT', 0, limit)
local res = {}
for _, jobId in ipairs(due) do
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if raw then
    rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
    res[#res + 1] = jobId
    res[#res + 1] = raw
  else
    rcall('ZREM', KEY_CHECKS, jobId)
  end
end
return {tostring(now), res}
