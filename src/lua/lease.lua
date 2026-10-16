--[[
  Reconciler, step 1: hand out in-flight jobs that are due for a check, and push their next check
  out so concurrent reconcilers do not look at the same job twice. Unconfirmed releases come back
  every PGRACE; confirmed jobs back off (GRACE, 2xGRACE, 4xGRACE ...) up to MAX_CHECK_MS, so a
  long-running job costs a handful of checks, not one every grace period.
  ARGV: base, limit
  Returns { serverNow, flat jobId, record list }.
]]
local limit = tonumber(ARGV[2])
local due = rcall('ZRANGEBYSCORE', KEY_CHECKS, '-inf', now, 'LIMIT', 0, limit)
local res = {}
for _, jobId in ipairs(due) do
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if raw then
    local rec = cjson.decode(raw)
    local delay = PGRACE_MS
    if rec.s ~= 'P' then
      local n = rec.c or 0
      delay = math.min(GRACE_MS * math.pow(2, n), MAX_CHECK_MS)
      rec.c = n + 1
      raw = cjson.encode(rec)
      rcall('HSET', KEY_INFLIGHT, jobId, raw)
    end
    rcall('ZADD', KEY_CHECKS, now + delay, jobId)
    res[#res + 1] = jobId
    res[#res + 1] = raw
  else
    rcall('ZREM', KEY_CHECKS, jobId)
  end
end
return {tostring(now), res}
