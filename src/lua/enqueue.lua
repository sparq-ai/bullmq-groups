--[[
  Add one or more jobs to their groups, then release whatever the touched groups have room for.
  ARGV: base, count, then per job: jobId, groupId, payload, delayMs, priority
  Returns { statuses[], released[] } where released is a flat jobId, nonce, payload list.
  A job id that is still pending or in flight is reported as 'duplicate' and not added again,
  which makes a retried add (lost reply, reconnect) harmless.
]]
local count = tonumber(ARGV[2])
local statuses, out, touched, order = {}, {}, {}, {}
for i = 0, count - 1 do
  local o = 3 + i * 5
  local jobId, gid, payload = ARGV[o], ARGV[o + 1], ARGV[o + 2]
  local delay, priority = tonumber(ARGV[o + 3]), tonumber(ARGV[o + 4])
  if rcall('HEXISTS', KEY_DATA, jobId) == 1 or rcall('HEXISTS', KEY_INFLIGHT, jobId) == 1 then
    statuses[#statuses + 1] = 'duplicate'
  else
    rcall('HSET', KEY_DATA, jobId, payload)
    if delay > 0 then
      rcall('ZADD', KEY_DELAYED, now + delay, jobId)
      rcall('HSET', KEY_DMETA, jobId, cjson.encode({g = gid, p = priority}))
      statuses[#statuses + 1] = 'delayed'
    else
      pushJob(gid, jobId, priority, now)
      statuses[#statuses + 1] = 'waiting'
      if not touched[gid] then
        touched[gid] = true
        order[#order + 1] = gid
      end
    end
  end
end
for _, gid in ipairs(order) do
  release(gid, out)
end
return {statuses, out}
