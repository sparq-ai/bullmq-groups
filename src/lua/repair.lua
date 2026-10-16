--[[
  Recount a group's in-flight jobs from their records and release what fits. Repairs a group whose
  count drifted (a write lost in a Redis failover) so that it looks full with nothing running.
  Reads every in-flight record, so it costs O(jobs in flight across the queue).
  ARGV: base, groupId
  Returns { previous count, recounted count, released[] }.
]]
local gid = ARGV[2]
local previous = tonumber(rcall('HGET', KEY_ACTIVE, gid) or '0')
local count = 0
local cursor = '0'
repeat
  local page = rcall('HSCAN', KEY_INFLIGHT, cursor, 'COUNT', 1000)
  cursor = page[1]
  local flat = page[2]
  for i = 1, #flat, 2 do
    if cjson.decode(flat[i + 1]).g == gid then
      count = count + 1
    end
  end
until cursor == '0'
if count > 0 then
  rcall('HSET', KEY_ACTIVE, gid, count)
else
  rcall('HDEL', KEY_ACTIVE, gid)
end
local out = {}
release(gid, out)
return {previous, count, withPolicy(out)}
