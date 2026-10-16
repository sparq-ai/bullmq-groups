--[[
  Remove jobs that are still waiting in their group (never handed to BullMQ).
  ARGV: base, mode, arg
    job       arg = jobId: remove that job if it is pending or delayed. Returns 1, or 0 when it is
              not waiting in a group (unknown, or already released to BullMQ).
    delayed   arg = limit: drop up to that many delayed jobs, soonest first. Returns how many.
  Finding a pending job walks its group's list, so removing one job costs O(group size).
]]
local mode, arg = ARGV[2], ARGV[3]

if mode == 'delayed' then
  local due = rcall('ZRANGE', KEY_DELAYED, 0, tonumber(arg) - 1)
  for _, member in ipairs(due) do
    local jobId = string.sub(member, 18)
    rcall('ZREM', KEY_DELAYED, member)
    rcall('HDEL', KEY_DMETA, jobId)
    dropPending(jobId)
  end
  return #due
end

local jobId = arg
local raw = rcall('HGET', KEY_DATA, jobId)
if not raw or rcall('HEXISTS', KEY_INFLIGHT, jobId) == 1 then
  return 0
end
local dm = rcall('HGET', KEY_DMETA, jobId)
if dm then
  local m = cjson.decode(dm)
  if not (m.s and rcall('ZREM', KEY_DELAYED, m.s) == 1) then
    -- Added before members were recorded: look for it.
    for _, member in ipairs(rcall('ZRANGE', KEY_DELAYED, 0, -1)) do
      if endsWithId(member, jobId) then
        rcall('ZREM', KEY_DELAYED, member)
        break
      end
    end
  end
  rcall('HDEL', KEY_DMETA, jobId)
  dropPending(jobId)
  return 1
end
local gid = cjson.decode(raw).g
local found = false
for _, e in ipairs(rcall('LRANGE', waitKey(gid), 0, -1)) do
  if endsWithId(e, jobId) then
    rcall('LREM', waitKey(gid), 1, e)
    found = true
    break
  end
end
if not found then
  for _, m in ipairs(rcall('ZRANGE', prioKey(gid), 0, -1)) do
    if endsWithId(m, jobId) then
      rcall('ZREM', prioKey(gid), m)
      found = true
      break
    end
  end
end
if not found then
  return 0
end
rcall('HINCRBY', KEY_META, 'waiting', -1)
refreshGroupIndex(gid)
dropPending(jobId)
return 1
