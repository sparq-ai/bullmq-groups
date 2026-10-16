--[[
  Move delayed jobs that are due into the tail of their group, then release what fits.
  A delayed job joins its group's FIFO when it becomes due, the same way BullMQ moves a due
  delayed job to the end of the wait list. Members carry an insertion sequence, so jobs due in the
  same millisecond keep the order they were added in.
  ARGV: base, limit
  Returns { ms until the next delayed job is due, or '-1', released[] }.
]]
local limit = tonumber(ARGV[2])
local due = rcall('ZRANGEBYSCORE', KEY_DELAYED, '-inf', now, 'WITHSCORES', 'LIMIT', 0, limit)
local out, touched, order = {}, {}, {}
for i = 1, #due, 2 do
  local member = due[i]
  local readyAt = tonumber(due[i + 1])
  rcall('ZREM', KEY_DELAYED, member)
  local jobId = string.sub(member, 18)
  local raw = rcall('HGET', KEY_DMETA, jobId)
  rcall('HDEL', KEY_DMETA, jobId)
  if raw then
    local m = cjson.decode(raw)
    pushJob(m.g, jobId, m.p, readyAt)
    if not touched[m.g] then
      touched[m.g] = true
      order[#order + 1] = m.g
    end
  end
end
for _, gid in ipairs(order) do
  release(gid, out)
end
local nxt = rcall('ZRANGE', KEY_DELAYED, 0, 0, 'WITHSCORES')
local wait = nxt[2] and tostring(math.max(0, tonumber(nxt[2]) - now)) or '-1'
return {wait, withPolicy(out)}
