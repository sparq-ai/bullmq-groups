--[[
  Status and pending-job count of each given group (see groupStatus in helpers).
  ARGV: base, then group ids
  Returns a flat list: status, count, status, count ...
]]
local res = {}
for i = 2, #ARGV do
  local gid = ARGV[i]
  local pending = pendingCount(gid)
  res[#res + 1] = groupStatus(gid, pending)
  res[#res + 1] = pending
end
return res
