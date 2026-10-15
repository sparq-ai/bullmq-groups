--[[
  Reconciler, step 2: walk a slice of the group index and release anything that has capacity.
  Normally a no-op, because every event that frees capacity releases in the same script. It
  repairs groups after a default-concurrency change or lost writes (e.g. a Redis failover).
  ARGV: base, start, count
  Returns { nextStart, released[], repairedGroups[] }.
]]
local start, count = tonumber(ARGV[2]), tonumber(ARGV[3])
local gids = rcall('ZRANGE', KEY_GROUPS, start, start + count - 1)
local out, repaired = {}, {}
for _, gid in ipairs(gids) do
  local before = #out
  release(gid, out)
  if #out > before then
    repaired[#repaired + 1] = gid
  end
end
local nextStart = start + count
if nextStart >= rcall('ZCARD', KEY_GROUPS) then
  nextStart = 0
end
return {tostring(nextStart), out, repaired}
