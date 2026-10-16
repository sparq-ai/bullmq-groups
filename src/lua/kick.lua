--[[
  Reconciler, step 2: release anything the given groups have room for. Normally a no-op, because
  every event that frees capacity releases in the same script. It repairs groups after lost
  writes (e.g. a Redis failover) or a default-concurrency change. Once failurePolicy is no longer
  'block', it also lets go of jobs that still block their group, idle groups included.
  ARGV: base, then group ids (one ZSCAN page of the group index, read by the caller: a full scan
  visits every group that waits throughout, however releases reorder the index meanwhile)
  Returns { released[], repairedGroups[] }.
]]
local out, repaired = {}, {}
if policy() ~= 'block' then
  local blocked = rcall('HKEYS', KEY_BLOCKED)
  for i = 1, math.min(#blocked, 100) do
    local before = #out
    unblockAll(blocked[i], out)
    if #out > before then
      repaired[#repaired + 1] = blocked[i]
    end
  end
end
for i = 2, #ARGV do
  local gid = ARGV[i]
  local before = #out
  release(gid, out)
  if #out > before then
    repaired[#repaired + 1] = gid
  end
end
return {withPolicy(out), repaired}
