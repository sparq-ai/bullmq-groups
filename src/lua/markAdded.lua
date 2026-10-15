--[[
  BullMQ now holds these released jobs: drop our copy of the payload and record that the add
  happened, so the reconciler can tell "never added" from "finished and removed".
  ARGV: base, then jobId, nonce pairs. The nonce stops a late confirmation from touching
  a newer job that reuses the same custom id.
]]
for i = 2, #ARGV, 2 do
  local jobId, nonce = ARGV[i], tonumber(ARGV[i + 1])
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if raw then
    local rec = cjson.decode(raw)
    if rec.r == nonce and rec.s == 'P' then
      rec.s = 'A'
      rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
      rcall('HDEL', KEY_DATA, jobId)
    end
  end
end
return 1
