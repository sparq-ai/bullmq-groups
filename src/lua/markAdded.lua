--[[
  BullMQ now holds these released jobs: drop our copy of the payload and record that the add
  happened, so the reconciler can tell "never added" from "finished and removed".
  ARGV: base, then jobId, nonce pairs. The nonce stops a late confirmation from touching a newer
  release of the same job id.
  A job is not confirmed when BullMQ holds a different job under its id (an older copy of the same
  job id kept as completed or failed): BullMQ ignored our add. Returns those ids. When BullMQ has no
  job at all, our add succeeded and the job already finished and was removed, so it is confirmed.
]]
local foreign = {}
for i = 2, #ARGV, 2 do
  local jobId, nonce = ARGV[i], tonumber(ARGV[i + 1])
  local raw = rcall('HGET', KEY_INFLIGHT, jobId)
  if raw then
    local rec = cjson.decode(raw)
    if rec.r == nonce and rec.s == 'P' then
      local held = bullNonce(jobId)
      if held == nil or held == nonce then
        rec.s = 'A'
        rcall('HSET', KEY_INFLIGHT, jobId, cjson.encode(rec))
        rcall('HDEL', KEY_DATA, jobId)
        rcall('ZADD', KEY_CHECKS, now + GRACE_MS, jobId)
      else
        foreign[#foreign + 1] = jobId
      end
    end
  end
end
return foreign
