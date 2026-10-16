--[[
  Add one or more jobs to their groups, then release whatever the touched groups have room for.
  ARGV: base, count, then per job: jobId, groupId, payload, delayMs, priority, dedupId ('' for
        none), maxSize (0 for none)
  Returns { statuses[], ids[], released[] }, where released is policy, then jobId, nonce, payload
  triples. ids[i] is the job's id, or for a deduplicated add the id of the job it matched.

  A job id that is still pending or in flight, or that BullMQ still holds (a finished job kept by
  removeOnComplete/removeOnFail), is reported as 'duplicate' and not added: BullMQ would ignore it.
  The same call arriving twice (ioredis resends a command whose reply was lost) is recognised by
  its identical payload: it reports the first outcome and hands back the job's release again.
  A job whose group already has maxSize jobs pending is not added: 'maxed'.
]]
local count = tonumber(ARGV[2])
local statuses, ids, out, touched, order = {}, {}, {}, {}, {}
for i = 0, count - 1 do
  local o = 3 + i * 7
  local jobId, gid, payload = ARGV[o], ARGV[o + 1], ARGV[o + 2]
  local delay, priority, dedupId = tonumber(ARGV[o + 3]), tonumber(ARGV[o + 4]), ARGV[o + 5]
  local maxSize = tonumber(ARGV[o + 6])
  local status, id = nil, jobId
  local dedupOf = dedupId ~= '' and rcall('HGET', KEY_DEDUP, dedupId)
  if rcall('HGET', KEY_DATA, jobId) == payload then
    local raw = rcall('HGET', KEY_INFLIGHT, jobId)
    if raw then
      local rec = cjson.decode(raw)
      status = 'waiting'
      if rec.s == 'P' then
        out[#out + 1] = jobId
        out[#out + 1] = tostring(rec.r)
        out[#out + 1] = payload
      end
    elseif rcall('HEXISTS', KEY_DMETA, jobId) == 1 then
      status = 'delayed'
    else
      status = 'waiting'
    end
  elseif dedupOf then
    status, id = 'duplicate', dedupOf
  elseif rcall('HEXISTS', KEY_DATA, jobId) == 1 or rcall('HEXISTS', KEY_INFLIGHT, jobId) == 1
    or rcall('EXISTS', BULL_JOB .. jobId) == 1 then
    status = 'duplicate'
  elseif maxSize > 0 and pendingCount(gid) >= maxSize then
    status = 'maxed'
  else
    rcall('HSET', KEY_DATA, jobId, payload)
    if dedupId ~= '' then
      rcall('HSET', KEY_DEDUP, dedupId, jobId)
      rcall('HSET', KEY_JDEDUP, jobId, dedupId)
    end
    if delay > 0 then
      local member = nextSeq() .. ':' .. jobId
      rcall('ZADD', KEY_DELAYED, now + delay, member)
      rcall('HSET', KEY_DMETA, jobId, cjson.encode({g = gid, p = priority, s = member}))
      status = 'delayed'
    else
      pushJob(gid, jobId, priority, now)
      status = 'waiting'
      if not touched[gid] then
        touched[gid] = true
        order[#order + 1] = gid
      end
    end
  end
  statuses[#statuses + 1] = status
  ids[#ids + 1] = id
end
for _, gid in ipairs(order) do
  release(gid, out)
end
return {statuses, ids, withPolicy(out)}
