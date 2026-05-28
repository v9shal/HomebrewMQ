-- enqueue.lua
-- Atomically check queue size and enqueue a job
--
-- KEYS[1]  target queue (readyKey or delayedKey or affinity key)
-- KEYS[2]  job hash key (job:{id})
--
-- ARGV[1]  maxQueueSize
-- ARGV[2]  score (priority or runAt)
-- ARGV[3]  jobId
-- ARGV[4..] flat key-value pairs for HSET

local targetQueue = KEYS[1]
local jobHashKey  = KEYS[2]

local maxSize = tonumber(ARGV[1])
local score   = tonumber(ARGV[2])
local jobId   = ARGV[3]

-- Atomic size check
local currentSize = redis.call('ZCARD', targetQueue)
if currentSize >= maxSize then
  return redis.error_reply('QUEUE_FULL')
end

-- Build HSET args from ARGV[4..]
local hashArgs = {}
for i = 4, #ARGV do
  hashArgs[#hashArgs + 1] = ARGV[i]
end

redis.call('HSET', jobHashKey, unpack(hashArgs))
redis.call('ZADD', targetQueue, score, jobId)

return 1
