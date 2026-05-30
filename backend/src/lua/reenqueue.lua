local readyQueue      = KEYS[1]
local processingQueue = KEYS[2]

-- Identify dead workers (heartbeat key expired or never set)
local workers = redis.call('SMEMBERS', '{homebrewmq}:workers')
local deadWorkers = {}

for _, worker in ipairs(workers) do
    local heartbeat = redis.call('GET', 'worker:heartbeat:' .. worker)
    if not heartbeat then
        deadWorkers[worker] = true
        redis.call('SREM', '{homebrewmq}:workers', worker)
        redis.call('DEL', 'worker:' .. worker)
    end
end

-- Scan every in-flight job; re-queue those owned by a dead worker
local jobIds = redis.call('ZRANGE', processingQueue, 0, -1)

for _, jobId in ipairs(jobIds) do
    local ownerWorkerId = redis.call('HGET', 'job:' .. jobId, 'workerId')
    if ownerWorkerId and deadWorkers[ownerWorkerId] then
        local priority = tonumber(redis.call('HGET', 'job:' .. jobId, 'priority') or 0)
        redis.call('ZREM',  processingQueue, jobId)
        -- Intentionally bypass maxQueueSize: these jobs are already
-- in-flight and must not be dropped during recovery.
        redis.call('ZADD',  readyQueue, priority, jobId)
        redis.call('HSET',  'job:' .. jobId, 'status', 'ready', 'workerId', '')
    end
end

--park overflow jobs in a holding queue
-- Add a requeueOverflow sorted set. If ZCARD readyQueue >= maxSize, put the job there instead.
-- A separate process drains it back into readyQueue as capacity opens up. More complex but adds proper backpressure

return true