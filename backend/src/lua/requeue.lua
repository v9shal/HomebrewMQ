local readyQueue      = KEYS[1]
local processingQueue = KEYS[2]

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

for worker, _ in pairs(deadWorkers) do
    local affinityKey = '{homebrewmq}:ready:' .. worker
    local affinityJobs = redis.call('ZRANGE', affinityKey, 0, -1, 'WITHSCORES')
    for i = 1, #affinityJobs, 2 do
        local jobId    = affinityJobs[i]
        local priority = tonumber(affinityJobs[i + 1])
        redis.call('ZREM', affinityKey, jobId)
        -- Intentionally bypass maxQueueSize: stranded jobs must not be dropped.
        redis.call('ZADD', readyQueue, priority, jobId)
        redis.call('HSET', 'job:' .. jobId, 'status', 'ready', 'affinity', '')
    end
end

local jobIds = redis.call('ZRANGE', processingQueue, 0, -1)

for _, jobId in ipairs(jobIds) do
    local ownerWorkerId = redis.call('HGET', 'job:' .. jobId, 'workerId')
    if ownerWorkerId and deadWorkers[ownerWorkerId] then
        local priority = tonumber(redis.call('HGET', 'job:' .. jobId, 'priority') or 0)
        redis.call('ZREM', processingQueue, jobId)
        -- Intentionally bypass maxQueueSize: in-flight jobs must not be dropped.
        redis.call('ZADD', readyQueue, priority, jobId)
        redis.call('HSET', 'job:' .. jobId, 'status', 'ready', 'workerId', '')
    end
end

return true