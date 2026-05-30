-- claim.lua

local function parseHash(arr)
  local result = {}

  for i = 1, #arr, 2 do
    result[arr[i]] = arr[i + 1]
  end

  return result
end

local readyQueue = KEYS[1]
local processingQueue = KEYS[2]
local failedQueue = KEYS[3]

local visibilityTimeout = tonumber(ARGV[1])

local now = redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 +
              math.floor(tonumber(now[2]) / 1000)
local affinity = ARGV[2]
local workerId = ARGV[3]
local queueName=ARGV[4]
local result = {}

-- Affinity queue first
if affinity and affinity ~= '' then
    local jobs = redis.call(
        'ZRANGE',
        '{homebrewmq}:ready:' .. affinity,
        0,
        0,
        'WITHSCORES'
    )

    if #jobs > 0 then
        result = jobs
    end
end

-- Global queue fallback
if #result == 0 then
    local jobs = redis.call(
        'ZRANGE',
        readyQueue,
        0,
        0,
        'WITHSCORES'
    )

    if #jobs > 0 then
        result = jobs
    end
end

if #result == 0 then
    return nil
end

local jobId = result[1]
local priority = tonumber(result[2])

local job = parseHash(
    redis.call('HGETALL', 'job:' .. jobId)
)

local queueName = job['queue']

--------------------------------------------------
-- Circuit breaker check BEFORE removing the job
--------------------------------------------------

if queueName and queueName ~= '' then
    local state = redis.call(
        'GET',
        '{homebrewmq}:cb:state:' .. queueName
    )

    if state == 'open' then
        return redis.error_reply('circuit_open')
    end

    if state == 'half-open' then
        local probe = redis.call(
            'SET',
            '{homebrewmq}:cb:probe:' .. queueName,
            workerId,
            'NX',
            'EX',
            5
        )

        if not probe then
            return nil
        end
    end
end

--------------------------------------------------
-- Try to claim the job
--------------------------------------------------

local removed

if affinity and affinity ~= '' then
    removed = redis.call(
        'ZREM',
        '{homebrewmq}:ready:' .. affinity,
        jobId
    )

    if removed == 0 then
        removed = redis.call(
            'ZREM',
            readyQueue,
            jobId
        )
    end
else
    removed = redis.call(
        'ZREM',
        readyQueue,
        jobId
    )
end

if removed == 0 then
    return nil
end

--------------------------------------------------
-- Existing retry logic
--------------------------------------------------

local attempts = tonumber(job['attempts'] or 0)
local maxRetries = tonumber(job['maxRetries'] or 0)

if attempts >= maxRetries then
    redis.call(
        'ZADD',
        failedQueue,
        nowMs,
        jobId
    )

    redis.call(
        'HSET',
        'job:' .. jobId,
        'status',
        'failed'
    )

    return nil
end

--------------------------------------------------
-- Move to processing
--------------------------------------------------

redis.call(
    'ZADD',
    processingQueue,
    nowMs + visibilityTimeout,
    jobId
)

redis.call(
    'HINCRBY',
    'job:' .. jobId,
    'attempts',
    1
)

redis.call(
    'HSET',
    'job:' .. jobId,
    'status',
    'processing',
    'workerId',
    workerId
)

return redis.call(
    'HGETALL',
    'job:' .. jobId
)