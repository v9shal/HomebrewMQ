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

local result = redis.call('ZPOPMIN', readyQueue)

if #result == 0 then
  return nil
end

local jobId = result[1]
local priority = result[2]

local job = parseHash(
  redis.call('HGETALL', 'job:' .. jobId)
)

local attempts = tonumber(job['attempts'] or 0)
local maxRetries = tonumber(job['maxRetries'] or 0)

if attempts >= maxRetries then
  redis.call('ZADD', failedQueue, priority, jobId)

  return "DLQ"
end

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
  'processing'
)

return redis.call('HGETALL', 'job:' .. jobId)