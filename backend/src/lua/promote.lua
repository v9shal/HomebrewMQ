-- promote.lua
local function parseHash(arr)
    local result={}
    for i =1,#arr,2 do
        result[arr[i]]=arr[i+1]
    end

    return result
end
local readyQueue = KEYS[1]
local delayedQueue=KEYS[2]
local now=redis.call('TIME')
local nowMs = tonumber(now[1]) * 1000 + 
              math.floor(tonumber(now[2]) / 1000)
local jobIds = redis.call('ZRANGEBYSCORE', delayedQueue, '-INF', nowMs,'LIMIT',0,100)

if #jobIds==0 then
    return nil
end


for _, jobId in ipairs(jobIds) do

    local priority = tonumber(redis.call('HMGET', 'job:'..jobId,'priority'))
    redis.call('ZREM',delayedQueue,jobId)
    redis.call('ZADD',readyQueue,priority,jobId)
    redis.call('HSET','job:'..jobId,'status','ready')
    
end


