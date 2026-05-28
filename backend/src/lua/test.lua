local function greet()
        local arg=ARGV[1]
        local count=redis.call('GET',arg)
        if(count==false) 
        then count=0
        end
        local newcount=redis.call('INCR',arg)
    return newcount
end
return greet();