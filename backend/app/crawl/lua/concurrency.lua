-- Expiring owner tokens cap in-flight requests; Redis time avoids clock skew.
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + tonumber(clock[2]) / 1000
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', now)
local allowed = 0
local wait = 0
if redis.call('ZCARD', KEYS[1]) < tonumber(ARGV[1]) then
    redis.call('ZADD', KEYS[1], now + tonumber(ARGV[2]), ARGV[3])
    allowed = 1
else
    local first = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
    wait = math.max(1, math.ceil(tonumber(first[2]) - now))
end
local last = redis.call('ZREVRANGE', KEYS[1], 0, 0, 'WITHSCORES')
if #last > 0 then
    redis.call('PEXPIRE', KEYS[1], math.ceil(tonumber(last[2]) - now) + 1000)
end
return {allowed, wait, math.floor(now)}
