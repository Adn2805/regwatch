-- One key, one atomic admission. All timing is in Redis milliseconds.
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + tonumber(clock[2]) / 1000
local interval = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local previous = redis.call('HMGET', KEYS[1], 'tokens', 'updated', 'interval', 'capacity')
-- Mixed worker configurations may make an existing bucket stricter, never looser.
interval = math.max(interval, tonumber(previous[3]) or interval)
capacity = math.min(capacity, tonumber(previous[4]) or capacity)
local updated = tonumber(previous[2]) or now
now = math.max(now, updated)
local tokens = math.min(capacity, (tonumber(previous[1]) or capacity) + (now - updated) / interval)
local allowed = 0
local wait = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
else
    wait = math.ceil((1 - tokens) * interval)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'updated', now, 'interval', interval, 'capacity', capacity)
-- Resetting after this TTL cannot grant capacity earlier than natural refill.
redis.call('PEXPIRE', KEYS[1], math.ceil(interval * capacity) + 1000)
return {allowed, wait, math.floor(now)}
