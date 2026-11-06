-- Publish and unlock atomically, only while this refresh still owns its lease.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('SET', KEYS[2], ARGV[2], 'PX', tonumber(ARGV[3]))
redis.call('DEL', KEYS[1])
return 1
