-- Owner-checked renewal or release; an old worker cannot touch a new lock.
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
if ARGV[2] == 'release' then
    return redis.call('DEL', KEYS[1])
end
return redis.call('PEXPIRE', KEYS[1], tonumber(ARGV[2]))
