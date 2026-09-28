local t=redis.call('TYPE',KEYS[1]).ok
if t~='none' and t~='hash' then return redis.error_reply('CORRUPT_TOKEN') end
local ttl=tonumber(ARGV[2])
if not ttl or ttl<1 or ARGV[1]=='' then return redis.error_reply('INVALID_ARGUMENT') end
local token=redis.call('HGET',KEYS[1],'token')
if token and not redis.call('HGET',KEYS[1],'request') then return token end
redis.call('DEL',KEYS[1])
redis.call('HSET',KEYS[1],'token',ARGV[1])
redis.call('PEXPIRE',KEYS[1],ttl)
return ARGV[1]
