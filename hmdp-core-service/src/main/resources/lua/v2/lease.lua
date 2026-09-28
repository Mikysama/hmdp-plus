local pointerType=redis.call('TYPE',KEYS[1]).ok
if pointerType~='string' and pointerType~='none' then return redis.error_reply('CORRUPT_POINTER') end
local active=redis.call('GET',KEYS[1])
local epoch=tonumber(ARGV[1]); local ttl=tonumber(ARGV[3])
if not epoch or not ttl or ttl<=0 then return redis.error_reply('INVALID_ARGUMENT') end
if redis.call('TYPE',KEYS[9]).ok~='string' then return redis.error_reply('CORRUPT_FENCE') end
if redis.call('GET',KEYS[9])~=ARGV[1] then return 'STALE' end
if active and tonumber(active)>epoch then return 'STALE' end
if ARGV[2]=='RENEW' and active~=ARGV[1] then return 'STALE' end
if redis.call('TYPE',KEYS[2]).ok~='hash' or redis.call('TYPE',KEYS[3]).ok~='string' then return redis.error_reply('CORRUPT_METADATA') end
local types={'hash','hash','hash','zset','zset'}
for i=4,8 do if redis.call('TYPE',KEYS[i]).ok~=types[i-3] then return redis.error_reply('CORRUPT_BINDINGS') end end
local stock=tonumber(redis.call('GET',KEYS[3]))
if not stock or stock<0 or stock%1~=0 then return redis.error_reply('CORRUPT_STOCK') end
if redis.call('HGET',KEYS[2],'epoch')~=ARGV[1] or redis.call('HGET',KEYS[2],'state')~='READY' then return redis.error_reply('NOT_READY') end
local t=redis.call('TIME'); local now=tonumber(t[1])*1000+math.floor(tonumber(t[2])/1000)
redis.call('HSET',KEYS[2],'leaseUntil',now+ttl)
if ARGV[2]=='ACTIVATE' then redis.call('SET',KEYS[1],ARGV[1]); return 'ACTIVATED' end
return 'RENEWED'
