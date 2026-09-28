-- A unique HTTP attempt owns each slot; request/reservation replay cannot share it.
local mode,id,capacity,lease=ARGV[1],ARGV[2],tonumber(ARGV[3]),tonumber(ARGV[4])
if (mode~='ENTER' and mode~='LEAVE') or id=='' or not capacity or capacity<1 or capacity%1~=0 or not lease or lease<1 then return redis.error_reply('INVALID_ATTEMPT_SLOT') end
local t=redis.call('TYPE',KEYS[1]).ok
if t~='none' and t~='zset' then return redis.error_reply('CORRUPT_ATTEMPT_SLOTS') end
if mode=='LEAVE' then redis.call('ZREM',KEYS[1],id);return 'LEFT' end
local tm=redis.call('TIME');local now=tonumber(tm[1])*1000+math.floor(tonumber(tm[2])/1000)
local previous=tonumber(redis.call('ZSCORE',KEYS[1],id) or '0')
-- A retry of ENTER for the same attempt does not renew its lease indefinitely.
if previous>now then return 'ENTERED' end
local active=redis.call('ZCOUNT',KEYS[1],now+1,'+inf')
if active>=capacity then return 'BUSY' end
redis.call('ZREMRANGEBYSCORE',KEYS[1],'-inf',now)
redis.call('ZADD',KEYS[1],now+lease,id)
redis.call('PEXPIRE',KEYS[1],lease)
return 'ENTERED'
