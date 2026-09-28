-- One shop/date slot. Cancellation dominates success regardless of delivery order.
local types={'zset','hash','hash'}
for i=1,3 do local t=redis.call('TYPE',KEYS[i]).ok;if t~='none' and t~=types[i] then return redis.error_reply('CORRUPT_STATS') end end
local event,kind,user,order=ARGV[1],ARGV[2],ARGV[3],ARGV[4]
if event=='' or user=='' or order=='' or (kind~='SUCCESS' and kind~='CANCEL') then return redis.error_reply('INVALID_STATS_EVENT') end
local fingerprint=cjson.encode({kind=kind,user=user,order=order})
local previous=redis.call('HGET',KEYS[2],event)
if previous then if previous~=fingerprint then return redis.error_reply('EVENT_CONFLICT') end;return 'DUPLICATE' end
local raw=redis.call('HGET',KEYS[3],order)
local record=nil
if raw then local ok;ok,record=pcall(cjson.decode,raw);if not ok or record.user~=user then return redis.error_reply('CORRUPT_STATS_ORDER') end end
local count=tonumber(redis.call('ZSCORE',KEYS[1],user) or '0')
if not count or count<0 then return redis.error_reply('CORRUPT_STATS_COUNT') end
local change=0
if not record then
 record={user=user,state=kind};if kind=='SUCCESS' then change=1 end
elseif record.state=='SUCCESS' and kind=='CANCEL' then record.state='CANCEL';change=-1 end
local encoded=cjson.encode(record)
local nextCount=count+change
if nextCount<0 then return redis.error_reply('CORRUPT_STATS_COUNT') end
if nextCount>0 then redis.call('ZADD',KEYS[1],nextCount,user) else redis.call('ZREM',KEYS[1],user) end
redis.call('HSET',KEYS[3],order,encoded)
redis.call('HSET',KEYS[2],event,fingerprint)
return 'APPLIED'
