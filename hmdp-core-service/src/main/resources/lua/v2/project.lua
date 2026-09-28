local types={'string','hash','string','hash','hash','hash','zset','zset'}
for i=1,#KEYS do
 if redis.call('TYPE',KEYS[i]).ok~=types[i] then return redis.error_reply('CORRUPT_OR_MISSING_'..i) end
end
local mutation=tonumber(redis.call('HGET',KEYS[2],'mutation') or '0')
if not mutation or mutation<0 or mutation%1~=0 or mutation>=9007199254740990 then return redis.error_reply('CORRUPT_MUTATION') end
local epoch,sequence,event,kind,user,order=ARGV[1],tonumber(ARGV[2]),ARGV[3],ARGV[4],ARGV[5],ARGV[6]
local delta=tonumber(ARGV[7])
if redis.call('GET',KEYS[1])~=epoch then return 'STALE' end
local current=tonumber(redis.call('HGET',KEYS[2],'sequence'))
local stock=tonumber(redis.call('GET',KEYS[3]))
if not sequence or sequence%1~=0 or not current or not stock or stock<0 or not delta then return redis.error_reply('CORRUPT_METADATA') end
if sequence<=current then return 'DUPLICATE' end
if sequence~=current+1 then return redis.error_reply('SEQUENCE_GAP') end
if kind~='ACCEPT' and kind~='COMMIT' and kind~='RELEASE' and kind~='CANCEL' and kind~='ADJUST' then return redis.error_reply('INVALID_KIND') end
local raw,record,encoded
if kind~='ADJUST' then
 raw=redis.call('HGET',KEYS[6],order)
 if not raw then return redis.error_reply('MISSING_RESERVATION') end
 local ok; ok,record=pcall(cjson.decode,raw)
 if not ok or tostring(record.epoch)~=epoch or tostring(record.userId)~=user or tostring(record.orderId)~=order then return redis.error_reply('RESERVATION_MISMATCH') end
 if kind=='ACCEPT' and record.state=='HELD' then record.state='ACCEPTED' end
 if kind=='COMMIT' and (record.state=='HELD' or record.state=='ACCEPTED') then record.state='COMMITTED' end
 if (kind=='RELEASE' or kind=='CANCEL') and record.state~='RELEASED' then
  if record.state=='COMMITTED' and kind~='CANCEL' then return redis.error_reply('CANCEL_REQUIRED') end
  record.state='RELEASED'; delta=1
 else delta=0 end
 encoded=cjson.encode(record)
elseif delta<0 or delta%1~=0 then return redis.error_reply('REBUILD_REQUIRED') end
if delta~=0 then redis.call('INCRBY',KEYS[3],delta) end
if record then
 redis.call('HSET',KEYS[6],order,encoded)
 if record.state=='RELEASED' and redis.call('HGET',KEYS[4],user)==order then redis.call('HDEL',KEYS[4],user) end
 redis.call('ZREM',KEYS[7],order); redis.call('ZREM',KEYS[8],order)
end
redis.call('HSET',KEYS[2],'sequence',sequence,'lastEvent',event)
redis.call('HINCRBY',KEYS[2],'mutation',1)
return 'APPLIED'
