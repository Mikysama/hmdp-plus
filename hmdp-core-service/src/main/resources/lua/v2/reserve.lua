-- All keys have the same voucher hash tag; identifiers remain strings (snowflake precision).
local types={'string','hash','string','hash','hash','hash','zset','zset','hash','hash','string','stream'}
for i=1,#KEYS do
 local t=redis.call('TYPE',KEYS[i]).ok
 if t~=types[i] and not (t=='none' and (i==9 or i==10 or i==12)) then return redis.error_reply('CORRUPT_OR_MISSING_'..i) end
end
local mutation=tonumber(redis.call('HGET',KEYS[2],'mutation') or '0')
if not mutation or mutation<0 or mutation%1~=0 or mutation>=9007199254740990 then return redis.error_reply('CORRUPT_MUTATION') end
local epoch,voucher,user,request,token,order=ARGV[1],ARGV[2],ARGV[3],ARGV[4],ARGV[5],ARGV[6]
local autoIssue=ARGV[10] or 'false'
if autoIssue~='true' and autoIssue~='false' then return redis.error_reply('INVALID_AUTO_ISSUE') end
local rate,capacity,slotTtl=tonumber(ARGV[7]),tonumber(ARGV[8]),tonumber(ARGV[9])
if not rate or rate<=0 or not capacity or capacity<=0 or not slotTtl or slotTtl<=0 or request=='' or order=='' then return redis.error_reply('INVALID_ARGUMENT') end
if redis.call('GET',KEYS[11])~=epoch or redis.call('GET',KEYS[1])~=epoch or redis.call('HGET',KEYS[2],'epoch')~=epoch then return redis.error_reply('STALE_EPOCH') end
local req=user..':'..request
local existing=redis.call('HGET',KEYS[5],req)
if existing then
 local old=redis.call('HGET',KEYS[6],existing)
 if not old then return redis.error_reply('CORRUPT_REQUEST') end
 local ok,record=pcall(cjson.decode,old)
 if not ok then return redis.error_reply('CORRUPT_REQUEST') end
 -- Rebuilt database bindings may omit the mode; their persisted result is already authoritative.
 if record.autoIssue~=nil and record.autoIssue~=(autoIssue=='true') then return redis.error_reply('AUTO_ISSUE_MISMATCH') end
 return old
end
local tm=redis.call('TIME'); local now=tonumber(tm[1])*1000+math.floor(tonumber(tm[2])/1000)
local lease=tonumber(redis.call('HGET',KEYS[2],'leaseUntil'))
local beginAt=tonumber(redis.call('HGET',KEYS[2],'begin'))
local endAt=tonumber(redis.call('HGET',KEYS[2],'end'))
local version=redis.call('HGET',KEYS[2],'ruleVersion')
local stock=tonumber(redis.call('GET',KEYS[3]))
if not lease or not beginAt or not endAt or not stock or stock<0 or stock%1~=0 or not version then return redis.error_reply('CORRUPT_METADATA') end
if redis.call('HGET',KEYS[2],'state')~='READY' or lease<=now then return redis.error_reply('ADMISSION_CLOSED') end
local status=redis.call('HGET',KEYS[2],'status')
if status~='1' and status~='ACTIVE' then return redis.error_reply('INACTIVE') end
if now<beginAt then return redis.error_reply('NOT_STARTED') end
if now>endAt then return redis.error_reply('ENDED') end
if redis.call('HGET',KEYS[10],'token')~=token then return redis.error_reply('INVALID_TOKEN') end
local bound=redis.call('HGET',KEYS[10],'request')
if bound and bound~=request then return redis.error_reply('TOKEN_BOUND') end
if redis.call('HEXISTS',KEYS[4],user)==1 then return redis.error_reply('ALREADY_RESERVED') end
if stock<=0 then return redis.error_reply('SOLD_OUT') end
-- The order ID is also the hold key; never overwrite an unrelated hold.
if redis.call('HEXISTS',KEYS[6],order)==1 then return redis.error_reply('ORDER_ID_CONFLICT') end
local used=redis.call('ZCOUNT',KEYS[8],now+1,'+inf')
if used>=capacity then return redis.error_reply('ADMISSION_BUSY') end
local tokens=tonumber(redis.call('HGET',KEYS[9],'tokens') or rate)
local previous=tonumber(redis.call('HGET',KEYS[9],'time') or now)
if not tokens or not previous then return redis.error_reply('CORRUPT_BUCKET') end
tokens=math.min(rate,tokens+math.max(0,now-previous)*rate/1000)
if tokens<1 then return redis.error_reply('RATE_LIMITED') end
local encoded=cjson.encode({requestId=request,orderId=order,voucherId=voucher,userId=user,epoch=epoch,ruleVersion=version,createdAt=now,state='HELD',autoIssue=autoIssue=='true'})
-- Persist delivery intent before the other writes: XADD errors cannot leave a hold without intent.
-- All keys share the voucher hash tag. Never trim or expire unacknowledged entries.
redis.call('XADD',KEYS[12],'*','reservation',encoded,'autoIssue',autoIssue)
redis.call('DECR',KEYS[3])
redis.call('HSET',KEYS[4],user,order)
redis.call('HSET',KEYS[5],req,order)
redis.call('HSET',KEYS[6],order,encoded)
redis.call('ZADD',KEYS[7],now,order)
redis.call('ZREMRANGEBYSCORE',KEYS[8],1,now)
redis.call('ZADD',KEYS[8],now+slotTtl,order)
redis.call('HSET',KEYS[9],'tokens',tokens-1,'time',now)
redis.call('HSET',KEYS[10],'request',request)
redis.call('HINCRBY',KEYS[2],'mutation',1)
return encoded
