-- Resumable, token-fenced staging. Never mutate an epoch after it is active.
local epoch,mode,worker=tonumber(ARGV[1]),ARGV[2],ARGV[3]
local stock,version,beginAt,endAt,seq=tonumber(ARGV[4]),ARGV[5],tonumber(ARGV[7]),tonumber(ARGV[8]),tonumber(ARGV[9])
if not epoch or not stock or stock<0 or stock%1~=0 or not beginAt or not endAt or beginAt>=endAt or not seq then return redis.error_reply('INVALID_SNAPSHOT') end
local pt=redis.call('TYPE',KEYS[1]).ok
local ft=redis.call('TYPE',KEYS[10]).ok
local active=pt=='string' and redis.call('GET',KEYS[1]) or nil
local fence=ft=='string' and redis.call('GET',KEYS[10]) or nil
-- Valid higher fences always win, even if the other authority key is corrupt.
if active and tonumber(active) and tonumber(active)>=epoch then return redis.error_reply('ACTIVE_EPOCH') end
if fence and tonumber(fence) and tonumber(fence)>epoch then return redis.error_reply('STAGING_FENCED') end
if mode~='BEGIN' then
 if pt~='none' and (pt~='string' or not tonumber(active)) then return redis.error_reply('CORRUPT_POINTER') end
 if ft~='string' or fence~=ARGV[1] then return redis.error_reply('STAGING_FENCED') end
end
local ok,records=pcall(cjson.decode,ARGV[10])
if not ok or type(records)~='table' or #records>100 then return redis.error_reply('INVALID_BINDINGS') end
local encoded={}
local users,orders={},{}
for i,r in ipairs(records) do
 if type(r.userId)~='string' or type(r.orderId)~='string' or type(r.requestId)~='string' or tostring(r.epoch)~=ARGV[1] or (r.state~='ACCEPTED' and r.state~='COMMITTED') then return redis.error_reply('INVALID_BINDING') end
 if orders[r.orderId] then return redis.error_reply('DUPLICATE_ORDER') end
 if users[r.userId] then return redis.error_reply('DUPLICATE_USER') end
 orders[r.orderId]=true;users[r.userId]=true
 encoded[i]=cjson.encode(r)
end
if mode=='BEGIN' then
 -- Only trusted DB-frozen rebuild may discard a corrupt pointer. Business scripts cannot.
 if pt~='none' and (pt~='string' or not tonumber(active)) then redis.call('DEL',KEYS[1]) end
 redis.call('SET',KEYS[10],ARGV[1])
 for i=2,9 do redis.call('DEL',KEYS[i]) end
 redis.call('HSET',KEYS[2],'epoch',ARGV[1],'state','BUILDING','worker',worker,'status',ARGV[6],'begin',ARGV[7],'end',ARGV[8],'ruleVersion',version,'sequence',ARGV[9],'leaseUntil','0','mutation','0')
 redis.call('SET',KEYS[3],ARGV[4])
 for i=4,6 do redis.call('HSET',KEYS[i],'__sentinel','1') end
 for i=7,8 do redis.call('ZADD',KEYS[i],0,'__sentinel') end
elseif mode=='APPEND' or mode=='FINISH' then
 local types={'hash','string','hash','hash','hash','zset','zset'}
 for i=2,8 do if redis.call('TYPE',KEYS[i]).ok~=types[i-1] then return redis.error_reply('CORRUPT_STAGE') end end
 if redis.call('HGET',KEYS[2],'worker')~=worker or redis.call('HGET',KEYS[2],'state')~='BUILDING' then return redis.error_reply('STAGING_FENCED') end
 -- Validate the whole batch before the first mutation.
 for _,r in ipairs(records) do
  local previous=redis.call('HGET',KEYS[6],r.orderId)
  if previous then
   local valid,old=pcall(cjson.decode,previous)
   if not valid or old.userId~=r.userId or old.requestId~=r.requestId or old.orderId~=r.orderId then return redis.error_reply('DUPLICATE_ORDER') end
  end
  local existing=redis.call('HGET',KEYS[4],r.userId)
  if existing and existing~=r.orderId then return redis.error_reply('DUPLICATE_USER') end
 end
 for i,r in ipairs(records) do
  redis.call('HSET',KEYS[4],r.userId,r.orderId)
  redis.call('HSET',KEYS[5],r.userId..':'..r.requestId,r.orderId)
  redis.call('HSET',KEYS[6],r.orderId,encoded[i])
 end
 if mode=='FINISH' then redis.call('HSET',KEYS[2],'state','READY') end
else return redis.error_reply('INVALID_MODE') end
return 'STAGED'
