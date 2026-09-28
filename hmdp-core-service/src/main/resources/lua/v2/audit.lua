-- Bounded atomic audit, never repairs or scans an unbounded hash.
local epoch,sequence,total,limit=ARGV[1],tonumber(ARGV[2]),tonumber(ARGV[3]),tonumber(ARGV[4])
if not sequence or not total or total<0 or not limit or limit<1 then return redis.error_reply('INVALID_AUDIT_ARGUMENT') end
local types={'string','hash','string','hash','hash','hash','zset','zset','string'}
for i=1,9 do if redis.call('TYPE',KEYS[i]).ok~=types[i] then return redis.error_reply('CORRUPT_AUDIT_TYPE_'..i) end end
if redis.call('GET',KEYS[1])~=epoch or redis.call('GET',KEYS[9])~=epoch or tonumber(redis.call('HGET',KEYS[2],'sequence'))~=sequence or redis.call('HGET',KEYS[2],'state')~='READY' then return 'BUSY' end
for i=4,6 do if redis.call('HLEN',KEYS[i])>limit+1 then return 'BUSY' end end
local stock=tonumber(redis.call('GET',KEYS[3]));if not stock or stock<0 then return redis.error_reply('CORRUPT_AUDIT_STOCK') end
local rows=redis.call('HGETALL',KEYS[6]);local live=0;local records={}
for i=1,#rows,2 do
 if rows[i]~='__sentinel' then
  local ok,r=pcall(cjson.decode,rows[i+1]);if not ok or tostring(r.epoch)~=epoch or r.orderId~=rows[i] then return redis.error_reply('CORRUPT_AUDIT_RESERVATION') end
  if r.state~='HELD' and r.state~='ACCEPTED' and r.state~='COMMITTED' and r.state~='RELEASED' then return redis.error_reply('CORRUPT_AUDIT_STATE') end
  if redis.call('HGET',KEYS[5],tostring(r.userId)..':'..r.requestId)~=r.orderId then return redis.error_reply('CORRUPT_AUDIT_REQUEST') end
  records[r.orderId]=r
  if r.state~='RELEASED' then
   live=live+1
   if redis.call('HGET',KEYS[4],tostring(r.userId))~=r.orderId then return redis.error_reply('CORRUPT_AUDIT_USER') end
  end
 end
end
local users=redis.call('HGETALL',KEYS[4])
for i=1,#users,2 do if users[i]~='__sentinel' then local r=records[users[i+1]];if not r or tostring(r.userId)~=users[i] or r.state=='RELEASED' then return redis.error_reply('CORRUPT_AUDIT_USER') end end end
local requests=redis.call('HGETALL',KEYS[5])
for i=1,#requests,2 do if requests[i]~='__sentinel' then local r=records[requests[i+1]];if not r or tostring(r.userId)..':'..r.requestId~=requests[i] then return redis.error_reply('CORRUPT_AUDIT_REQUEST') end end end
if stock+live~=total then return redis.error_reply('CORRUPT_AUDIT_TOTAL') end
return 'MATCH'
