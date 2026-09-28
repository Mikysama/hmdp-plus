-- Optimistic paged audit: every page verifies the same mutation counter.
local epoch,seq,phase,cursor,expected=ARGV[1],tonumber(ARGV[2]),ARGV[3],ARGV[4],ARGV[5]
local types={'string','hash','string','hash','hash','hash','zset','zset','string'}
for i=1,9 do if redis.call('TYPE',KEYS[i]).ok~=types[i] then return redis.error_reply('CORRUPT_AUDIT_TYPE_'..i) end end
local version=redis.call('HGET',KEYS[2],'mutation') or '0'
if redis.call('GET',KEYS[1])~=epoch or redis.call('GET',KEYS[9])~=epoch or tonumber(redis.call('HGET',KEYS[2],'sequence'))~=seq or redis.call('HGET',KEYS[2],'state')~='READY' or (expected~='' and version~=expected) then return cjson.encode({status='BUSY'}) end
local stock=tonumber(redis.call('GET',KEYS[3]));if not stock or stock<0 then return redis.error_reply('CORRUPT_AUDIT_STOCK') end
if phase=='finish' then return cjson.encode({status='PAGE',version=version,stock=stock,cursor='0',live={}}) end
local ix=phase=='reservations' and 6 or (phase=='users' and 4 or (phase=='requests' and 5 or nil))
if not ix then return redis.error_reply('INVALID_AUDIT_PHASE') end
local scan=redis.call('HSCAN',KEYS[ix],cursor,'COUNT',100);local entries=scan[2];local live={}
for i=1,#entries,2 do
 local field,value=entries[i],entries[i+1]
 if field~='__sentinel' then
  local raw=ix==6 and value or redis.call('HGET',KEYS[6],value)
  if not raw then return redis.error_reply('CORRUPT_AUDIT_RESERVATION') end
  local ok,r=pcall(cjson.decode,raw)
  if not ok or tostring(r.epoch)~=epoch or not r.orderId or not r.requestId then return redis.error_reply('CORRUPT_AUDIT_RESERVATION') end
  if r.state~='HELD' and r.state~='ACCEPTED' and r.state~='COMMITTED' and r.state~='RELEASED' then return redis.error_reply('CORRUPT_AUDIT_STATE') end
  local req=tostring(r.userId)..':'..r.requestId
  if ix==6 and r.orderId~=field then return redis.error_reply('CORRUPT_AUDIT_RESERVATION') end
  if ix==4 and (r.orderId~=value or tostring(r.userId)~=field or r.state=='RELEASED') then return redis.error_reply('CORRUPT_AUDIT_USER') end
  if ix==5 and (r.orderId~=value or req~=field) then return redis.error_reply('CORRUPT_AUDIT_REQUEST') end
  if redis.call('HGET',KEYS[5],req)~=r.orderId then return redis.error_reply('CORRUPT_AUDIT_REQUEST') end
  if r.state~='RELEASED' then
   if redis.call('HGET',KEYS[4],tostring(r.userId))~=r.orderId then return redis.error_reply('CORRUPT_AUDIT_USER') end
   if ix==6 then table.insert(live,r.orderId) end
  end
 end
end
return cjson.encode({status='PAGE',version=version,stock=stock,cursor=scan[1],live=live})
