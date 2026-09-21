local orderStateKey = KEYS[1]
local orderId = ARGV[1]
local state = redis.call('hget', orderStateKey, orderId)

if state == 'COMMITTED' then
    return 0
end
if state ~= 'RESERVED' then
    return 10009
end

redis.call('hset', orderStateKey, orderId, 'COMMITTED')
return 0
