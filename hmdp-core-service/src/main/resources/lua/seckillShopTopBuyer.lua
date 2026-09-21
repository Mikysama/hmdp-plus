local rankingKey = KEYS[1]
local dedupKey = KEYS[2]
local orderId = ARGV[1]
local userId = ARGV[2]
local ttlSeconds = tonumber(ARGV[3])

if redis.call('hexists', dedupKey, orderId) == 1 then
    return 0
end

redis.call('zincrby', rankingKey, 1, userId)
redis.call('hset', dedupKey, orderId, '1')
redis.call('expire', rankingKey, ttlSeconds)
redis.call('expire', dedupKey, ttlSeconds)
return 1
