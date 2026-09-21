local stockKey = KEYS[1]
local seckillUserKey = KEYS[2]
local traceLogKey = KEYS[3]
local orderStateKey = KEYS[4]
local voucherId = ARGV[1]
local userId = (ARGV[2])
local orderId = ARGV[3]
local seckillVoucherOrderOperate = tonumber(ARGV[4])
local traceId = ARGV[5]
local logType = ARGV[6]
local beforeQty = tonumber(ARGV[7])
local changeQty = tonumber(ARGV[8])
local afterQty = tonumber(ARGV[9])
local orderState = redis.call('hget', orderStateKey, orderId)
-- A rollback is successful only once. A committed order must never restore stock.
if orderState == 'ROLLED_BACK' or orderState == 'CANCELLED' then
    return 0
end
if orderState == 'COMMITTED' and seckillVoucherOrderOperate ~= 2 then
    return 0
end
if orderState ~= 'RESERVED' and orderState ~= 'COMMITTED' then
    return 10009
end
local stock = redis.call('get', stockKey);
if not stock then
    return 10004
end
-- Restore only the quantity deducted by the corresponding seckill request.
-- Deleting the stock key makes every later request and rollback fail.
redis.call('incrby', stockKey, changeQty)
if seckillVoucherOrderOperate == 1 then
    if (redis.call('sismember', seckillUserKey, userId) == 1) then
        redis.call('srem', seckillUserKey, userId)
    end
end
if seckillVoucherOrderOperate == 2 then
    redis.call('hset', orderStateKey, orderId, 'CANCELLED')
else
    redis.call('hset', orderStateKey, orderId, 'ROLLED_BACK')
end
local timeArr = redis.call('TIME')
local nowMillis = tonumber(timeArr[1]) * 1000 + math.floor(tonumber(timeArr[2]) / 1000)
local logEntry = cjson.encode({
    logType = logType,
    ts = nowMillis,
    orderId = orderId,
    traceId = traceId,
    userId = userId,
    voucherId = voucherId,
    beforeQty = beforeQty,
    changeQty = changeQty,
    afterQty = afterQty
})
redis.call('hset', traceLogKey, traceId, logEntry)
return 0
