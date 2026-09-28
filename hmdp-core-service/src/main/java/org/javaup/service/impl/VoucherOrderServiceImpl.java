package org.javaup.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollectionUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.core.RedisKeyManage;
import org.javaup.core.SpringUtil;
import org.javaup.dto.CancelVoucherOrderDto;
import org.javaup.dto.GetVoucherOrderByVoucherIdDto;
import org.javaup.dto.GetVoucherOrderDto;
import org.javaup.dto.Result;
import org.javaup.dto.VoucherReconcileLogDto;
import org.javaup.entity.SeckillVoucher;
import org.javaup.entity.UserInfo;
import org.javaup.entity.Voucher;
import org.javaup.entity.VoucherOrder;
import org.javaup.entity.VoucherOrderRouter;
import org.javaup.enums.BaseCode;
import org.javaup.enums.BusinessType;
import org.javaup.enums.LogType;
import org.javaup.enums.OrderStatus;
import org.javaup.enums.SeckillVoucherOrderOperate;
import org.javaup.exception.HmdpFrameException;
import org.javaup.kafka.message.SeckillVoucherMessage;
import org.javaup.kafka.producer.SeckillVoucherProducer;
import org.javaup.kafka.redis.RedisVoucherData;
import org.javaup.lua.SeckillVoucherDomain;
import org.javaup.lua.SeckillVoucherOperate;
import org.javaup.mapper.VoucherOrderMapper;
import org.javaup.mapper.VoucherOrderRouterMapper;
import org.javaup.message.MessageExtend;
import org.javaup.model.SeckillVoucherFullModel;
import org.javaup.redis.RedisCacheImpl;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.repeatexecutelimit.annotion.RepeatExecuteLimit;
import org.javaup.service.ISeckillVoucherService;
import org.javaup.service.IUserInfoService;
import org.javaup.service.IVoucherOrderRouterService;
import org.javaup.service.IVoucherOrderService;
import org.javaup.service.IVoucherReconcileLogService;
import org.javaup.service.IVoucherService;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.javaup.utils.RedisIdWorker;
import org.javaup.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamInfo;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.javaup.constant.Constant.SECKILL_VOUCHER_TOPIC;
import static org.javaup.constant.RepeatExecuteLimitConstants.SECKILL_VOUCHER_ORDER;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 优惠券订单 接口实现
 * @author: 阿星不是程序员
 **/
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private IVoucherService voucherService;
    
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;
    
    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;
    
    @Resource
    private SeckillVoucherOperate seckillVoucherOperate;

    @Resource
    private SeckillVoucherProducer seckillVoucherProducer;
    
    @Resource
    private RedisCacheImpl redisCache;
    
    @Resource
    private IVoucherOrderRouterService voucherOrderRouterService;
    
    @Resource
    private IUserInfoService userInfoService;
    
    @Resource
    private VoucherOrderMapper voucherOrderMapper;
    
    @Resource
    private VoucherOrderRouterMapper voucherOrderRouterMapper;
    
    @Resource
    private RedisVoucherData redisVoucherData;
    
    @Resource
    private IVoucherReconcileLogService voucherReconcileLogService;
    

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    public static final ThreadPoolExecutor SECKILL_ORDER_EXECUTOR =
            new ThreadPoolExecutor(
                    1,
                    1,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(1024),
                    new NamedThreadFactory("seckill-order-", false),
                    new ThreadPoolExecutor.CallerRunsPolicy()
            );

    private static class NamedThreadFactory implements ThreadFactory {
        private final String namePrefix;
        private final boolean daemon;
        private final AtomicInteger index = new AtomicInteger(1);

        public NamedThreadFactory(String namePrefix, boolean daemon) {
            this.namePrefix = namePrefix;
            this.daemon = daemon;
        }
        // 方法功能：实现 VoucherOrderServiceImpl.NamedThreadFactory 的业务逻辑。

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, namePrefix + index.getAndIncrement());
            t.setDaemon(daemon);
            t.setUncaughtExceptionHandler((thread, ex) ->
                    log.error("未捕获异常，线程={}, err={}", thread.getName(), ex.getMessage(), ex)
            );
            return t;
        }
        // 方法功能：创建带业务前缀和守护标记的线程。
    }
    
    
    @PostConstruct
    private void init(){
        // 这是黑马点评的普通版本，升级版本中不再使用此方式
        //SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    // 方法功能：初始化当前组件需要的脚本、缓存、线程或启动数据。

    @PreDestroy
    private void destroy(){
        try {
            SECKILL_ORDER_EXECUTOR.shutdown();
            if (!SECKILL_ORDER_EXECUTOR.awaitTermination(5, TimeUnit.SECONDS)) {
                SECKILL_ORDER_EXECUTOR.shutdownNow();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            SECKILL_ORDER_EXECUTOR.shutdownNow();
        }
    }
    // 方法功能：关闭秒杀订单消费线程池资源。
    
    /***
     * 这是黑马点评的普通版本，升级版本中不再使用此方式
     */
    private class VoucherOrderHandler implements Runnable{
        private final String queueName = "stream.orders";
        @Override
        public void run() {
            while (true) {
                try {
                    // 0.初始化stream
                    initStream();
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }
        // 方法功能：循环消费 Redis Stream 中的秒杀订单消息。

        public void initStream(){
            Boolean exists = stringRedisTemplate.hasKey(queueName);
            if (BooleanUtil.isFalse(exists)) {
                log.info("stream不存在，开始创建stream");
                // 不存在，需要创建
                stringRedisTemplate.opsForStream().createGroup(queueName, ReadOffset.latest(), "g1");
                log.info("stream和group创建完毕");
                return;
            }
            // stream存在，判断group是否存在
            StreamInfo.XInfoGroups groups = stringRedisTemplate.opsForStream().groups(queueName);
            if(groups.isEmpty()){
                log.info("group不存在，开始创建group");
                // group不存在，创建group
                stringRedisTemplate.opsForStream().createGroup(queueName, ReadOffset.latest(), "g1");
                log.info("group创建完毕");
            }
        }
        // 方法功能：初始化秒杀订单 Redis Stream 及消费者组。

        private void handlePendingList() {
            while (true) {
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    // 2.判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        break;
                    }
                    // 解析数据
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
                    // 3.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 4.确认消息 XACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
        // 方法功能：处理 Redis Stream pending-list 中未确认的秒杀订单消息。
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getId();
        // 创建锁对象
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if(!isLock){
            // 获取锁失败，返回错误或重试
            log.error("不允许重复下单");
            return;
        }
        try {
            // 获取代理对象（事务）
            createVoucherOrderV1(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }
    // 方法功能：处理单条 Redis Stream 秒杀订单并确认消费进度。

    IVoucherOrderService proxy;
    /**
     * 抢优惠券下单
     * */
    @Override
    public Result<Long> seckillVoucher(Long voucherId) {
        //return doSeckillVoucherV1(voucherId);
        return doSeckillVoucherV2(voucherId);
    }
    // 方法功能：校验秒杀访问令牌后发起秒杀下单。
    
    public Result<Long> doSeckillVoucherV1(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        long orderId = snowflakeIdGenerator.nextId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4.返回订单id
        return Result.ok(orderId);
    }
    // 方法功能：使用数据库事务和锁的方式执行秒杀下单。
    
    public Result<Long> doSeckillVoucherV2(Long voucherId) {
        SeckillVoucherFullModel seckillVoucherFullModel = seckillVoucherService.queryByVoucherId(voucherId);
        seckillVoucherService.loadVoucherStock(voucherId);
        Long userId = UserHolder.getUser().getId();
        verifyUserLevel(seckillVoucherFullModel,userId);
        long orderId = snowflakeIdGenerator.nextId();
        long traceId = snowflakeIdGenerator.nextId();
        List<String> keys = ListUtil.of(
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId).getRelKey(),
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId).getRelKey(),
                RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_TRACE_LOG_TAG_KEY, voucherId).getRelKey()
        );
        String[] args = new String[9];
        args[0] = voucherId.toString();
        args[1] = userId.toString();
        args[2] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getBeginTime()));
        args[3] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getEndTime()));
        args[4] = String.valueOf(seckillVoucherFullModel.getStatus());
        args[5] = String.valueOf(orderId);
        args[6] = String.valueOf(traceId);
        args[7] = String.valueOf(LogType.DEDUCT.getCode());
        long secondsUntilEnd = Duration.between(LocalDateTimeUtil.now(), seckillVoucherFullModel.getEndTime()).getSeconds();
        long ttlSeconds = Math.max(1L, secondsUntilEnd + Duration.ofDays(1).getSeconds());
        args[8] = String.valueOf(ttlSeconds);
        SeckillVoucherDomain seckillVoucherDomain = seckillVoucherOperate.execute(
                keys,
                args
        );
        if (!seckillVoucherDomain.getCode().equals(BaseCode.SUCCESS.getCode())) {
            throw new HmdpFrameException(Objects.requireNonNull(BaseCode.getRc(seckillVoucherDomain.getCode())));
        }
        SeckillVoucherMessage seckillVoucherMessage = new SeckillVoucherMessage(
                userId,
                voucherId,
                orderId,
                traceId,
                seckillVoucherDomain.getBeforeQty(),
                seckillVoucherDomain.getDeductQty(),
                seckillVoucherDomain.getAfterQty(),
                Boolean.FALSE
        );
        seckillVoucherProducer.sendPayload(
                SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                seckillVoucherMessage);

        return Result.ok(orderId);
    }
    // 方法功能：使用 Redis Lua 校验和 Kafka 消息方式执行异步秒杀下单。
    
    public void verifyUserLevel(SeckillVoucherFullModel seckillVoucherFullModel,Long userId){
        String allowedLevelsStr = seckillVoucherFullModel.getAllowedLevels();
        Integer minLevel = seckillVoucherFullModel.getMinLevel();
        boolean hasLevelRule = StrUtil.isNotBlank(allowedLevelsStr) || Objects.nonNull(minLevel);
        if (!hasLevelRule) {
            return;
        }
        UserInfo userInfo = userInfoService.getByUserId(userId);
        if (Objects.isNull(userInfo)) {
            throw new HmdpFrameException(BaseCode.USER_NOT_EXIST);
        }
        boolean allowed = true;
        Integer level = userInfo.getLevel();
        if (StrUtil.isNotBlank(allowedLevelsStr)) {
            try {
                Set<Integer> allowedLevels = Arrays.stream(allowedLevelsStr.split(","))
                        .map(String::trim)
                        .filter(StrUtil::isNotBlank)
                        .map(Integer::valueOf)
                        .collect(Collectors.toSet());
                if (CollectionUtil.isNotEmpty(allowedLevels)) {
                    allowed = allowedLevels.contains(level);
                }
            } catch (Exception parseEx) {
                log.warn("allowedLevels 解析失败, voucherId={}, raw={}",
                        seckillVoucherFullModel.getVoucherId(), 
                        allowedLevelsStr, parseEx);
            }
        }
        if (allowed && Objects.nonNull(minLevel)) {
            allowed = Objects.nonNull(level) && level >= minLevel;
        }
        if (!allowed) {
            throw new HmdpFrameException("当前会员级别不满足参与条件");
        }
    }
    // 方法功能：校验用户会员等级是否满足秒杀券参与规则。

   
    private static class AudienceRule {
        public Set<Integer> allowedLevels;
        public Integer minLevel;
        public Set<String> allowedCities;
        
        boolean hasLevelRule(){
            return (allowedLevels != null && !allowedLevels.isEmpty()) || minLevel != null;
        }
        // 方法功能：判断受众规则中是否配置会员等级限制。
        boolean hasCityRule(){
            return allowedCities != null && !allowedCities.isEmpty();
        }
        // 方法功能：判断受众规则中是否配置城市限制。
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public void createVoucherOrderV1(VoucherOrder voucherOrder) {
        // 5.一人一单
        Long userId = voucherOrder.getUserId();

        // 5.1.查询订单
        Long count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        // 5.2.判断是否存在
        if (count > 0) {
            // 用户已经购买过了
            log.error("用户已经购买过一次！");
            return;
        }
        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                // set stock = stock - 1
                .setSql("stock = stock - 1")
                // where id = ? and stock > 0
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0) 
                .update();
        if (!success) {
            // 扣减失败
            log.error("库存不足！");
            return;
        }
        // 7.创建订单
        save(voucherOrder);
    }
    // 方法功能：使用同步事务方式创建秒杀订单并扣减数据库库存。
    
    
    @Override
    @RepeatExecuteLimit(name = SECKILL_VOUCHER_ORDER,keys = {"#message.uuid"})
    @Transactional(rollbackFor = Exception.class)
    public boolean createVoucherOrderV2(MessageExtend<SeckillVoucherMessage> message) {
        SeckillVoucherMessage messageBody = message.getMessageBody();
        Long userId = messageBody.getUserId();
        VoucherOrder normalVoucherOrder = lambdaQuery()
                .eq(VoucherOrder::getVoucherId, messageBody.getVoucherId())
                .eq(VoucherOrder::getUserId, userId)
                .eq(VoucherOrder::getStatus,OrderStatus.NORMAL.getCode())
                .one();
        if (Objects.nonNull(normalVoucherOrder)) {
            log.warn("已存在此订单，voucherId：{},userId：{}", normalVoucherOrder.getVoucherId(), userId);
            throw new HmdpFrameException(BaseCode.VOUCHER_ORDER_EXIST);
        }
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", messageBody.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            throw new HmdpFrameException("优惠券库存不足！优惠券id:" + messageBody.getVoucherId());
        }
        VoucherOrder voucherOrder = new VoucherOrder();
        voucherOrder.setId(messageBody.getOrderId());
        voucherOrder.setUserId(messageBody.getUserId());
        voucherOrder.setVoucherId(messageBody.getVoucherId());
        voucherOrder.setCreateTime(LocalDateTimeUtil.now());
        save(voucherOrder);
        VoucherOrderRouter voucherOrderRouter = new VoucherOrderRouter();
        voucherOrderRouter.setId(snowflakeIdGenerator.nextId());
        voucherOrderRouter.setOrderId(voucherOrder.getId());
        voucherOrderRouter.setUserId(userId);
        voucherOrderRouter.setVoucherId(voucherOrder.getVoucherId());
        voucherOrderRouter.setCreateTime(LocalDateTimeUtil.now());
        voucherOrderRouter.setUpdateTime(LocalDateTimeUtil.now());
        voucherOrderRouterService.save(voucherOrderRouter);
        redisCache.set(RedisKeyBuild.createRedisKey(
                RedisKeyManage.DB_SECKILL_ORDER_KEY,messageBody.getOrderId()),
                voucherOrder,
                60,
                TimeUnit.SECONDS
        );
        voucherReconcileLogService.saveReconcileLog(
                LogType.DEDUCT.getCode(),
                BusinessType.SUCCESS.getCode(),
                "order created",
                message
        );
        return true;
    }
    // 方法功能：消费秒杀消息并创建订单、路由和对账日志。
    
    @Override
    public Long getSeckillVoucherOrder(GetVoucherOrderDto getVoucherOrderDto) {
        VoucherOrder voucherOrder = 
                redisCache.get(RedisKeyBuild.createRedisKey(
                        RedisKeyManage.DB_SECKILL_ORDER_KEY, 
                        getVoucherOrderDto.getOrderId()), 
                        VoucherOrder.class);
        if (Objects.nonNull(voucherOrder)) {
            return voucherOrder.getId();
        }
        VoucherOrderRouter voucherOrderRouter = 
                voucherOrderRouterService.lambdaQuery()
                        .eq(VoucherOrderRouter::getOrderId, getVoucherOrderDto.getOrderId())
                        .one();
        if (Objects.nonNull(voucherOrderRouter)) {
            return voucherOrderRouter.getOrderId();
        }
        return null;
    }
    // 方法功能：查询指定用户和秒杀券对应的订单 ID。
    
    @Override
    public Long getSeckillVoucherOrderIdByVoucherId(GetVoucherOrderByVoucherIdDto getVoucherOrderByVoucherIdDto) {
        VoucherOrder voucherOrder = lambdaQuery()
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, getVoucherOrderByVoucherIdDto.getVoucherId())
                .eq(VoucherOrder::getStatus, OrderStatus.NORMAL.getCode())
                .one();
        if (Objects.nonNull(voucherOrder)) {
            return voucherOrder.getId();
        }
        return null;
    }
    // 方法功能：按秒杀券 ID 查询当前用户的订单 ID。
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Boolean cancel(CancelVoucherOrderDto cancelVoucherOrderDto) {
        VoucherOrder voucherOrder = lambdaQuery()
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .eq(VoucherOrder::getStatus, OrderStatus.NORMAL.getCode())
                .one();
        if (Objects.isNull(voucherOrder)) {
            throw new HmdpFrameException(BaseCode.SECKILL_VOUCHER_ORDER_NOT_EXIST);
        }
        SeckillVoucher seckillVoucher = seckillVoucherService.lambdaQuery()
                .eq(SeckillVoucher::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .one();
        if (Objects.isNull(seckillVoucher)) {
            throw new HmdpFrameException(BaseCode.SECKILL_VOUCHER_NOT_EXIST);
        }
        boolean updateResult = lambdaUpdate().set(VoucherOrder::getStatus, OrderStatus.CANCEL.getCode())
                .set(VoucherOrder::getUpdateTime, LocalDateTimeUtil.now())
                .eq(VoucherOrder::getUserId, UserHolder.getUser().getId())
                .eq(VoucherOrder::getVoucherId, cancelVoucherOrderDto.getVoucherId())
                .update();
        long traceId = snowflakeIdGenerator.nextId();
        VoucherReconcileLogDto voucherReconcileLogDto = new VoucherReconcileLogDto();
        voucherReconcileLogDto.setOrderId(voucherOrder.getId());
        voucherReconcileLogDto.setUserId(voucherOrder.getUserId());
        voucherReconcileLogDto.setVoucherId(voucherOrder.getVoucherId());
        voucherReconcileLogDto.setDetail("cancel voucher order ");
        voucherReconcileLogDto.setBeforeQty(seckillVoucher.getStock());
        voucherReconcileLogDto.setChangeQty(1);
        voucherReconcileLogDto.setAfterQty(seckillVoucher.getStock() + 1);
        voucherReconcileLogDto.setTraceId(traceId);
        voucherReconcileLogDto.setLogType(LogType.RESTORE.getCode());
        voucherReconcileLogDto.setBusinessType( BusinessType.CANCEL.getCode());
        boolean saveReconcileLogResult = voucherReconcileLogService.saveReconcileLog(voucherReconcileLogDto);
        
        boolean rollbackStockResult = seckillVoucherService.rollbackStock(cancelVoucherOrderDto.getVoucherId());
        
        Boolean result = updateResult && saveReconcileLogResult && rollbackStockResult;
        if (result) {
            redisVoucherData.rollbackRedisVoucherData(
                    SeckillVoucherOrderOperate.YES,
                    traceId,
                    voucherOrder.getVoucherId(),
                    voucherOrder.getUserId(),
                    voucherOrder.getId(),
                    seckillVoucher.getStock(),
                    1,
                    seckillVoucher.getStock() + 1
            );
            redisCache.delForHash(RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_SUBSCRIBE_STATUS_TAG_KEY,
                    cancelVoucherOrderDto.getVoucherId()),
                    String.valueOf(voucherOrder.getUserId()));
            Voucher voucher = voucherService.getById(voucherOrder.getVoucherId());
            if (Objects.nonNull(voucher)) {
                String day = voucherOrder.getCreateTime().format(DateTimeFormatter.BASIC_ISO_DATE);
                RedisKeyBuild dailyKey = RedisKeyBuild.createRedisKey(
                        RedisKeyManage.SECKILL_SHOP_TOP_BUYERS_DAILY_TAG_KEY,
                        voucher.getShopId(),
                        day
                );
                redisCache.incrementScoreForSortedSet(dailyKey, String.valueOf(voucherOrder.getUserId()), -1.0);
            }

            try {
                autoIssueVoucherToEarliestSubscriber(
                        voucherOrder.getVoucherId(),
                        voucherOrder.getUserId()
                );
            } catch (Exception e) {
                log.warn("自动发券失败，voucherId={}, err=\n{}", voucherOrder.getVoucherId(), e.getMessage());
            }
        }
        return result;
    }
    // 方法功能：取消秒杀订单并回滚库存、订阅和缓存状态。
    
    @Override
    public boolean autoIssueVoucherToEarliestSubscriber(final Long voucherId, final Long excludeUserId) {
        SeckillVoucherFullModel seckillVoucherFullModel = seckillVoucherService.queryByVoucherId(voucherId);
        if (Objects.isNull(seckillVoucherFullModel) 
                || 
                Objects.isNull(seckillVoucherFullModel.getBeginTime()) 
                ||
                Objects.isNull(seckillVoucherFullModel.getEndTime())) {
            return false;
        }
        seckillVoucherService.loadVoucherStock(voucherId);
        String candidateUserIdStr = findEarliestCandidate(voucherId, excludeUserId);
        if (StrUtil.isBlank(candidateUserIdStr)) {
            return false;
        }
        return issueToCandidate(voucherId, candidateUserIdStr, seckillVoucherFullModel);
    }
    // 方法功能：尝试向最早订阅且符合条件的用户自动补发秒杀券。
    
    private String findEarliestCandidate(final Long voucherId, final Long excludeUserId) {
        RedisKeyBuild subscribeZSetKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_SUBSCRIBE_ZSET_TAG_KEY, voucherId);
        RedisKeyBuild purchasedSetKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId);
        
        final long pageCount = 1L;
        long offset = 0L;
        while (true) {
            Set<ZSetOperations.TypedTuple<String>> page = redisCache.rangeByScoreWithScoreForSortedSet(
                    subscribeZSetKey,
                    Double.NEGATIVE_INFINITY,
                    Double.POSITIVE_INFINITY,
                    offset,
                    pageCount,
                    String.class
            );
            if (CollectionUtil.isEmpty(page)) {
                return null;
            }
            ZSetOperations.TypedTuple<String> tuple = page.iterator().next();
            if (Objects.isNull(tuple) || Objects.isNull(tuple.getValue())) {
                offset++;
                continue;
            }
            String uidStr = tuple.getValue();
            if (StrUtil.isBlank(uidStr)) {
                offset++;
                continue;
            }
            if (Objects.nonNull(excludeUserId) && Objects.equals(uidStr, String.valueOf(excludeUserId))) {
                offset++;
                continue;
            }
            Boolean purchased = redisCache.isMemberForSet(purchasedSetKey, uidStr);
            if (BooleanUtil.isTrue(purchased)) {
                offset++;
                continue;
            }
            return uidStr;
        }
    }
    // 方法功能：从订阅集合中查找最早且未被排除的候选用户。
    
    private boolean issueToCandidate(final Long voucherId, 
                                     final String candidateUserIdStr, 
                                     final SeckillVoucherFullModel seckillVoucherFullModel) {
        Long candidateUserId = Long.valueOf(candidateUserIdStr);
        try {
            verifyUserLevel(seckillVoucherFullModel, candidateUserId);
        } catch (Exception e) {
            log.info("候选用户不满足人群规则，自动发券跳过。voucherId={}, userId={}", voucherId, candidateUserId);
            return false;
        }
        List<String> keys = buildSeckillKeys(voucherId);
        long orderId = snowflakeIdGenerator.nextId();
        long traceId = snowflakeIdGenerator.nextId();
        String[] args = buildSeckillArgs(voucherId, candidateUserIdStr, seckillVoucherFullModel, orderId, traceId);
        SeckillVoucherDomain domain = seckillVoucherOperate.execute(keys, args);
        if (!Objects.equals(domain.getCode(), BaseCode.SUCCESS.getCode())) {
            log.info("自动发券Lua扣减失败，code={}, voucherId={}, userId={}", domain.getCode(), voucherId, candidateUserId);
            return false;
        }
        SeckillVoucherMessage message = new SeckillVoucherMessage(
                candidateUserId,
                voucherId,
                orderId,
                traceId,
                domain.getBeforeQty(),
                domain.getDeductQty(),
                domain.getAfterQty(),
                Boolean.TRUE
        );
        seckillVoucherProducer.sendPayload(
                SpringUtil.getPrefixDistinctionName() + "-" + SECKILL_VOUCHER_TOPIC,
                message
        );
        return true;
    }
    // 方法功能：向候选用户执行秒杀券自动发放并发送订单消息。
    
    private List<String> buildSeckillKeys(final Long voucherId) {
        String stockKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_STOCK_TAG_KEY, voucherId).getRelKey();
        String userKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_USER_TAG_KEY, voucherId).getRelKey();
        String traceKey = RedisKeyBuild.createRedisKey(RedisKeyManage.SECKILL_TRACE_LOG_TAG_KEY, voucherId).getRelKey();
        return ListUtil.of(stockKey, userKey, traceKey);
    }
    // 方法功能：构造秒杀 Lua 脚本所需的 Redis key 列表。
    
    private String[] buildSeckillArgs(final Long voucherId,
                                      final String userIdStr,
                                      final SeckillVoucherFullModel seckillVoucherFullModel,
                                      final long orderId,
                                      final long traceId) {
        String[] args = new String[9];
        args[0] = voucherId.toString();
        args[1] = userIdStr;
        args[2] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getBeginTime()));
        args[3] = String.valueOf(LocalDateTimeUtil.toEpochMilli(seckillVoucherFullModel.getEndTime()));
        args[4] = String.valueOf(seckillVoucherFullModel.getStatus());
        args[5] = String.valueOf(orderId);
        args[6] = String.valueOf(traceId);
        args[7] = String.valueOf(LogType.DEDUCT.getCode());
        args[8] = String.valueOf(computeTtlSeconds(seckillVoucherFullModel));
        return args;
    }
    // 方法功能：构造秒杀 Lua 脚本所需的参数数组。
    
    private long computeTtlSeconds(final SeckillVoucherFullModel seckillVoucherFullModel) {
        long secondsUntilEnd = Duration.between(LocalDateTimeUtil.now(), seckillVoucherFullModel.getEndTime()).getSeconds();
        return Math.max(1L, secondsUntilEnd + Duration.ofDays(1).getSeconds());
    }
    // 方法功能：根据秒杀券结束时间计算 Redis 业务数据 TTL。

    /*
    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private class VoucherOrderHandler implements Runnable{
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        int r = result.intValue();
        // 2.判断结果是否为0
        if (r != 0) {
            // 2.1.不为0 ，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        // 2.2.为0 ，有购买资格，把下单信息保存到阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.4.用户id
        voucherOrder.setUserId(userId);
        // 2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        // 2.6.放入阻塞队列
        orderTasks.add(voucherOrder);
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy()
        // 4.返回订单id
        return Result.ok(orderId);
    }*/
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        // 1.查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        // 2.判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀尚未开始！");
        }
        // 3.判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            // 尚未开始
            return Result.fail("秒杀已经结束！");
        }
        // 4.判断库存是否充足
        if (voucher.getStock() < 1) {
            // 库存不足
            return Result.fail("库存不足！");
        }

        Long userId = UserHolder.getUser().getId();
        // 创建锁对象
        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if(!isLock){
            // 获取锁失败，返回错误或重试
            return Result.fail("不允许重复下单");
        }
        try {
            // 获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            // 释放锁
            lock.unlock();
        }
    }*/


    /*@Transactional
    public Result createVoucherOrder(Long voucherId) {
        // 5.一人一单
        Long userId = UserHolder.getUser().getId();

        synchronized (userId.toString().intern()) {
            // 5.1.查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5.2.判断是否存在
            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买过一次！");
            }

            // 6.扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1") // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // where id = ? and stock > 0
                    .update();
            if (!success) {
                // 扣减失败
                return Result.fail("库存不足！");
            }

            // 7.创建订单
            VoucherOrder voucherOrder = new VoucherOrder();
            // 7.1.订单id
            long orderId = redisIdWorker.nextId("order");
            voucherOrder.setId(orderId);
            // 7.2.用户id
            voucherOrder.setUserId(userId);
            // 7.3.代金券id
            voucherOrder.setVoucherId(voucherId);
            save(voucherOrder);

            // 7.返回订单id
            return Result.ok(orderId);
        }
    }*/

}
