package org.javaup.service.impl;

import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.core.RedisKeyManage;
import org.javaup.cache.CacheRebuildGuard;
import org.javaup.cache.CacheTtlPolicy;
import org.javaup.dto.Result;
import org.javaup.entity.Shop;
import org.javaup.handler.BloomFilterHandlerFactory;
import org.javaup.mapper.ShopMapper;
import org.javaup.redis.RedisCache;
import org.javaup.redis.RedisKeyBuild;
import org.javaup.service.IShopService;
import org.javaup.servicelock.LockType;
import org.javaup.toolkit.SnowflakeIdGenerator;
import org.javaup.util.ServiceLockTool;
import org.javaup.utils.CacheClient;
import org.javaup.utils.SystemConstants;
import org.redisson.api.RLock;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.javaup.constant.Constant.BLOOM_FILTER_HANDLER_SHOP;
import static org.javaup.utils.RedisConstants.CACHE_SHOP_KEY;
import static org.javaup.utils.RedisConstants.CACHE_SHOP_TTL;
import static org.javaup.utils.RedisConstants.CACHE_NULL_TTL;
import static org.javaup.utils.RedisConstants.LOCK_SHOP_KEY;
import static org.javaup.utils.RedisConstants.SHOP_GEO_KEY;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 商铺 接口实现
 * @author: 阿星不是程序员
 **/
@Slf4j
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
    
    @Resource
    private ServiceLockTool serviceLockTool;
    
    @Resource
    private RedisCache redisCache;
    
    @Resource
    private BloomFilterHandlerFactory bloomFilterHandlerFactory;
    
    @Resource
    private SnowflakeIdGenerator snowflakeIdGenerator;

    @Resource
    private CacheTtlPolicy cacheTtlPolicy;

    @Resource
    private CacheRebuildGuard cacheRebuildGuard;
    
    
    @Override
    @Transactional(rollbackFor = Exception.class)
    public Result<Long> saveShop(final Shop shop) {
        // 写入数据库
        shop.setId(snowflakeIdGenerator.nextId());
        save(shop);
        // 写入布隆过滤器（商铺业务）
        bloomFilterHandlerFactory.get(BLOOM_FILTER_HANDLER_SHOP).add(String.valueOf(shop.getId()));
        scheduleGeoSync(null, GeoSnapshot.from(shop));
        // 返回店铺id
        return Result.ok(shop.getId());
    }
    
    @Override
    public Result queryById(Long id) {
        //Shop shop = queryByIdV1(id);

        // 互斥锁解决缓存击穿
        //shop = queryByIdV2(id);

        // 逻辑过期解决缓存击穿
        //shop = queryByIdV3(id);
        
        // 🚀完美的方案！使用双重检测锁和空值配置，解决缓存击穿和缓存穿透
        Shop shop = queryByIdV4(id);
        
        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        return Result.ok(shop);
    }
    
    public Shop queryByIdV1(Long id){
        return cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
    }
    
    public Shop queryByIdV2(Long id){
        return cacheClient
                .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);
    }
    
    public Shop queryByIdV3(Long id){
        return cacheClient
                .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);
    }
    
    public Shop queryByIdV4(Long id){
        Shop shop =
                redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY, id), Shop.class);
        if (Objects.nonNull(shop)) {
            return shop;
        }
        log.info("查询商铺 从Redis缓存没有查询到 商铺id : {}",id);
        if (!bloomFilterHandlerFactory.get(BLOOM_FILTER_HANDLER_SHOP).contains(String.valueOf(id))) {
            log.info("查询商铺 布隆过滤器判断不存在 商铺id : {}",id);
            throw new RuntimeException("查询商铺不存在");
        }
        Boolean existResult = redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY_NULL, id));
        if (existResult){
            throw new RuntimeException("查询商铺不存在");
        }
        RLock lock = serviceLockTool.getLock(LockType.Reentrant, LOCK_SHOP_KEY, new String[]{String.valueOf(id)});
        lock.lock();
        try {
            existResult = redisCache.hasKey(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY_NULL, id));
            if (existResult){
                throw new RuntimeException("查询商铺不存在");
            }
            shop = redisCache.get(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY, id), Shop.class);
            if (Objects.nonNull(shop)) {
                return shop;
            }
            shop = cacheRebuildGuard.execute(() -> getById(id));
            if (Objects.isNull(shop)) {
                redisCache.set(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY_NULL, id),
                        "这是一个空值",
                        cacheTtlPolicy.withJitter(CACHE_NULL_TTL),
                        TimeUnit.MINUTES);
                throw new RuntimeException("查询商铺不存在");
            }
            redisCache.set(RedisKeyBuild.createRedisKey(RedisKeyManage.CACHE_SHOP_KEY, id),shop,
                    cacheTtlPolicy.withJitter(CACHE_SHOP_TTL),
                    TimeUnit.MINUTES);
            return shop;
        }finally {
            lock.unlock();
        }
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        Shop oldShop = getById(id);
        if (oldShop == null) {
            return Result.fail("店铺不存在");
        }
        // 1.更新数据库
        if (!updateById(shop)) {
            return Result.fail("店铺更新失败");
        }
        Shop updatedShop = getById(id);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        scheduleGeoSync(GeoSnapshot.from(oldShop), GeoSnapshot.from(updatedShop));
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if ((x == null) != (y == null)) {
            return Result.fail("经纬度必须同时提供");
        }
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }

    private void scheduleGeoSync(GeoSnapshot oldShop, GeoSnapshot newShop) {
        Runnable syncTask = () -> syncGeo(oldShop, newShop);
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            syncTask.run();
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                syncTask.run();
            }
        });
    }

    private void syncGeo(GeoSnapshot oldShop, GeoSnapshot newShop) {
        try {
            if (oldShop != null && oldShop.typeId() != null && oldShop.id() != null) {
                stringRedisTemplate.opsForGeo().remove(
                        SHOP_GEO_KEY + oldShop.typeId(),
                        oldShop.id().toString()
                );
            }
            if (newShop != null && newShop.hasLocation()) {
                stringRedisTemplate.opsForGeo().add(
                        SHOP_GEO_KEY + newShop.typeId(),
                        new Point(newShop.x(), newShop.y()),
                        newShop.id().toString()
                );
            }
        } catch (Exception e) {
            log.error("同步商户GEO失败 oldShop={} newShop={}", oldShop, newShop, e);
        }
    }

    private record GeoSnapshot(Long id, Long typeId, Double x, Double y) {
        private static GeoSnapshot from(Shop shop) {
            return shop == null ? null : new GeoSnapshot(shop.getId(), shop.getTypeId(), shop.getX(), shop.getY());
        }

        private boolean hasLocation() {
            return id != null && typeId != null && x != null && y != null;
        }
    }
}
