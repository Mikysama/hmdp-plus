package org.javaup;

import jakarta.annotation.Resource;
import org.javaup.entity.Shop;
import org.javaup.service.impl.ShopServiceImpl;
import org.javaup.utils.CacheClient;
import org.javaup.utils.RedisIdWorker;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.javaup.utils.RedisConstants.CACHE_SHOP_KEY;
import static org.javaup.utils.RedisConstants.SHOP_GEO_KEY;

@org.junit.jupiter.api.Disabled("Legacy test connects to development data. Use isolated org.javaup.seckill tests.")
@SpringBootTest
class HmDianPingApplicationTests {

    @Resource
    private CacheClient cacheClient;

    @Resource
    private ShopServiceImpl shopService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    private ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    void testIdWorker() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(300);

        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
            latch.countDown();
        };
        long begin = System.currentTimeMillis();
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
        latch.await();
        long end = System.currentTimeMillis();
        System.out.println("time = " + (end - begin));
    }
    // 方法功能：并发测试 Redis 分布式 ID 生成器的吞吐和唯一性。

    @Test
    void testSaveShop() throws InterruptedException {
        Shop shop = shopService.getById(1L);
        cacheClient.setWithLogicalExpire(CACHE_SHOP_KEY + 1L, shop, 10L, TimeUnit.SECONDS);
    }
    // 方法功能：测试商铺缓存预热和逻辑过期写入。

    @Test
    void loadShopData() {
        // 1.查询店铺信息
        List<Shop> list = shopService.list();
        // 2.把店铺分组，按照typeId分组，typeId一致的放到一个集合
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        // 3.分批完成写入Redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            // 3.1.获取类型id
            Long typeId = entry.getKey();
            String key = SHOP_GEO_KEY + typeId;
            // 3.2.获取同类型的店铺的集合
            List<Shop> value = entry.getValue();
            List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());
            // 3.3.写入redis GEOADD key 经度 纬度 member
            for (Shop shop : value) {
                // stringRedisTemplate.opsForGeo().add(key, new Point(shop.getX(), shop.getY()), shop.getId().toString());
                locations.add(new RedisGeoCommands.GeoLocation<>(
                        shop.getId().toString(),
                        new Point(shop.getX(), shop.getY())
                ));
            }
            stringRedisTemplate.opsForGeo().add(key, locations);
        }
    }
    // 方法功能：将商铺经纬度数据按类型加载到 Redis GEO。

    @Test
    void testHyperLogLog() {
        String[] values = new String[1000];
        int j = 0;
        for (int i = 0; i < 1000000; i++) {
            j = i % 1000;
            values[j] = "user_" + i;
            if(j == 999){
                // 发送到Redis
                stringRedisTemplate.opsForHyperLogLog().add("hl2", values);
            }
        }
        // 统计数量
        Long count = stringRedisTemplate.opsForHyperLogLog().size("hl2");
        System.out.println("count = " + count);
    }
    // 方法功能：测试 Redis HyperLogLog 的基数统计能力。
}
