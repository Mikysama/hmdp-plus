package org.javaup.init;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import lombok.extern.slf4j.Slf4j;
import org.javaup.entity.Shop;
import org.javaup.service.IShopService;
import org.javaup.utils.RedisConstants;
import org.springframework.core.annotation.Order;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Order(4)
@Component
public class ShopGeoDataInit {

    @Resource
    private IShopService shopService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @PostConstruct
    public void init() {
        clearExistingGeoKeys();
        List<Shop> shops = shopService.list();
        int loaded = 0;
        for (Shop shop : shops) {
            if (shop.getId() == null || shop.getTypeId() == null || shop.getX() == null || shop.getY() == null) {
                continue;
            }
            stringRedisTemplate.opsForGeo().add(
                    RedisConstants.SHOP_GEO_KEY + shop.getTypeId(),
                    new Point(shop.getX(), shop.getY()),
                    shop.getId().toString()
            );
            loaded++;
        }
        log.info("商户GEO数据重建完成，加载数量={}", loaded);
    }

    private void clearExistingGeoKeys() {
        ScanOptions options = ScanOptions.scanOptions()
                .match(RedisConstants.SHOP_GEO_KEY + "*")
                .count(100)
                .build();
        List<String> batch = new ArrayList<>(100);
        try (Cursor<String> cursor = stringRedisTemplate.scan(options)) {
            while (cursor.hasNext()) {
                batch.add(cursor.next());
                if (batch.size() == 100) {
                    stringRedisTemplate.delete(batch);
                    batch.clear();
                }
            }
            if (!batch.isEmpty()) {
                stringRedisTemplate.delete(batch);
            }
        }
    }
}
