package org.javaup.handler;


import org.javaup.core.SpringUtil;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.redisson.api.RScript;
import org.redisson.client.codec.StringCodec;
import java.util.List;
import java.util.UUID;
import static org.redisson.RedissonObject.suffixName;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料 
 * @description: 单个布隆过滤器封装
 * @author: 阿星不是程序员
 **/
public class BloomFilterHandler {

    private final RBloomFilter<String> bloomFilter;
    private final long expectedInsertions;
    private final double falseProbability;
    private final RScript scripts;
    private final List<Object> loadKeys;
    // State and bitmap/config share a Redis Cluster slot. Missing data invalidates readiness.
    private static final String CHECK_DATA = "if redis.call('EXISTS',KEYS[1],KEYS[2])~=2 then "
            + "redis.call('DEL',KEYS[3]); return nil end; ";

    public BloomFilterHandler(RedissonClient redissonClient, 
                              String name, 
                              Long expectedInsertions, 
                              Double falseProbability){
        RBloomFilter<String> bf = redissonClient.getBloomFilter(
                SpringUtil.getPrefixDistinctionName() 
                        + "-" 
                        + name);
        this.expectedInsertions = expectedInsertions == null ? 20000L : expectedInsertions;
        this.falseProbability = falseProbability == null ? 0.01D : falseProbability;
        this.bloomFilter = bf;
        this.scripts = redissonClient.getScript(StringCodec.INSTANCE);
        this.loadKeys = List.of(bf.getName(), suffixName(bf.getName(), "config"),
                suffixName(bf.getName(), "catalog-load"));
        readLoadState();
        bf.tryInit(this.expectedInsertions, this.falseProbability);
    }

    public boolean add(String data) {
        readLoadState();
        return bloomFilter.add(data);
    }

    /** Recreate configuration after Redis loss without clearing an existing filter. */
    public boolean ensureInitializedAndAdd(String data) {
        readLoadState();
        bloomFilter.tryInit(expectedInsertions, falseProbability);
        return bloomFilter.add(data);
    }

    /** Start a full additive database load; concurrent loaders are fenced by the token. */
    public String beginLoad() {
        // Materialize a bitmap even for an empty catalog. This is not a real voucher ID.
        ensureInitializedAndAdd("__catalog_seed__");
        String token = UUID.randomUUID().toString();
        String started = scripts.eval(RScript.Mode.READ_WRITE,
                CHECK_DATA + "redis.call('SET',KEYS[3],ARGV[1]); return ARGV[1]",
                RScript.ReturnType.VALUE, loadKeys, "LOADING:" + token);
        if (started == null) throw new IllegalStateException("BLOOM_DATA_MISSING");
        return token;
    }

    public boolean completeLoad(String token) {
        String result = scripts.eval(RScript.Mode.READ_WRITE,
                CHECK_DATA + "if redis.call('GET',KEYS[3])~=ARGV[1] then return nil end; "
                        + "redis.call('SET',KEYS[3],ARGV[2]); return ARGV[2]",
                RScript.ReturnType.VALUE, loadKeys, "LOADING:" + token, "READY:" + token);
        return result != null;
    }

    public String loadedGeneration() {
        String state = readLoadState();
        return state != null && state.startsWith("READY:") ? state : null;
    }

    private String readLoadState() {
        return scripts.eval(RScript.Mode.READ_WRITE,
                CHECK_DATA + "return redis.call('GET',KEYS[3])",
                RScript.ReturnType.VALUE, loadKeys);
    }

    public boolean contains(String data) {
        return bloomFilter.contains(data);
    }
}