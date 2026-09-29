package org.javaup.handler;


import org.javaup.core.SpringUtil;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料 
 * @description: 单个布隆过滤器封装
 * @author: 阿星不是程序员
 **/
public class BloomFilterHandler {

    private final RBloomFilter<String> bloomFilter;
    private final long expectedInsertions;
    private final double falseProbability;

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
        bf.tryInit(this.expectedInsertions, this.falseProbability);
        this.bloomFilter = bf;
    }

    public boolean add(String data) {
        return bloomFilter.add(data);
    }

    /** Recreate configuration after Redis loss without clearing an existing filter. */
    public boolean ensureInitializedAndAdd(String data) {
        bloomFilter.tryInit(expectedInsertions, falseProbability);
        return bloomFilter.add(data);
    }

    public boolean contains(String data) {
        return bloomFilter.contains(data);
    }
}