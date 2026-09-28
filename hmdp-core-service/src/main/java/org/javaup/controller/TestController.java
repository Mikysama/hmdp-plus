package org.javaup.controller;


import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.Resource;
import org.javaup.dto.Result;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;


/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 测试
 * @author: 阿星不是程序员
 **/
@RestController
@RequestMapping("/test")
public class TestController {

    @Resource
    private MeterRegistry meterRegistry;
    
    @RequestMapping("/meter")
    public Result<String> queryShopById() {
        Counter counter = meterRegistry.counter("test_query_shop", "method", "queryShopById");
        counter.increment();
        return Result.ok("指标上报成功，当前计数: " + counter.count());
    }
    // 方法功能：查询 shop by id 相关数据并返回结果。
}
