package org.javaup.controller;


import jakarta.annotation.Resource;
import jakarta.validation.Valid;
import org.javaup.dto.DelayVoucherReminderDto;
import org.javaup.dto.GetSeckillVoucherDto;
import org.javaup.dto.Result;
import org.javaup.dto.SeckillVoucherDto;
import org.javaup.dto.UpdateSeckillVoucherDto;
import org.javaup.dto.UpdateSeckillVoucherStockDto;
import org.javaup.dto.VoucherDto;
import org.javaup.dto.VoucherSubscribeBatchDto;
import org.javaup.dto.VoucherSubscribeDto;
import org.javaup.entity.Voucher;
import org.javaup.seckill.SeckillCatalogService;
import org.javaup.seckill.SeckillTransactions;
import org.javaup.seckill.SeckillFailure;
import org.javaup.utils.UserHolder;
import java.util.Map;
import org.javaup.service.IVoucherService;
import org.javaup.vo.GetSubscribeStatusVo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;


/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 优惠券api
 * @author: 阿星不是程序员
 **/
@RestController
@RequestMapping("/voucher")
public class VoucherController {

    @Resource
    private IVoucherService voucherService;
    
    @Resource
    private SeckillCatalogService catalog;

    @Resource
    private SeckillTransactions transactions;

    private long userId() {
        if (UserHolder.getUser() == null) throw new SeckillFailure("UNAUTHENTICATED", 401);
        return UserHolder.getUser().getId();
    }
    
    @PostMapping("/get")
    public Result<Map<String,Object>> get(@Valid @RequestBody GetSeckillVoucherDto getSeckillVoucherDto) {
        return Result.ok(catalog.get(getSeckillVoucherDto.getVoucherId()));
    }
    // 方法功能：按请求参数查询并返回对应业务数据。
    
    @PostMapping("/seckill")
    public Result<String> addSeckillVoucher(@Valid @RequestBody SeckillVoucherDto seckillVoucherDto) {
        final long voucherId = catalog.addSeckill(seckillVoucherDto);
        return Result.ok(String.valueOf(voucherId));
    }
    // 方法功能：新增秒杀券并返回秒杀券 ID。

    @PostMapping("/update/seckill")
    public Result<Void> updateSeckillVoucher(@Valid @RequestBody UpdateSeckillVoucherDto updateSeckillVoucherDto) {
        catalog.update(updateSeckillVoucherDto);
        return Result.ok();
    }
    // 方法功能：更新秒杀券基础信息、规则和相关缓存。
    
    @PostMapping("/update/seckill/stock")
    public Result<Map<String,Object>> updateSeckillVoucherStock(@Valid @RequestBody UpdateSeckillVoucherStockDto updateSeckillVoucherDto) {
        return Result.ok(catalog.adjust(updateSeckillVoucherDto));
    }
    // 方法功能：更新秒杀券库存并同步数据库、Redis 和本地缓存。

    @PostMapping
    public Result<String> addVoucher(@Valid @RequestBody VoucherDto voucherDto) {
        final long voucherId = catalog.addOrdinary(voucherDto);
        return Result.ok(String.valueOf(voucherId));
    }
    // 方法功能：新增普通优惠券并返回券 ID。
    
    @GetMapping("/list/{shopId}")
    public Result<List<Voucher>> queryVoucherOfShop(@PathVariable("shopId") Long shopId) {
       return voucherService.queryVoucherOfShop(shopId);
    }
    // 方法功能：查询指定商铺可用优惠券列表。
    
    @PostMapping("/subscribe")
    public Result<Void> subscribe(@Valid @RequestBody VoucherSubscribeDto voucherSubscribeDto){
        transactions.subscribe(voucherSubscribeDto.getVoucherId(), userId(), true);
        return Result.ok();
    }
    // 方法功能：订阅指定秒杀券并记录用户订阅关系。
    
    @PostMapping("/unsubscribe")
    public Result<Void> unsubscribe(@Valid @RequestBody VoucherSubscribeDto voucherSubscribeDto){
        transactions.subscribe(voucherSubscribeDto.getVoucherId(), userId(), false);
        return Result.ok();
    }
    // 方法功能：取消指定秒杀券订阅并移除用户订阅关系。
    
    @PostMapping("/get/subscribe/status")
    public Result<Integer> getSubscribeStatus(@Valid @RequestBody VoucherSubscribeDto voucherSubscribeDto){
        return Result.ok(transactions.subscribeStatus(voucherSubscribeDto.getVoucherId(), userId()));
    }
    // 方法功能：查询用户对指定秒杀券的订阅状态。
    
    @PostMapping("/get/subscribe/status/batch")
    public Result<List<GetSubscribeStatusVo>> getSubscribeStatusBatch(@Valid @RequestBody VoucherSubscribeBatchDto voucherSubscribeBatchDto){
        long currentUser = userId();
        return Result.ok(voucherSubscribeBatchDto.getVoucherIdList().stream().distinct()
                .map(id -> new GetSubscribeStatusVo(id, transactions.subscribeStatus(id, currentUser))).toList());
    }
    // 方法功能：批量查询用户对多个秒杀券的订阅状态。
    
    @PostMapping("/delay/voucher/reminder")
    public Result<Void> delayVoucherReminder(@Valid @RequestBody DelayVoucherReminderDto delayVoucherReminderDto){
        catalog.scheduleReminder(delayVoucherReminderDto);
        return Result.ok();
    }
    // 方法功能：校验并提交指定秒杀券的延迟提醒任务。
}
