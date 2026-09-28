package org.javaup.delay.consumer;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.javaup.core.ConsumerTask;
import org.javaup.core.SpringUtil;
import org.javaup.delay.message.DelayedVoucherReminderMessage;
import org.javaup.seckill.SeckillCatalogService;
import org.javaup.service.IUserInfoService;
import org.springframework.stereotype.Component;
import static org.javaup.constant.Constant.DELAY_VOUCHER_REMINDER;

/** V2 reminders read persisted subscriptions; Redis audience sets are not authoritative. */
@Slf4j
@Component
public class ConsumerDelayedVoucherReminder implements ConsumerTask {
    private final SeckillCatalogService catalog;
    private final IUserInfoService users;
    public ConsumerDelayedVoucherReminder(SeckillCatalogService catalog,IUserInfoService users) {
        this.catalog=catalog;this.users=users;
    }
    @Override public void execute(String content) {
        var message=JSON.parseObject(content,DelayedVoucherReminderMessage.class);
        if(message==null||message.getVoucherId()==null||message.getBeginTime()==null)
            throw new IllegalArgumentException("INVALID_REMINDER_MESSAGE");
        long voucherId=message.getVoucherId(),after=0;int persisted=0;
        var voucher=catalog.get(voucherId);
        if(!message.getBeginTime().equals(voucher.get("beginTime")))return;
        while(true) {
            var page=catalog.reminderSubscribers(voucherId,after,100);
            if(page.isEmpty())break;
            for(long userId:page) {
                // User data may live on a different shard: read outside the voucher transaction.
                var user=users.getByUserId(userId);
                if(catalog.recordReminder(voucherId,userId,message.getBeginTime(),user==null?null:user.getLevel()))persisted++;
            }
            after=page.get(page.size()-1);
        }
        log.info("Opening reminder inbox records created voucher={} count={} (external push not configured)",voucherId,persisted);
    }
    @Override public String topic(){return SpringUtil.getPrefixDistinctionName()+"-"+DELAY_VOUCHER_REMINDER;}
}
