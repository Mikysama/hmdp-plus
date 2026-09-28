package org.javaup.kafka.message;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Data;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 秒杀券消息
 * @author: 阿星不是程序员
 **/

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SeckillVoucherMessage {

    private Long userId;
    
    private Long voucherId;
    
    private Long orderId;

    private Long traceId;

    private Integer beforeQty; // 扣减前库存
    
    private Integer changeQty; // 扣减数量
    
    private Integer afterQty; // 扣减后库存
    
    private Boolean autoIssue; // 是否自动发放
}
