package org.javaup.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Delete;
import org.javaup.entity.VoucherOrderRouter;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 优惠券订单路由 Mapper
 * @author: 阿星不是程序员
 **/
@Mapper
public interface VoucherOrderRouterMapper extends BaseMapper<VoucherOrderRouter> {
    
    /**
     * 根据订单 ID 删除订单路由记录。
     *
     * @param orderId 订单 ID
     * @return 受影响行数
     */
    @Delete("DELETE FROM tb_voucher_order_router where order_id = #{orderId}")
    Integer deleteVoucherOrderRouter(@Param("orderId") Long orderId);
    
    /**
     * 查询指定商铺最近一段时间内的 Top 购买用户。
     *
     * @param shopId 商铺 ID
     * @param limit 返回数量
     * @param days 统计最近天数
     * @return 用户 ID 列表
     */
    @Select("SELECT vor.user_id FROM tb_voucher_order_router vor " +
            "JOIN tb_voucher v ON v.id = vor.voucher_id " +
            "WHERE v.shop_id = #{shopId} AND vor.create_time >= DATE_SUB(NOW(), INTERVAL #{days} DAY) " +
            "GROUP BY vor.user_id ORDER BY COUNT(1) DESC LIMIT #{limit}")
    java.util.List<Long> findTopBuyerUserIdsByShop(@Param("shopId") Long shopId,
                                                  @Param("limit") int limit,
                                                  @Param("days") int days);
}
