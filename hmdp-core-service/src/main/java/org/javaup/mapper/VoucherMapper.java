package org.javaup.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.javaup.entity.Voucher;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 优惠券 Mapper
 * @author: 阿星不是程序员
 **/
@Mapper
public interface VoucherMapper extends BaseMapper<Voucher> {

    /**
     * 查询指定商铺已上架的优惠券，包含秒杀券扩展库存和生效时间。
     *
     * @param shopId 商铺 ID
     * @return 优惠券列表
     */
    List<Voucher> queryVoucherOfShop(@Param("shopId") Long shopId);
}
