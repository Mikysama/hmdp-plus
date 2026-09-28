package org.javaup.config;

import com.baomidou.mybatisplus.annotation.DbType;
import com.baomidou.mybatisplus.extension.plugins.MybatisPlusInterceptor;
import com.baomidou.mybatisplus.extension.plugins.inner.PaginationInnerInterceptor;
import org.springframework.boot.autoconfigure.jackson.Jackson2ObjectMapperBuilderCustomizer;
import org.springframework.context.annotation.Bean;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 通用配置
 * @author: 阿星不是程序员
 **/

public class HmdpCommonAutoConfig {
    
    @Bean
    public Jackson2ObjectMapperBuilderCustomizer jacksonCustom(){
        return new JacksonCustom();
    }
    // 方法功能：注册全局 Jackson 自定义配置器。
    
    @Bean
    public MybatisPlusInterceptor mybatisPlusInterceptor() {
        MybatisPlusInterceptor interceptor = new MybatisPlusInterceptor();
        interceptor.addInnerInterceptor(new PaginationInnerInterceptor(DbType.MYSQL));
        return interceptor;
    }
    // 方法功能：创建 MyBatis Plus 分页拦截器并配置 MySQL 方言。
}
