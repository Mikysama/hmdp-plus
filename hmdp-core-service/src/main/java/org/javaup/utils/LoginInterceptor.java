package org.javaup.utils;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;

/**
 * @program: 黑马点评-plus升级版实战项目。添加 阿星不是程序员 微信，添加时备注 点评 来获取项目的完整资料
 * @description: 登录验证过滤器
 * @author: 阿星不是程序员
 **/
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // 1.判断是否需要拦截（ThreadLocal中是否有用户）
        if (UserHolder.getUser() == null) {
            // 没有，需要拦截，设置状态码
            response.setStatus(401);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().write("{\"success\":false,\"code\":\"UNAUTHENTICATED\",\"errorMsg\":\"请先登录\"}");
            // 拦截
            return false;
        }
        // 有用户，则放行
        return true;
    }
    // 方法功能：在请求进入控制器前执行登录或 token 刷新校验。
}
