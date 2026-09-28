package org.javaup.seckill;

import jakarta.servlet.http.*;
import java.util.*;
import java.util.stream.Collectors;
import org.javaup.utils.UserHolder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class SeckillSecurityInterceptor implements HandlerInterceptor {
  private final Set<Long> admins;

  public SeckillSecurityInterceptor(
      @Value("${seckill.security.admin-user-ids:}") String configured) {
    admins =
        Arrays.stream(configured.split(","))
            .map(String::trim)
            .filter(x -> !x.isEmpty())
            .map(Long::valueOf)
            .collect(Collectors.toUnmodifiableSet());
  }

  @Override
  public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) {
    String p =
        req.getRequestURI()
            .substring(req.getContextPath().length())
            .replaceAll(";[^/]*", "")
            .replaceAll("/+$", "");
    boolean admin =
        p.equals("/voucher")
            || p.equals("/voucher/seckill")
            || p.startsWith("/voucher/update/")
            || p.equals("/voucher/delay/voucher/reminder")
            || p.startsWith("/voucher-order/reconciliation/");
    if (admin && (UserHolder.getUser() == null || !admins.contains(UserHolder.getUser().getId())))
      throw new SeckillFailure("FORBIDDEN", 403);
    return true;
  }
}
