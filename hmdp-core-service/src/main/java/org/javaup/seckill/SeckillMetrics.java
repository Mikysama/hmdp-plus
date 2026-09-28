package org.javaup.seckill;

import io.micrometer.core.instrument.MeterRegistry;
import java.util.concurrent.TimeUnit;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;

/** Bounded tag cardinality: do not put voucher/user/request IDs in latency series. */
@Aspect
@Component
public class SeckillMetrics {
  private final MeterRegistry registry;

  public SeckillMetrics(MeterRegistry registry) {
    this.registry = registry;
  }

  @Around(
      "execution(* org.javaup.seckill.SeckillFacade.submit(..)) || execution(*"
          + " org.javaup.seckill.SeckillTransactions.accept(..)) || execution(*"
          + " org.javaup.seckill.SeckillTransactions.fulfill(..)) || execution(*"
          + " org.javaup.seckill.SeckillRecovery.recover(..)) || execution(*"
          + " org.javaup.seckill.redis.RedisAdmissionGateway.reserve(..))")
  public Object measure(ProceedingJoinPoint call) throws Throwable {
    long start = System.nanoTime();
    String outcome = "ok";
    try {
      return call.proceed();
    } catch (Throwable failure) {
      outcome = "error";
      throw failure;
    } finally {
      io.micrometer.core.instrument.Timer.builder("seckill_v2_latency")
          .tags(
              "operation",
              call.getSignature().getDeclaringType().getSimpleName()
                  + "."
                  + call.getSignature().getName(),
              "outcome",
              outcome)
          .publishPercentileHistogram()
          .register(registry)
          .record(System.nanoTime() - start, TimeUnit.NANOSECONDS);
    }
  }
}
