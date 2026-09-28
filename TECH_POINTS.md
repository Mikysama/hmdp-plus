# HMDP Plus 技术点总结

## 项目定位

HMDP Plus 是黑马点评的工程化升级版本，核心不是普通 CRUD，而是围绕本地生活、商户查询、达人探店、优惠券秒杀、订阅通知和订单一致性构建的高并发分布式实战项目。

项目重点关注：

- 高并发秒杀链路稳定性
- Redis 缓存治理和热点保护
- MQ 可靠投递和消费幂等
- Redis、数据库、订单之间的数据一致性
- 分布式锁、限流、布隆过滤器、延迟队列等通用组件封装
- 分库分表、全局 ID、对账补偿和可观测性

## 模块划分

### hmdp-core-service

核心业务服务模块，包含 Controller、Service、Mapper、Entity 和业务流程编排。

主要业务：

- 用户登录、用户信息
- 商户查询、商户类型
- 博客探店、点赞、评论
- 用户关注、共同关注
- 优惠券、秒杀券
- 秒杀下单、订单路由
- 订单对账、失败回滚、补偿任务
- 开抢提醒、订阅通知
- Kafka 生产者和消费者接入

### hmdp-common

公共基础模块。

技术点：

- 公共常量
- 业务枚举
- 自定义异常
- Spring 工具类
- ThreadLocal 参数上下文
- 日期工具

### hmdp-parameter

接口参数和返回模型模块。

技术点：

- DTO/VO 拆分
- 秒杀券参数模型
- 订单参数模型
- 订阅通知参数模型
- 对账日志参数模型

### hmdp-redis-tool-framework

Redis 工具组件模块。

子模块：

- `hmdp-redis-common-framework`
- `hmdp-redis-framework`
- `hmdp-redis-rate-limit-framework`

技术点：

- RedisTemplate 封装
- Redis Key 统一管理
- 缓存工具类
- Redis Lua 脚本执行
- 令牌桶限流
- 滑动窗口限流
- 秒杀访问令牌
- 限流事件监听和惩罚策略扩展

### hmdp-redisson-framework

Redisson 能力封装模块。

子模块：

- `hmdp-redisson-common-framework`
- `hmdp-service-lock-framework`
- `hmdp-bloom-filter-framework`
- `hmdp-repeat-execute-limit-framework`
- `hmdp-service-delay-queue-framework`

技术点：

- Redisson 自动配置
- 注解式分布式锁
- 可重入锁、公平锁、读写锁
- 分布式锁超时策略
- 布隆过滤器自动配置
- 防重复提交
- 延迟队列生产和消费
- 隔离区域选择
- 消费任务抽象

### hmdp-id-generator-framework

分布式 ID 生成模块。

技术点：

- 雪花算法
- workerId 和 dataCenterId 管理
- Redis + Lua 分配机器标识
- Spring Boot 自动配置

### hmdp-mq-framework

消息队列抽象模块。

子模块：

- `hmdp-mq-common-framework`
- `hmdp-mq-producer-framework`
- `hmdp-mq-consumer-framework`

技术点：

- Kafka 消息模型扩展
- 生产者抽象
- 消费者抽象
- 消息发送成功/失败扩展点
- 消费处理模板
- 消费失败处理基础能力

### hmdp-sharding

分库分表相关模块。

技术点：

- ShardingSphere JDBC
- 分片数据源配置
- 订单路由设计
- 分库分表下的全局 ID
- 分片表对账和补偿基础

### hmdp-vue3

前端模块。

技术点：

- Vue3
- Vite
- 前后端接口联调
- 登录、商户、博客、优惠券等页面

## 核心业务技术点

### 登录和会话

- Redis 共享 Session
- 登录拦截器
- Token 刷新拦截器
- ThreadLocal 保存当前用户
- 手机号校验和验证码登录

### 商户缓存

- Redis 缓存
- 空值缓存解决缓存穿透
- 布隆过滤器拦截非法 ID
- 逻辑过期解决缓存击穿
- 分布式锁控制缓存重建
- 本地缓存 + Redis 多级缓存思路

### 秒杀优惠券

- Redis 预扣库存
- Lua 脚本保证库存扣减和资格校验原子性
- 一人一单校验
- 秒杀资格令牌
- 令牌桶限流
- Kafka 异步创建订单
- 消费幂等
- 失败回滚
- 对账补偿

### 分布式锁

- 手写 Redis 锁
- Redisson 锁
- 注解式锁
- 可重入锁
- 公平锁
- 读写锁
- 锁超时处理策略

### MQ 可靠性

- Kafka 生产者
- Kafka 消费者
- 手动 ACK
- 生产重试
- 消费失败处理
- 死信消费思路
- 消息幂等
- 订单创建异步化

### 数据一致性

- Redis 库存和数据库库存一致性
- 订单创建失败后的回滚
- 回滚失败日志
- 订单对账日志
- 定时对账任务
- 补偿任务
- 最终一致性设计

### 限流和流量控制

- Redis 令牌桶
- Redis 滑动窗口
- 秒杀访问令牌
- 按用户、接口、活动维度限流
- 限流拒绝策略
- 限流事件监听

### 延迟队列和通知

- Redisson 延迟队列
- 开抢前提醒
- 优惠券订阅
- 库存回流通知
- 消费任务隔离

### 分库分表

- ShardingSphere JDBC
- 订单表拆分
- 订单路由表
- 全局 ID 生成
- 分片后查询路由
- 分片数据一致性校验

### 可观测性

- Spring Boot Actuator
- Prometheus 指标暴露
- 日志分级
- 链路耗时分析思路
- 异常聚合和问题定位

## Java 和 Spring 学习点

- Spring Boot 3 项目结构
- Maven 多模块工程
- 自定义 Spring Boot Starter
- `AutoConfiguration.imports` 自动装配
- `@ConfigurationProperties` 配置绑定
- AOP 注解式能力封装
- 拦截器和 WebMvcConfigurer
- 全局异常处理
- Jackson 序列化定制
- MyBatis-Plus CRUD 和分页
- RedisTemplate 使用和封装
- Redisson 客户端使用
- KafkaTemplate 和 `@KafkaListener`
- Lua 脚本和 Redis 原子操作
- ThreadLocal 用户上下文
- 幂等设计
- 分布式 ID
- 分库分表
- 最终一致性和补偿事务

## 适合写进简历的亮点

- 基于 Redis + Lua 实现秒杀库存原子扣减和一人一单校验。
- 基于令牌桶、滑动窗口和秒杀访问令牌实现入口流量控制。
- 封装 Redisson 分布式锁组件，支持注解式使用和多种锁类型。
- 使用 Kafka 异步化秒杀下单链路，提升接口吞吐量。
- 通过消费幂等、失败回滚、对账日志和补偿任务保障最终一致性。
- 使用布隆过滤器、空值缓存、逻辑过期和分布式锁解决缓存穿透与击穿。
- 引入 ShardingSphere 和雪花 ID，支持订单数据分库分表。
- 抽象 Redis、MQ、Redisson、ID 生成等通用框架模块，提高工程复用性。

## 建议学习顺序

1. 先看 `hmdp-core-service` 中的用户、商户、博客基础业务。
2. 学 Redis 缓存：商户查询、缓存穿透、缓存击穿。
3. 学秒杀链路：Lua 扣库存、令牌、限流、一人一单。
4. 学 Kafka 异步下单和消费幂等。
5. 学回滚、对账、补偿任务。
6. 学 Redisson 锁、布隆过滤器、延迟队列组件。
7. 学自定义 starter 和自动装配。
8. 学 ShardingSphere 分库分表和全局 ID。
