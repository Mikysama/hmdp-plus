# 秒杀入口网关

Nginx 8080 提供 Vue 静态资源及 `/api/` 反向代理，后端为 127.0.0.1:8085。
开发 Vite 的 `/api` 也转发至 8080，保留前缀，由 Nginx 去掉前缀。

## 启动

先构建前端：在 `hmdp-vue3` 执行 `npm ci && npm run build`。
准备后端 JAR 和业务依赖后，在仓库根目录执行：

```sh
docker compose config --quiet
docker compose run --rm --no-deps gateway nginx -t
docker compose up -d backend gateway
```

访问 http://localhost:8080 。修改配置后先 `nginx -t`，再执行
`docker compose exec gateway nginx -s reload`。
Compose 使用现有 Linux host 网络模式，后端仅绑定回环地址，网关只信任连接来源 IP，覆盖客户端提供的转发头。
应用只信任网关的 127.0.0.1。使用 systemd 启动后端时也须设置
`SERVER_ADDRESS=127.0.0.1` 和
`SPRING_APPLICATION_JSON={"rate-limit":{"trusted-proxies":["127.0.0.1"]}}`。
网关前若增加负载均衡器，须配置明确可信来源的 real_ip，不能直接信任任意 X-Forwarded-For。

## 初始限额

| 场景 | 实例总速率/突发额度 | 单 IP 速率/突发额度 |
|---|---|---|
| 秒杀提交 | 500r/s / 100 | 10r/s / 10 |
| 获取令牌 | 200r/s / 50 | 10r/s / 10 |
| 结果查询 | 1000r/s / 200 | 30r/s / 30 |

配置在 nginx.conf，三个场景完全独立，其他 API 不使用这些额度。
使用漏桶 `limit_req` 和 `nodelay`：额度内立即转发，超额返回 429 JSON 和 Retry-After: 1。
前端继续复用原 requestId。总量额度跨券共享，单 IP 额度也跨券共享。
这些是开发起始值，不是容量承诺；压测后按入口和 Redis 承载能力调整。
共享区只在同一 Nginx 实例的 worker 间共享，多实例不会自动共享总限额。
网关保留应用全部业务限流，accessToken 与令牌桶中的令牌是不同概念。

## 隔离验证

```sh
NGINX_BIN=nginx python3 tests/gateway/smoke.py
```

测试启动临时 Nginx 和模拟后端，不连接业务数据库或 Redis，验证配置、突发拒绝、
JSON 429、独立查询/令牌额度、路径转发和转发头覆盖。
