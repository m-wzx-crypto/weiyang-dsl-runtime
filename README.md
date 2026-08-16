# ModuMind Runtime Base

多租户 SaaS 的**运行基座**与**事件驱动 DSL 流程引擎**。

本仓库剥离了业务代码，只保留可直接运行的基础设施层：

- **网关**：基于 OpenResty + Lua 的 API 网关（JWT 鉴权、租户限额、IP 黑名单、限流、WebSocket、SSE）
- **基础设施编排**：PostgreSQL / Redis / Qdrant / MinIO 的 Docker Compose 定义
- **监控**：Prometheus + Grafana + 各类 exporter
- **日志采集**：Filebeat（腾讯云 CLS 输出，环境变量注入）
- **DSL 流程引擎**：零业务依赖的 Go 事件驱动编排引擎（本仓库的核心亮点）

## DSL 流程引擎

一个 JSON 定义、事件驱动的轻量流程编排引擎，核心四件套零外部依赖（仅条件表达式使用 [expr](https://github.com/expr-lang/expr)）：

| 模块 | 职责 |
| --- | --- |
| `parser.go` | 将 JSON DSL 解析为 `ProcessDef`，含版本兼容校验 |
| `validator.go` | 结构校验：节点类型、迁移完整性、condition 默认分支、`when` 表达式语法 |
| `executor.go` | 事件驱动执行：按 `when` 条件表达式求值分支，无 `when` 时按事件兜底匹配 |
| `simulator.go` | BFS 路径枚举 + 循环检测，用于流程可达性与死循环预检 |

### 快速体验

```bash
cd dsl
go test ./... -v
```

### DSL 示例

```json
{
  "id": "leave_approval",
  "name": "请假审批",
  "version": "1.0",
  "nodes": [
    {
      "id": "start",
      "type": "start",
      "label": "提交请假",
      "transitions": [{ "event": "submit", "next": "amount_check" }]
    },
    {
      "id": "amount_check",
      "type": "condition",
      "label": "金额判断",
      "transitions": [
        { "when": "amount > 10000", "next": "gm_approve" },
        { "when": "amount <= 10000", "next": "manager_approve" },
        { "event": "*", "next": "manager_approve" }
      ]
    },
    { "id": "gm_approve", "type": "approval", "label": "总经理审批", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "manager_approve", "type": "approval", "label": "经理审批", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "end", "type": "end", "label": "结束", "transitions": [] }
  ]
}
```

> 说明：`condition` 节点优先按 `when` 表达式求值；未命中时回退到按 `event` 匹配，保证向后兼容。

## 架构

```
客户端 ──► OpenResty 网关 (gateway/)
             ├─ JWT 鉴权 / 租户限额 / IP 黑名单 / 限流
             ├─ 反向代理到 BFF / Biz / AI / MinIO
             └─ WebSocket / SSE / 大文件下载
                  │
        ┌─────────┼──────────┐
        ▼         ▼          ▼
       BFF      Biz       AI 服务（业务层，本仓库不含）
        │         │          │
        └─────────┼──────────┘
                  ▼
      PostgreSQL / Redis / Qdrant / MinIO
                  │
             Prometheus ──► Grafana
```

## 目录结构

```
├── gateway/                       # OpenResty 网关
│   ├── nginx.conf                 # 网关配置（JWT/限额/限流/路由）
│   ├── lua/                       # Lua 模块（auth/tenant_limit/ip_blacklist 等）
│   ├── lua/resty/                 # 第三方 OpenResty 生态库（见 NOTICE）
│   ├── ssl/                       # 自签名证书生成
│   └── Dockerfile
├── dsl/                           # DSL 流程引擎（独立 Go module）
│   ├── parser.go / validator.go / executor.go / simulator.go
│   └── *_test.go + testdata/
├── monitoring/                    # Prometheus + Grafana + exporters
├── filebeat/                      # 容器日志采集
├── scripts/                       # dev.sh（本地拉起）/ deploy.sh（部署）
├── shared/proto/                  # DSL 的 proto 接口契约
├── docker-compose.base.yml        # 基础设施编排
└── .github/workflows/ci.yml       # DSL 引擎 CI
```

## 快速开始

```bash
# 1. 配置环境
cp .env.example .env

# 2. 拉起基础设施（postgres/redis/qdrant/minio）
./scripts/dev.sh

# 3. （可选）监控栈
docker compose -f monitoring/docker-compose.monitoring.yml up -d
```

网关构建：`docker build -t gateway ./gateway`

## 设计要点

- **多租户安全**：网关层完成 JWT 校验并注入租户/用户上下文，业务层不直接暴露
- **Fail-closed 启动**：关键密钥缺失时网关拒绝启动，避免带病运行
- **限流与配额**：全局/租户/用户/接口四层限流，Redis 原子同步
- **DSL 向后兼容**：`when` 条件分支与事件驱动语义共存，新老 DSL 定义均可执行
- **最小依赖**：DSL 引擎仅依赖 `expr`，可独立编译、测试、嵌入

## License

[Apache License 2.0](LICENSE)

## NOTICE

第三方组件：

- `gateway/lua/resty/*`：来自 OpenResty 生态（lua-resty-string、lua-resty-jwt 等），版权归原作者所有
- `expr-lang/expr`：MIT License，用于 DSL 条件表达式求值
