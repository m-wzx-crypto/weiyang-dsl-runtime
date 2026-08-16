# Weiyang DSL Runtime

The **runtime base** for multi-tenant SaaS, featuring an **event-driven DSL workflow engine**.

This repository is stripped of business code and keeps only the directly runnable infrastructure layer:

- **API Gateway**: OpenResty + Lua based (JWT auth, tenant rate limits, IP blacklist, throttling, WebSocket, SSE)
- **Infrastructure Orchestration**: Docker Compose definitions for PostgreSQL / Redis / Qdrant / MinIO
- **Monitoring**: Prometheus + Grafana with assorted exporters
- **Log Collection**: Filebeat (Tencent Cloud CLS output, injected via environment variables)
- **DSL Workflow Engine**: a zero-business-dependency Go event-driven orchestration engine (the core highlight of this repo)

## DSL Workflow Engine

A lightweight JSON-defined, event-driven workflow orchestration engine. The core four components have zero external dependencies (only conditional expressions use [expr](https://github.com/expr-lang/expr)):

| Module | Responsibility |
| --- | --- |
| `parser.go` | Parses JSON DSL into `ProcessDef`, including version compatibility validation |
| `validator.go` | Structural validation: node types, transition integrity, condition default branches, `when` expression syntax |
| `executor.go` | Event-driven execution: branches resolved by evaluating `when` expressions, falling back to event matching when `when` is absent |
| `simulator.go` | BFS path enumeration + cycle detection for reachability analysis and infinite-loop preflight |

### Quick Start

```bash
cd dsl
go test ./... -v
```

### DSL Example

```json
{
  "id": "leave_approval",
  "name": "Leave Approval",
  "version": "1.0",
  "nodes": [
    {
      "id": "start",
      "type": "start",
      "label": "Submit Leave",
      "transitions": [{ "event": "submit", "next": "amount_check" }]
    },
    {
      "id": "amount_check",
      "type": "condition",
      "label": "Amount Check",
      "transitions": [
        { "when": "amount > 10000", "next": "gm_approve" },
        { "when": "amount <= 10000", "next": "manager_approve" },
        { "event": "*", "next": "manager_approve" }
      ]
    },
    { "id": "gm_approve", "type": "approval", "label": "GM Approval", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "manager_approve", "type": "approval", "label": "Manager Approval", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "end", "type": "end", "label": "End", "transitions": [] }
  ]
}
```

> Note: `condition` nodes evaluate `when` expressions first; if no expression matches, they fall back to `event` matching for backward compatibility.

## Architecture

```
Clients ──► OpenResty Gateway (gateway/)
             ├─ JWT auth / Tenant rate limit / IP blacklist / Throttling
             ├─ Reverse proxy to BFF / Biz / AI / MinIO
             └─ WebSocket / SSE / Large file download
                  │
        ┌─────────┼──────────┐
        ▼         ▼          ▼
       BFF      Biz       AI services (business layer, not included in this repo)
        │         │          │
        └─────────┼──────────┘
                  ▼
      PostgreSQL / Redis / Qdrant / MinIO
                  │
             Prometheus ──► Grafana
```

## Directory Layout

```
├── gateway/                       # OpenResty gateway
│   ├── nginx.conf                 # Gateway config (JWT / limits / throttling / routing)
│   ├── lua/                       # Lua modules (auth / tenant_limit / ip_blacklist, etc.)
│   ├── lua/resty/                 # Third-party OpenResty ecosystem libraries (see NOTICE)
│   ├── ssl/                       # Self-signed certificate generation
│   └── Dockerfile
├── dsl/                           # DSL workflow engine (standalone Go module)
│   ├── parser.go / validator.go / executor.go / simulator.go
│   └── *_test.go + testdata/
├── monitoring/                    # Prometheus + Grafana + exporters
├── filebeat/                      # Container log collection
├── scripts/                       # dev.sh (local bootstrap) / deploy.sh (deployment)
├── shared/proto/                  # DSL proto interface contract
├── docker-compose.base.yml        # Infrastructure orchestration
└── .github/workflows/ci.yml       # DSL engine CI
```

## Getting Started

```bash
# 1. Configure environment
cp .env.example .env

# 2. Bring up infrastructure (postgres/redis/qdrant/minio)
./scripts/dev.sh

# 3. (Optional) Monitoring stack
docker compose -f monitoring/docker-compose.monitoring.yml up -d
```

Build the gateway: `docker build -t gateway ./gateway`

## Design Highlights

- **Multi-tenant security**: JWT validation happens at the gateway, which injects tenant/user context; business layers are never directly exposed
- **Fail-closed startup**: the gateway refuses to start when critical secrets are missing, preventing degraded operation
- **Rate limiting & quotas**: four-layer throttling (global / tenant / user / endpoint) with Redis-atomic coordination
- **DSL backward compatibility**: `when` conditional branches coexist with event-driven semantics, so both new and legacy DSL definitions are executable
- **Minimal dependencies**: the DSL engine depends only on `expr` and can be compiled, tested, and embedded independently

## License

[Apache License 2.0](LICENSE)

## NOTICE

Third-party components:

- `gateway/lua/resty/*`: from the OpenResty ecosystem (lua-resty-string, lua-resty-jwt, etc.), copyright belongs to their respective authors
- `expr-lang/expr`: MIT License, used for DSL condition expression evaluation
