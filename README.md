# Weiyang DSL Runtime

The **runtime base** for multi-tenant SaaS, featuring an **event-driven DSL workflow engine**.

This repository is stripped of business code and keeps only the directly runnable infrastructure layer:

- **API Gateway**: OpenResty + Lua based (JWT auth, tenant rate limits, IP blacklist, throttling, WebSocket, SSE)
- **Infrastructure Orchestration**: Docker Compose definitions for PostgreSQL / Redis / Qdrant / MinIO
- **Monitoring**: Prometheus + Grafana with assorted exporters
- **Log Collection**: Filebeat (Tencent Cloud CLS output, injected via environment variables)
- **DSL Workflow Engine**: a zero-business-dependency Go event-driven orchestration engine (the core highlight of this repo)

## DSL Workflow Engine

A lightweight JSON-defined, event-driven workflow orchestration engine. The core components have zero external dependencies (only conditional expressions use [expr](https://github.com/expr-lang/expr)):

| Module | Responsibility |
| --- | --- |
| `parser.go` | Parses JSON DSL into `ProcessDef`, including version compatibility validation and fork/join config |
| `validator.go` | Structural validation: node types, transition integrity, condition default branches, `when` expression syntax, parallel/join semantics |
| `executor.go` | Single-step executor: from `ExecutionContext` to `ExecutionResult` (state transition + side-effect commands + next actions) |
| `runtime.go` | True Runtime: lifecycle state machine (pending→running→waiting→resume→completed/failed), event-driven resume, idempotency, parallel fork/join orchestration, savepoint/restore persistence |
| `context.go` | Unified `ExecutionContext` (Process/Instance/Execution IDs, variables, events, metadata, parallel scopes, engine binding) + execution state machine + JSON serialization |
| `sideeffect.go` | Side-effect decoupling: Executor emits `SideEffectCommand`, real work is done by a pluggable `SideEffectExecutor` with retry/idempotency |
| `parallel.go` | Parallel/Join semantics: fork mode (all/any), explicit or auto-detected join node, convergence (all/any/n_of_m counting successes only), branch failure policy (continue/fail), partial success, timeout, compensation routing via join conditions |
| `simulator.go` | BFS path enumeration + cycle detection (legacy reachability view) |
| `analyzer.go` | Static analyzer: unreachable node / dead end / cycle / duplicate transition / invalid terminal / path complexity |
| `expression.go` | Shared Expression Engine (validate/evaluate/type-check) used by executor, validator and type checker |
| `types.go` + `typechecker.go` | Type System: string/number/boolean/object/array/enum/date/money with static type checks before execution |

### Runtime execution model

The engine distinguishes *what should happen* (declared by the Executor) from *how it happens*
(executed by the Runtime/Workers):

```
DSL → Executor → State Transition + SideEffectCommands → Runtime/Worker → actual execution
```

`Runtime` maintains a per-instance state machine, deduplicates events by `EventID` for
idempotency, auto-advances linear flows and parallel branches, and lets a `SideEffectExecutor`
own retry/timeout/async semantics — so business side effects like notifications, inventory
deduction or AI calls never block or entangle the state transition.

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
        { "next": "manager_approve" }
      ]
    },
    { "id": "gm_approve", "type": "approval", "label": "GM Approval", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "manager_approve", "type": "approval", "label": "Manager Approval", "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] },
    { "id": "end", "type": "end", "label": "End", "transitions": [] }
  ]
}
```

> Note: `condition` nodes evaluate `when` expressions first; if no expression matches, the first `when`-less transition acts as the default branch. (There is no `"*"` wildcard event — the README previously suggested otherwise; that transition above is a plain default.)

## DSL v2 — Contract-Based Process Runtime

DSL v2 evolves the engine from a *graph interpreter* into a *contract-based process runtime*. Set `"version": "2"` to unlock the three contracts; **v1 definitions keep running unchanged** (v2 fields are ignored in v1 documents).

### 1. Data contract — typed variables & node-level I/O mapping

Variables are declared with types at the top level; nodes map data in (`input`) and out (`output`) instead of sharing a global variable soup. `when` expressions are **statically type-checked against the schema at validation time**, so `amount > "hello"` fails at deploy time, not at runtime.

```json
{
  "id": "expense_approval",
  "version": "2.0",
  "variables": {
    "amount": { "type": "money", "init": 0 },
    "order":  { "type": "object", "fields": { "vip": "boolean", "level": { "type": "enum", "values": ["low", "high"] } } }
  },
  "nodes": [
    { "id": "check", "type": "condition",
      "input":  { "limit": "order.vip ? 10000 : 1000" },
      "output": { "channel": "amount > limit ? \"gm\" : \"manager\"" },
      "transitions": [
        { "when": "amount > limit", "next": "gm_approve" },
        { "next": "manager_approve" }
      ] }
  ]
}
```

Supported types: `string`, `number`/`money`, `boolean`, `date`, `enum`, `array`, `object` (arbitrarily nested). Output writes are type-checked at runtime; a mismatch fails the node instead of corrupting the variables.

### 2. Time contract — timers & deadlines

The engine is no longer passively waiting for the next event: it maintains *waiting slots* and exposes

- `Runtime.NextWakeup()` — the earliest moment the host scheduler must wake the instance;
- `Runtime.WakeDue(now)` — fires all due slots as idempotent, engine-generated transitions.

```json
{ "id": "remind", "type": "timer", "duration": "2h",
  "transitions": [{ "next": "notify" }] },
{ "id": "approve", "type": "approval",
  "deadline": { "after": "24h", "next": "escalate" },
  "transitions": [{ "event": "approve", "next": "end" }, { "event": "reject", "next": "end" }] }
```

`timer` auto-advances when it fires; an approval `deadline` escalates to the declared `next` node if no event arrives in time — whichever happens first wins.

### 3. Behavior contract — declarative compensation (Saga)

Side effects can declare their inverse operation. Every successfully executed effect enters the instance's undo stack; on failure (or on an explicit `Runtime.Compensate()`) compensations fire in reverse order, each with its own idempotency key:

```json
{ "id": "charge", "type": "action",
  "sideEffects": [{ "type": "charge_card", "target": "payment",
                    "compensation": { "type": "refund_card", "target": "payment" } }],
  "transitions": [{ "next": "approve" }] }
```

Enable automatic compensation on failure with `WithAutoCompensate()`.

### v2 runtime notes

- Side-effect idempotency keys now include the node's visit count, so loops that legitimately pass the same node twice execute their side effects twice (previously the second execution was silently deduplicated).
- Automatic nodes (`action` / `notification`) now auto-advance along a single unconditional `next` transition in linear flows, matching the semantics parallel branches always had.
- All waiting/timer/deadline/undo state survives `Savepoint()` / `RestoreExecutionContext()`.

### Event delivery semantics (all versions)

- An event no matching waiting node/branch accepts is **returned, not burned**: the instance stays `waiting`, the event is not recorded as consumed, and redelivery with the same event ID is processed normally.
- Instances in a terminal state (`completed` / `failed` / `canceled` / `timed_out`) reject new events instead of silently processing them.
- Parallel scope convergence timeouts fire through `WakeDue` on the active timeline instead of being re-checked only when the next event happens to arrive.
- The convergence config (`mode` / `required` / `timeout`) declared **on the `join` node itself** is now honored; it fills any fields not declared on the `parallel` node.

## Deterministic Kernel (Journal)

Phase 1 of the engine roadmap: execution is now **event-sourced**. Every state change the engine makes — events consumed, transitions taken, variables written, scopes forked/joined, wait slots registered, side-effect commands issued and their outcomes, compensations — is appended to a `Journal` as an immutable `Occurrence`. The instance state is the **fold of its log**:

```
state = fold(journal)        // fold is exact, verified by property tests
```

```go
j := dsl.NewMemoryJournal()
r := dsl.NewRuntime(def, exec, dsl.WithJournal(j))
r.Start(...) // every operation appends facts

// Time travel: rebuild the instance as of any point in its history.
past, _ := dsl.FoldTo(def, j.Occurrences(), 42)

// Recovery: rebuild a runnable runtime from the log on any process.
r2, _ := dsl.Resume(def, j.Occurrences(), exec, dsl.WithJournal(j2))
// idempotency table, undo stack, wait slots and side-effect results all survive

// Outbox view: commands issued but never resolved (e.g. process died mid-flight).
pending := dsl.PendingCommands(j.Occurrences()) // re-dispatch safely — command IDs are idempotency keys
```

Guarantees:

- **Fold exactness** — for every flow, folding the journal reproduces the live context field-for-field (enforced by `journal_test.go` property tests, not by convention).
- **Zero cost when off** — no `WithJournal` means no journaling and byte-identical behavior.
- **WAL semantics** — journal append failures are surfaced via `Runtime.JournalError()` instead of being swallowed.
- **Pluggable storage** — the core depends only on the one-method `Journal` interface; the in-memory implementation ships with the engine, a persistent (Postgres) journal is phase 2.

`Snapshot()`/`Savepoint()` remain available as compaction primitives on top of the log.

## Persistence SPI & Runtime Manager

Phase 2 turns the kernel into an embeddable process service. The core defines two storage interfaces (zero dependencies) and a facade that ties them together:

- `Journal` + `JournalReader` — the fact log (append + load per instance);
- `InstanceStore` — a queryable **index** of instances: `PutInstance` / `GetInstance` / `ListWakeable(now)` / `ListByStatus(status)`. The instance's true state is always `fold(journal)`; the store is a rebuildable summary.

`Manager` is the entry point hosts embed:

```go
m := dsl.NewManager([]*dsl.ProcessDef{def}, mySideEffects,
    dsl.WithManagerJournal(journal),   // e.g. Postgres
    dsl.WithManagerStore(store),
    dsl.WithManagerAutoCompensate())

m.Start("order_flow", "inst-1", "tenant-a", vars, dsl.Event{ID: "e1", Name: "submit"})
m.Feed("inst-1", dsl.Event{ID: "e2", Name: "approve"})
m.WakeDueSweep(time.Now(), 100) // scheduler entry: fire all due timers/deadlines
```

Every operation follows the same rhythm: **load (fold the journal) → execute → sync the summary record**. Idempotency tables, undo stacks and wait slots ride along in the log, so a "crashed" instance resumes on any process with byte-identical semantics. Commands issued but never resolved (crash between dispatch and result) are **re-dispatched automatically on load** — command idempotency keys make that safe: at-least-once delivery + idempotent effects.

A Postgres reference implementation ships as a separate module (`store-postgres/`) — driver-agnostic (`*sql.DB` injection, schema in `schema.sql`), so the core keeps its zero-dependency guarantee.

## AI-Native Nodes (Bounded Agency)

Phase 4 adds a first-class `ai` node built on the v2 contracts. The design principle is **bounded agency**: the model may only choose among transitions the DSL *declares* — it can never invent nodes, routes or side effects. What the AI is allowed to produce is exactly what the schema says.

```json
{ "id": "triage", "type": "ai",
  "ai": {
    "prompt": "classify ticket {{ticket}} for customer {{customer.name}}",
    "output": { "confidence": { "type": "number" } },
    "choose": ["billing", "technical", "other"],
    "onError": "manual_review"
  },
  "transitions": [
    { "case": "billing",   "next": "billing" },
    { "case": "technical", "next": "technical" },
    { "case": "other",     "next": "other" }
  ] }
```

Execution model:

1. On arrival the node emits an **inference command** (`ai_infer`) — prompt (variable-interpolated), output schema and candidate list included — as a regular journaled side effect. A crash mid-inference is recovered through the outbox: no lost or duplicated requests.
2. The instance parks until the host feeds the callback event (default `ai_result`) with `{choice, output, error}`.
3. The engine validates `output` against the declared schema (violations escalate to `onError` — model misbehavior never corrupts variables), then routes by `case` under bounded agency: a choice outside the declared candidates **cannot** move the flow anywhere but the escalation path.
4. Without `choose`, the node degrades to a structured-enrichment node: outputs become variables and routing follows plain `when` conditions — the DSL stays in charge either way.

Parallel branches each get their own inference request; callback results are correlated per request via `request_id`, so two branches waiting on the same callback event never cross wires. An optional `deadline` escalates to human review when the model is silent.

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
