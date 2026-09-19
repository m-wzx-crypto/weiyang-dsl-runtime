-- Weiyang DSL Runtime — Postgres persistence schema (phase 2 reference impl).
-- 幂等执行:可重复执行,全部使用 IF NOT EXISTS。

-- 实例摘要(可查询的索引;实例完整状态 = fold(dsl_journal),损坏可重建)。
CREATE TABLE IF NOT EXISTS dsl_instances (
    instance_id   TEXT PRIMARY KEY,
    definition_id TEXT NOT NULL,
    tenant_id     TEXT NOT NULL DEFAULT '',
    status        TEXT NOT NULL,
    current_node  TEXT NOT NULL DEFAULT '',
    wake_up_at    TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dsl_instances_wake
    ON dsl_instances (wake_up_at)
    WHERE status = 'waiting' AND wake_up_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_dsl_instances_status
    ON dsl_instances (status);

-- 事实日志(append-only;实例状态的唯一事实源)。
-- seq 由 (instance_id, MAX(seq)+1) 在同语句内原子分配——与引擎"单实例单写者"
-- 并发契约一致;同实例并发写会触发主键冲突,宿主应重试而非并发写同一实例。
CREATE TABLE IF NOT EXISTS dsl_journal (
    instance_id TEXT        NOT NULL,
    seq         BIGINT      NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, seq)
);

-- M1 归属索引:按行为主体(kind + id)检索决策事实——人类审批与模型推理
-- 同一查询口径("谁做了哪些决策")。部分索引,仅覆盖带 actor 的事实
-- (event_consumed 且携带 principal);instance_id 兜尾,跨实例与单实例
-- 两种查询形状都能走索引。表达式索引依赖 JSONB 载荷的 actor 形状:
-- payload->'actor'->>'kind' / ->>'id'(引擎侧 Occurrence.Actor)。
CREATE INDEX IF NOT EXISTS idx_dsl_journal_actor
    ON dsl_journal ( (payload->'actor'->>'kind'), (payload->'actor'->>'id'), instance_id )
    WHERE (payload->'actor') IS NOT NULL;

-- 实例上下文快照(恢复加速:状态 = 快照 + 其后增量日志;日志仍是唯一事实源,
-- 快照损坏可随时删除重建,只影响恢复速度)。
CREATE TABLE IF NOT EXISTS dsl_snapshots (
    instance_id TEXT PRIMARY KEY,
    upto_seq    BIGINT      NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
