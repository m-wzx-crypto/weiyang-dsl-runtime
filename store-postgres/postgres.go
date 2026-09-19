// Package storepostgres 提供 DSL 引擎持久化 SPI 的 Postgres 参考实现:
// Journal(事实日志)与 InstanceStore(实例摘要),两者覆盖 Manager 所需的
// 全部持久化面。
//
// 设计约定:
//   - 本包只依赖 database/sql,不绑定任何驱动——宿主用 pgx(stdlib 模式)、
//     lib/pq 或任意 Postgres 驱动打开 *sql.DB 后注入即可;
//   - 表结构见 schema.sql(Migrate 幂等执行);
//   - 实例完整状态永远 = fold(dsl_journal),本存储损坏可由日志重建;
//   - 与引擎并发契约一致:同一实例由单写者串行追加,并发写同一实例会因
//     主键冲突失败,由宿主重试。
package storepostgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	dsl "github.com/m-wzx-crypto/weiyang-dsl-runtime/dsl"
)

// Schema 是持久化层的 DDL(与 schema.sql 一致),Migrate 幂等执行。
const Schema = `
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
CREATE INDEX IF NOT EXISTS idx_dsl_instances_status ON dsl_instances (status);
CREATE TABLE IF NOT EXISTS dsl_journal (
    instance_id TEXT        NOT NULL,
    seq         BIGINT      NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (instance_id, seq)
);

-- M1 归属索引:按行为主体(kind + id)检索决策事实(见 schema.sql 注释)。
CREATE INDEX IF NOT EXISTS idx_dsl_journal_actor
    ON dsl_journal ( (payload->'actor'->>'kind'), (payload->'actor'->>'id'), instance_id )
    WHERE (payload->'actor') IS NOT NULL;

CREATE TABLE IF NOT EXISTS dsl_snapshots (
    instance_id TEXT PRIMARY KEY,
    upto_seq    BIGINT      NOT NULL,
    payload     JSONB       NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
`

// Migrate 幂等创建表与索引。
func Migrate(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, Schema)
	return err
}

// PostgresJournal 是 Journal + JournalReader 的 Postgres 实现。
type PostgresJournal struct {
	db *sql.DB
}

// NewPostgresJournal 绑定一个已打开的 *sql.DB。
func NewPostgresJournal(db *sql.DB) *PostgresJournal { return &PostgresJournal{db: db} }

// Append 追加一笔事实:seq 在同一语句内按 (instance_id, MAX(seq)+1) 原子分配,
// 并以 jsonb_set 同步写进载荷——载荷内的 seq 与行序号严格一致(FoldTo /
// occurrencesAfter 等按 Seq 定位的恢复路径依赖它;旧行为是载荷携带零值,
// 从持久化日志做时间旅行/增量重放会静默失效)。
func (p *PostgresJournal) Append(occ *dsl.Occurrence) error {
	payload, err := json.Marshal(occ)
	if err != nil {
		return fmt.Errorf("marshal occurrence: %w", err)
	}
	var seq int64
	err = p.db.QueryRow(`
        INSERT INTO dsl_journal (instance_id, seq, payload)
        SELECT $1, s.seq, jsonb_set($2::jsonb, '{seq}', to_jsonb(s.seq))
        FROM (SELECT COALESCE(MAX(seq), 0) + 1 AS seq FROM dsl_journal WHERE instance_id = $1) AS s
        RETURNING seq`,
		occ.InstanceID, payload).Scan(&seq)
	if err != nil {
		return fmt.Errorf("append occurrence: %w", err)
	}
	// 与 MemoryJournal 的契约一致:分配结果回写给调用方持有的事实。
	occ.Seq = seq
	return nil
}

// LoadOccurrences 按序读出实例的全部事实。行序号(seq 列)是唯一权威:
// 载荷解码后以行序号对齐(历史遗留的零值 seq 载荷由此在读取侧治愈)。
func (p *PostgresJournal) LoadOccurrences(instanceID string) ([]dsl.Occurrence, error) {
	rows, err := p.db.Query(
		`SELECT seq, payload FROM dsl_journal WHERE instance_id = $1 ORDER BY seq ASC`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load occurrences: %w", err)
	}
	defer rows.Close()
	return scanOccurrences(rows)
}

// ListDecisionsByPrincipal 按行为主体检索决策事实:精确匹配事实载荷中
// actor 的 kind 与 id——人类审批与模型推理同一查询口径("谁做了哪些决策")。
// principal.Kind 与 principal.ID 必须非空;instanceID 非空时限定单实例,
// 为空则跨实例(按 instance_id, seq 升序)。命中 idx_dsl_journal_actor
// 部分索引,不做全载荷扫描。
func (p *PostgresJournal) ListDecisionsByPrincipal(ctx context.Context, principal dsl.Principal, instanceID string, limit int) ([]dsl.Occurrence, error) {
	if principal.Kind == "" || principal.ID == "" {
		return nil, fmt.Errorf("list decisions by principal: kind and id are required")
	}
	if limit <= 0 {
		limit = 100
	}
	q := `SELECT seq, payload FROM dsl_journal
	      WHERE (payload->'actor'->>'kind') = $1 AND (payload->'actor'->>'id') = $2`
	args := []interface{}{string(principal.Kind), principal.ID}
	if instanceID != "" {
		q += ` AND instance_id = $3 ORDER BY seq ASC LIMIT $4`
		args = append(args, instanceID, limit)
	} else {
		q += ` ORDER BY instance_id ASC, seq ASC LIMIT $3`
		args = append(args, limit)
	}
	rows, err := p.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("list decisions by principal: %w", err)
	}
	defer rows.Close()
	return scanOccurrences(rows)
}

// scanOccurrences 把 dsl_journal 的 (seq, payload) 行解码为事实序列,
// seq 以行序号为准(LoadOccurrences 与 ListDecisionsByPrincipal 共用)。
func scanOccurrences(rows *sql.Rows) ([]dsl.Occurrence, error) {
	var out []dsl.Occurrence
	for rows.Next() {
		var seq int64
		var payload []byte
		if err := rows.Scan(&seq, &payload); err != nil {
			return nil, fmt.Errorf("scan occurrence: %w", err)
		}
		var occ dsl.Occurrence
		if err := json.Unmarshal(payload, &occ); err != nil {
			return nil, fmt.Errorf("unmarshal occurrence: %w", err)
		}
		occ.Seq = seq
		out = append(out, occ)
	}
	return out, rows.Err()
}

// MaxSeq 返回实例日志的最大序号(空日志返回 0)。dsl.JournalProgress 实现,
// Manager 的快照机制据此判定"距上次快照又累积了多少事实"。
func (p *PostgresJournal) MaxSeq(instanceID string) (int64, error) {
	var maxSeq int64
	err := p.db.QueryRow(
		`SELECT COALESCE(MAX(seq), 0) FROM dsl_journal WHERE instance_id = $1`, instanceID).
		Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("max seq %q: %w", instanceID, err)
	}
	return maxSeq, nil
}

// PostgresInstanceStore 是 InstanceStore 的 Postgres 实现。
type PostgresInstanceStore struct {
	db *sql.DB
}

// NewPostgresInstanceStore 绑定一个已打开的 *sql.DB。
func NewPostgresInstanceStore(db *sql.DB) *PostgresInstanceStore {
	return &PostgresInstanceStore{db: db}
}

// PutInstance 创建或更新实例摘要。
func (p *PostgresInstanceStore) PutInstance(rec dsl.InstanceRecord) error {
	var wakeUpAt interface{}
	if rec.WakeUpAt != nil {
		wakeUpAt = *rec.WakeUpAt
	}
	_, err := p.db.Exec(`
        INSERT INTO dsl_instances
            (instance_id, definition_id, tenant_id, status, current_node, wake_up_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (instance_id) DO UPDATE SET
            definition_id = EXCLUDED.definition_id,
            tenant_id     = EXCLUDED.tenant_id,
            status        = EXCLUDED.status,
            current_node  = EXCLUDED.current_node,
            wake_up_at    = EXCLUDED.wake_up_at,
            updated_at    = now()`,
		rec.InstanceID, rec.DefinitionID, rec.TenantID, rec.Status, rec.CurrentNode, wakeUpAt, rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("put instance %q: %w", rec.InstanceID, err)
	}
	return nil
}

// CreateInstance 原子创建实例摘要:已存在时返回 dsl.ErrInstanceExists,不覆盖。
// Manager.Start 依赖本方法的原子性消除"先查后写"的 TOCTOU 竞态。
func (p *PostgresInstanceStore) CreateInstance(rec dsl.InstanceRecord) error {
	var wakeUpAt interface{}
	if rec.WakeUpAt != nil {
		wakeUpAt = *rec.WakeUpAt
	}
	tag, err := p.db.Exec(`
        INSERT INTO dsl_instances
            (instance_id, definition_id, tenant_id, status, current_node, wake_up_at, created_at, updated_at)
        VALUES ($1, $2, $3, $4, $5, $6, $7, now())
        ON CONFLICT (instance_id) DO NOTHING`,
		rec.InstanceID, rec.DefinitionID, rec.TenantID, rec.Status, rec.CurrentNode, wakeUpAt, rec.CreatedAt)
	if err != nil {
		return fmt.Errorf("create instance %q: %w", rec.InstanceID, err)
	}
	affected, err := tag.RowsAffected()
	if err != nil {
		return fmt.Errorf("create instance %q: rows affected: %w", rec.InstanceID, err)
	}
	if affected == 0 {
		return dsl.ErrInstanceExists
	}
	return nil
}

// PutSnapshot 保存实例快照(覆盖旧快照)。dsl.SnapshotStore 实现。
func (p *PostgresInstanceStore) PutSnapshot(snap dsl.InstanceSnapshot) error {
	_, err := p.db.Exec(`
        INSERT INTO dsl_snapshots (instance_id, upto_seq, payload, created_at)
        VALUES ($1, $2, $3, now())
        ON CONFLICT (instance_id) DO UPDATE SET
            upto_seq   = EXCLUDED.upto_seq,
            payload    = EXCLUDED.payload,
            created_at = now()`,
		snap.InstanceID, snap.UptoSeq, snap.Data)
	if err != nil {
		return fmt.Errorf("put snapshot %q: %w", snap.InstanceID, err)
	}
	return nil
}

// LatestSnapshot 返回实例最新快照;不存在时返回 dsl.ErrSnapshotNotFound。
// dsl.SnapshotStore 实现。
func (p *PostgresInstanceStore) LatestSnapshot(instanceID string) (*dsl.InstanceSnapshot, error) {
	var snap dsl.InstanceSnapshot
	var payload []byte
	err := p.db.QueryRow(`
        SELECT instance_id, upto_seq, payload, created_at
        FROM dsl_snapshots WHERE instance_id = $1`, instanceID).
		Scan(&snap.InstanceID, &snap.UptoSeq, &payload, &snap.CreatedAt)
	if errorsIsNotFound(err) {
		return nil, dsl.ErrSnapshotNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("latest snapshot %q: %w", instanceID, err)
	}
	snap.Data = payload
	return &snap, nil
}

// GetInstance 返回实例摘要;不存在时返回 dsl.ErrInstanceNotFound。
func (p *PostgresInstanceStore) GetInstance(instanceID string) (*dsl.InstanceRecord, error) {
	var rec dsl.InstanceRecord
	var wakeUpAt sql.NullTime
	err := p.db.QueryRow(`
        SELECT instance_id, definition_id, tenant_id, status, current_node, wake_up_at, created_at, updated_at
        FROM dsl_instances WHERE instance_id = $1`, instanceID).
		Scan(&rec.InstanceID, &rec.DefinitionID, &rec.TenantID, &rec.Status,
			&rec.CurrentNode, &wakeUpAt, &rec.CreatedAt, &rec.UpdatedAt)
	if errorsIsNotFound(err) {
		return nil, dsl.ErrInstanceNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get instance %q: %w", instanceID, err)
	}
	if wakeUpAt.Valid {
		w := wakeUpAt.Time
		rec.WakeUpAt = &w
	}
	return &rec, nil
}

// ListWakeable 返回到期且仍在等待的实例(WakeUpAt 升序)。
func (p *PostgresInstanceStore) ListWakeable(now time.Time, limit int) ([]dsl.InstanceRecord, error) {
	rows, err := p.db.Query(`
        SELECT instance_id, definition_id, tenant_id, status, current_node, wake_up_at, created_at, updated_at
        FROM dsl_instances
        WHERE status = 'waiting' AND wake_up_at IS NOT NULL AND wake_up_at <= $1
        ORDER BY wake_up_at ASC
        LIMIT $2`, now, limit)
	if err != nil {
		return nil, fmt.Errorf("list wakeable: %w", err)
	}
	return scanInstances(rows)
}

// ListByStatus 按状态检索实例(创建时间升序)。
func (p *PostgresInstanceStore) ListByStatus(status string, limit int) ([]dsl.InstanceRecord, error) {
	rows, err := p.db.Query(`
        SELECT instance_id, definition_id, tenant_id, status, current_node, wake_up_at, created_at, updated_at
        FROM dsl_instances
        WHERE status = $1
        ORDER BY created_at ASC
        LIMIT $2`, status, limit)
	if err != nil {
		return nil, fmt.Errorf("list by status: %w", err)
	}
	return scanInstances(rows)
}

func scanInstances(rows *sql.Rows) ([]dsl.InstanceRecord, error) {
	defer rows.Close()
	var out []dsl.InstanceRecord
	for rows.Next() {
		var rec dsl.InstanceRecord
		var wakeUpAt sql.NullTime
		if err := rows.Scan(&rec.InstanceID, &rec.DefinitionID, &rec.TenantID, &rec.Status,
			&rec.CurrentNode, &wakeUpAt, &rec.CreatedAt, &rec.UpdatedAt); err != nil {
			return nil, fmt.Errorf("scan instance: %w", err)
		}
		if wakeUpAt.Valid {
			w := wakeUpAt.Time
			rec.WakeUpAt = &w
		}
		out = append(out, rec)
	}
	return out, rows.Err()
}

// errorsIsNotFound 用字符串判别"无行"错误,避免绑定具体驱动的错误类型。
func errorsIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	return err.Error() == "sql: no rows in result set"
}
