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

// Append 追加一笔事实:seq 在同一语句内按 (instance_id, MAX(seq)+1) 原子分配。
func (p *PostgresJournal) Append(occ *dsl.Occurrence) error {
	payload, err := json.Marshal(occ)
	if err != nil {
		return fmt.Errorf("marshal occurrence: %w", err)
	}
	_, err = p.db.Exec(`
        INSERT INTO dsl_journal (instance_id, seq, payload)
        VALUES ($1, (SELECT COALESCE(MAX(seq), 0) + 1 FROM dsl_journal WHERE instance_id = $1), $2)`,
		occ.InstanceID, payload)
	if err != nil {
		return fmt.Errorf("append occurrence: %w", err)
	}
	return nil
}

// LoadOccurrences 按序读出实例的全部事实。
func (p *PostgresJournal) LoadOccurrences(instanceID string) ([]dsl.Occurrence, error) {
	rows, err := p.db.Query(
		`SELECT payload FROM dsl_journal WHERE instance_id = $1 ORDER BY seq ASC`, instanceID)
	if err != nil {
		return nil, fmt.Errorf("load occurrences: %w", err)
	}
	defer rows.Close()

	var out []dsl.Occurrence
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("scan occurrence: %w", err)
		}
		var occ dsl.Occurrence
		if err := json.Unmarshal(payload, &occ); err != nil {
			return nil, fmt.Errorf("unmarshal occurrence: %w", err)
		}
		out = append(out, occ)
	}
	return out, rows.Err()
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
