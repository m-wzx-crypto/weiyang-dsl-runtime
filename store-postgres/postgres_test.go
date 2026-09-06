package storepostgres

import (
	"context"
	"database/sql"
	"os"
	"testing"
	"time"

	dsl "github.com/m-wzx-crypto/weiyang-dsl-runtime/dsl"
)

// TestPostgresStore 端到端验证 Postgres 持久化(Journal + Store + Manager 恢复)。
//
// 本包刻意不绑定任何驱动;集成测试需要宿主环境提供:
//   - TEST_POSTGRES_DSN    连接串(如 postgres://user:pass@localhost/dsl_test)
//   - TEST_POSTGRES_DRIVER 驱动名(如 pgx / postgres),驱动须已注册
//
// 通常由宿主的测试基建(blank import 驱动)提供;两者任一缺失则跳过。
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	driver := os.Getenv("TEST_POSTGRES_DRIVER")
	if dsn == "" || driver == "" {
		t.Skip("TEST_POSTGRES_DSN / TEST_POSTGRES_DRIVER not set; skipping postgres integration test")
	}
	ctx := context.Background()
	db, err := sql.Open(driver, dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if err := Migrate(ctx, db); err != nil {
		t.Fatalf("migrate: %v", err)
	}

	j := NewPostgresJournal(db)
	s := NewPostgresInstanceStore(db)

	def := &dsl.ProcessDef{
		ID: "pg_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*dsl.Node{
			"start": {ID: "start", Type: "start",
				Transitions: []dsl.Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval",
				Transitions: []dsl.Transition{{Event: "approve", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}

	m := dsl.NewManager([]*dsl.ProcessDef{def}, dsl.NewInMemorySideEffectExecutor(),
		dsl.WithManagerJournal(j), dsl.WithManagerStore(s))
	if _, err := m.Start("pg_flow", "pg-inst-1", "t1", nil, dsl.Event{ID: "e1", Name: "submit"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 用全新 Manager(同库)恢复并完成流程。
	m2 := dsl.NewManager([]*dsl.ProcessDef{def}, dsl.NewInMemorySideEffectExecutor(),
		dsl.WithManagerJournal(j), dsl.WithManagerStore(s))
	if res, err := m2.Feed("pg-inst-1", dsl.Event{ID: "e2", Name: "approve"}); err != nil || res.HasErrors() {
		t.Fatalf("recovered feed: %v / %v", err, res.Errors)
	}
	rec, err := m2.GetStatus("pg-inst-1")
	if err != nil || rec.Status != "completed" {
		t.Fatalf("expected completed, got %+v / %v", rec, err)
	}

	// 摘要查询。
	wakeable, err := s.ListWakeable(time.Now(), 10)
	if err != nil || len(wakeable) != 0 {
		t.Fatalf("expected no wakeable, got %v / %v", wakeable, err)
	}
	completed, err := s.ListByStatus("completed", 10)
	if err != nil || len(completed) != 1 {
		t.Fatalf("expected 1 completed, got %v / %v", completed, err)
	}
}
