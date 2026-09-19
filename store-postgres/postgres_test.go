package storepostgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"reflect"
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
	// 实例 ID 带运行 nonce:CreateInstance 对已存在 ID 报错,固定 ID 会让
	// 测试在复用的库上第二次运行即失败(CI 每次新库,本地/重跑未必)。
	inst := fmt.Sprintf("pg-inst-%d", time.Now().UnixNano())
	if _, err := m.Start("pg_flow", inst, "t1", nil, dsl.Event{ID: "e1", Name: "submit"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 用全新 Manager(同库)恢复并完成流程。
	m2 := dsl.NewManager([]*dsl.ProcessDef{def}, dsl.NewInMemorySideEffectExecutor(),
		dsl.WithManagerJournal(j), dsl.WithManagerStore(s))
	if res, err := m2.Feed(inst, dsl.Event{ID: "e2", Name: "approve"}); err != nil || res.HasErrors() {
		t.Fatalf("recovered feed: %v / %v", err, res.Errors)
	}
	rec, err := m2.GetStatus(inst)
	if err != nil || rec.Status != "completed" {
		t.Fatalf("expected completed, got %+v / %v", rec, err)
	}

	// 摘要查询。
	wakeable, err := s.ListWakeable(time.Now(), 10)
	if err != nil || len(wakeable) != 0 {
		t.Fatalf("expected no wakeable, got %v / %v", wakeable, err)
	}
	completed, err := s.ListByStatus("completed", 10)
	if err != nil {
		t.Fatalf("list by status: %v", err)
	}
	found := false
	for _, rec := range completed {
		if rec.InstanceID == inst {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected instance %q among completed records, got %+v", inst, completed)
	}
}

// principalRoundTripDef:submit(human)→ ai 分诊(模型决策)→ 审批(强制归属)→ end。
func principalRoundTripDef() *dsl.ProcessDef {
	return &dsl.ProcessDef{
		ID: "pg_principal", Version: "2.0", StartNode: "start",
		Nodes: map[string]*dsl.Node{
			"start": {ID: "start", Type: "start",
				Transitions: []dsl.Transition{{Event: "submit", Next: "triage"}}},
			"triage": {ID: "triage", Type: "ai",
				Ai: &dsl.AIConfig{
					Prompt:      "classify {{ticket}}",
					OutputTypes: map[string]*dsl.Type{"confidence": dsl.NumberType()},
					Choose:      []string{"approve_it", "reject_it"},
				},
				Transitions: []dsl.Transition{
					{Case: "approve_it", Next: "approve"},
					{Case: "reject_it", Next: "end"},
				}},
			"approve": {ID: "approve", Type: "approval", RequirePrincipal: true,
				Transitions: []dsl.Transition{{Event: "approve", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

// TestPostgresStore_PrincipalRoundTrip 验证 M1 归属的持久化闭环(PLAN W2):
//  1. 归属字段(含推理三元组 model/model_version/prompt_version)随 occurrence
//     JSON 全量落库,载入后逐字段一致;
//  2. 从持久化日志折叠,principal 逐字段复现(含时间旅行回模型决策点);
//  3. 按 principal 查询(idx_dsl_journal_actor 索引):人类审批与模型推理
//     同一查询口径,可跨实例、可限定实例。
//
// 运行条件与 TestPostgresStore 相同(TEST_POSTGRES_DSN / TEST_POSTGRES_DRIVER)。
func TestPostgresStore_PrincipalRoundTrip(t *testing.T) {
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
	def := principalRoundTripDef()
	inst := fmt.Sprintf("pg-principal-%d", time.Now().UnixNano())
	// 模型/主体标识带运行 nonce:跨实例查询(不限定 instanceID)在复用的
	// 测试库上也不受历史行干扰,断言保持精确。
	nonce := fmt.Sprintf("gpt-4o-test-%d", time.Now().UnixNano())
	modelID, modelVer := nonce, "2024-08-06-"+nonce

	m := dsl.NewManager([]*dsl.ProcessDef{def}, dsl.NewInMemorySideEffectExecutor(),
		dsl.WithManagerJournal(j), dsl.WithManagerStore(s))
	if _, err := m.Start("pg_principal", inst, "t1", map[string]interface{}{"ticket": "T-1"},
		dsl.Event{ID: "s1", Name: "submit",
			Principal: &dsl.Principal{Kind: dsl.PrincipalHuman, ID: "u-2001", DisplayName: "李提交"}}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 模型决策:回调载荷携带归属三元组,引擎固化为模型 principal 入账。
	if res, err := m.Feed(inst, dsl.Event{ID: "a1", Name: dsl.DefaultAIEventType,
		Payload: map[string]interface{}{
			"choice": "approve_it", "output": map[string]interface{}{"confidence": 0.9},
			"model": modelID, "model_version": modelVer, "prompt_version": "triage-v3",
		}}); err != nil || res.HasErrors() {
		t.Fatalf("ai decision: %v / %v", err, res.Errors)
	}

	// 无归属审批被强制点回拒;补上归属重投通过。
	res, err := m.Feed(inst, dsl.Event{ID: "a2", Name: "approve"})
	if err != nil || !res.HasErrors() {
		t.Fatalf("principal-less approval must be rejected, got %v / %v", err, res)
	}
	if !hasPrincipalRequired(res) {
		t.Fatalf("rejection must be attributable to dsl.ErrPrincipalRequired, got %v", res.Errors)
	}
	if res, err := m.Feed(inst, dsl.Event{ID: "a2", Name: "approve",
		Principal: &dsl.Principal{Kind: dsl.PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}}); err != nil || res.HasErrors() {
		t.Fatalf("attributed approval: %v / %v", err, res.Errors)
	}

	// ---- 1) round-trip:归属字段随 occurrence JSON 全量持久化,逐字段还原。----
	occs, err := j.LoadOccurrences(inst)
	if err != nil {
		t.Fatalf("load occurrences: %v", err)
	}
	// 行序号与载荷 seq 严格一致且从 1 连续递增:FoldTo / 增量重放(快照恢复)
	// 按 Seq 定位,载荷携带零值 seq 会让时间旅行静默失效(回归守卫)。
	for i, occ := range occs {
		if occ.Seq != int64(i+1) {
			t.Fatalf("occurrence seq must be 1..N in order: occ[%d].Seq = %d", i, occ.Seq)
		}
	}
	wantModel := dsl.Principal{
		Kind:          dsl.PrincipalModel,
		ID:            modelID,
		Model:         modelID,
		ModelVersion:  modelVer,
		PromptVersion: "triage-v3",
	}
	var modelSeq int64
	modelFound, humanFound, submitterFound := false, false, false
	for _, occ := range occs {
		if occ.Kind != dsl.OccEventConsumed || occ.Actor == nil || occ.Event == nil {
			continue
		}
		switch occ.Event.ID {
		case "s1":
			submitterFound = occ.Actor.ID == "u-2001" && occ.Actor.DisplayName == "李提交"
		case "a1":
			if !reflect.DeepEqual(*occ.Actor, wantModel) {
				t.Fatalf("model attribution lost in round-trip: %+v", occ.Actor)
			}
			modelFound, modelSeq = true, occ.Seq
		case "a2":
			if occ.Actor.Kind != dsl.PrincipalHuman || occ.Actor.ID != "u-1001" ||
				occ.Actor.DisplayName != "王审批" {
				t.Fatalf("human attribution lost in round-trip: %+v", occ.Actor)
			}
			humanFound = true
		}
	}
	if !submitterFound || !modelFound || !humanFound {
		t.Fatalf("round-trip must preserve all attribution (submitter=%v model=%v approver=%v)",
			submitterFound, modelFound, humanFound)
	}

	// ---- 2) fold:从持久化日志折叠,principal 逐字段复现。----
	folded, err := dsl.Fold(def, occs)
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if folded.Status != dsl.StatusCompleted {
		t.Fatalf("expected folded status completed, got %s", folded.Status)
	}
	if folded.CurrentEvent == nil || folded.CurrentEvent.Principal == nil ||
		folded.CurrentEvent.Principal.ID != "u-1001" {
		t.Fatalf("fold must reproduce the latest decision principal, got %+v", folded.CurrentEvent)
	}
	atAI, err := dsl.FoldTo(def, occs, modelSeq)
	if err != nil {
		t.Fatalf("foldTo(%d): %v", modelSeq, err)
	}
	if atAI.CurrentEvent == nil || atAI.CurrentEvent.Principal == nil ||
		!reflect.DeepEqual(*atAI.CurrentEvent.Principal, wantModel) {
		t.Fatalf("time travel to the model decision must reproduce its principal, got %+v",
			atAI.CurrentEvent)
	}

	// ---- 3) 按 principal 查询(走 idx_dsl_journal_actor)。----
	modelDecisions, err := j.ListDecisionsByPrincipal(ctx, wantModel, "", 10)
	if err != nil {
		t.Fatalf("list by model principal: %v", err)
	}
	if len(modelDecisions) != 1 || modelDecisions[0].Actor == nil ||
		!reflect.DeepEqual(*modelDecisions[0].Actor, wantModel) {
		t.Fatalf("expected exactly 1 gpt-4o decision across instances, got %+v", modelDecisions)
	}
	humanDecisions, err := j.ListDecisionsByPrincipal(ctx,
		dsl.Principal{Kind: dsl.PrincipalHuman, ID: "u-1001"}, inst, 10)
	if err != nil {
		t.Fatalf("list by human principal: %v", err)
	}
	if len(humanDecisions) != 1 || humanDecisions[0].Actor.ID != "u-1001" {
		t.Fatalf("expected exactly 1 u-1001 decision in instance, got %+v", humanDecisions)
	}
	none, err := j.ListDecisionsByPrincipal(ctx,
		dsl.Principal{Kind: dsl.PrincipalHuman, ID: "nobody"}, "", 10)
	if err != nil || len(none) != 0 {
		t.Fatalf("unknown principal must match nothing, got %v / %v", none, err)
	}

	// 索引真实存在(而非被静默跳过):Migrate 后应能在 pg_indexes 里查到。
	var idxName string
	err = db.QueryRowContext(ctx,
		`SELECT indexname FROM pg_indexes WHERE tablename = 'dsl_journal' AND indexname = 'idx_dsl_journal_actor'`).
		Scan(&idxName)
	if err != nil || idxName != "idx_dsl_journal_actor" {
		t.Fatalf("idx_dsl_journal_actor must exist after Migrate, got %q / %v", idxName, err)
	}
}

// hasPrincipalRequired 判别结果是否携带强制点回拒(哨兵可判别)。
func hasPrincipalRequired(res *dsl.ExecutionResult) bool {
	for _, err := range res.Errors {
		if errors.Is(err, dsl.ErrPrincipalRequired) {
			return true
		}
	}
	return false
}
