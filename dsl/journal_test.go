package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

// journal_test.go — 确定性内核的安全网。
//
// 核心性质(fold exactness):任意流程执行后,从空日志折叠出的上下文必须与
// 活上下文逐字段一致。任何绕过记账的状态变更都会在这里现形。
// 时间字段以 2 秒容差比较(活状态取自 time.Now(),事实时间与其相差微秒)。

const foldTimeTolerance = 2 * time.Second

func shadowOf(t *testing.T, ctx *ExecutionContext) executionContextJSON {
	t.Helper()
	data, err := json.Marshal(ctx)
	if err != nil {
		t.Fatalf("marshal ctx: %v", err)
	}
	var j executionContextJSON
	if err := json.Unmarshal(data, &j); err != nil {
		t.Fatalf("unmarshal ctx: %v", err)
	}
	return j
}

func withinTolerance(a, b time.Time) bool {
	if a.IsZero() || b.IsZero() {
		return a.IsZero() == b.IsZero()
	}
	d := a.Sub(b)
	if d < 0 {
		d = -d
	}
	return d < foldTimeTolerance
}

// requireFoldEq 断言:折叠(journal 全量) == 活上下文。
func requireFoldEq(t *testing.T, def *ProcessDef, r *Runtime) {
	t.Helper()
	j, ok := r.Journal.(*MemoryJournal)
	if !ok {
		t.Fatalf("expected *MemoryJournal, got %T", r.Journal)
	}
	folded, err := Fold(def, j.Occurrences())
	if err != nil {
		t.Fatalf("fold: %v", err)
	}
	if err := compareCtx(shadowOf(t, r.Ctx), shadowOf(t, folded)); err != nil {
		t.Fatalf("fold is not exact: %v", err)
	}
}

func compareCtx(a, b executionContextJSON) error {
	eqStr := func(field, x, y string) error {
		if x != y {
			return fmt.Errorf("%s: %q != %q", field, x, y)
		}
		return nil
	}
	checks := []error{
		eqStr("processId", a.ProcessID, b.ProcessID),
		eqStr("definitionId", a.DefinitionID, b.DefinitionID),
		eqStr("instanceId", a.InstanceID, b.InstanceID),
		eqStr("executionId", a.ExecutionID, b.ExecutionID),
		eqStr("currentNode", a.CurrentNode, b.CurrentNode),
		eqStr("status", a.Status, b.Status),
	}
	for _, err := range checks {
		if err != nil {
			return err
		}
	}
	if a.Attempt != b.Attempt {
		return fmt.Errorf("attempt: %d != %d", a.Attempt, b.Attempt)
	}
	if !reflect.DeepEqual(a.Variables, b.Variables) {
		return fmt.Errorf("variables: %v != %v", a.Variables, b.Variables)
	}
	if !reflect.DeepEqual(a.Metadata, b.Metadata) {
		return fmt.Errorf("metadata: %v != %v", a.Metadata, b.Metadata)
	}
	if !reflect.DeepEqual(a.VisitCounts, b.VisitCounts) {
		return fmt.Errorf("visitCounts: %v != %v", a.VisitCounts, b.VisitCounts)
	}
	if !withinTolerance(a.StartedAt, b.StartedAt) {
		return fmt.Errorf("startedAt drift")
	}
	if !withinTolerance(a.UpdatedAt, b.UpdatedAt) {
		return fmt.Errorf("updatedAt drift")
	}
	if !withinTolerance(a.CompletedAt, b.CompletedAt) {
		return fmt.Errorf("completedAt drift")
	}

	// 事件
	if (a.CurrentEvent == nil) != (b.CurrentEvent == nil) {
		return fmt.Errorf("currentEvent nil-ness: %v != %v", a.CurrentEvent, b.CurrentEvent)
	}
	if a.CurrentEvent != nil {
		if a.CurrentEvent.ID != b.CurrentEvent.ID || a.CurrentEvent.Name != b.CurrentEvent.Name {
			return fmt.Errorf("currentEvent: %+v != %+v", a.CurrentEvent, b.CurrentEvent)
		}
	}
	// 幂等表:键一致,时间戳微秒级差异忽略
	if len(a.ProcessedEvents) != len(b.ProcessedEvents) {
		return fmt.Errorf("processedEvents len: %d != %d", len(a.ProcessedEvents), len(b.ProcessedEvents))
	}
	for k, av := range a.ProcessedEvents {
		bv, ok := b.ProcessedEvents[k]
		if !ok {
			return fmt.Errorf("processedEvents: key %q missing in fold", k)
		}
		if av != bv && !withinTolerance(time.Unix(0, av), time.Unix(0, bv)) {
			return fmt.Errorf("processedEvents[%q]: %d != %d", k, av, bv)
		}
	}

	// 等待槽
	if len(a.Waitings) != len(b.Waitings) {
		return fmt.Errorf("waitings: %v != %v", keysOfW(a.Waitings), keysOfW(b.Waitings))
	}
	for k, aw := range a.Waitings {
		bw, ok := b.Waitings[k]
		if !ok {
			return fmt.Errorf("waitings: slot %q missing in fold", k)
		}
		if aw.Kind != bw.Kind || aw.NodeID != bw.NodeID || aw.Visit != bw.Visit || aw.Next != bw.Next {
			return fmt.Errorf("waitings[%q]: %+v != %+v", k, *aw, *bw)
		}
		if !withinTolerance(aw.Until, bw.Until) {
			return fmt.Errorf("waitings[%q].until drift", k)
		}
	}

	// 并行作用域
	if len(a.Scopes) != len(b.Scopes) {
		return fmt.Errorf("scopes: %d != %d", len(a.Scopes), len(b.Scopes))
	}
	for i, as := range a.Scopes {
		bs := b.Scopes[i]
		if as.ID != bs.ID || as.ForkNode != bs.ForkNode || as.JoinNode != bs.JoinNode ||
			as.Mode != bs.Mode || as.OnFail != bs.OnFail || as.Required != bs.Required || as.Status != bs.Status {
			return fmt.Errorf("scopes[%d]: %+v != %+v", i, *as, *bs)
		}
		if len(as.Branches) != len(bs.Branches) {
			return fmt.Errorf("scopes[%d].branches: %d != %d", i, len(as.Branches), len(bs.Branches))
		}
		for bid, ab := range as.Branches {
			bb, ok := bs.Branches[bid]
			if !ok {
				return fmt.Errorf("scopes[%d]: branch %q missing in fold", i, bid)
			}
			if ab.ID != bb.ID || ab.StartNode != bb.StartNode || ab.CurrentNode != bb.CurrentNode ||
				ab.Status != bb.Status || ab.Done != bb.Done || ab.ArrivedJoin != bb.ArrivedJoin {
				return fmt.Errorf("scopes[%d].branches[%q]: %+v != %+v", i, bid, *ab, *bb)
			}
			if !withinTolerance(ab.FinishedAt, bb.FinishedAt) {
				return fmt.Errorf("scopes[%d].branches[%q].finishedAt drift", i, bid)
			}
		}
		if !withinTolerance(as.StartedAt, bs.StartedAt) {
			return fmt.Errorf("scopes[%d].startedAt drift", i)
		}
	}

	// 副作用结果
	if len(a.SideEffectResults) != len(b.SideEffectResults) {
		return fmt.Errorf("sideEffectResults: %d != %d", len(a.SideEffectResults), len(b.SideEffectResults))
	}
	for i := range a.SideEffectResults {
		x, y := a.SideEffectResults[i], b.SideEffectResults[i]
		if x.CommandID != y.CommandID || x.Status != y.Status || x.ErrText != y.ErrText ||
			!reflect.DeepEqual(x.Outcome, y.Outcome) {
			return fmt.Errorf("sideEffectResults[%d]: %+v != %+v", i, x, y)
		}
	}

	// undo 栈
	if len(a.UndoStack) != len(b.UndoStack) {
		return fmt.Errorf("undoStack: %d != %d", len(a.UndoStack), len(b.UndoStack))
	}
	for i := range a.UndoStack {
		x, y := a.UndoStack[i], b.UndoStack[i]
		if x.NodeID != y.NodeID || x.CommandID != y.CommandID || x.Key != y.Key ||
			x.Done != y.Done || x.Effect.Type != y.Effect.Type || x.Effect.Target != y.Effect.Target {
			return fmt.Errorf("undoStack[%d]: %+v != %+v", i, x, y)
		}
	}
	return nil
}

func keysOfW(m map[string]*WaitingState) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// ---- 折叠精确性:五类流程逐操作断言 ----

func TestFold_Exact_LinearWithCondition(t *testing.T) {
	def := mustParseV2Contract(t)
	exec := NewInMemorySideEffectExecutor()
	r := NewRuntime(def, exec, WithJournal(NewMemoryJournal()))

	r.Start("i", "e", map[string]interface{}{
		"amount": float64(5000), "order": map[string]interface{}{"vip": true, "level": "high"},
	}, Event{ID: "s1", Name: "submit"})
	requireFoldEq(t, def, r)

	r.Feed(Event{ID: "s2", Name: "approve"})
	requireFoldEq(t, def, r)
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
}

func TestFold_Exact_Parallel(t *testing.T) {
	def := parallelDef()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))

	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	requireFoldEq(t, def, r)
	r.Feed(Event{ID: "b1", Name: "b1ok"})
	requireFoldEq(t, def, r)
	r.Feed(Event{ID: "b2", Name: "b2ok"})
	requireFoldEq(t, def, r)
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
}

func TestFold_Exact_Temporal(t *testing.T) {
	def := temporalDef()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))

	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	requireFoldEq(t, def, r)

	r.WakeDue(time.Now().Add(3 * time.Hour)) // timer 触发 → 停靠 approval(deadline)
	requireFoldEq(t, def, r)

	until, _ := r.NextWakeup()
	r.WakeDue(until.Add(time.Minute)) // deadline 触发 → escalate
	requireFoldEq(t, def, r)
	if r.Ctx.CurrentNode != "escalate" {
		t.Fatalf("expected escalated, got %q", r.Ctx.CurrentNode)
	}
}

func TestFold_Exact_Compensation(t *testing.T) {
	exec := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	def := sagaDef()
	r := NewRuntime(def, exec, WithJournal(NewMemoryJournal()), WithAutoCompensate())

	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	requireFoldEq(t, def, r)
	r.Feed(Event{ID: "s2", Name: "reject"}) // broken 节点失败 → 自动补偿
	requireFoldEq(t, def, r)
	if r.Status() != StatusFailed {
		t.Fatalf("expected failed, got %s", r.Status())
	}
	if len(r.Ctx.UndoStack) != 2 || !r.Ctx.UndoStack[0].Done || !r.Ctx.UndoStack[1].Done {
		t.Fatalf("expected 2 compensated undo entries, got %+v", r.Ctx.UndoStack)
	}
}

func TestFold_Exact_ScopeTimeout(t *testing.T) {
	def := scopeTimeoutDef()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))

	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})
	requireFoldEq(t, def, r)
	r.WakeDue(time.Now().Add(2 * time.Hour)) // scope 超时主动触发
	requireFoldEq(t, def, r)
	if r.Status() != StatusTimedOut {
		t.Fatalf("expected timed_out, got %s", r.Status())
	}
}

// ---- 恢复:日志即真相 ----

func TestRecovery_ContinueFromLog(t *testing.T) {
	exec1 := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	def := sagaDef()
	j1 := NewMemoryJournal()
	r := NewRuntime(def, exec1, WithJournal(j1), WithAutoCompensate())
	r.Start("i", "e1", nil, Event{ID: "s1", Name: "submit"}) // book+charge 完成,停靠 approve
	if r.Status() != StatusWaiting {
		t.Fatalf("setup: expected waiting, got %s", r.Status())
	}

	// "崩溃"后从日志恢复:幂等表/undo 栈/副作用结果全部随日志回来。
	exec2 := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	j2 := NewMemoryJournal()
	r2, err := Resume(def, j1.Occurrences(), exec2, WithJournal(j2), WithAutoCompensate())
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if r2.Status() != StatusWaiting || r2.Ctx.CurrentNode != "approve" {
		t.Fatalf("recovered state: %s @ %q", r2.Status(), r2.Ctx.CurrentNode)
	}
	if r2.Ctx.IsProcessedEvent("s1") != true {
		t.Fatal("idempotency table must survive recovery")
	}

	// 恢复后重复事件仍被拒绝。
	if res := r2.Feed(Event{ID: "s1", Name: "submit"}); !res.HasErrors() {
		t.Fatal("duplicate event must be rejected after recovery")
	}

	// 继续推进至失败,undo 栈(已随日志恢复)驱动自动补偿。
	if res := r2.Feed(Event{ID: "s2", Name: "reject"}); !res.HasErrors() {
		t.Fatal("expected broken node failure")
	}
	if r2.Status() != StatusFailed {
		t.Fatalf("expected failed, got %s", r2.Status())
	}
	refundKey, cancelKey := r2.Ctx.UndoStack[1].Key, r2.Ctx.UndoStack[0].Key
	var refunds, cancels int
	for _, o := range exec2.Outcomes() {
		if o.CommandID == refundKey {
			refunds++
		}
		if o.CommandID == cancelKey {
			cancels++
		}
	}
	if refunds != 1 || cancels != 1 {
		t.Fatalf("expected post-recovery compensations, refunds=%d cancels=%d", refunds, cancels)
	}

	// 新旧日志合并折叠 == 恢复后的活上下文(续写无缝)。
	merged := append(append([]Occurrence(nil), j1.Occurrences()...), j2.Occurrences()...)
	folded, err := Fold(def, merged)
	if err != nil {
		t.Fatalf("fold merged: %v", err)
	}
	if err := compareCtx(shadowOf(t, r2.Ctx), shadowOf(t, folded)); err != nil {
		t.Fatalf("merged fold not exact: %v", err)
	}
}

func TestRecovery_WakeIdempotencySurvivesRestart(t *testing.T) {
	def := temporalDef()
	j1 := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(j1))
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})

	// 恢复后唤醒一次 timer。
	r2, err := Resume(def, j1.Occurrences(), NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	now := time.Now()
	if res := r2.WakeDue(now.Add(3 * time.Hour)); res.HasErrors() {
		t.Fatalf("wake failed: %v", res.Errors)
	}
	if r2.Ctx.CurrentNode != "approve" {
		t.Fatalf("expected parked at approval, got %q", r2.Ctx.CurrentNode)
	}
	// 同一时刻重复唤醒:幂等(唤醒 ID 已随日志进入幂等表)。
	res := r2.WakeDue(now.Add(3 * time.Hour))
	if res.HasErrors() || r2.Ctx.CurrentNode != "approve" {
		t.Fatalf("repeated wake must be idempotent, got %v / %q", res.Errors, r2.Ctx.CurrentNode)
	}
}

// ---- 时间旅行 ----

func TestTimeTravel_FoldTo(t *testing.T) {
	def := parallelDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(j))
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	r.Feed(Event{ID: "b1", Name: "b1ok"})

	occs := j.Occurrences()

	// 第 1 条事实(started)时刻:实例尚未真正启动。
	early, err := FoldTo(def, occs, 1)
	if err != nil {
		t.Fatalf("foldTo(1): %v", err)
	}
	if early.Status != StatusPending || early.InstanceID != "i" {
		t.Fatalf("expected pending with ids bound, got %s / %q", early.Status, early.InstanceID)
	}

	// b0 分支(StartNode=b1)停靠事实时刻:该分支已等待、scope 活跃;实例状态仍是
	// running(settleScope 的 waiting 状态在分支事实之后落账)。注意分支在 map 中
	// 的推进顺序是随机的,必须按分支 ID 定位事实,不能用"第一条分支事实"。
	b0Park := -1
	for _, o := range occs {
		if o.Kind == OccBranchUpdated && o.Branch != nil && o.Branch.ID == "parallel.b0" {
			b0Park = int(o.Seq)
			break
		}
	}
	if b0Park < 0 {
		t.Fatal("no branch fact found")
	}
	mid, err := FoldTo(def, occs, int64(b0Park))
	if err != nil {
		t.Fatalf("foldTo(%d): %v", b0Park, err)
	}
	if mid.Status != StatusRunning || len(mid.Scopes) != 1 {
		t.Fatalf("expected running with 1 scope, got %s / %d", mid.Status, len(mid.Scopes))
	}
	if b := mid.Scopes[0].Branches["parallel.b0"]; b == nil || b.Status != StatusWaiting {
		t.Fatalf("expected b0 waiting at checkpoint, got %+v", b)
	}

	// 终点:流程尚未走完(b2 未喂),实例等待。
	tail, err := Fold(def, occs)
	if err != nil {
		t.Fatalf("fold tail: %v", err)
	}
	if tail.Status != StatusWaiting {
		t.Fatalf("expected waiting at tail, got %s", tail.Status)
	}
}

// ---- Outbox 视图 ----

func TestPendingCommands_IssuedWithoutResult(t *testing.T) {
	occs := []Occurrence{
		{Kind: OccCommandIssued, Command: &SideEffectCommand{ID: "a"}},
		{Kind: OccCommandResult, CommandID: "a", Result: "completed"},
		{Kind: OccCommandIssued, Command: &SideEffectCommand{ID: "b"}},
		{Kind: OccCommandIssued, Command: &SideEffectCommand{ID: "c"}},
		{Kind: OccCommandResult, CommandID: "c", Result: "failed"},
	}
	pending := PendingCommands(occs)
	if len(pending) != 1 || pending[0].ID != "b" {
		t.Fatalf("expected only command b pending, got %v", pending)
	}
}

func TestPendingCommands_RealCrashBetweenIssueAndResult(t *testing.T) {
	// 用一个"派发后崩溃"的执行器模拟进程在 issued 与 result 之间死亡:
	// 首个命令执行时 panic 无法模拟中途落账,这里以失败执行器保证 result 存在,
	// 而以手工构造验证 outbox 语义(见上);本测试验证真实日志无未解决命令。
	def := sagaDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor("book_hotel", "charge_card"), WithJournal(j))
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	if pending := PendingCommands(j.Occurrences()); len(pending) != 0 {
		t.Fatalf("expected no pending commands in healthy log, got %v", pending)
	}
}

// ---- 审计流 ----

func TestAudit_OccurrenceKindsInOrder(t *testing.T) {
	def := sagaDef() // 含副作用与补偿声明,审计流覆盖命令事实
	j := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card"), WithJournal(j), WithAutoCompensate())
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	r.Feed(Event{ID: "s2", Name: "reject"})

	var kinds []string
	for _, o := range j.Occurrences() {
		kinds = append(kinds, o.Kind.String())
	}
	joined := strings.Join(kinds, ",")
	for _, want := range []string{"started", "event_consumed", "transition", "command_issued", "command_result", "compensation_pushed", "compensation_fired"} {
		if !strings.Contains(joined, want) {
			t.Fatalf("audit log missing %q: %s", want, joined)
		}
	}
	// 事实单调递增、可序列化留档。
	occs := j.Occurrences()
	for i := 1; i < len(occs); i++ {
		if occs[i].Seq <= occs[i-1].Seq {
			t.Fatalf("seq not monotonic at %d", i)
		}
	}
	if _, err := marshalOccs(occs); err != nil {
		t.Fatalf("marshal occurrences: %v", err)
	}
}

// ---- Journal 实现契约 ----

type failingJournal struct{ MemoryJournal }

func (f *failingJournal) Append(occ *Occurrence) error { return errors.New("disk full") }

func TestJournal_AppendFailureIsVisible(t *testing.T) {
	def := parallelDef()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(&failingJournal{}))
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	if r.JournalError() == nil || !strings.Contains(r.JournalError().Error(), "disk full") {
		t.Fatalf("journal failure must be surfaced, got %v", r.JournalError())
	}
}

func TestFold_UnknownKindRejected(t *testing.T) {
	_, err := Fold(parallelDef(), []Occurrence{{Seq: 1, Kind: OccKind(99)}})
	if err == nil || !strings.Contains(err.Error(), "unknown kind") {
		t.Fatalf("expected unknown kind error, got %v", err)
	}
}
