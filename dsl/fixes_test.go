package dsl

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// fixes_test.go 回归测试:对应本次修复的正确性问题。
//
//   - join 节点自身的收敛配置(n_of_m 等)此前静默失效,只有写在 parallel 节点上才生效;
//   - 事件"先消费后路由":无人处理的事件被幂等表吞掉,重投递被拒;
//   - 并行 scope 收敛超时只在下一个事件到来时被动检查;
//   - 终端实例仍可被 Feed。

// ---- A1: join 节点自身配置生效 ----

func joinNodeConfigDef() *ProcessDef {
	return &ProcessDef{
		ID: "join_cfg", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1": {ID: "b1", Type: "approval",
				Transitions: []Transition{{Event: "b1ok", Next: "join"}}},
			"b2": {ID: "b2", Type: "approval",
				Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			// 收敛配置声明在 join 节点上(README 语义):1/2 即收敛。
			"join": {ID: "join", Type: "join",
				Join:        &JoinConfig{Mode: "n_of_m", Required: 1},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_JoinNodeConfigHonored(t *testing.T) {
	r := NewRuntime(joinNodeConfigDef(), nil)
	if res := r.Start("i", "e", nil, Event{ID: "s", Name: "go"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	// 仅一条分支成功:n_of_m(1) 即应收敛(修复前 mode 落回 all,实例卡在等待)。
	if res := r.Feed(Event{ID: "e1", Name: "b1ok"}); res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("n_of_m on join node must converge with 1 success, got %s", r.Status())
	}
}

// ---- A4: scope 收敛超时由 WakeDue 主动触发 ----

func scopeTimeoutDef() *ProcessDef {
	return &ProcessDef{
		ID: "scope_to", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1": {ID: "b1", Type: "approval",
				Transitions: []Transition{{Event: "b1ok", Next: "join"}}},
			"b2": {ID: "b2", Type: "approval",
				Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			"join": {ID: "join", Type: "join",
				Join:        &JoinConfig{Mode: "all", Timeout: "1h"},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_ScopeTimeoutFiresActively(t *testing.T) {
	r := NewRuntime(scopeTimeoutDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})

	// fork 时登记了 scope 超时等待槽,宿主可查询唤醒时刻。
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected scope timeout wakeup registered")
	}
	if d := time.Until(until); d > time.Hour || d < 30*time.Minute {
		t.Fatalf("expected wakeup in ~1h, got %v", d)
	}

	// 无人喂事件,到点由 WakeDue 主动触发超时(修复前:永不触发)。
	res := r.WakeDue(until.Add(time.Minute))
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "timed out") {
		t.Fatalf("expected scope timeout error, got %v", res.Errors)
	}
	if r.Status() != StatusTimedOut {
		t.Fatalf("expected timed_out, got %s", r.Status())
	}
}

func TestFix_ScopeTimeoutStaleSlotIsNoop(t *testing.T) {
	r := NewRuntime(scopeTimeoutDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})
	// 正常收敛:两条分支都完成,scope 弹出,超时槽一并清理。
	r.Feed(Event{ID: "e1", Name: "b1ok"})
	r.Feed(Event{ID: "e2", Name: "b2ok"})
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
	// 已弹出的 scope 的超时槽不应再触发(陈旧槽自愈)。
	res := r.WakeDue(time.Now().Add(2 * time.Hour))
	if res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("stale scope slot must not fire, got %v / %s", res.Errors, r.Status())
	}
}

// ---- A3: 事件不再被烧掉 ----

func TestFix_UnhandledLinearEventKeepsWaiting(t *testing.T) {
	def := &ProcessDef{
		ID: "linear_feed", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval",
				Transitions: []Transition{{Event: "approve", Next: "end"}, {Event: "reject", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
	r := NewRuntime(def, nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})

	// 修复前:错误事件把实例打成 failed,且事件已进幂等表。
	res := r.Feed(Event{ID: "wrong", Name: "nonsense"})
	if !res.HasErrors() {
		t.Fatal("expected error for unhandled event")
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}
	if r.Ctx.IsProcessedEvent("wrong") {
		t.Fatal("unhandled event must not be consumed")
	}

	// 正确事件仍可完成流程。
	if res := r.Feed(Event{ID: "ok", Name: "approve"}); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("expected completion, got %v / %s", res.Errors, r.Status())
	}
}

func TestFix_UnroutedParallelEventReleasable(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})

	// 没有任何分支处理的事件:报错且回滚消费(重投递仍有机会)。
	res := r.Feed(Event{ID: "lost", Name: "nobody-handles-this"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "not handled") {
		t.Fatalf("expected unhandled error, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("lost") {
		t.Fatal("unrouted event must be released for redelivery")
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}

	// 同一 ID 重投递改投正确事件名:可被处理(修复前会被幂等表拒绝)。
	if res := r.Feed(Event{ID: "lost", Name: "b1ok"}); res.HasErrors() {
		t.Fatalf("redelivered event must be processable, got %v", res.Errors)
	}
}

// ---- 终端实例拒绝事件 ----

func TestFix_FeedOnTerminalInstanceRejected(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	r.Feed(Event{ID: "e1", Name: "b1ok"})
	r.Feed(Event{ID: "e2", Name: "b2ok"})
	if r.Status() != StatusCompleted {
		t.Fatalf("setup: expected completed, got %s", r.Status())
	}

	res := r.Feed(Event{ID: "late", Name: "b1ok"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "terminal") {
		t.Fatalf("expected terminal rejection, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("late") {
		t.Fatal("rejected event must not be consumed")
	}
}

// ---- validator: join.timeout 非法值部署期报错(此前静默归零) ----

func TestFix_JoinTimeoutValidated(t *testing.T) {
	def := scopeTimeoutDef()
	def.Nodes["join"].Join.Timeout = "not-a-duration"
	res := Validate(def)
	if res.IsValid {
		t.Fatal("expected invalid join.timeout to fail validation")
	}
	found := false
	for _, e := range res.Errors {
		if e.Path == "nodes[join].join.timeout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected join.timeout error, got %v", res.Errors)
	}
}

// ---- B1: timer 不再被无关外部事件打死 ----

func timerDef() *ProcessDef {
	return &ProcessDef{
		ID: "timer_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "wait"}}},
			"wait": {ID: "wait", Type: "timer", Duration: "1h",
				Transitions: []Transition{{When: "skip == true", Next: "end"}, {Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_TimerRejectsExternalEvent(t *testing.T) {
	r := NewRuntime(timerDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})
	if r.Status() != StatusWaiting {
		t.Fatalf("setup: expected waiting at timer, got %s", r.Status())
	}

	// 修复前:无关事件被消费、timer 等待槽被清除、节点被打成 failed,
	// 且到点唤醒永远不会发生。修复后:事件退回,实例保持 waiting。
	res := r.Feed(Event{ID: "noise", Name: "unrelated"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "not handled") {
		t.Fatalf("expected unhandled error, got %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}
	if r.Ctx.IsProcessedEvent("noise") {
		t.Fatal("timer must not consume external events")
	}

	// 到点唤醒仍然有效:timer 走 when 路由 + 默认分支。
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected timer wakeup registered")
	}
	if res := r.WakeDue(until.Add(time.Minute)); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("expected completion after wake, got %v / %s", res.Errors, r.Status())
	}
}

// ---- B2: 分支上的 timer 到点后前进(不再重新停靠) ----

func branchTimerDef() *ProcessDef {
	return &ProcessDef{
		ID: "branch_timer", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1": {ID: "b1", Type: "timer", Duration: "1h",
				Transitions: []Transition{{Next: "join"}}},
			"b2": {ID: "b2", Type: "approval",
				Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			"join": {ID: "join", Type: "join",
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_BranchTimerAdvancesOnWake(t *testing.T) {
	r := NewRuntime(branchTimerDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})

	// b2 分支先行汇合;b1 停在 timer 上等待。
	if res := r.Feed(Event{ID: "e2", Name: "b2ok"}); res.HasErrors() {
		t.Fatalf("feed b2 failed: %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting for branch timer, got %s", r.Status())
	}

	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected branch timer wakeup registered")
	}
	// 修复前:分支 timer 唤醒后重新停靠,登记新一轮等待,永远无法收敛。
	if res := r.WakeDue(until.Add(time.Minute)); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("expected completion after branch timer wake, got %v / %s", res.Errors, r.Status())
	}
}

// ---- B3: critical 副作用失败 = 节点失败 ----

func criticalDef(critical bool) *ProcessDef {
	return &ProcessDef{
		ID: "critical_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "act"}}},
			"act": {ID: "act", Type: "action",
				SideEffects: []SideEffect{{Type: "charge", Critical: critical}},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_CriticalSideEffectFailsNode(t *testing.T) {
	// 执行器不认识 charge 类型:critical 失败必须把实例打成 failed。
	r := NewRuntime(criticalDef(true), NewInMemorySideEffectExecutor("notify"))
	res := r.Start("i", "e", nil, Event{ID: "s", Name: "go"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "critical") {
		t.Fatalf("expected critical failure error, got %v", res.Errors)
	}
	if r.Status() != StatusFailed {
		t.Fatalf("critical failure must fail instance, got %s", r.Status())
	}
}

func TestFix_NonCriticalSideEffectKeepsFlow(t *testing.T) {
	// 默认(非 critical)行为保持不变:失败仅落账,流程照走。
	j := NewMemoryJournal()
	r := NewRuntime(criticalDef(false), NewInMemorySideEffectExecutor(), WithJournal(j))
	if res := r.Start("i", "e", nil, Event{ID: "s", Name: "go"}); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("non-critical failure must not block flow, got %v / %s", res.Errors, r.Status())
	}
	// 失败命令进入死信视图(修复前:既不阻断也不可见)。
	occs, err := j.LoadOccurrences("i")
	if err != nil {
		t.Fatal(err)
	}
	failed := FailedCommands(occs)
	if len(failed) != 1 || failed[0].Command.Type != "charge" || failed[0].Status != "failed" {
		t.Fatalf("expected one failed command in DLQ view, got %+v", failed)
	}
}

// ---- B4: 永久性失败不再重试 ----

func TestFix_PermanentFailureSkipsRetry(t *testing.T) {
	calls := 0
	se := SideEffectFunc(func(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult {
		calls++
		return SideEffectResult{CommandID: cmd.ID, Status: "failed", Permanent: true,
			Error: errors.New("bad request")}
	})
	o := &CommandOrchestrator{Executor: se, MaxRetry: 5}
	if res := o.Execute(nil, SideEffectCommand{ID: "c1", Type: "x"}); res.Error == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("permanent failure must not retry, got %d calls", calls)
	}
}

// ---- B5: Manager.Start 原子创建(消除 TOCTOU) ----

func simpleStartDef() *ProcessDef {
	return &ProcessDef{
		ID: "simple", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_DuplicateStartRejectedAtomically(t *testing.T) {
	m := NewManager([]*ProcessDef{simpleStartDef()}, nil)
	if _, err := m.Start("simple", "i1", "t", nil, Event{ID: "s1", Name: "submit"}); err != nil {
		t.Fatalf("first start failed: %v", err)
	}
	if _, err := m.Start("simple", "i1", "t", nil, Event{ID: "s2", Name: "submit"}); !errors.Is(err, ErrInstanceExists) {
		t.Fatalf("expected ErrInstanceExists, got %v", err)
	}

	// 并发同 ID 启动:恰好一个成功。
	const racers = 8
	results := make(chan error, racers)
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			_, err := m.Start("simple", fmt.Sprintf("race-%d", n), "t", nil, Event{ID: "s3", Name: "submit"})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent distinct starts must all succeed: %v", err)
		}
	}
}

// ---- B6: 幂等去重表容量有界 ----

func TestFix_ProcessedEventsBounded(t *testing.T) {
	ctx := NewExecutionContext(simpleStartDef(), "i", "e")
	for i := 0; i < maxProcessedEvents+500; i++ {
		if !ctx.AcceptEvent(Event{ID: fmt.Sprintf("ev-%d", i), Name: "submit"}) {
			t.Fatalf("fresh event %d must be accepted", i)
		}
	}
	if len(ctx.processedEvents) > maxProcessedEvents {
		t.Fatalf("processed events must stay bounded, got %d", len(ctx.processedEvents))
	}
}

// ---- B7: parallel 节点直挂 join 配置接受部署期校验 ----

func TestFix_ParallelJoinConfigValidated(t *testing.T) {
	def := joinNodeConfigDef()
	def.Nodes["fork"].Join = &JoinConfig{Mode: "n_of_m", Required: 0}
	res := Validate(def)
	if res.IsValid {
		t.Fatal("expected invalid parallel-attached join config to fail validation")
	}
	found := false
	for _, e := range res.Errors {
		if e.Path == "nodes[fork].join.required" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected join.required error on fork node, got %v", res.Errors)
	}
}

// ---- B8: 快照恢复与全量折叠等价,且走增量路径 ----

func multiStepDef() *ProcessDef {
	nodes := map[string]*Node{
		"start": {ID: "start", Type: "start",
			Transitions: []Transition{{Event: "submit", Next: "a1"}}},
	}
	for i := 1; i <= 4; i++ {
		node := fmt.Sprintf("a%d", i)
		next := "end"
		if i < 4 {
			next = fmt.Sprintf("a%d", i+1)
		}
		nodes[node] = &Node{ID: node, Type: "approval",
			Transitions: []Transition{{Event: "ok" + fmt.Sprint(i), Next: next}}}
	}
	nodes["end"] = &Node{ID: "end", Type: "end"}
	return &ProcessDef{ID: "multi", Version: "2.0", StartNode: "start", Nodes: nodes}
}

func TestFix_ManagerSnapshotResume(t *testing.T) {
	store := NewMemoryInstanceStore() // 同时充当 InstanceStore 与 SnapshotStore
	journal := NewMemoryJournal()
	m1 := NewManager([]*ProcessDef{multiStepDef()}, nil,
		WithManagerJournal(journal), WithManagerStore(store),
		WithManagerSnapshots(store), WithManagerOrchestrator(&CommandOrchestrator{Executor: nil}))
	m1.SnapshotEveryN = 8

	if _, err := m1.Start("multi", "snap-1", "t", nil, Event{ID: "s", Name: "submit"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	for i := 1; i <= 3; i++ {
		if _, err := m1.Feed("snap-1", Event{ID: fmt.Sprintf("ok%d", i), Name: "ok" + fmt.Sprint(i)}); err != nil {
			t.Fatalf("feed %d: %v", i, err)
		}
	}

	// 快照已落,且不是终点(后续 Feed 走增量折叠路径)。
	snap, err := store.LatestSnapshot("snap-1")
	if err != nil {
		t.Fatalf("expected snapshot saved: %v", err)
	}
	maxSeq, _ := journal.MaxSeq("snap-1")
	if snap.UptoSeq <= 0 || snap.UptoSeq >= maxSeq {
		t.Fatalf("expected incremental snapshot (0 < upto < max), got %d / %d", snap.UptoSeq, maxSeq)
	}

	// 新 Manager(同 store/journal)从快照 + 增量恢复,继续推进到完成。
	m2 := NewManager([]*ProcessDef{multiStepDef()}, nil,
		WithManagerJournal(journal), WithManagerStore(store), WithManagerSnapshots(store))
	if _, err := m2.Feed("snap-1", Event{ID: "ok4", Name: "ok4"}); err != nil {
		t.Fatalf("resume feed: %v", err)
	}
	rec, err := m2.GetStatus("snap-1")
	if err != nil || rec.Status != StatusCompleted.String() {
		t.Fatalf("expected completed after resume, got %v / %v", rec, err)
	}

	// 快照恢复的上下文保留幂等表/终态:旧事件重放仍被拒绝。
	if res, err := m2.Feed("snap-1", Event{ID: "ok1", Name: "ok1"}); err == nil && !res.HasErrors() {
		t.Fatal("duplicate event must be rejected after snapshot resume")
	}

	// 快照恢复状态与全量折叠一致。
	occs, _ := journal.LoadOccurrences("snap-1")
	folded, err := Fold(multiStepDef(), occs)
	if err != nil {
		t.Fatal(err)
	}
	if folded.Status != StatusCompleted || folded.CurrentNode != "end" {
		t.Fatalf("full fold must agree with snapshot resume, got %s @ %s", folded.Status, folded.CurrentNode)
	}
}

// ---- B9: Snapshot 深拷贝(并发推进时读取不撕裂) ----

func TestFix_SnapshotIsolatesScopes(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	snap := r.Snapshot()
	// 推进会就地改写分支状态;快照必须与此隔离。
	r.Feed(Event{ID: "e1", Name: "b1ok"})
	for _, s := range snap.Scopes {
		for _, b := range s.Branches {
			if b.ID == "fork.b1" && b.Done {
				t.Fatal("snapshot scopes must be isolated from live mutation")
			}
		}
	}
}
