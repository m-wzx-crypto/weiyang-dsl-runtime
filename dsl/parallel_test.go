package dsl

import "testing"

// joinFlow 构造带命名 join 节点的并行流程：
//
//	start --submit--> fork(parallel) --+--> b1(approval) --b1ok--> sync(join) --+--> end
//	                                    +--> b2(approval) --b2ok--> sync -------+
//
// fork 可注入不同 ForkConfig；不注入时验证静态推导出的汇合点即 sync。
func joinFlow(fork *ForkConfig) *ProcessDef {
	return &ProcessDef{
		ID:        "join_flow",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "fork"}}},
			"fork": {
				ID:          "fork",
				Type:        "parallel",
				Fork:        fork,
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}},
			},
			"b1":   {ID: "b1", Type: "approval", Transitions: []Transition{{Event: "b1ok", Next: "sync"}}},
			"b2":   {ID: "b2", Type: "approval", Transitions: []Transition{{Event: "b2ok", Next: "sync"}}},
			"sync": {ID: "sync", Type: "join", Transitions: []Transition{{Next: "end"}}},
			"end":  {ID: "end", Type: "end"},
		},
	}
}

func TestRuntime_ExplicitJoinNode(t *testing.T) {
	r := NewRuntime(joinFlow(&ForkConfig{Mode: "all", JoinNode: "sync"}), nil)
	if res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting at branches, got %s", r.Status())
	}
	r.Feed(Event{ID: "b1", Name: "b1ok"})
	if r.Status() != StatusWaiting {
		t.Fatalf("one branch joined should still wait, got %s", r.Status())
	}
	res := r.Feed(Event{ID: "b2", Name: "b2ok"})
	if res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed through join, got %s", r.Status())
	}
	// 关键断言：流程真正穿过 sync（join）再到 end，而不是在 fork 处直接判完成。
	if r.Ctx.CurrentNode != "end" {
		t.Fatalf("expected to advance through join to end, stopped at %q", r.Ctx.CurrentNode)
	}
	if len(r.Ctx.Scopes) != 0 {
		t.Fatalf("scope should be popped after join, got %d", len(r.Ctx.Scopes))
	}
	if v, _ := r.Ctx.Variables["parallel_failed"].(int); v != 0 {
		t.Fatalf("expected parallel_failed=0, got %v", v)
	}
}

func TestRuntime_AutoDetectedJoinNode(t *testing.T) {
	// 不声明 joinNode：引擎应静态推导出 sync（b1/b2 公共可达、距离和最小）。
	r := NewRuntime(joinFlow(nil), nil)
	if res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	scope := r.Ctx.ActiveScope()
	if scope == nil {
		t.Fatal("expected active scope after fork")
	}
	if scope.JoinNode != "sync" {
		t.Fatalf("expected auto-detected joinNode=sync, got %q", scope.JoinNode)
	}
	r.Feed(Event{ID: "b1", Name: "b1ok"})
	r.Feed(Event{ID: "b2", Name: "b2ok"})
	if r.Status() != StatusCompleted || r.Ctx.CurrentNode != "end" {
		t.Fatalf("expected completed at end, got status=%s node=%q", r.Status(), r.Ctx.CurrentNode)
	}
}

func TestRuntime_AnyModePartialSuccess(t *testing.T) {
	// any 模式：一个分支成功即收敛，其余等待分支被取消（partial success）。
	r := NewRuntime(joinFlow(&ForkConfig{Mode: "any", JoinNode: "sync"}), nil)
	if res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	res := r.Feed(Event{ID: "b1", Name: "b1ok"})
	if res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed after first success in any-mode, got %s", r.Status())
	}
	scope := r.Snapshot().Scopes
	if len(scope) != 0 {
		t.Fatalf("scope should be popped, got %d", len(scope))
	}
}

func TestRuntime_FailFast(t *testing.T) {
	// onFail=fail：b1 条件求值失败 → 取消 b2、实例失败。
	def := &ProcessDef{
		ID:        "failfast",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "fork"}}},
			"fork": {
				ID:          "fork",
				Type:        "parallel",
				Fork:        &ForkConfig{Mode: "all", OnFail: "fail"},
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}},
			},
			// amount 未定义 → 求值失败 → 分支失败。
			"b1":   {ID: "b1", Type: "condition", Transitions: []Transition{{When: "amount > 1", Next: "sync"}}},
			"b2":   {ID: "b2", Type: "approval", Transitions: []Transition{{Event: "b2ok", Next: "sync"}}},
			"sync": {ID: "sync", Type: "join", Transitions: []Transition{{Next: "end"}}},
			"end":  {ID: "end", Type: "end"},
		},
	}
	r := NewRuntime(def, nil)
	res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"})
	if r.Status() != StatusFailed {
		t.Fatalf("expected instance failed under onFail=fail, got %s", r.Status())
	}
	if !res.HasErrors() {
		t.Fatal("expected failure error in result")
	}
	if len(r.Ctx.Scopes) != 0 {
		t.Fatalf("scope should be popped on fail-fast, got %d", len(r.Ctx.Scopes))
	}
}

func TestRuntime_JoinCompensationRouting(t *testing.T) {
	// continue 策略（默认）：失败分支随流汇合，join 按 parallel_failed 路由到补偿终态。
	def := &ProcessDef{
		ID:        "comp",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "fork"}}},
			"fork": {
				ID:          "fork",
				Type:        "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}},
			},
			"b1":   {ID: "b1", Type: "condition", Transitions: []Transition{{When: "amount > 1", Next: "sync"}}},
			"b2":   {ID: "b2", Type: "approval", Transitions: []Transition{{Event: "b2ok", Next: "sync"}}},
			"sync": {
				ID:   "sync",
				Type: "join",
				Transitions: []Transition{
					{When: "parallel_failed > 0", Next: "compensate"},
					{Next: "end"},
				},
			},
			"compensate": {ID: "compensate", Type: "end"},
			"end":        {ID: "end", Type: "end"},
		},
	}
	r := NewRuntime(def, nil)
	if res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	res := r.Feed(Event{ID: "b2", Name: "b2ok"})
	if res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
	if r.Ctx.CurrentNode != "compensate" {
		t.Fatalf("expected join to route to compensate, stopped at %q", r.Ctx.CurrentNode)
	}
}

func TestRuntime_SavepointRestore(t *testing.T) {
	def := joinFlow(&ForkConfig{Mode: "all", JoinNode: "sync"})
	r := NewRuntime(def, nil)
	if res := r.Start("i1", "e1", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	r.Feed(Event{ID: "b1", Name: "b1ok"})
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting mid-flight, got %s", r.Status())
	}

	// 崩溃 → 从 Savepoint 恢复 → 换一个 Runtime 实例继续执行。
	sp, err := r.Savepoint()
	if err != nil {
		t.Fatalf("savepoint failed: %v", err)
	}
	ctx2, err := RestoreExecutionContext(sp)
	if err != nil {
		t.Fatalf("restore failed: %v", err)
	}
	r2 := NewRuntime(def, nil, WithExecutionContext(ctx2))

	// 幂等去重表随快照持久化：恢复后重放同一事件仍被拒绝。
	if res := r2.Feed(Event{ID: "b1", Name: "b1ok"}); !res.HasErrors() {
		t.Fatal("expected duplicate event to be rejected after restore")
	}
	// 恢复后正常推进剩余分支直至完成。
	if res := r2.Feed(Event{ID: "b2", Name: "b2ok"}); res.HasErrors() {
		t.Fatalf("feed after restore failed: %v", res.Errors)
	}
	if r2.Status() != StatusCompleted || r2.Ctx.CurrentNode != "end" {
		t.Fatalf("expected completed at end after restore, got status=%s node=%q", r2.Status(), r2.Ctx.CurrentNode)
	}
}

// falseEngine 恒假引擎：验证 Step 求值走的是上下文注入的引擎（单一事实源），
// 而非包级默认引擎。
type falseEngine struct{}

func (falseEngine) Validate(string) error                        { return nil }
func (falseEngine) Evaluate(string, map[string]interface{}) (bool, error) {
	return false, nil
}
func (falseEngine) TypeCheck(string, *TypeSchema) []TypeError { return nil }

func TestStep_UsesContextEngine(t *testing.T) {
	def := &ProcessDef{
		ID:        "engine",
		Version:   "1.0",
		StartNode: "check",
		Nodes: map[string]*Node{
			"check": {
				ID:   "check",
				Type: "condition",
				Transitions: []Transition{
					{When: "amount > 10", Next: "high"},
					{Next: "low"},
				},
			},
			"high": {ID: "high", Type: "end"},
			"low":  {ID: "low", Type: "end"},
		},
	}
	ctx := NewExecutionContext(def, "i1", "e1")
	ctx.CurrentNode = "check"
	ctx.SetVariable("amount", 100) // 默认引擎下必然走 high

	ctx.Engine = falseEngine{}
	res := Step(def, ctx)
	if res.HasErrors() {
		t.Fatalf("step failed: %v", res.Errors)
	}
	if res.Transition == nil || res.Transition.To != "low" {
		t.Fatalf("expected injected engine to force default branch (low), got %+v", res.Transition)
	}
}

func TestValidate_ForkJoinNodeUnknown(t *testing.T) {
	def := &ProcessDef{
		ID:        "v",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {
				ID:          "fork",
				Type:        "parallel",
				Fork:        &ForkConfig{JoinNode: "ghost"},
				Transitions: []Transition{{Next: "end"}},
			},
			"end": {ID: "end", Type: "end"},
		},
	}
	res := Validate(def)
	found := false
	for _, e := range res.Errors {
		if e.Path == "nodes[fork].fork.joinNode" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected joinNode-unknown error, got %+v", res.Errors)
	}
}
