package dsl

import (
	"testing"
)

// 构造一个可并行 fork/join 的流程定义：
//   start --submit--> parallel --(无事件分支)--> b1(b1ok) , b2(b2ok)
//   b1: approval，事件 b1ok 后到 end；b2: approval，事件 b2ok 后到 end。
func parallelDef() *ProcessDef {
	return &ProcessDef{
		ID:        "parallel_demo",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "parallel"}}},
			"parallel": {
				ID:   "parallel",
				Type: "parallel",
				Transitions: []Transition{
					{Next: "b1"},
					{Next: "b2"},
				},
			},
			"b1": {ID: "b1", Type: "approval", Transitions: []Transition{{Event: "b1ok", Next: "end"}}},
			"b2": {ID: "b2", Type: "approval", Transitions: []Transition{{Event: "b2ok", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestRuntime_LinearFlow(t *testing.T) {
	def := &ProcessDef{
		ID:        "linear",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":  {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":    {ID: "end", Type: "end"},
		},
	}

	r := NewRuntime(def, nil)
	res := r.Start("inst-1", "exec-1", nil, Event{ID: "e-start", Name: "submit"})
	if res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting after reaching approval, got %s", r.Status())
	}

	res = r.Feed(Event{ID: "e-approve", Name: "approve"})
	if res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
}

func TestRuntime_ParallelJoin(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	res := r.Start("inst-1", "exec-1", nil, Event{ID: "e-start", Name: "submit"})
	if res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting while parallel branches pending, got %s", r.Status())
	}
	if len(r.Ctx.Scopes) != 1 {
		t.Fatalf("expected 1 active parallel scope, got %d", len(r.Ctx.Scopes))
	}

	// 仅完成一条分支不足以 join（all 模式）。
	r.Feed(Event{ID: "e-b1", Name: "b1ok"})
	if r.Status() != StatusWaiting {
		t.Fatalf("expected still waiting after one branch, got %s", r.Status())
	}

	// 完成第二条分支后整体收敛完成。
	res = r.Feed(Event{ID: "e-b2", Name: "b2ok"})
	if res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed after all branches joined, got %s", r.Status())
	}
	if len(r.Ctx.Scopes) != 0 {
		t.Fatalf("expected parallel scope popped after join, got %d", len(r.Ctx.Scopes))
	}
}

func TestRuntime_IdempotentEvent(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("inst-1", "exec-1", nil, Event{ID: "dup-1", Name: "submit"})
	if r.Status() != StatusWaiting {
		t.Fatalf("unexpected initial status %s", r.Status())
	}

	// 用同一个事件 ID 重发 start 事件，应被幂等去重。
	res := r.Start("inst-1", "exec-1", nil, Event{ID: "dup-1", Name: "submit"})
	if !res.HasErrors() {
		t.Fatal("expected idempotency error for duplicate start event")
	}

	// 分支事件也应去重：同一 ID 喂两次，第二次报错。
	r.Feed(Event{ID: "b1ok", Name: "b1ok"})
	res = r.Feed(Event{ID: "b1ok", Name: "b1ok"})
	if !res.HasErrors() {
		t.Fatal("expected idempotency error for duplicate branch event")
	}
}

func TestRuntime_SideEffectsDispatched(t *testing.T) {
	def := &ProcessDef{
		ID:        "se",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "approve"}},
			},
			"approve": {
				ID:   "approve",
				Type: "approval",
				SideEffects: []SideEffect{{Type: "notify", Target: "manager"}},
				Transitions: []Transition{{Event: "approve", Next: "end"}},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	exec := NewInMemorySideEffectExecutor("notify")
	r := NewRuntime(def, exec)
	r.Start("inst-1", "exec-1", nil, Event{ID: "e1", Name: "submit"})
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting at approval, got %s", r.Status())
	}
	// 副作用在审批通过事件触发时被交付执行。
	r.Feed(Event{ID: "e2", Name: "approve"})

	results := exec.Outcomes()
	if len(results) == 0 {
		t.Fatal("expected at least one side effect outcome")
	}
	if results[0].Status != "completed" {
		t.Errorf("expected side effect completed, got %q", results[0].Status)
	}
}

func TestRuntime_StatusCannotAdvanceAfterTerminal(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("inst-1", "exec-1", nil, Event{ID: "s", Name: "submit"})
	r.Feed(Event{ID: "b1", Name: "b1ok"})
	r.Feed(Event{ID: "b2", Name: "b2ok"})
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
}