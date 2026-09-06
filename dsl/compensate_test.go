package dsl

import (
	"testing"
)

// v2 行为契约测试:副作用补偿声明、undo 栈逆序补偿、幂等、自动补偿。

func sagaDef() *ProcessDef {
	return &ProcessDef{
		ID:        "saga",
		Version:   "2.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "book"}}},
			"book": {ID: "book", Type: "action",
				SideEffects: []SideEffect{{
					Type: "book_hotel", Target: "hotel",
					Compensation: &SideEffect{Type: "cancel_hotel", Target: "hotel"},
				}},
				Transitions: []Transition{{Next: "charge"}}},
			"charge": {ID: "charge", Type: "action",
				SideEffects: []SideEffect{{
					Type: "charge_card", Target: "payment",
					Compensation: &SideEffect{Type: "refund_card", Target: "payment"},
				}},
				Transitions: []Transition{{Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval",
				Transitions: []Transition{{Event: "reject", Next: "broken"}}},
			// 故障注入点:output 表达式求值失败,节点失败 → 实例失败。
			"broken": {ID: "broken", Type: "action",
				Output:      map[string]string{"x": `1 + "not-a-number"`},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestV2_CompensationReverseOrder(t *testing.T) {
	exec := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	r := NewRuntime(sagaDef(), exec)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"}) // book + charge 成功,停靠 approve

	// 审批拒绝 → broken 节点失败 → 实例失败 → 自动补偿未开启,undo 栈仍在。
	if res := r.Feed(Event{ID: "s2", Name: "reject"}); !res.HasErrors() {
		t.Fatal("expected broken node failure")
	}
	if r.Status() != StatusFailed {
		t.Fatalf("expected failed, got %s", r.Status())
	}
	if len(r.Ctx.UndoStack) != 2 {
		t.Fatalf("expected 2 undo entries, got %d", len(r.Ctx.UndoStack))
	}

	// 显式补偿:逆序 refund(charge 的补偿)→ cancel(book 的补偿)。
	results := r.Compensate()
	if len(results) != 2 {
		t.Fatalf("expected 2 compensation results, got %d", len(results))
	}
	refundKey := r.Ctx.UndoStack[1].Key // 栈顶 = charge 的补偿
	cancelKey := r.Ctx.UndoStack[0].Key // 栈底 = book 的补偿
	if results[0].CommandID != refundKey || results[1].CommandID != cancelKey {
		t.Fatalf("expected reverse order [refund %s, cancel %s], got [%s, %s]",
			refundKey, cancelKey, results[0].CommandID, results[1].CommandID)
	}
	if results[0].Status != "completed" || results[1].Status != "completed" {
		t.Fatalf("compensations must complete, got %s / %s", results[0].Status, results[1].Status)
	}

	// 幂等:重复 Compensate 不再执行。
	if again := r.Compensate(); len(again) != 0 {
		t.Fatalf("repeated compensate must be no-op, got %d", len(again))
	}
}

func TestV2_AutoCompensateOnFailure(t *testing.T) {
	exec := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	r := NewRuntime(sagaDef(), exec, WithAutoCompensate())
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	r.Feed(Event{ID: "s2", Name: "reject"})

	if r.Status() != StatusFailed {
		t.Fatalf("expected failed, got %s", r.Status())
	}
	refundKey, cancelKey := r.Ctx.UndoStack[1].Key, r.Ctx.UndoStack[0].Key
	var refunds, cancels int
	for _, o := range exec.Outcomes() {
		if o.Status != "completed" {
			continue
		}
		if o.CommandID == refundKey {
			refunds++
		}
		if o.CommandID == cancelKey {
			cancels++
		}
	}
	if refunds != 1 || cancels != 1 {
		t.Fatalf("expected auto compensation executed once each, refunds=%d cancels=%d", refunds, cancels)
	}
}

func TestV2_CompensationOnlyForSuccessfulEffects(t *testing.T) {
	// 副作用执行失败(类型未注册)不入 undo 栈:流程只有一个失败副作用,无可补偿。
	def := &ProcessDef{
		ID: "fail_se", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "ship"}}},
			"ship": {ID: "ship", Type: "action",
				SideEffects: []SideEffect{{
					Type: "ship_order", Target: "logistics",
					Compensation: &SideEffect{Type: "recall_order", Target: "logistics"},
				}},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
	exec := NewInMemorySideEffectExecutor() // ship_order 未注册 → 失败
	r := NewRuntime(def, exec)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "go"})
	if len(r.Ctx.UndoStack) != 0 {
		t.Fatalf("failed side effect must not enter undo stack, got %d", len(r.Ctx.UndoStack))
	}
	if r.Compensate(); len(exec.Outcomes()) > 1 {
		t.Fatalf("no compensation command should be dispatched for failed effect, got %d outcomes", len(exec.Outcomes()))
	}
}

func TestV2_LoopSideEffectsNotDeduped(t *testing.T) {
	// 环路流程二次经过同一 approval 节点:幂等键含访问序号,副作用必须执行三次。
	def := &ProcessDef{
		ID: "loop", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "tick"}}},
			"tick": {ID: "tick", Type: "approval",
				SideEffects: []SideEffect{{Type: "beat", Target: "x"}},
				Transitions: []Transition{{Event: "again", Next: "tick"}, {Event: "stop", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
	exec := NewInMemorySideEffectExecutor("beat")
	r := NewRuntime(def, exec)
	r.Start("i", "e", nil, Event{ID: "e1", Name: "go"})
	r.Feed(Event{ID: "e2", Name: "again"})
	r.Feed(Event{ID: "e3", Name: "again"})
	r.Feed(Event{ID: "e4", Name: "stop"})

	beats := 0
	for _, o := range exec.Outcomes() {
		if o.Status == "completed" {
			beats++
		}
	}
	if beats != 3 {
		t.Fatalf("expected 3 completed beats across loop iterations, got %d", beats)
	}
}
