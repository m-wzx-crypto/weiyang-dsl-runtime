package dsl

// m3a_gate_test.go — M3a(W6):任意点重放并从该点继续 + 运行对比 v1。
// 门禁测试先于实现编写:ResumeAt 不得混入原日志后缀;CompareRuns 必须给出
// 路由一致性、升级率与模型调用成本代理指标。

import (
	"testing"
	"time"
)

func m3ReplayDef() *ProcessDef {
	return &ProcessDef{ID: "m3_replay", Version: "1.0", StartNode: "start", Nodes: map[string]*Node{
		"start":  {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "first"}}},
		"first":  {ID: "first", Type: "approval", Transitions: []Transition{{Event: "first_ok", Next: "second"}}},
		"second": {ID: "second", Type: "approval", Transitions: []Transition{{Event: "second_ok", Next: "end"}}},
		"end":    {ID: "end", Type: "end"},
	}}
}

func TestM3A_ResumeAtContinuesWithoutOriginalSuffix(t *testing.T) {
	def := m3ReplayDef()
	originalLog := NewMemoryJournal()
	original := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(originalLog))
	original.Start("inst-replay", "exec-original", nil, Event{ID: "start-1", Name: "submit"})
	if original.Status() != StatusWaiting || original.Ctx.CurrentNode != "first" {
		t.Fatalf("setup: expected first approval, got %s @ %s", original.Status(), original.Ctx.CurrentNode)
	}

	// 原实例后来走过一段后缀;ResumeAt 只能看到第一个等待点之前的前缀。
	prefix := append([]Occurrence(nil), originalLog.Occurrences()...)
	original.Feed(Event{ID: "first-original", Name: "first_ok"})
	if original.Ctx.CurrentNode != "second" {
		t.Fatalf("setup: expected original suffix at second approval, got %s", original.Ctx.CurrentNode)
	}

	branchLog := NewMemoryJournal()
	resumed, err := ResumeAt(def, prefix, prefix[len(prefix)-1].Seq,
		NewInMemorySideEffectExecutor(), WithJournal(branchLog))
	if err != nil {
		t.Fatalf("ResumeAt: %v", err)
	}
	if resumed.Status() != StatusWaiting || resumed.Ctx.CurrentNode != "first" {
		t.Fatalf("ResumeAt state = %s @ %s, want waiting @ first", resumed.Status(), resumed.Ctx.CurrentNode)
	}
	if res := resumed.Feed(Event{ID: "first-branch", Name: "first_ok"}); res.HasErrors() {
		t.Fatalf("continue first approval: %v", res.Errors)
	}
	if res := resumed.Feed(Event{ID: "second-branch", Name: "second_ok"}); res.HasErrors() {
		t.Fatalf("continue second approval: %v", res.Errors)
	}
	if resumed.Status() != StatusCompleted {
		t.Fatalf("continued execution status = %s, want completed", resumed.Status())
	}

	// 新分支日志与所选前缀合并后可精确折叠,不包含原实例的 other suffix。
	merged := append(append([]Occurrence(nil), prefix...), branchLog.Occurrences()...)
	folded, err := Fold(def, merged)
	if err != nil {
		t.Fatalf("fold continued branch: %v", err)
	}
	if folded.Status != StatusCompleted || folded.CurrentNode != "end" {
		t.Fatalf("continued fold = %s @ %s, want completed @ end", folded.Status, folded.CurrentNode)
	}
	if folded.IsProcessedEvent("first-original") {
		t.Fatal("ResumeAt must not absorb occurrences after the requested point")
	}
	if !folded.IsProcessedEvent("first-branch") || !folded.IsProcessedEvent("second-branch") {
		t.Fatal("continued events must be present after folding prefix + branch log")
	}
}

func TestM3A_CompareRunsReportsRoutingEscalationAndCost(t *testing.T) {
	def := &ProcessDef{ID: "m3_compare", Version: "1.0", StartNode: "ai", Nodes: map[string]*Node{
		"ai":    {ID: "ai", Type: "ai", Ai: &AIConfig{OnError: "human"}},
		"human": {ID: "human", Type: "approval"},
		"end":   {ID: "end", Type: "end"},
	}}
	model := &Principal{Kind: PrincipalModel, ID: "model-1", Model: "gpt-x", ModelVersion: "1", PromptVersion: "p1"}
	base := func(instance string) []Occurrence {
		return []Occurrence{
			{Seq: 1, Time: time.Unix(1, 0), InstanceID: instance, ExecutionID: "exec-" + instance, Kind: OccStarted},
			{Seq: 2, Time: time.Unix(2, 0), InstanceID: instance, ExecutionID: "exec-" + instance, Kind: OccEventConsumed,
				Event: &Event{ID: "ai-1", Name: "ai_result"}, Actor: model},
		}
	}
	a := append(base("a"), Occurrence{Seq: 3, InstanceID: "a", ExecutionID: "exec-a", Kind: OccTransition,
		FromNode: "ai", ToNode: "human", TransitionEvent: "ai_result"})
	b := append(base("b"), Occurrence{Seq: 3, InstanceID: "b", ExecutionID: "exec-b", Kind: OccTransition,
		FromNode: "ai", ToNode: "end", TransitionEvent: "ai_result"})

	cmp, err := CompareRuns(def, a, b)
	if err != nil {
		t.Fatalf("CompareRuns: %v", err)
	}
	if cmp.SameRoutes || cmp.RoutingAgreement != 0 {
		t.Fatalf("different routes: SameRoutes=%v agreement=%v, want false/0", cmp.SameRoutes, cmp.RoutingAgreement)
	}
	if cmp.A.ModelCalls != 1 || cmp.B.ModelCalls != 1 || cmp.ModelCallDelta != 0 {
		t.Fatalf("model call proxy mismatch: A=%d B=%d delta=%d", cmp.A.ModelCalls, cmp.B.ModelCalls, cmp.ModelCallDelta)
	}
	if cmp.A.Escalations != 1 || cmp.A.Decisions != 1 || cmp.A.EscalationRate != 1 {
		t.Fatalf("run A escalation metrics = %+v, want 1/1 = 1", cmp.A)
	}
	if cmp.B.Escalations != 0 || cmp.B.EscalationRate != 0 || cmp.EscalationRateDelta != 1 {
		t.Fatalf("run B/delta escalation metrics = %+v / %v", cmp.B, cmp.EscalationRateDelta)
	}

	// 路由一致时 agreement=1;同一路由重复比较必须稳定。
	b[2].ToNode = "human"
	same, err := CompareRuns(def, a, b)
	if err != nil {
		t.Fatalf("CompareRuns same route: %v", err)
	}
	if !same.SameRoutes || same.RoutingAgreement != 1 {
		t.Fatalf("same routes: SameRoutes=%v agreement=%v, want true/1", same.SameRoutes, same.RoutingAgreement)
	}
}
