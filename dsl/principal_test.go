package dsl

// principal_test.go — M1(Principals)W1 切片:人类归属端到端。
//
// 契约:事件携带的 principal 以 Actor 落账,穿越 fold / FoldTo / JSON 持久化
// 逐字段复现;日志入账与调用方对象隔离(append-only,不许被改写)。
// "缺 principal 即拒绝"的强制点在 W2 落地,本文件不含该断言。

import (
	"encoding/json"
	"reflect"
	"testing"
)

func approvalDefWithIDs() *ProcessDef {
	return &ProcessDef{
		ID: "attr_flow", Version: "1.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start":   {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":     {ID: "end", Type: "end"},
		},
	}
}

// seqOfEvent 返回某事件被消费那笔事实的 Seq(找不到则报错)。
func seqOfEvent(t *testing.T, occs []Occurrence, eventID string) int64 {
	t.Helper()
	for _, occ := range occs {
		if occ.Kind == OccEventConsumed && occ.Event != nil && occ.Event.ID == eventID {
			return occ.Seq
		}
	}
	t.Fatalf("event %q not found in journal", eventID)
	return 0
}

func TestPrincipal_EventAttributionJournaledAndFolded(t *testing.T) {
	def := approvalDefWithIDs()
	j := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(j))

	r.Start("inst-attr", "exec-1", nil, Event{ID: "s1", Name: "submit",
		Principal: &Principal{Kind: PrincipalHuman, ID: "u-2001", DisplayName: "李提交"}})
	r.Feed(Event{ID: "a1", Name: "approve",
		Principal: &Principal{Kind: PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}})

	// 1) 日志:每笔事件消费带 Actor,且与事件 principal 逐字段一致。
	for _, occ := range j.Occurrences() {
		if occ.Kind != OccEventConsumed {
			continue
		}
		if occ.Event.Principal == nil {
			t.Fatalf("occ#%d: event %q carried no principal", occ.Seq, occ.Event.ID)
		}
		if occ.Actor == nil || !reflect.DeepEqual(*occ.Actor, *occ.Event.Principal) {
			t.Fatalf("occ#%d: actor %+v != event principal %+v", occ.Seq, occ.Actor, occ.Event.Principal)
		}
	}

	// 2) 折叠:活上下文与折叠上下文逐字段一致(含 principal)。
	requireFoldEq(t, def, r)

	// 3) 时间旅行:FoldTo 回到审批前,当前决策的归属是提交人而非审批人。
	foldedEarly, err := FoldTo(def, j.Occurrences(), seqOfEvent(t, j.Occurrences(), "s1"))
	if err != nil {
		t.Fatalf("foldTo: %v", err)
	}
	if foldedEarly.CurrentEvent == nil || foldedEarly.CurrentEvent.Principal == nil ||
		foldedEarly.CurrentEvent.Principal.ID != "u-2001" {
		t.Fatalf("time travel: expected submitter principal, got %+v", foldedEarly.CurrentEvent)
	}

	// 4) JSON 回传(持久化路径):occurrence 序列化不破坏归属。
	data, err := marshalOccs(j.Occurrences())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back []Occurrence
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, occ := range back {
		if occ.Kind == OccEventConsumed && occ.Event.ID == "a1" {
			if occ.Actor == nil || occ.Actor.ID != "u-1001" || occ.Actor.Kind != PrincipalHuman {
				t.Fatalf("json round-trip lost attribution: %+v", occ.Actor)
			}
		}
	}
}

func TestPrincipal_JournalSnapshotIsolatedFromCaller(t *testing.T) {
	def := approvalDefWithIDs()
	j := NewMemoryJournal()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(j))

	p := &Principal{Kind: PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}
	r.Start("inst-iso", "exec-1", nil, Event{ID: "s1", Name: "submit"})
	r.Feed(Event{ID: "a1", Name: "approve", Principal: p})

	// 调用方事后篡改自己的对象,不能改写已发生的账。
	p.ID = "tampered"
	p.Kind = PrincipalAgent

	for _, occ := range j.Occurrences() {
		if occ.Kind == OccEventConsumed && occ.Event.ID == "a1" {
			if occ.Actor == nil || occ.Actor.ID != "u-1001" || occ.Actor.Kind != PrincipalHuman {
				t.Fatalf("journal was mutated through caller aliasing: %+v", occ.Actor)
			}
		}
	}
	if r.Ctx.CurrentEvent.Principal.ID != "u-1001" || r.Ctx.CurrentEvent.Principal.Kind != PrincipalHuman {
		t.Fatalf("live state was mutated through caller aliasing: %+v", r.Ctx.CurrentEvent.Principal)
	}
}

func TestPrincipal_NilPrincipalBackwardCompatible(t *testing.T) {
	def := approvalDefWithIDs()
	r := NewRuntime(def, NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))

	r.Start("inst-legacy", "exec-1", nil, Event{ID: "s1", Name: "submit"})
	r.Feed(Event{ID: "a1", Name: "approve"})
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}

	// 无归属的事件照常入账(Actor 为空),折叠精确性不受影响。
	for _, occ := range r.Journal.(*MemoryJournal).Occurrences() {
		if occ.Kind == OccEventConsumed && occ.Actor != nil {
			t.Fatalf("occ#%d: unexpected actor on principal-less event", occ.Seq)
		}
	}
	requireFoldEq(t, def, r)
}
