package dsl

// m2_gate_test.go — M2(Attributed Rules)W5b:激活状态机 + M2 gate。
//
// 验收(PLAN W5 / ROADMAP M2):
//  1. 激活状态机 draft → active;激活要求 proposer + owner 双字段齐备。
//  2. 激活动作本身记为 journal 事实(定义级,与实例级日志分账)。
//  3. gate(ROADMAP M2):无 proposer/owner 的激活失败 ✓;
//     两版本产出稳定结构 diff ✓。
//
// 这些测试在实现完成前应全部失败。

import (
	"errors"
	"strings"
	"testing"
)

func w5Register(t *testing.T, reg *DefinitionRegistry, def *ProcessDef, info *VersionInfo) *VersionEntry {
	t.Helper()
	entry, err := reg.RegisterVersion(def, info)
	if err != nil {
		t.Fatalf("register %s@%s: %v", def.ID, def.Version, err)
	}
	return entry
}

func w5Human(id string) *Principal {
	return &Principal{Kind: PrincipalHuman, ID: id, DisplayName: id}
}

// assertActivationEq 逐字段比较激活状态(折叠精确性口径:逐字段复现)。
func assertActivationEq(t *testing.T, what string, want, got *ActivationState) {
	t.Helper()
	if got == nil {
		t.Fatalf("%s: state is nil, want %+v", what, want)
	}
	if want.DefID != got.DefID || want.Version != got.Version ||
		want.Hash != got.Hash || want.Status != got.Status ||
		want.Proposer != got.Proposer || want.Owner != got.Owner {
		t.Fatalf("%s: state mismatch\n--- want ---\n%+v\n--- got ---\n%+v", what, want, got)
	}
	if !want.ActivatedAt.Equal(got.ActivatedAt) {
		t.Fatalf("%s: ActivatedAt mismatch: want %v got %v", what, want.ActivatedAt, got.ActivatedAt)
	}
	wp, gp := want.ActivatedBy, got.ActivatedBy
	if (wp == nil) != (gp == nil) {
		t.Fatalf("%s: ActivatedBy presence mismatch: want %+v got %+v", what, wp, gp)
	}
	if wp != nil && (wp.Kind != gp.Kind || wp.ID != gp.ID || wp.DisplayName != gp.DisplayName) {
		t.Fatalf("%s: ActivatedBy mismatch: want %+v got %+v", what, wp, gp)
	}
}

// ---- M2 gate (1/2):无 proposer/owner 的激活失败 ----

func TestM2Gate_ActivationWithoutProposerOrOwnerFails(t *testing.T) {
	reg := NewVersionRegistry()
	journal := NewMemoryJournal()
	act := NewActivator(reg, journal)

	// 草稿可以不带归属注册(draft 只是提案);激活才是问责门。
	w5Register(t, reg, w4OrderV1(), nil)
	if _, err := act.Activate("order_flow", "1.0", w5Human("u-3001")); !errors.Is(err, ErrAttributionRequired) {
		t.Fatalf("activation of unattributed version must fail with ErrAttributionRequired, got %v", err)
	}

	// 只有 proposer / 只有 owner 一样不算齐备。
	partial := w4OrderV1()
	partial.Version = "1.1"
	w5Register(t, reg, partial, &VersionInfo{Proposer: "u-1001"})
	ownerOnly := w4OrderV1()
	ownerOnly.Version = "1.2"
	w5Register(t, reg, ownerOnly, &VersionInfo{Owner: "u-2001"})
	for _, v := range []string{"1.1", "1.2"} {
		if _, err := act.Activate("order_flow", v, w5Human("u-3001")); !errors.Is(err, ErrAttributionRequired) {
			t.Fatalf("activation with partial attribution (%s) must fail, got %v", v, err)
		}
	}

	// 失败的激活不得改状态、不得入账。
	if got := act.State("order_flow", "1.0"); got == nil || got.Status != ActivationDraft {
		t.Fatalf("failed activation must leave the version in draft, got %+v", got)
	}
	if n := journal.Len(); n != 0 {
		t.Fatalf("failed activation must not be journaled, got %d facts", n)
	}

	// 双字段齐备 → draft → active。
	full := w4OrderV1()
	full.Version = "1.3"
	entry := w5Register(t, reg, full, &VersionInfo{Proposer: "u-1001", Owner: "u-2001", Note: "attributed"})
	st, err := act.Activate("order_flow", "1.3", w5Human("u-3001"))
	if err != nil {
		t.Fatalf("attributed activation must succeed: %v", err)
	}
	if st.Status != ActivationActive {
		t.Fatalf("activation must move draft -> active, got %s", st.Status)
	}
	// 归属与 hash 复用注册表条目,不另造 identity。
	if st.Hash != entry.Hash || st.Proposer != "u-1001" || st.Owner != "u-2001" {
		t.Fatalf("activation must reuse registry hash/attribution\n--- state ---\n%+v\n--- entry ---\n%+v", st, entry.Info)
	}
}

// ---- M2 gate (2/2):两版本产出稳定结构 diff ----

func TestM2Gate_TwoVersionsProduceStableStructuralDiff(t *testing.T) {
	v1, v2 := w4OrderV1(), w4OrderV2()
	first := strings.Join(Diff(v1, v2).Changes, "\n")
	if first == "" {
		t.Fatal("two different versions must produce a non-empty structural diff")
	}
	// 同输入必产出同 diff:重复调用逐字节一致。
	for i := 0; i < 8; i++ {
		got := strings.Join(Diff(v1, v2).Changes, "\n")
		if got != first {
			t.Fatalf("diff must be stable across calls (run %d)\n--- first ---\n%s\n--- got ---\n%s", i+1, first, got)
		}
	}
	// 同一定义的两版在注册表里各有独立 hash;同版 diff 为空。
	reg := NewVersionRegistry()
	e1 := w5Register(t, reg, v1, &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})
	e2 := w5Register(t, reg, v2, &VersionInfo{Proposer: "u-1002", Owner: "u-2001"})
	if e1.Hash == e2.Hash {
		t.Fatal("different versions of one definition must hash differently")
	}
	if len(Diff(v1, v1).Changes) != 0 {
		t.Fatal("a version diffed against itself must be empty")
	}
}

// ---- 状态机:draft → active,身份复用注册表 ----

func TestM2_ActivationStateMachineDraftToActive(t *testing.T) {
	reg := NewVersionRegistry()
	journal := NewMemoryJournal()
	act := NewActivator(reg, journal)

	entry := w5Register(t, reg, w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})

	// 注册即 draft。
	st := act.State("order_flow", "1.0")
	if st == nil || st.Status != ActivationDraft || !st.ActivatedAt.IsZero() {
		t.Fatalf("registered version must start as draft, got %+v", st)
	}
	// 未注册版本没有状态。
	if got := act.State("order_flow", "9.9"); got != nil {
		t.Fatalf("unregistered version must have no state, got %+v", got)
	}
	// 未注册版本不可激活。
	if _, err := act.Activate("order_flow", "9.9", w5Human("u-3001")); err == nil {
		t.Fatal("activating an unregistered version must fail")
	}

	if _, err := act.Activate("order_flow", "1.0", w5Human("u-3001")); err != nil {
		t.Fatalf("activate: %v", err)
	}

	// 重复激活幂等:状态不翻动、不追加事实。
	before := act.State("order_flow", "1.0")
	if _, err := act.Activate("order_flow", "1.0", w5Human("u-3009")); err != nil {
		t.Fatalf("re-activate: %v", err)
	}
	after := act.State("order_flow", "1.0")
	assertActivationEq(t, "re-activation", before, after)
	if n := journal.Len(); n != 1 {
		t.Fatalf("re-activation must not append a second fact, got %d", n)
	}
	if after.Hash != entry.Hash {
		t.Fatalf("state must reuse registry hash, got %s want %s", after.Hash, entry.Hash)
	}
}

// ---- 激活即事实:定义级入账,与实例级日志分账 ----

func TestM2_ActivationIsJournaledAsDefinitionFact(t *testing.T) {
	reg := NewVersionRegistry()
	journal := NewMemoryJournal()
	act := NewActivator(reg, journal)
	w5Register(t, reg, w4OrderV2(), &VersionInfo{Proposer: "u-1002", Owner: "u-2001", Note: "add ai triage"})

	by := w5Human("u-3001")
	st, err := act.Activate("order_flow", "2.0", by)
	if err != nil {
		t.Fatalf("activate: %v", err)
	}

	occs := journal.Occurrences()
	if len(occs) != 1 {
		t.Fatalf("activation must append exactly one fact, got %d", len(occs))
	}
	occ := occs[0]
	if occ.Kind != OccDefActivated {
		t.Fatalf("fact kind = %s, want %s", occ.Kind, OccDefActivated)
	}
	if occ.Kind.String() != "def_activated" {
		t.Fatalf("OccKind.String must stay in sync, got %q", occ.Kind.String())
	}
	// 分账:定义级事实不带实例/执行标识,携带定义键。
	if occ.InstanceID != "" || occ.ExecutionID != "" {
		t.Fatalf("definition-level fact must not carry instance/execution ids, got %+v", occ)
	}
	if occ.DefID != "order_flow" || occ.DefVersion != "2.0" {
		t.Fatalf("fact must carry the definition key, got %s@%s", occ.DefID, occ.DefVersion)
	}
	if occ.Actor == nil || occ.Actor.ID != "u-3001" {
		t.Fatalf("fact must record who activated, got %+v", occ.Actor)
	}
	// 快照自包含:hash + 归属 + 行为主体随事实离线可复现。
	if occ.Activation == nil || occ.Activation.Hash != st.Hash ||
		occ.Activation.Proposer != "u-1002" || occ.Activation.Owner != "u-2001" ||
		occ.Activation.Status != ActivationActive {
		t.Fatalf("fact must carry a self-contained activation snapshot, got %+v", occ.Activation)
	}

	// 防篡改:入账副本与调用方对象解耦。
	by.ID = "hacker"
	after := journal.Occurrences()[0]
	if after.Actor.ID != "u-3001" || after.Activation.ActivatedBy.ID != "u-3001" {
		t.Fatal("journaled attribution must not change when the caller mutates its objects")
	}

	// 分账续:按实例取数看不到定义级事实。
	if got, _ := journal.LoadOccurrences("inst-1"); len(got) != 0 {
		t.Fatalf("instance-scoped reads must not see definition facts, got %d", len(got))
	}
}

// ---- 事实先行:账记不下来,激活就不成立 ----

type w5ErrJournal struct{}

func (w5ErrJournal) Append(*Occurrence) error { return errors.New("journal unavailable") }

func TestM2_ActivationFailsWhenJournalFails(t *testing.T) {
	reg := NewVersionRegistry()
	act := NewActivator(reg, w5ErrJournal{})
	w5Register(t, reg, w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})

	if _, err := act.Activate("order_flow", "1.0", w5Human("u-3001")); err == nil {
		t.Fatal("activation must fail when the fact cannot be journaled (fact-first)")
	}
	if got := act.State("order_flow", "1.0"); got == nil || got.Status != ActivationDraft {
		t.Fatalf("failed journaling must not flip the state machine, got %+v", got)
	}
}

// ---- 折叠精确性:定义账折叠出的激活状态与活状态逐字段一致 ----

func TestM2_FoldActivationReproducesState(t *testing.T) {
	reg := NewVersionRegistry()
	journal := NewMemoryJournal()
	act := NewActivator(reg, journal)

	other := w4OrderV1()
	other.ID = "refund_flow"
	draft := w4OrderV1()
	draft.Version = "1.9"
	w5Register(t, reg, w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})
	w5Register(t, reg, w4OrderV2(), &VersionInfo{Proposer: "u-1002", Owner: "u-2001"})
	w5Register(t, reg, other, &VersionInfo{Proposer: "u-1003", Owner: "u-2002"})
	w5Register(t, reg, draft, &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})

	for _, a := range []struct {
		id, version, actor string
	}{
		{"order_flow", "1.0", "u-3001"},
		{"order_flow", "2.0", "u-3002"},
		{"refund_flow", "1.0", "u-3003"},
	} {
		if _, err := act.Activate(a.id, a.version, w5Human(a.actor)); err != nil {
			t.Fatalf("activate %s@%s: %v", a.id, a.version, err)
		}
	}

	folded := foldActivation(journal.Occurrences())
	if len(folded) != 3 {
		t.Fatalf("fold must reproduce exactly the activated versions, got %d", len(folded))
	}
	for _, key := range []registryKey{
		{"order_flow", "1.0"},
		{"order_flow", "2.0"},
		{"refund_flow", "1.0"},
	} {
		assertActivationEq(t, "fold("+key.id+"@"+key.version+")", act.State(key.id, key.version), folded[key])
	}
}

// ---- 激活的行为主体是问责链的一部分 ----

func TestM2_ActivationRequiresActorPrincipal(t *testing.T) {
	reg := NewVersionRegistry()
	journal := NewMemoryJournal()
	act := NewActivator(reg, journal)
	w5Register(t, reg, w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})

	if _, err := act.Activate("order_flow", "1.0", nil); !errors.Is(err, ErrPrincipalRequired) {
		t.Fatalf("activation without an actor must be rejected, got %v", err)
	}
	if _, err := act.Activate("order_flow", "1.0", &Principal{Kind: PrincipalHuman}); !errors.Is(err, ErrPrincipalRequired) {
		t.Fatalf("activation with an unidentified actor must be rejected, got %v", err)
	}
	if n := journal.Len(); n != 0 {
		t.Fatalf("rejected activations must not be journaled, got %d", n)
	}
	if got := act.State("order_flow", "1.0"); got == nil || got.Status != ActivationDraft {
		t.Fatalf("rejected activations must leave the version in draft, got %+v", got)
	}
}
