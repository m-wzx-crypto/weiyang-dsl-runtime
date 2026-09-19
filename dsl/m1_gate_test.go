package dsl

// m1_gate_test.go — M1(Principals)门禁测试(ROADMAP §4 M1 / PLAN W2)。
//
// Gate 断言(ROADMAP 原文):
//  1. a decision recorded without a principal is rejected —— 要求归属的节点上,
//     缺 principal(或不完整)的决策被结构性拒绝:不消费、不入账、错误可见
//     且可判别(ErrPrincipalRequired)、实例保持等待;补上归属的同一事件
//     重投即被接受(证明回拒只针对归属缺失,而非事件本身)。
//  2. a folded instance reproduces principals field-for-field —— 每一笔带
//     Actor 的事件消费事实,时间旅行(FoldTo)到该时刻都逐字段复现 principal;
//     全量折叠与活上下文一致。
//
// W2 切片另锁定的推理归属契约:ai 回调载荷携带 model / model_version /
// prompt_version(宿主作为数据传入,内核不做任何模型调用——原则 3),引擎
// 固化为模型 principal 随事件入账,日志读者不窥探载荷即可回答"哪个模型、
// 哪个 prompt 版本"。

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

// m1GateDef:submit(human)→ ai 分诊(模型决策)→ 审批(要求归属的人类决策)→ end。
func m1GateDef() *ProcessDef {
	return &ProcessDef{
		ID: "m1_gate", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "triage"}}},
			"triage": {ID: "triage", Type: "ai",
				Ai: &AIConfig{
					Prompt:      "classify {{ticket}}",
					OutputTypes: map[string]*Type{"confidence": NumberType()},
					Choose:      []string{"approve_it", "reject_it"},
				},
				Transitions: []Transition{
					{Case: "approve_it", Next: "approve"},
					{Case: "reject_it", Next: "end"},
				}},
			"approve": {ID: "approve", Type: "approval", RequirePrincipal: true,
				Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

// gateModelPrincipal 是 ai 回调载荷声明归属后应落账的模型 principal。
func gateModelPrincipal() Principal {
	return Principal{
		Kind:          PrincipalModel,
		ID:            "gpt-4o",
		Model:         "gpt-4o",
		ModelVersion:  "2024-08-06",
		PromptVersion: "triage-v3",
	}
}

// aiAttributedResult 构造带推理归属的 ai 回调事件(载荷即宿主回传形状)。
func aiAttributedResult(id, choice string) Event {
	return Event{ID: id, Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice":         choice,
		"output":         map[string]interface{}{"confidence": 0.9},
		"model":          "gpt-4o",
		"model_version":  "2024-08-06",
		"prompt_version": "triage-v3",
	}}
}

// hasErrPrincipalRequired 判别结果中是否携带强制点回拒(哨兵可判别,不靠字符串)。
func hasErrPrincipalRequired(res *ExecutionResult) bool {
	for _, err := range res.Errors {
		if errors.Is(err, ErrPrincipalRequired) {
			return true
		}
	}
	return false
}

// findConsumed 返回指定事件 ID 的消费事实(找不到则报错)。
func findConsumed(t *testing.T, occs []Occurrence, eventID string) *Occurrence {
	t.Helper()
	for i := range occs {
		if occs[i].Kind == OccEventConsumed && occs[i].Event != nil && occs[i].Event.ID == eventID {
			cp := occs[i]
			return &cp
		}
	}
	t.Fatalf("event %q not found in journal", eventID)
	return nil
}

// ---- Gate 1:无 principal 的决策被结构性拒绝 ----

func TestM1Gate_DecisionWithoutPrincipalRejected(t *testing.T) {
	def := m1GateDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, &captureExecutor{}, WithJournal(j))

	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})
	if res := r.Feed(aiAttributedResult("a0", "approve_it")); res.HasErrors() {
		t.Fatalf("setup: attributed ai decision rejected: %v", res.Errors)
	}
	if r.Status() != StatusWaiting || r.Ctx.CurrentNode != "approve" {
		t.Fatalf("setup: expected waiting at approve, got %s @ %q", r.Status(), r.Ctx.CurrentNode)
	}

	before := len(j.Occurrences())

	// 无 principal 的审批决策:拒绝入账(不消费、不记账、可见、保持等待)。
	res := r.Feed(Event{ID: "a1", Name: "approve"})
	if !res.HasErrors() || !hasErrPrincipalRequired(res) {
		t.Fatalf("principal-less decision must be rejected with ErrPrincipalRequired, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("a1") {
		t.Fatal("rejected decision must not be consumed (idempotency table untouched)")
	}
	if len(j.Occurrences()) != before {
		t.Fatalf("rejected decision must not be recorded: journal grew %d -> %d", before, len(j.Occurrences()))
	}
	if r.Status() != StatusWaiting || r.Ctx.CurrentNode != "approve" {
		t.Fatalf("instance must stay waiting after rejection, got %s @ %q", r.Status(), r.Ctx.CurrentNode)
	}

	// 退化 principal(无稳定 ID)同样拒绝:没有身份的归属不是归属。
	res = r.Feed(Event{ID: "a2", Name: "approve", Principal: &Principal{Kind: PrincipalHuman}})
	if !res.HasErrors() || !hasErrPrincipalRequired(res) {
		t.Fatalf("identity-less principal must be rejected, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("a2") {
		t.Fatal("identity-less principal must not be consumed")
	}

	// 补上归属的同一事件重投即被接受:回拒只针对归属缺失,不针对事件本身。
	res = r.Feed(Event{ID: "a1", Name: "approve",
		Principal: &Principal{Kind: PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}})
	if res.HasErrors() {
		t.Fatalf("reattributed decision must be accepted: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed after attributed approval, got %s", r.Status())
	}
	requireFoldEq(t, def, r)
}

// ---- Gate 1(ai 面):推理决策缺(完整)模型归属被拒绝 ----

func TestM1Gate_PrincipallessAIDecisionRejected(t *testing.T) {
	def := m1GateDef()
	def.Nodes["triage"].RequirePrincipal = true
	j := NewMemoryJournal()
	r := NewRuntime(def, &captureExecutor{}, WithJournal(j))

	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})
	if r.Status() != StatusWaiting || r.Ctx.CurrentNode != "triage" {
		t.Fatalf("setup: expected parked at ai, got %s @ %q", r.Status(), r.Ctx.CurrentNode)
	}

	before := len(j.Occurrences())

	// 完全无归属的回调:拒绝。
	res := r.Feed(Event{ID: "a1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "approve_it",
		"output": map[string]interface{}{"confidence": 0.9},
	}})
	if !res.HasErrors() || !hasErrPrincipalRequired(res) {
		t.Fatalf("unattributed ai decision must be rejected with ErrPrincipalRequired, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("a1") || len(j.Occurrences()) != before {
		t.Fatal("rejected ai decision must be neither consumed nor recorded")
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}

	// 部分归属(有 model,缺 model_version / prompt_version):推理决策必须能
	// 回答"哪个模型、哪个 prompt 版本",不完整同样拒绝。
	res = r.Feed(Event{ID: "a2", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "approve_it",
		"output": map[string]interface{}{"confidence": 0.9},
		"model":  "gpt-4o",
	}})
	if !res.HasErrors() || !hasErrPrincipalRequired(res) {
		t.Fatalf("partial model attribution must be rejected, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("a2") {
		t.Fatal("partially-attributed decision must not be consumed")
	}

	// 完整归属:接受并路由。
	res = r.Feed(aiAttributedResult("a3", "approve_it"))
	if res.HasErrors() {
		t.Fatalf("fully-attributed ai decision must be accepted: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "approve" {
		t.Fatalf("expected routed to approve, got %q", r.Ctx.CurrentNode)
	}
	requireFoldEq(t, def, r)
}

// ---- Gate 2 + 推理归属:AI 决策可回答"哪个模型、哪个 prompt 版本" ----

func TestM1Gate_AIAttributionJournaledAndFolded(t *testing.T) {
	def := m1GateDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, &captureExecutor{}, WithJournal(j))

	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"},
		Event{ID: "s1", Name: "submit"})
	if res := r.Feed(aiAttributedResult("a1", "approve_it")); res.HasErrors() {
		t.Fatalf("ai decision rejected: %v", res.Errors)
	}

	// 1) 日志:回调的消费事实带模型 Actor——不窥探载荷即可回答归属问题。
	occ := findConsumed(t, j.Occurrences(), "a1")
	want := gateModelPrincipal()
	if occ.Actor == nil || !reflect.DeepEqual(*occ.Actor, want) {
		t.Fatalf("ai decision must be attributed to the model principal, got %+v", occ.Actor)
	}

	// 2) 折叠精确性:活上下文 == 折叠上下文(compareCtx 逐字段比较 principal)。
	requireFoldEq(t, def, r)

	// 3) 时间旅行:FoldTo 回到模型决策入账时刻,CurrentEvent.Principal 逐字段复现。
	folded, err := FoldTo(def, j.Occurrences(), occ.Seq)
	if err != nil {
		t.Fatalf("foldTo(%d): %v", occ.Seq, err)
	}
	if folded.CurrentEvent == nil || folded.CurrentEvent.Principal == nil ||
		!reflect.DeepEqual(*folded.CurrentEvent.Principal, want) {
		t.Fatalf("time travel must reproduce the model principal, got %+v", folded.CurrentEvent)
	}

	// 4) JSON 回传(持久化路径):模型归属三元组不丢失。
	data, err := marshalOccs(j.Occurrences())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back []Occurrence
	if err := json.Unmarshal(data, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if occ2 := findConsumed(t, back, "a1"); occ2.Actor == nil || !reflect.DeepEqual(*occ2.Actor, want) {
		t.Fatalf("json round-trip lost model attribution: %+v", occ2.Actor)
	}
}

// ---- Gate 2 总账:fold 后 principal 逐字段复现(人类 + 模型混合流)----

func TestM1Gate_FoldReproducesPrincipalsFieldByField(t *testing.T) {
	def := m1GateDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, &captureExecutor{}, WithJournal(j))

	submitter := &Principal{Kind: PrincipalHuman, ID: "u-2001", DisplayName: "李提交"}
	approver := &Principal{Kind: PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"},
		Event{ID: "s1", Name: "submit", Principal: submitter})
	r.Feed(aiAttributedResult("a1", "approve_it"))
	r.Feed(Event{ID: "a2", Name: "approve", Principal: approver})
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}

	// 全量折叠与活上下文一致(含全部归属)。
	requireFoldEq(t, def, r)

	occs := j.Occurrences()
	seenHuman, seenModel := false, false
	for i := range occs {
		if occs[i].Kind != OccEventConsumed || occs[i].Actor == nil {
			continue
		}
		folded, err := FoldTo(def, occs, occs[i].Seq)
		if err != nil {
			t.Fatalf("foldTo(%d): %v", occs[i].Seq, err)
		}
		if folded.CurrentEvent == nil || folded.CurrentEvent.Principal == nil ||
			!reflect.DeepEqual(*folded.CurrentEvent.Principal, *occs[i].Actor) {
			t.Fatalf("occ#%d: fold does not reproduce principal field-for-field: %+v vs %+v",
				occs[i].Seq, folded.CurrentEvent.Principal, occs[i].Actor)
		}
		switch occs[i].Actor.Kind {
		case PrincipalHuman:
			seenHuman = true
		case PrincipalModel:
			seenModel = true
		}
	}
	if !seenHuman || !seenModel {
		t.Fatalf("gate must exercise both human and model attribution (human=%v model=%v)", seenHuman, seenModel)
	}
}

// ---- 宿主显式归属优先于载荷派生 ----

func TestM1Gate_HostProvidedPrincipalWinsOverPayload(t *testing.T) {
	def := m1GateDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, &captureExecutor{}, WithJournal(j))

	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	// 宿主未给 principal、载荷携带归属:引擎派生模型 principal。
	r.Feed(aiAttributedResult("a1", "approve_it"))
	occ := findConsumed(t, j.Occurrences(), "a1")
	if occ.Actor == nil || occ.Actor.Kind != PrincipalModel || occ.Actor.Model != "gpt-4o" {
		t.Fatalf("payload attribution must be lifted to a model principal, got %+v", occ.Actor)
	}

	// 宿主显式给了 principal:优先于载荷(即使载荷也带归属,以宿主声明为准)。
	r.Feed(Event{ID: "a2", Name: "approve",
		Principal: &Principal{Kind: PrincipalHuman, ID: "u-1001", DisplayName: "王审批"}})
	occ = findConsumed(t, j.Occurrences(), "a2")
	if occ.Actor == nil || occ.Actor.Kind != PrincipalHuman || occ.Actor.ID != "u-1001" {
		t.Fatalf("host-provided principal must win, got %+v", occ.Actor)
	}
	requireFoldEq(t, def, r)

	// 宿主显式给完整模型 principal、载荷不带归属:同样成立(另一条通道)。
	j2 := NewMemoryJournal()
	r2 := NewRuntime(def, &captureExecutor{}, WithJournal(j2))
	r2.Start("i2", "e2", map[string]interface{}{"ticket": "T-2"}, Event{ID: "s1", Name: "submit"})
	hostModel := &Principal{
		Kind: PrincipalModel, ID: "claude-3-5", DisplayName: "Claude 3.5",
		Model: "claude-3-5", ModelVersion: "2024-10", PromptVersion: "triage-v9",
	}
	r2.Feed(Event{ID: "a1", Name: DefaultAIEventType, Principal: hostModel, Payload: map[string]interface{}{
		"choice": "approve_it",
		"output": map[string]interface{}{"confidence": 0.8},
	}})
	occ = findConsumed(t, j2.Occurrences(), "a1")
	if occ.Actor == nil || !reflect.DeepEqual(*occ.Actor, *hostModel) {
		t.Fatalf("host-provided model principal must be journaled verbatim, got %+v", occ.Actor)
	}
	requireFoldEq(t, def, r2)
}

// ---- 声明面:requirePrincipal 的解析与校验 ----

func TestM1Gate_RequirePrincipalDeclaration(t *testing.T) {
	// JSON 声明解析。
	def, err := ParseDSL([]byte(`{
		"id": "decl_flow", "version": "2.0",
		"nodes": [
			{ "id": "start", "type": "start",
			  "transitions": [{ "event": "submit", "next": "gate" }] },
			{ "id": "gate", "type": "approval", "requirePrincipal": true,
			  "transitions": [{ "event": "approve", "next": "end" }] },
			{ "id": "end", "type": "end" }
		]
	}`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !def.Nodes["gate"].RequirePrincipal {
		t.Fatal("requirePrincipal must be parsed from the declaration")
	}
	if res := Validate(def); !res.IsValid {
		t.Fatalf("requirePrincipal on an approval node must validate, got %v", res.Errors)
	}

	// ai 节点声明合法。
	def.Nodes["gate"] = &Node{ID: "gate", Type: "ai",
		Ai:               &AIConfig{Prompt: "p", Choose: []string{"x"}},
		Transitions:      []Transition{{Case: "x", Next: "end"}},
		RequirePrincipal: true}
	if res := Validate(def); !res.IsValid {
		t.Fatalf("requirePrincipal on an ai node must validate, got %v", res.Errors)
	}

	// 非决策节点上声明:部署期拒绝。
	def.Nodes["gate"] = &Node{ID: "gate", Type: "action", RequirePrincipal: true,
		Transitions: []Transition{{Next: "end"}}}
	if res := Validate(def); res.IsValid {
		t.Fatal("requirePrincipal on a non-decision node must fail validation")
	}
}
