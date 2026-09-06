package dsl

import (
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

// ai_test.go — 阶段 4:AI 原生节点(有界代理 + schema 约束输出)。

const aiTriageJSON = `{
  "id": "ai_triage", "version": "2.0",
  "nodes": [
    { "id": "start", "type": "start",
      "transitions": [{ "event": "submit", "next": "triage" }] },
    { "id": "triage", "type": "ai",
      "ai": {
        "prompt": "classify ticket {{ticket}} for customer {{customer.name}}",
        "output": { "confidence": { "type": "number" } },
        "choose": ["billing", "technical", "other"],
        "onError": "manual_review"
      },
      "transitions": [
        { "case": "billing",   "next": "billing" },
        { "case": "technical", "next": "technical" },
        { "case": "other",     "next": "other" }
      ] },
    { "id": "billing",   "type": "approval", "transitions": [{ "event": "approve", "next": "end" }] },
    { "id": "technical", "type": "approval", "transitions": [{ "event": "approve", "next": "end" }] },
    { "id": "other",     "type": "approval", "transitions": [{ "event": "approve", "next": "end" }] },
    { "id": "manual_review", "type": "approval",
      "transitions": [{ "event": "approve", "next": "end" }] },
    { "id": "end", "type": "end" }
  ]
}`

// captureExecutor 捕获推理命令供断言。
type captureExecutor struct {
	mu   sync.Mutex
	cmds []SideEffectCommand
}

func (c *captureExecutor) Handle(_ *ExecutionContext, cmd SideEffectCommand) SideEffectResult {
	c.mu.Lock()
	c.cmds = append(c.cmds, cmd)
	c.mu.Unlock()
	return SideEffectResult{CommandID: cmd.ID, Status: "completed"}
}

func (c *captureExecutor) last() SideEffectCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cmds[len(c.cmds)-1]
}

func aiDef(t *testing.T) *ProcessDef {
	t.Helper()
	def, err := ParseDSL([]byte(aiTriageJSON))
	if err != nil {
		t.Fatalf("parse ai dsl: %v", err)
	}
	return def
}

func aiResultEvent(id, choice string, confidence float64) Event {
	return Event{ID: id, Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": choice,
		"output": map[string]interface{}{"confidence": confidence},
	}}
}

func TestAI_ParkEmitsBoundedRequest(t *testing.T) {
	def := aiDef(t)
	cap := &captureExecutor{}
	r := NewRuntime(def, cap, WithJournal(NewMemoryJournal()))

	r.Start("i", "e", map[string]interface{}{
		"ticket": "T-1", "customer": map[string]interface{}{"name": "Acme"},
	}, Event{ID: "s1", Name: "submit"})

	if r.Status() != StatusWaiting || r.Ctx.CurrentNode != "triage" {
		t.Fatalf("expected parked at ai node, got %s / %q", r.Status(), r.Ctx.CurrentNode)
	}
	if len(cap.cmds) != 1 {
		t.Fatalf("expected exactly one inference request, got %d", len(cap.cmds))
	}
	cmd := cap.last()
	if cmd.Type != DefaultAICommandType || cmd.Target != DefaultAITarget {
		t.Fatalf("unexpected command type/target: %s/%s", cmd.Type, cmd.Target)
	}
	var req map[string]interface{}
	if err := json.Unmarshal(cmd.Payload, &req); err != nil {
		t.Fatalf("payload: %v", err)
	}
	if req["prompt"] != "classify ticket T-1 for customer Acme" {
		t.Fatalf("prompt not interpolated: %v", req["prompt"])
	}
	cands, _ := req["candidates"].([]interface{})
	if len(cands) != 3 {
		t.Fatalf("candidates must be declared to the model, got %v", req["candidates"])
	}
	if _, ok := req["output_schema"]; !ok {
		t.Fatal("output schema must be declared to the model")
	}
	// 停靠型槽位(Until 零值)不产生唤醒时刻。
	if _, ok := r.NextWakeup(); ok {
		t.Fatal("ai node without deadline must not schedule a wakeup")
	}
}

func TestAI_ChoiceRoutesWithSchemaOutput(t *testing.T) {
	def := aiDef(t)
	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	res := r.Feed(aiResultEvent("a1", "billing", 0.9))
	if res.HasErrors() {
		t.Fatalf("ai result rejected: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "billing" {
		t.Fatalf("expected routed to billing, got %q", r.Ctx.CurrentNode)
	}
	if conf, ok := r.Ctx.Variables["confidence"].(float64); !ok || conf != 0.9 {
		t.Fatalf("schema output must be written to variables, got %v", r.Ctx.Variables["confidence"])
	}
	requireFoldEq(t, def, r)
}

func TestAI_BoundedAgencyRejectsOutsideChoice(t *testing.T) {
	def := aiDef(t)
	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	// 模型试图选择候选集之外的分支:有界代理拦截,升级到 onError。
	res := r.Feed(aiResultEvent("a1", "delete_all_data", 1.0))
	if res.HasErrors() {
		t.Fatalf("onError must absorb the violation: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "manual_review" {
		t.Fatalf("expected escalation to manual_review, got %q", r.Ctx.CurrentNode)
	}
}

func TestAI_InferenceFailureEscalates(t *testing.T) {
	def := aiDef(t)
	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	res := r.Feed(Event{ID: "a1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"error": "model timeout",
	}})
	if res.HasErrors() {
		t.Fatalf("onError must absorb inference failure: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "manual_review" {
		t.Fatalf("expected escalation, got %q", r.Ctx.CurrentNode)
	}
}

func TestAI_SchemaViolationEscalates(t *testing.T) {
	def := aiDef(t)
	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	// confidence 声明为 number,模型回了 string:模型输出畸形属于推理失败类,
	// 升级到 onError(人工复核),而不是炸掉实例;违约值不写入变量。
	res := r.Feed(Event{ID: "a1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "billing",
		"output": map[string]interface{}{"confidence": "very-high"},
	}})
	if res.HasErrors() {
		t.Fatalf("onError must absorb schema violation: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "manual_review" {
		t.Fatalf("expected escalation to manual_review, got %q", r.Ctx.CurrentNode)
	}
	if _, exists := r.Ctx.Variables["confidence"]; exists {
		t.Fatal("schema-invalid output must not be written")
	}

	// 未声明 onError 时,数据违约使节点失败(显式失败优于静默吞掉)。
	def2 := aiDef(t)
	def2.Nodes["triage"].Ai.OnError = ""
	r2 := NewRuntime(def2, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r2.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})
	res = r2.Feed(Event{ID: "a1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "billing",
		"output": map[string]interface{}{"confidence": "very-high"},
	}})
	if !res.HasErrors() || r2.Status() != StatusFailed {
		t.Fatalf("schema violation without onError must fail the node, got %v / %s", res.Errors, r2.Status())
	}
}

func TestAI_WithoutChoose_UsesWhenRouting(t *testing.T) {
	def := &ProcessDef{
		ID: "ai_enrich", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "summarize"}}},
			"summarize": {ID: "summarize", Type: "ai",
				Ai: &AIConfig{
					Prompt:      "summarize {{text}}",
					OutputTypes: map[string]*Type{"priority": EnumType("low", "high")},
				},
				Transitions: []Transition{
					{When: "priority == \"high\"", Next: "urgent"},
					{Next: "normal"},
				}},
			"urgent": {ID: "urgent", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"normal": {ID: "normal", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":    {ID: "end", Type: "end"},
		},
	}
	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"text": "server on fire"}, Event{ID: "s1", Name: "go"})
	res := r.Feed(Event{ID: "a1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"output": map[string]interface{}{"priority": "high"},
	}})
	if res.HasErrors() || r.Ctx.CurrentNode != "urgent" {
		t.Fatalf("expected when-routed to urgent, got %v / %q", res.Errors, r.Ctx.CurrentNode)
	}
}

func TestAI_DeadlineEscalatesWhenModelSilent(t *testing.T) {
	def := aiDef(t)
	def.Nodes["triage"].Ai.OnError = ""
	def.Nodes["triage"].Deadline = &DeadlineConfig{After: "1h", Next: "manual_review"}

	r := NewRuntime(def, &captureExecutor{}, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"ticket": "T-1"}, Event{ID: "s1", Name: "submit"})

	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("ai deadline must schedule a wakeup")
	}
	if res := r.WakeDue(until.Add(time.Minute)); res.HasErrors() {
		t.Fatalf("deadline wake failed: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "manual_review" {
		t.Fatalf("expected deadline escalation, got %q", r.Ctx.CurrentNode)
	}
}

func TestAI_ParallelBranchesGetDistinctRequests(t *testing.T) {
	def := &ProcessDef{
		ID: "ai_parallel", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "a1"}, {Next: "a2"}}},
			"a1": {ID: "a1", Type: "ai",
				Ai:          &AIConfig{Prompt: "left {{v}}", Choose: []string{"x"}},
				Transitions: []Transition{{Case: "x", Next: "join"}}},
			"a2": {ID: "a2", Type: "ai",
				Ai:          &AIConfig{Prompt: "right {{v}}", Choose: []string{"x"}},
				Transitions: []Transition{{Case: "x", Next: "join"}}},
			"join": {ID: "join", Type: "join", Transitions: []Transition{{Next: "end"}}},
			"end":  {ID: "end", Type: "end"},
		},
	}
	cap := &captureExecutor{}
	r := NewRuntime(def, cap, WithJournal(NewMemoryJournal()))
	r.Start("i", "e", map[string]interface{}{"v": "1"}, Event{ID: "s1", Name: "go"})

	if len(cap.cmds) != 2 {
		t.Fatalf("expected 2 inference requests (one per branch), got %d", len(cap.cmds))
	}
	if cap.cmds[0].ID == cap.cmds[1].ID {
		t.Fatal("branch inference requests must have distinct idempotency keys")
	}

	// 分支推进顺序随机(引擎按 map 序推进并行分支),按载荷内容定位两份请求。
	var leftID, rightID string
	for _, c := range cap.cmds {
		switch {
		case strings.Contains(string(c.Payload), "left 1"):
			leftID = c.ID
		case strings.Contains(string(c.Payload), "right 1"):
			rightID = c.ID
		}
	}
	if leftID == "" || rightID == "" || leftID == rightID {
		t.Fatalf("expected two distinct interpolated requests, got %q / %q", leftID, rightID)
	}

	// 两条分支各自回调(request_id 精确关联),汇合完成。
	r1 := Event{ID: "r1", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "x", "output": map[string]interface{}{}, "request_id": leftID,
	}}
	r2 := Event{ID: "r2", Name: DefaultAIEventType, Payload: map[string]interface{}{
		"choice": "x", "output": map[string]interface{}{}, "request_id": rightID,
	}}
	if res := r.Feed(r1); res.HasErrors() {
		t.Fatalf("branch1 result: %v", res.Errors)
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("expected still waiting after one ai branch, got %s", r.Status())
	}
	if res := r.Feed(r2); res.HasErrors() {
		t.Fatalf("branch2 result: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed after both ai branches, got %s", r.Status())
	}
}

func TestAI_ValidatorRules(t *testing.T) {
	// case 不在候选集内。
	def := aiDef(t)
	def.Nodes["triage"].Transitions = append(def.Nodes["triage"].Transitions,
		Transition{Case: "hack", Next: "billing"})
	if res := Validate(def); res.IsValid {
		t.Fatal("expected case outside choose to fail validation")
	}

	// 候选集缺对应 case。
	def2 := aiDef(t)
	def2.Nodes["triage"].Ai.Choose = append(def2.Nodes["triage"].Ai.Choose, "vip")
	if res := Validate(def2); res.IsValid {
		t.Fatal("expected choose candidate without case to fail validation")
	}

	// prompt 为空。
	def3 := aiDef(t)
	def3.Nodes["triage"].Ai.Prompt = "  "
	if res := Validate(def3); res.IsValid {
		t.Fatal("expected empty prompt to fail validation")
	}

	// onError 目标不存在。
	def4 := aiDef(t)
	def4.Nodes["triage"].Ai.OnError = "nope"
	if res := Validate(def4); res.IsValid {
		t.Fatal("expected missing onError target to fail validation")
	}

	// case 用在非 ai 节点上。
	def5 := aiDef(t)
	def5.Nodes["billing"].Transitions = []Transition{{Case: "x", Next: "end"}}
	if res := Validate(def5); res.IsValid {
		t.Fatal("expected case on non-ai node to fail validation")
	}

	// 合法定义应当通过。
	if res := Validate(aiDef(t)); !res.IsValid {
		t.Fatalf("valid ai def must pass, got %v", res.Errors)
	}
}
