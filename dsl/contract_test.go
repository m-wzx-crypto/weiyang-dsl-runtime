package dsl

import (
	"strings"
	"testing"
)

// v2 数据契约测试:变量 schema、节点 input/output 映射、类型检查接入 Validate。

const contractV2JSON = `{
  "id": "expense_approval",
  "name": "Expense Approval",
  "version": "2.0",
  "variables": {
    "amount": { "type": "money", "init": 0 },
    "requester": { "type": "string" },
    "order": { "type": "object", "fields": { "vip": "boolean", "level": { "type": "enum", "values": ["low", "high"] } } }
  },
  "nodes": [
    { "id": "start", "type": "start",
      "transitions": [{ "event": "submit", "next": "check" }] },
    { "id": "check", "type": "condition",
      "input":  { "limit": "order.vip ? 10000 : 1000" },
      "output": { "channel": "amount > limit ? \"gm\" : \"manager\"" },
      "transitions": [
        { "when": "amount > limit", "next": "gm_approve" },
        { "next": "manager_approve" }
      ] },
    { "id": "gm_approve", "type": "approval",
      "transitions": [{ "event": "approve", "next": "record" }, { "event": "reject", "next": "end" }] },
    { "id": "manager_approve", "type": "approval",
      "transitions": [{ "event": "approve", "next": "record" }, { "event": "reject", "next": "end" }] },
    { "id": "record", "type": "action",
      "output": { "closed": "true" },
      "transitions": [{ "next": "end" }] },
    { "id": "end", "type": "end" }
  ]
}`

func mustParseV2Contract(t *testing.T) *ProcessDef {
	t.Helper()
	def, err := ParseDSL([]byte(contractV2JSON))
	if err != nil {
		t.Fatalf("parse v2 contract DSL: %v", err)
	}
	return def
}

func TestV2_VariableSchemaAndInit(t *testing.T) {
	def := mustParseV2Contract(t)
	if def.VarSchema["amount"] == nil || def.VarSchema["amount"].Kind != TypeNumber {
		t.Fatalf("expected amount declared as number/money, got %v", def.VarSchema["amount"])
	}
	if def.VarSchema["order"].Kind != TypeObject {
		t.Fatalf("expected order declared as object, got %v", def.VarSchema["order"].Kind)
	}
	if def.VarInit["amount"] != float64(0) {
		t.Fatalf("expected init amount=0, got %v", def.VarInit["amount"])
	}

	// schema 可以喂给 TypeChecker:类型错误的表达式应被检出。
	schema := def.TypeSchema()
	if schema == nil {
		t.Fatal("expected non-nil TypeSchema")
	}
	if errs := DefaultExpressionEngine.TypeCheck(`amount > "hello"`, schema); len(errs) == 0 {
		t.Fatal("expected type error for amount > string")
	}
	if errs := DefaultExpressionEngine.TypeCheck(`amount > 100 && order.vip`, schema); len(errs) != 0 {
		t.Fatalf("expected clean type check, got %v", errs)
	}
}

func TestV2_InputMappingScopesCondition(t *testing.T) {
	def := mustParseV2Contract(t)
	exec := NewInMemorySideEffectExecutor()
	r := NewRuntime(def, exec)

	// VIP 订单:limit=10000,amount=5000 → 不超限 → manager。
	r.Start("inst-1", "exec-1", map[string]interface{}{
		"amount": float64(5000), "order": map[string]interface{}{"vip": true, "level": "high"},
	}, Event{ID: "e1", Name: "submit"})
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting at an approval, got %s", r.Status())
	}
	if got := r.Ctx.CurrentNode; got != "manager_approve" {
		t.Fatalf("expected vip 5000 -> manager_approve, got %q", got)
	}
	// output 映射已把路由结论写回变量。
	if ch, _ := r.Ctx.Variables["channel"].(string); ch != "manager" {
		t.Fatalf("expected channel=manager written by output mapping, got %v", r.Ctx.Variables["channel"])
	}

	// 普通订单:limit=1000,amount=5000 → 超 limit → gm。
	r2 := NewRuntime(def, NewInMemorySideEffectExecutor())
	r2.Start("inst-2", "exec-2", map[string]interface{}{
		"amount": float64(5000), "order": map[string]interface{}{"vip": false, "level": "low"},
	}, Event{ID: "e1", Name: "submit"})
	if got := r2.Ctx.CurrentNode; got != "gm_approve" {
		t.Fatalf("expected 5000 -> gm_approve, got %q", got)
	}
}

func TestV2_OutputTypeMismatchFailsNode(t *testing.T) {
	def := mustParseV2Contract(t)
	// 篡改 record 节点:output 写入与 schema 冲突的值(closed 是未声明变量,
	// 这里改用 amount 承接字符串,触发 money 类型校验失败)。
	def.Nodes["record"].Output = map[string]string{"amount": `"not a number"`}

	r := NewRuntime(def, NewInMemorySideEffectExecutor())
	r.Start("inst-1", "exec-1", map[string]interface{}{
		"amount": float64(100), "order": map[string]interface{}{"vip": true, "level": "high"},
	}, Event{ID: "e1", Name: "submit"})
	// manager approve -> record:output 类型冲突,实例失败。
	res := r.Feed(Event{ID: "e2", Name: "approve"})
	if !res.HasErrors() {
		t.Fatalf("expected type mismatch error, got %+v", res.Errors)
	}
	if r.Status() != StatusFailed {
		t.Fatalf("expected failed status, got %s", r.Status())
	}
	if _, ok := r.Ctx.Variables["amount"].(string); ok {
		t.Fatal("type-invalid value must not be written into variables")
	}
}

func TestV2_TypeCheckWiredIntoValidate(t *testing.T) {
	def := mustParseV2Contract(t)
	// 注入一个类型上不可能成立的条件:order.vip 是 boolean,不能与字符串比较。
	def.Nodes["check"].Transitions[0].When = `order.vip > "yes"`
	res := Validate(def)
	if res.IsValid {
		t.Fatal("expected validation to catch type error via schema")
	}
	found := false
	for _, e := range res.Errors {
		if strings.Contains(e.Error(), "order.vip") {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected type error mentioning order.vip, got %v", res.Errors)
	}

	// 对照:v1(无 schema)同样定义不报类型错——类型契约是 opt-in 的。
	v1 := mustParseV2Contract(t)
	v1.VarSchema = nil
	v1.VarInit = nil
	if res := Validate(v1); !res.IsValid {
		t.Fatalf("v1 without schema should not type-check, got %v", res.Errors)
	}
}

func TestV1_DefinitionsUnaffected(t *testing.T) {
	// v1 JSON 里即使误带 v2 字段也按 v1 语义忽略,不报错。
	v1WithExtras := `{
      "id": "legacy", "version": "1.0",
      "variables": { "amount": { "type": "money" } },
      "nodes": [
        { "id": "start", "type": "start",
          "input": { "x": "1" }, "deadline": { "after": "1h", "next": "start" },
          "transitions": [{ "event": "submit", "next": "end" }] },
        { "id": "end", "type": "end" }
      ]
    }`
	def, err := ParseDSL([]byte(v1WithExtras))
	if err != nil {
		t.Fatalf("v1 parse must ignore v2 fields: %v", err)
	}
	if def.VarSchema != nil {
		t.Fatal("v1 must not build variable schema")
	}
	if def.Nodes["start"].Deadline != nil {
		t.Fatal("v1 must not parse deadline config")
	}
}
