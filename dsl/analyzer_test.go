package dsl

import "testing"

func TestAnalyze_UnreachableNode(t *testing.T) {
	def := &ProcessDef{
		ID:        "a",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":       {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":         {ID: "end", Type: "end"},
			"unreachable": {ID: "unreachable", Type: "approval", Transitions: []Transition{{Event: "next", Next: "end"}}},
		},
	}

	report := Analyze(def)
	if report.IsClean {
		t.Fatal("expected non-clean report")
	}
	if !hasDiagnostic(report, "unreachable_node") {
		t.Errorf("expected unreachable_node diagnostic, got %+v", report.Diagnostics)
	}
}

func TestAnalyze_DeadEnd(t *testing.T) {
	def := &ProcessDef{
		ID:        "a",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":  {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "dead"}}},
			"dead":   {ID: "dead", Type: "approval"}, // 非终止节点无出口
		},
	}

	report := Analyze(def)
	if !hasDiagnostic(report, "dead_end") {
		t.Errorf("expected dead_end diagnostic, got %+v", report.Diagnostics)
	}
}

func TestAnalyze_Cycle(t *testing.T) {
	def := &ProcessDef{
		ID:        "a",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "a"}}},
			"a":     {ID: "a", Type: "action", Transitions: []Transition{{Event: "again", Next: "b"}, {Event: "finish", Next: "end"}}},
			"b":     {ID: "b", Type: "action", Transitions: []Transition{{Event: "back", Next: "a"}}},
			"end":   {ID: "end", Type: "end"},
		},
	}

	report := Analyze(def)
	if !hasDiagnostic(report, "cycle") {
		t.Errorf("expected cycle diagnostic, got %+v", report.Diagnostics)
	}
}

func TestAnalyze_InvalidTerminal(t *testing.T) {
	def := &ProcessDef{
		ID:        "a",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":          {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":            {ID: "end", Type: "end", Transitions: []Transition{{Event: "next", Next: "start"}}},
		},
	}

	report := Analyze(def)
	if !hasDiagnostic(report, "invalid_terminal") {
		t.Errorf("expected invalid_terminal diagnostic, got %+v", report.Diagnostics)
	}
}

func TestAnalyze_CleanFlow(t *testing.T) {
	def := &ProcessDef{
		ID:        "a",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":   {ID: "end", Type: "end"},
		},
	}

	report := Analyze(def)
	if !report.IsClean {
		t.Errorf("expected clean report, got %+v", report.Diagnostics)
	}
}

func hasDiagnostic(report AnalysisReport, code string) bool {
	for _, d := range report.Diagnostics {
		if d.Code == code {
			return true
		}
	}
	return false
}

// ---- Type System / Type Checker ----

func TestTypeCheck_RejectsTypeMismatch(t *testing.T) {
	schema := NewTypeSchema()
	schema.Declare("amount", NumberType())

	errs := DefaultExpressionEngine.TypeCheck(`amount > "hello"`, schema)
	if len(errs) == 0 {
		t.Fatal("expected type error for number > string")
	}
}

func TestTypeCheck_AcceptsValid(t *testing.T) {
	schema := NewTypeSchema()
	schema.Declare("amount", NumberType())
	schema.Declare("level", StringType())

	errs := DefaultExpressionEngine.TypeCheck(`amount > 1000 && level == "vip"`, schema)
	if len(errs) != 0 {
		t.Errorf("expected no type errors, got %v", errs)
	}
}

func TestTypeCheck_MemberAccess(t *testing.T) {
	schema := NewTypeSchema()
	order := ObjectType(
		NewField("amount", NumberType()),
		NewField("status", EnumType("pending", "approved")),
	)
	schema.Declare("order", order)

	errs := DefaultExpressionEngine.TypeCheck(`order.status == "approved"`, schema)
	if len(errs) != 0 {
		t.Errorf("expected no type errors for enum member access, got %v", errs)
	}

	// 数值字段与字符串比较应报类型错误。
	errs = DefaultExpressionEngine.TypeCheck(`order.amount == "big"`, schema)
	if len(errs) == 0 {
		t.Error("expected type error comparing number field to string")
	}
}