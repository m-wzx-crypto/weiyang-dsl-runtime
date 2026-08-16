package dsl

import (
	"testing"
)

func TestExecute_NormalTransition(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "submit", Next: "approve"},
				},
			},
			"approve": {
				ID:   "approve",
				Type: "approval",
				SideEffects: []SideEffect{
					{Type: "notify", Target: "manager"},
				},
				Transitions: []Transition{
					{Event: "approve", Next: "end"},
					{Event: "reject", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	output := Execute(def, "start", "submit", nil)
	if output.Status != "running" {
		t.Errorf("expected status 'running', got %q", output.Status)
	}
	if output.NextNode != "approve" {
		t.Errorf("expected next node 'approve', got %q", output.NextNode)
	}

	output = Execute(def, "approve", "approve", nil)
	if output.Status != "running" {
		t.Errorf("expected status 'running', got %q", output.Status)
	}
	if output.NextNode != "end" {
		t.Errorf("expected next node 'end', got %q", output.NextNode)
	}
	if len(output.SideEffects) != 1 {
		t.Errorf("expected 1 side effect, got %d", len(output.SideEffects))
	}
	if output.SideEffects[0].Type != "notify" {
		t.Errorf("expected side effect type 'notify', got %q", output.SideEffects[0].Type)
	}
}

func TestExecute_EndNode(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":   {ID: "end", Type: "end"},
		},
	}

	output := Execute(def, "end", "", nil)
	if output.Status != "completed" {
		t.Errorf("expected status 'completed', got %q", output.Status)
	}
	if output.NextNode != "" {
		t.Errorf("expected empty next node for end, got %q", output.NextNode)
	}
}

func TestExecute_InvalidEvent(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "submit", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	output := Execute(def, "start", "nonexistent_event", nil)
	if output.Status != "error" {
		t.Errorf("expected status 'error', got %q", output.Status)
	}
	if output.Error == nil {
		t.Error("expected error for invalid event")
	}
}

func TestExecute_NodeNotFound(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start"},
		},
	}

	output := Execute(def, "nonexistent", "event", nil)
	if output.Status != "error" {
		t.Errorf("expected status 'error', got %q", output.Status)
	}
	if output.Error == nil {
		t.Error("expected error for nonexistent node")
	}
}

func TestExecute_ConditionWhen(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "submit", Next: "check"},
				},
			},
			"check": {
				ID:   "check",
				Type: "condition",
				Transitions: []Transition{
					{Event: "above_threshold", When: "amount > 10000", Next: "high"},
					{Event: "below_threshold", When: "amount <= 10000", Next: "low"},
				},
			},
			"high": {ID: "high", Type: "end"},
			"low":  {ID: "low", Type: "end"},
		},
	}

	output := Execute(def, "check", "", map[string]interface{}{"amount": 15000.0})
	if output.Status != "running" {
		t.Fatalf("expected status 'running', got %q", output.Status)
	}
	if output.NextNode != "high" {
		t.Errorf("expected next 'high' for amount 15000, got %q", output.NextNode)
	}

	output = Execute(def, "check", "", map[string]interface{}{"amount": 5000.0})
	if output.NextNode != "low" {
		t.Errorf("expected next 'low' for amount 5000, got %q", output.NextNode)
	}
}

func TestExecute_ConditionWhenFallbackToEvent(t *testing.T) {
	// 无 when 的 condition 节点应回退到按 event 匹配，保持向后兼容。
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"check": {
				ID:   "check",
				Type: "condition",
				Transitions: []Transition{
					{Event: "above_threshold", Next: "high"},
					{Event: "below_threshold", Next: "low"},
				},
			},
			"high": {ID: "high", Type: "end"},
			"low":  {ID: "low", Type: "end"},
		},
	}

	output := Execute(def, "check", "above_threshold", nil)
	if output.Status != "running" {
		t.Fatalf("expected status 'running', got %q", output.Status)
	}
	if output.NextNode != "high" {
		t.Errorf("expected next 'high', got %q", output.NextNode)
	}
}

func TestExecute_ConditionDefaultBranch(t *testing.T) {
	// DSL-7：全部 when 不匹配时，第一个无 when 的 transition 作为默认分支。
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "submit", Next: "check"},
				},
			},
			"check": {
				ID:   "check",
				Type: "condition",
				Transitions: []Transition{
					{When: "amount > 10000", Next: "high"},
					{When: "amount > 5000", Next: "mid"},
					{Event: "fallback", Next: "low"},
				},
			},
			"high": {ID: "high", Type: "end"},
			"mid":  {ID: "mid", Type: "end"},
			"low":  {ID: "low", Type: "end"},
		},
	}

	// 两个 when 都不匹配 → 走默认分支 low。
	output := Execute(def, "check", "", map[string]interface{}{"amount": 1000.0})
	if output.Status != "running" {
		t.Fatalf("expected status 'running', got %q", output.Status)
	}
	if output.NextNode != "low" {
		t.Errorf("expected default branch 'low', got %q", output.NextNode)
	}

	// 带 when 分支命中时优先于默认分支。
	output = Execute(def, "check", "", map[string]interface{}{"amount": 20000.0})
	if output.NextNode != "high" {
		t.Errorf("expected branch 'high', got %q", output.NextNode)
	}
}

func TestExecute_ConditionWhenError(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"check": {
				ID:   "check",
				Type: "condition",
				Transitions: []Transition{
					{When: "amount > 10000", Next: "high"},
				},
			},
			"high": {ID: "high", Type: "end"},
		},
	}

	// 变量缺失时求值应报错。
	output := Execute(def, "check", "", nil)
	if output.Status != "error" {
		t.Fatalf("expected status 'error', got %q", output.Status)
	}
	if output.Error == nil {
		t.Error("expected error when variable is missing")
	}
}

func TestGetNextSteps(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "approve", Next: "end"},
					{Event: "reject", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	steps := GetNextSteps(def, "start")
	if len(steps) != 2 {
		t.Errorf("expected 2 next steps, got %d", len(steps))
	}

	steps = GetNextSteps(def, "end")
	if len(steps) != 0 {
		t.Errorf("expected 0 next steps for end node, got %d", len(steps))
	}

	steps = GetNextSteps(def, "nonexistent")
	if steps != nil {
		t.Errorf("expected nil for nonexistent node, got %v", steps)
	}
}

func TestGetCurrentNode(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Label: "Begin"},
		},
	}

	node := GetCurrentNode(def, "start")
	if node == nil {
		t.Fatal("expected node, got nil")
	}
	if node.Label != "Begin" {
		t.Errorf("expected label 'Begin', got %q", node.Label)
	}

	node = GetCurrentNode(def, "nonexistent")
	if node != nil {
		t.Errorf("expected nil for nonexistent node, got %v", node)
	}
}

func TestExecute_SimpleApprovalFlow(t *testing.T) {
	data := loadTestData(t, "simple_approval_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	output := Execute(def, "start", "submit", nil)
	if output.NextNode != "manager_approve" {
		t.Errorf("step 1: expected next 'manager_approve', got %q", output.NextNode)
	}

	output = Execute(def, "manager_approve", "approve", nil)
	if output.NextNode != "end" {
		t.Errorf("step 2: expected next 'end', got %q", output.NextNode)
	}

	output = Execute(def, "end", "", nil)
	if output.Status != "completed" {
		t.Errorf("step 3: expected status 'completed', got %q", output.Status)
	}
}
