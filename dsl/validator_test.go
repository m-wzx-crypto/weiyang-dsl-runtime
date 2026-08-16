package dsl

import (
	"testing"
)

func TestValidate_ValidSimpleApproval(t *testing.T) {
	data := loadTestData(t, "simple_approval_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Validate(def)
	if !result.IsValid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidate_ValidBranchFlow(t *testing.T) {
	data := loadTestData(t, "branch_flow_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Validate(def)
	if !result.IsValid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidate_ValidCycleFlow(t *testing.T) {
	data := loadTestData(t, "cycle_flow_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Validate(def)
	if !result.IsValid {
		t.Errorf("expected valid, got errors: %v", result.Errors)
	}
}

func TestValidate_InvalidNodeType(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "bad"}}},
			"bad":   {ID: "bad", Type: "invalid_type", Transitions: []Transition{}},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for bad node type")
	}
	found := false
	for _, e := range result.Errors {
		if e.Path == "nodes[bad].type" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected error at nodes[bad].type, got: %v", result.Errors)
	}
}

func TestValidate_ConditionNodeTooFewTransitions(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "cond"}}},
			"cond":  {ID: "cond", Type: "condition", Transitions: []Transition{{Event: "yes", Next: "end"}}},
			"end":   {ID: "end", Type: "end"},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for condition with < 2 transitions")
	}
}

func TestValidate_ConditionNodeRequiresDefaultTransition(t *testing.T) {
	// DSL-7：condition 节点的全部 transition 都带 when 且无默认分支时必须报错。
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "cond"}}},
			"cond": {
				ID:   "cond",
				Type: "condition",
				Transitions: []Transition{
					{When: "amount > 10000", Next: "end"},
					{When: "amount <= 10000", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for condition without default transition")
	}
	found := false
	for _, e := range result.Errors {
		if e.Description == "condition_node_requires_default_transition" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected condition_node_requires_default_transition error, got: %v", result.Errors)
	}
}

func TestValidate_OrphanNode(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":  {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":    {ID: "end", Type: "end"},
			"orphan": {ID: "orphan", Type: "approval", Transitions: []Transition{{Event: "next", Next: "end"}}},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for orphan node")
	}
	found := false
	for _, e := range result.Errors {
		if e.Path == "nodes[orphan]" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected orphan error, got: %v", result.Errors)
	}
}

func TestValidate_UnreachableNode(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":       {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "end"}}},
			"end":         {ID: "end", Type: "end"},
			"unreachable": {ID: "unreachable", Type: "approval", Transitions: []Transition{{Event: "next", Next: "end"}}},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for unreachable node")
	}
}

func TestValidate_TransitionToNonExistent(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "ghost"}}},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for transition to non-existent node")
	}
}

func TestValidate_MissingProcessID(t *testing.T) {
	def := &ProcessDef{
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start"},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for missing process id")
	}
}

func TestValidate_MissingNodeID(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start"},
			"":      {ID: "", Type: "end"},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for missing node id")
	}
}

func TestValidate_MissingNodeType(t *testing.T) {
	def := &ProcessDef{
		ID:        "test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start":  {ID: "start", Type: "start", Transitions: []Transition{{Event: "next", Next: "notype"}}},
			"notype": {ID: "notype", Type: ""},
		},
	}

	result := Validate(def)
	if result.IsValid {
		t.Fatal("expected invalid for missing node type")
	}
}
