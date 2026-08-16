package dsl

import (
	"testing"
)

func TestSimulate_SimpleApproval(t *testing.T) {
	data := loadTestData(t, "simple_approval_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Simulate(def)

	if len(result.Paths) == 0 {
		t.Fatal("expected at least one path")
	}

	completeCount := 0
	for _, p := range result.Paths {
		if p.IsComplete {
			completeCount++
		}
		if p.IsAbnormal {
			t.Errorf("path %d should not be abnormal", p.PathIndex)
		}
	}

	if completeCount < 2 {
		t.Errorf("expected at least 2 complete paths (approve/reject), got %d", completeCount)
	}

	hasApprovePath := false
	hasRejectPath := false
	for _, p := range result.Paths {
		for _, node := range p.NodeSequence {
			if node == "end" {
				hasApprovePath = true
			}
			if node == "rejected_end" {
				hasRejectPath = true
			}
		}
	}
	if !hasApprovePath {
		t.Error("expected a path ending at 'end'")
	}
	if !hasRejectPath {
		t.Error("expected a path ending at 'rejected_end'")
	}
}

func TestSimulate_BranchFlow(t *testing.T) {
	data := loadTestData(t, "branch_flow_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Simulate(def)

	if len(result.Paths) < 3 {
		t.Errorf("expected at least 3 paths for branch flow, got %d", len(result.Paths))
	}

	completeCount := 0
	for _, p := range result.Paths {
		if p.IsComplete {
			completeCount++
		}
	}
	if completeCount < 3 {
		t.Errorf("expected at least 3 complete paths, got %d", completeCount)
	}
}

func TestSimulate_CycleDetection(t *testing.T) {
	data := loadTestData(t, "cycle_flow_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	result := Simulate(def)

	if len(result.CyclesDetected) == 0 {
		t.Error("expected cycle detection in cycle_flow")
	}

	hasComplete := false
	for _, p := range result.Paths {
		if p.IsComplete {
			hasComplete = true
		}
	}
	if !hasComplete {
		t.Error("expected at least one complete path (approve path)")
	}

	hasAbnormal := false
	for _, p := range result.Paths {
		if p.IsAbnormal {
			hasAbnormal = true
		}
	}
	if !hasAbnormal {
		t.Error("expected at least one abnormal path (reject→revise→review loop)")
	}
}

func TestSimulate_LinearFlow(t *testing.T) {
	def := &ProcessDef{
		ID:        "linear",
		Version:   "1.0",
		StartNode: "a",
		Nodes: map[string]*Node{
			"a": {ID: "a", Type: "start", Transitions: []Transition{{Event: "next", Next: "b"}}},
			"b": {ID: "b", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "c"}}},
			"c": {ID: "c", Type: "end"},
		},
	}

	result := Simulate(def)

	if len(result.Paths) != 1 {
		t.Errorf("expected 1 path for linear flow, got %d", len(result.Paths))
	}
	if len(result.Paths) > 0 {
		p := result.Paths[0]
		if !p.IsComplete {
			t.Error("expected complete path")
		}
		if len(p.NodeSequence) != 3 {
			t.Errorf("expected 3 nodes in path, got %d", len(p.NodeSequence))
		}
		if len(p.EventSequence) != 2 {
			t.Errorf("expected 2 events in path, got %d", len(p.EventSequence))
		}
	}
}

func TestSimulate_NoStartNode(t *testing.T) {
	def := &ProcessDef{
		ID:      "empty",
		Version: "1.0",
		Nodes:   map[string]*Node{},
	}

	result := Simulate(def)
	if len(result.Paths) != 0 {
		t.Errorf("expected 0 paths for empty process, got %d", len(result.Paths))
	}
}
