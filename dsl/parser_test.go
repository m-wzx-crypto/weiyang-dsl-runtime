package dsl

import (
	"os"
	"testing"
)

func loadTestData(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("failed to load test data %s: %v", name, err)
	}
	return data
}

func TestParseDSL_ValidSimpleApproval(t *testing.T) {
	data := loadTestData(t, "simple_approval_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	if def.ID != "simple_approval" {
		t.Errorf("expected ID 'simple_approval', got %q", def.ID)
	}
	if def.Version != "1.0" {
		t.Errorf("expected Version '1.0', got %q", def.Version)
	}
	if def.StartNode != "start" {
		t.Errorf("expected StartNode 'start', got %q", def.StartNode)
	}
	if len(def.Nodes) != 4 {
		t.Errorf("expected 4 nodes, got %d", len(def.Nodes))
	}

	startNode, ok := def.Nodes["start"]
	if !ok {
		t.Fatal("start node not found")
	}
	if startNode.Type != "start" {
		t.Errorf("expected start node type 'start', got %q", startNode.Type)
	}
	if len(startNode.Transitions) != 1 {
		t.Errorf("expected 1 transition, got %d", len(startNode.Transitions))
	}
	if startNode.Transitions[0].Event != "submit" {
		t.Errorf("expected transition event 'submit', got %q", startNode.Transitions[0].Event)
	}
	if startNode.Transitions[0].Next != "manager_approve" {
		t.Errorf("expected transition next 'manager_approve', got %q", startNode.Transitions[0].Next)
	}

	approveNode := def.Nodes["manager_approve"]
	if len(approveNode.SideEffects) != 1 {
		t.Errorf("expected 1 side effect, got %d", len(approveNode.SideEffects))
	}
	if approveNode.SideEffects[0].Type != "notify" {
		t.Errorf("expected side effect type 'notify', got %q", approveNode.SideEffects[0].Type)
	}
	if len(approveNode.Transitions) != 2 {
		t.Errorf("expected 2 transitions, got %d", len(approveNode.Transitions))
	}
}

func TestParseDSL_ValidBranchFlow(t *testing.T) {
	data := loadTestData(t, "branch_flow_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}

	if len(def.Nodes) != 6 {
		t.Errorf("expected 6 nodes, got %d", len(def.Nodes))
	}

	condNode := def.Nodes["check_amount"]
	if condNode.Type != "condition" {
		t.Errorf("expected condition node, got %q", condNode.Type)
	}
	if len(condNode.Transitions) != 3 {
		t.Errorf("expected 3 transitions on condition (2 when + 1 default), got %d", len(condNode.Transitions))
	}
	if condNode.Transitions[0].When != "amount <= 10000" {
		t.Errorf("expected transitions[0].When 'amount <= 10000', got %q", condNode.Transitions[0].When)
	}
	if condNode.Transitions[1].When != "amount > 10000" {
		t.Errorf("expected transitions[1].When 'amount > 10000', got %q", condNode.Transitions[1].When)
	}
	if condNode.Transitions[2].When != "" {
		t.Errorf("expected transitions[2].When empty as default branch, got %q", condNode.Transitions[2].When)
	}
}

func TestParseDSL_MissingVersion(t *testing.T) {
	_, err := ParseDSL([]byte(`{"id": "test", "nodes": []}`))
	if err == nil {
		t.Fatal("expected error for missing version")
	}
}

func TestParseDSL_UnsupportedVersion(t *testing.T) {
	_, err := ParseDSL([]byte(`{"id": "test", "version": "2.0", "nodes": []}`))
	if err == nil {
		t.Fatal("expected error for unsupported version")
	}
}

func TestParseDSL_MissingID(t *testing.T) {
	_, err := ParseDSL([]byte(`{"version": "1.0", "nodes": [{"id": "start", "type": "start"}]}`))
	if err == nil {
		t.Fatal("expected error for missing process id")
	}
}

func TestParseDSL_NoNodes(t *testing.T) {
	_, err := ParseDSL([]byte(`{"id": "test", "version": "1.0", "nodes": []}`))
	if err == nil {
		t.Fatal("expected error for empty nodes")
	}
}

func TestParseDSL_DuplicateNodeID(t *testing.T) {
	_, err := ParseDSL([]byte(`{
		"id": "test", "version": "1.0",
		"nodes": [
			{"id": "dup", "type": "start", "transitions": [{"event": "next", "next": "dup"}]},
			{"id": "dup", "type": "end"}
		]
	}`))
	if err == nil {
		t.Fatal("expected error for duplicate node id")
	}
}

func TestParseDSL_NoStartNode(t *testing.T) {
	_, err := ParseDSL([]byte(`{
		"id": "test", "version": "1.0",
		"nodes": [
			{"id": "a", "type": "approval", "transitions": [{"event": "next", "next": "b"}]},
			{"id": "b", "type": "end"}
		]
	}`))
	if err == nil {
		t.Fatal("expected error for missing start node")
	}
}

func TestParseDSL_TransitionToNonExistent(t *testing.T) {
	_, err := ParseDSL([]byte(`{
		"id": "test", "version": "1.0",
		"nodes": [
			{"id": "start", "type": "start", "transitions": [{"event": "next", "next": "nonexistent"}]},
			{"id": "end", "type": "end"}
		]
	}`))
	if err == nil {
		t.Fatal("expected error for transition to non-existent node")
	}
}

func TestParseDSL_InvalidJSON(t *testing.T) {
	_, err := ParseDSL([]byte(`{invalid json}`))
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
}

func TestParseDSL_NodeWithoutID(t *testing.T) {
	_, err := ParseDSL([]byte(`{
		"id": "test", "version": "1.0",
		"nodes": [
			{"id": "", "type": "start"}
		]
	}`))
	if err == nil {
		t.Fatal("expected error for node without id")
	}
}

func TestParseDSL_VersionRouting(t *testing.T) {
	data := loadTestData(t, "simple_approval_v1.json")
	def, err := ParseDSL(data)
	if err != nil {
		t.Fatalf("ParseDSL failed: %v", err)
	}
	if def.Version != "1.0" {
		t.Errorf("expected version 1.0, got %q", def.Version)
	}
}
