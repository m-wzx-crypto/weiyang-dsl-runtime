package dsl

import (
	"sync"
	"testing"
)

func TestOptimisticLockConcurrency(t *testing.T) {
	def := &ProcessDef{
		ID:        "concurrent_test",
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
				Transitions: []Transition{
					{Event: "approve", Next: "end"},
					{Event: "reject", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	type stepResult struct {
		output ExecutionOutput
		err    error
	}

	results := make(chan stepResult, 2)
	var wg sync.WaitGroup

	wg.Add(2)

	go func() {
		defer wg.Done()
		output := Execute(def, "approve", "approve", nil)
		results <- stepResult{output: output}
	}()

	go func() {
		defer wg.Done()
		output := Execute(def, "approve", "reject", nil)
		results <- stepResult{output: output}
	}()

	wg.Wait()
	close(results)

	var outputs []ExecutionOutput
	for r := range results {
		if r.err != nil {
			t.Errorf("unexpected error: %v", r.err)
		}
		outputs = append(outputs, r.output)
	}

	if len(outputs) != 2 {
		t.Fatalf("expected 2 results, got %d", len(outputs))
	}

	for _, o := range outputs {
		if o.NextNode != "end" {
			t.Errorf("expected next node 'end', got %q", o.NextNode)
		}
		if o.Status != "running" {
			t.Errorf("expected status 'running', got %q", o.Status)
		}
	}
}

func TestConcurrentExecuteStep_SameInstance(t *testing.T) {
	def := &ProcessDef{
		ID:        "concurrent_instance_test",
		Version:   "1.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {
				ID:   "start",
				Type: "start",
				Transitions: []Transition{
					{Event: "submit", Next: "review"},
				},
			},
			"review": {
				ID:   "review",
				Type: "approval",
				Transitions: []Transition{
					{Event: "approve", Next: "end"},
					{Event: "reject", Next: "end"},
				},
			},
			"end": {ID: "end", Type: "end"},
		},
	}

	numGoroutines := 10
	results := make(chan ExecutionOutput, numGoroutines)
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			output := Execute(def, "review", "approve", nil)
			results <- output
		}()
	}

	wg.Wait()
	close(results)

	count := 0
	for o := range results {
		count++
		if o.NextNode != "end" {
			t.Errorf("result %d: expected next 'end', got %q", count, o.NextNode)
		}
	}

	if count != numGoroutines {
		t.Errorf("expected %d results, got %d", numGoroutines, count)
	}
}

func TestIsEndNodeByDef(t *testing.T) {
	def := &ProcessDef{
		Nodes: map[string]*Node{
			"end":          {ID: "end", Type: "end"},
			"approved_end": {ID: "approved_end", Type: "end"},
			"approve":      {ID: "approve", Type: "approval"},
		},
	}

	if !IsEndNodeByDef(def, "end") {
		t.Error("expected 'end' to be an end node")
	}
	if !IsEndNodeByDef(def, "approved_end") {
		t.Error("expected 'approved_end' to be an end node")
	}
	if IsEndNodeByDef(def, "approve") {
		t.Error("expected 'approve' not to be an end node")
	}
	if IsEndNodeByDef(def, "nonexistent") {
		t.Error("expected nonexistent node not to be an end node")
	}
}
