package dsl

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func w3ParallelDef() *ProcessDef {
	return &ProcessDef{
		ID: "w3_parallel", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "fork"}}},
			"fork":  {ID: "fork", Type: "parallel", Fork: &ForkConfig{Mode: "all", JoinNode: "join"}, Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1":    {ID: "b1", Type: "approval", RequirePrincipal: true, Transitions: []Transition{{Event: "b1ok", Next: "join"}}},
			"b2":    {ID: "b2", Type: "approval", RequirePrincipal: true, Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			"join":  {ID: "join", Type: "join", Transitions: []Transition{{Next: "end"}}},
			"end":   {ID: "end", Type: "end"},
		},
	}
}

func hasPrincipalRequiredError(res *ExecutionResult) bool {
	for _, err := range res.Errors {
		if errors.Is(err, ErrPrincipalRequired) {
			return true
		}
	}
	return false
}

func TestW3_ParallelDecisionRequiresPrincipalBeforeJournal(t *testing.T) {
	def := w3ParallelDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, nil, WithJournal(j))
	if res := r.Start("i", "e", nil, Event{ID: "s", Name: "submit"}); res.HasErrors() {
		t.Fatalf("start: %v", res.Errors)
	}
	before := len(j.Occurrences())
	res := r.Feed(Event{ID: "b1", Name: "b1ok"})
	if !hasPrincipalRequiredError(res) {
		t.Fatalf("parallel principal-less decision must be rejected: %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("b1") || len(j.Occurrences()) != before {
		t.Fatal("parallel rejected decision must not be consumed or journaled")
	}

	actor := &Principal{Kind: PrincipalHuman, ID: "u-branch"}
	if res = r.Feed(Event{ID: "b1", Name: "b1ok", Principal: actor}); res.HasErrors() {
		t.Fatalf("attributed branch decision: %v", res.Errors)
	}
	if res = r.Feed(Event{ID: "b2", Name: "b2ok", Principal: actor}); res.HasErrors() {
		t.Fatalf("second attributed branch decision: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
	var branchActor bool
	for _, occ := range j.Occurrences() {
		if occ.Kind == OccBranchUpdated && occ.Branch != nil && occ.Branch.Actor != nil && reflect.DeepEqual(*occ.Branch.Actor, *actor) {
			branchActor = true
		}
	}
	if !branchActor {
		t.Fatal("branch snapshot must retain the decision principal")
	}
	requireFoldEq(t, def, r)
}

func TestW3_DeadlineAndWakeupAreSystemAttributed(t *testing.T) {
	def := temporalDef()
	j := NewMemoryJournal()
	r := NewRuntime(def, nil, WithJournal(j))
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	if res := r.WakeDue(time.Now().Add(3 * time.Hour)); res.HasErrors() {
		t.Fatalf("timer wake: %v", res.Errors)
	}
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("approval deadline must be registered")
	}
	if res := r.WakeDue(until.Add(time.Minute)); res.HasErrors() {
		t.Fatalf("deadline wake: %v", res.Errors)
	}
	var woke, deadlineTransition, waitingActor bool
	for _, occ := range j.Occurrences() {
		if occ.Kind == OccWoke && occ.Actor != nil && occ.Actor.Kind == PrincipalSystem && occ.Actor.ID == "weiyang-runtime" {
			woke = true
		}
		if occ.Kind == OccTransition && occ.TransitionEvent == "deadline" && occ.Actor != nil && occ.Actor.Kind == PrincipalSystem {
			deadlineTransition = true
		}
		if occ.Kind == OccWaitingSet && occ.Waiting != nil && occ.Waiting.Actor != nil && occ.Waiting.Actor.Kind == PrincipalSystem {
			waitingActor = true
		}
	}
	if !woke || !deadlineTransition || !waitingActor {
		t.Fatalf("expected system attribution (woke=%v deadline=%v waiting=%v)", woke, deadlineTransition, waitingActor)
	}
	requireFoldEq(t, def, r)
}
