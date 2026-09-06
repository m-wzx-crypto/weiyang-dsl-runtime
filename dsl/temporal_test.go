package dsl

import (
	"testing"
	"time"
)

// v2 时间契约测试:timer 节点、approval deadline、NextWakeup/WakeDue 主动时间轴。

func temporalDef() *ProcessDef {
	return &ProcessDef{
		ID:        "temporal",
		Version:   "2.0",
		StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "wait"}}},
			// timer:2 小时后自动提醒。
			"wait": {ID: "wait", Type: "timer", Duration: "2h",
				Transitions: []Transition{{Next: "remind"}}},
			"remind": {ID: "remind", Type: "notification",
				Transitions: []Transition{{Event: "submit", Next: "approve"}, {Next: "approve"}}},
			// approval 带 24h deadline:超时升级到 escalate。
			"approve": {ID: "approve", Type: "approval",
				Deadline:    &DeadlineConfig{After: "24h", Next: "escalate"},
				Transitions: []Transition{{Event: "approve", Next: "end"}, {Event: "reject", Next: "end"}}},
			"escalate": {ID: "escalate", Type: "approval",
				Transitions: []Transition{{Event: "approve", Next: "end"}, {Event: "reject", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestV2_TimerWaitsAndWakes(t *testing.T) {
	r := NewRuntime(temporalDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	if r.Status() != StatusWaiting {
		t.Fatalf("expected waiting at timer, got %s", r.Status())
	}
	if r.Ctx.CurrentNode != "wait" {
		t.Fatalf("expected parked at timer node, got %q", r.Ctx.CurrentNode)
	}

	// 宿主可查询下一次唤醒时刻。
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected a wakeup time for timer")
	}
	if d := time.Until(until); d > 2*time.Hour || d < time.Hour {
		t.Fatalf("expected wakeup in ~2h, got %v", d)
	}

	// 未到期唤醒:不应推进。
	res := r.WakeDue(time.Now().Add(time.Hour))
	if res.HasErrors() || r.Ctx.CurrentNode != "wait" {
		t.Fatalf("timer must not fire before due, node=%q", r.Ctx.CurrentNode)
	}

	// 到期唤醒:自动前进到 remind,再停靠在 approval(带 deadline)。
	res = r.WakeDue(time.Now().Add(3 * time.Hour))
	if res.HasErrors() {
		t.Fatalf("wake failed: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "approve" || r.Status() != StatusWaiting {
		t.Fatalf("expected advanced to approval, got %q / %s", r.Ctx.CurrentNode, r.Status())
	}
	// 重复唤醒同一时刻:幂等,不产生第二次迁移。
	r.WakeDue(time.Now().Add(3 * time.Hour))
	if r.Ctx.CurrentNode != "approve" {
		t.Fatalf("wake must be idempotent, got %q", r.Ctx.CurrentNode)
	}
}

func TestV2_DeadlineEscalationBeforeEvent(t *testing.T) {
	r := NewRuntime(temporalDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	r.WakeDue(time.Now().Add(3 * time.Hour)) // timer 触发,停靠在 approve

	// 审批事件先到:正常路径,deadline 被清除。
	if res := r.Feed(Event{ID: "a1", Name: "approve"}); res.HasErrors() {
		t.Fatalf("approve failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
	if _, ok := r.NextWakeup(); ok {
		t.Fatal("no wakeup expected after completion")
	}
}

func TestV2_DeadlineFiresWhenEventLate(t *testing.T) {
	r := NewRuntime(temporalDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})
	r.WakeDue(time.Now().Add(3 * time.Hour))

	// 24 小时无审批:deadline 触发,升级到 escalate。
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected deadline wakeup registered on approval")
	}
	res := r.WakeDue(until.Add(time.Minute))
	if res.HasErrors() {
		t.Fatalf("deadline wake failed: %v", res.Errors)
	}
	if r.Ctx.CurrentNode != "escalate" || r.Status() != StatusWaiting {
		t.Fatalf("expected escalated to 'escalate', got %q / %s", r.Ctx.CurrentNode, r.Status())
	}

	// 之后审批事件仍可完成流程(在 escalate 节点上)。
	if res := r.Feed(Event{ID: "a1", Name: "approve"}); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("expected completion after escalation, got %v / %s", res.Errors, r.Status())
	}
}

func TestV2_SavepointCarriesWaitings(t *testing.T) {
	r := NewRuntime(temporalDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})

	data, err := r.Savepoint()
	if err != nil {
		t.Fatalf("savepoint: %v", err)
	}
	ctx, err := RestoreExecutionContext(data)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	r2 := NewRuntime(temporalDef(), nil, WithExecutionContext(ctx))
	until, ok := r2.NextWakeup()
	if !ok {
		t.Fatal("waitings must survive savepoint/restore")
	}
	res := r2.WakeDue(until.Add(time.Minute))
	if res.HasErrors() || r2.Ctx.CurrentNode != "approve" {
		t.Fatalf("restored instance must wake identically, got %q %v", r2.Ctx.CurrentNode, res.Errors)
	}
}

func TestV2_InvalidTemporalConfigRejected(t *testing.T) {
	cases := []struct {
		name     string
		breakDef func(*ProcessDef)
		wantPath string
	}{
		{"timer duration invalid", func(d *ProcessDef) { d.Nodes["wait"].Duration = "not-a-duration" }, "nodes[wait].duration"},
		{"deadline non-positive", func(d *ProcessDef) { d.Nodes["approve"].Deadline.After = "0s" }, "nodes[approve].deadline.after"},
		{"deadline target missing", func(d *ProcessDef) { d.Nodes["approve"].Deadline.Next = "nope" }, "nodes[approve].deadline.next"},
	}
	for _, tc := range cases {
		def := temporalDef()
		tc.breakDef(def)
		res := Validate(def)
		if res.IsValid {
			t.Fatalf("%s: expected validation failure", tc.name)
		}
		found := false
		for _, e := range res.Errors {
			if e.Path == tc.wantPath {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: expected error at %s, got %v", tc.name, tc.wantPath, res.Errors)
		}
	}
}
