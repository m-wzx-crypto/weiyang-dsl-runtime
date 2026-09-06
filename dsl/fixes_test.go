package dsl

import (
	"strings"
	"testing"
	"time"
)

// fixes_test.go 回归测试:对应本次修复的正确性问题。
//
//   - join 节点自身的收敛配置(n_of_m 等)此前静默失效,只有写在 parallel 节点上才生效;
//   - 事件"先消费后路由":无人处理的事件被幂等表吞掉,重投递被拒;
//   - 并行 scope 收敛超时只在下一个事件到来时被动检查;
//   - 终端实例仍可被 Feed。

// ---- A1: join 节点自身配置生效 ----

func joinNodeConfigDef() *ProcessDef {
	return &ProcessDef{
		ID: "join_cfg", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1": {ID: "b1", Type: "approval",
				Transitions: []Transition{{Event: "b1ok", Next: "join"}}},
			"b2": {ID: "b2", Type: "approval",
				Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			// 收敛配置声明在 join 节点上(README 语义):1/2 即收敛。
			"join": {ID: "join", Type: "join",
				Join:        &JoinConfig{Mode: "n_of_m", Required: 1},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_JoinNodeConfigHonored(t *testing.T) {
	r := NewRuntime(joinNodeConfigDef(), nil)
	if res := r.Start("i", "e", nil, Event{ID: "s", Name: "go"}); res.HasErrors() {
		t.Fatalf("start failed: %v", res.Errors)
	}
	// 仅一条分支成功:n_of_m(1) 即应收敛(修复前 mode 落回 all,实例卡在等待)。
	if res := r.Feed(Event{ID: "e1", Name: "b1ok"}); res.HasErrors() {
		t.Fatalf("feed failed: %v", res.Errors)
	}
	if r.Status() != StatusCompleted {
		t.Fatalf("n_of_m on join node must converge with 1 success, got %s", r.Status())
	}
}

// ---- A4: scope 收敛超时由 WakeDue 主动触发 ----

func scopeTimeoutDef() *ProcessDef {
	return &ProcessDef{
		ID: "scope_to", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "go", Next: "fork"}}},
			"fork": {ID: "fork", Type: "parallel",
				Transitions: []Transition{{Next: "b1"}, {Next: "b2"}}},
			"b1": {ID: "b1", Type: "approval",
				Transitions: []Transition{{Event: "b1ok", Next: "join"}}},
			"b2": {ID: "b2", Type: "approval",
				Transitions: []Transition{{Event: "b2ok", Next: "join"}}},
			"join": {ID: "join", Type: "join",
				Join:        &JoinConfig{Mode: "all", Timeout: "1h"},
				Transitions: []Transition{{Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

func TestFix_ScopeTimeoutFiresActively(t *testing.T) {
	r := NewRuntime(scopeTimeoutDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})

	// fork 时登记了 scope 超时等待槽,宿主可查询唤醒时刻。
	until, ok := r.NextWakeup()
	if !ok {
		t.Fatal("expected scope timeout wakeup registered")
	}
	if d := time.Until(until); d > time.Hour || d < 30*time.Minute {
		t.Fatalf("expected wakeup in ~1h, got %v", d)
	}

	// 无人喂事件,到点由 WakeDue 主动触发超时(修复前:永不触发)。
	res := r.WakeDue(until.Add(time.Minute))
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "timed out") {
		t.Fatalf("expected scope timeout error, got %v", res.Errors)
	}
	if r.Status() != StatusTimedOut {
		t.Fatalf("expected timed_out, got %s", r.Status())
	}
}

func TestFix_ScopeTimeoutStaleSlotIsNoop(t *testing.T) {
	r := NewRuntime(scopeTimeoutDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "go"})
	// 正常收敛:两条分支都完成,scope 弹出,超时槽一并清理。
	r.Feed(Event{ID: "e1", Name: "b1ok"})
	r.Feed(Event{ID: "e2", Name: "b2ok"})
	if r.Status() != StatusCompleted {
		t.Fatalf("expected completed, got %s", r.Status())
	}
	// 已弹出的 scope 的超时槽不应再触发(陈旧槽自愈)。
	res := r.WakeDue(time.Now().Add(2 * time.Hour))
	if res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("stale scope slot must not fire, got %v / %s", res.Errors, r.Status())
	}
}

// ---- A3: 事件不再被烧掉 ----

func TestFix_UnhandledLinearEventKeepsWaiting(t *testing.T) {
	def := &ProcessDef{
		ID: "linear_feed", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval",
				Transitions: []Transition{{Event: "approve", Next: "end"}, {Event: "reject", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
	r := NewRuntime(def, nil)
	r.Start("i", "e", nil, Event{ID: "s1", Name: "submit"})

	// 修复前:错误事件把实例打成 failed,且事件已进幂等表。
	res := r.Feed(Event{ID: "wrong", Name: "nonsense"})
	if !res.HasErrors() {
		t.Fatal("expected error for unhandled event")
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}
	if r.Ctx.IsProcessedEvent("wrong") {
		t.Fatal("unhandled event must not be consumed")
	}

	// 正确事件仍可完成流程。
	if res := r.Feed(Event{ID: "ok", Name: "approve"}); res.HasErrors() || r.Status() != StatusCompleted {
		t.Fatalf("expected completion, got %v / %s", res.Errors, r.Status())
	}
}

func TestFix_UnroutedParallelEventReleasable(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})

	// 没有任何分支处理的事件:报错且回滚消费(重投递仍有机会)。
	res := r.Feed(Event{ID: "lost", Name: "nobody-handles-this"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "not handled") {
		t.Fatalf("expected unhandled error, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("lost") {
		t.Fatal("unrouted event must be released for redelivery")
	}
	if r.Status() != StatusWaiting {
		t.Fatalf("instance must stay waiting, got %s", r.Status())
	}

	// 同一 ID 重投递改投正确事件名:可被处理(修复前会被幂等表拒绝)。
	if res := r.Feed(Event{ID: "lost", Name: "b1ok"}); res.HasErrors() {
		t.Fatalf("redelivered event must be processable, got %v", res.Errors)
	}
}

// ---- 终端实例拒绝事件 ----

func TestFix_FeedOnTerminalInstanceRejected(t *testing.T) {
	r := NewRuntime(parallelDef(), nil)
	r.Start("i", "e", nil, Event{ID: "s", Name: "submit"})
	r.Feed(Event{ID: "e1", Name: "b1ok"})
	r.Feed(Event{ID: "e2", Name: "b2ok"})
	if r.Status() != StatusCompleted {
		t.Fatalf("setup: expected completed, got %s", r.Status())
	}

	res := r.Feed(Event{ID: "late", Name: "b1ok"})
	if !res.HasErrors() || !strings.Contains(res.Errors[0].Error(), "terminal") {
		t.Fatalf("expected terminal rejection, got %v", res.Errors)
	}
	if r.Ctx.IsProcessedEvent("late") {
		t.Fatal("rejected event must not be consumed")
	}
}

// ---- validator: join.timeout 非法值部署期报错(此前静默归零) ----

func TestFix_JoinTimeoutValidated(t *testing.T) {
	def := scopeTimeoutDef()
	def.Nodes["join"].Join.Timeout = "not-a-duration"
	res := Validate(def)
	if res.IsValid {
		t.Fatal("expected invalid join.timeout to fail validation")
	}
	found := false
	for _, e := range res.Errors {
		if e.Path == "nodes[join].join.timeout" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected join.timeout error, got %v", res.Errors)
	}
}
