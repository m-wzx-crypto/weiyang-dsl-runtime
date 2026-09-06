package dsl

import (
	"errors"
	"testing"
	"time"
)

// manager_test.go — 阶段 2:持久化 SPI 与运行时门面。

func TestMemoryInstanceStore_Basics(t *testing.T) {
	s := NewMemoryInstanceStore()
	now := time.Now()
	wake := now.Add(-time.Hour) // 已到期 → 可唤醒

	if _, err := s.GetInstance("nope"); !errors.Is(err, ErrInstanceNotFound) {
		t.Fatalf("expected ErrInstanceNotFound, got %v", err)
	}
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	must(s.PutInstance(InstanceRecord{InstanceID: "b", DefinitionID: "d", Status: "waiting", WakeUpAt: &wake}))
	must(s.PutInstance(InstanceRecord{InstanceID: "a", DefinitionID: "d", Status: "waiting"}))
	must(s.PutInstance(InstanceRecord{InstanceID: "c", DefinitionID: "d", Status: "completed"}))

	wakeable, err := s.ListWakeable(now, 10)
	must(err)
	if len(wakeable) != 1 || wakeable[0].InstanceID != "b" {
		t.Fatalf("expected only 'b' wakeable, got %+v", wakeable)
	}

	running, _ := s.ListByStatus("waiting", 10)
	if len(running) != 2 {
		t.Fatalf("expected 2 waiting, got %d", len(running))
	}
	completed, _ := s.ListByStatus("completed", 10)
	if len(completed) != 1 {
		t.Fatalf("expected 1 completed, got %d", len(completed))
	}
}

func managerDefs() []*ProcessDef {
	def := &ProcessDef{
		ID: "order_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start",
				Transitions: []Transition{{Event: "submit", Next: "pack"}}},
			"pack": {ID: "pack", Type: "action",
				SideEffects: []SideEffect{{Type: "notify", Target: "ops"}},
				Transitions: []Transition{{Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval",
				Transitions: []Transition{{Event: "approve", Next: "end"}, {Event: "reject", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
	return []*ProcessDef{def}
}

func TestManager_StartFeedLifecycle(t *testing.T) {
	exec := NewInMemorySideEffectExecutor("notify")
	m := NewManager(managerDefs(), exec)

	res, err := m.Start("order_flow", "inst-1", "tenant-a", nil, Event{ID: "e1", Name: "submit"})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if res.HasErrors() {
		t.Fatalf("start errors: %v", res.Errors)
	}
	rec, err := m.GetStatus("inst-1")
	if err != nil || rec.Status != "waiting" || rec.CurrentNode != "approve" || rec.TenantID != "tenant-a" {
		t.Fatalf("record not synced: %+v err=%v", rec, err)
	}

	if _, err := m.Start("order_flow", "inst-1", "", nil, Event{ID: "x", Name: "submit"}); !errors.Is(err, ErrInstanceExists) {
		t.Fatalf("expected ErrInstanceExists, got %v", err)
	}
	if _, err := m.Start("nope", "inst-2", "", nil, Event{ID: "x", Name: "submit"}); !errors.Is(err, ErrDefinitionUnknown) {
		t.Fatalf("expected ErrDefinitionUnknown, got %v", err)
	}

	res, err = m.Feed("inst-1", Event{ID: "e2", Name: "approve"})
	if err != nil || res.HasErrors() {
		t.Fatalf("feed: %v / %v", err, res.Errors)
	}
	rec, _ = m.GetStatus("inst-1")
	if rec.Status != "completed" {
		t.Fatalf("expected completed, got %s", rec.Status)
	}
	if len(exec.Outcomes()) == 0 {
		t.Fatal("side effects must have been delivered")
	}
}

func TestManager_CrashRecoveryContinuesInstance(t *testing.T) {
	j := NewMemoryJournal()
	s := NewMemoryInstanceStore()
	exec1 := NewInMemorySideEffectExecutor("notify")

	m1 := NewManager(managerDefs(), exec1, WithManagerJournal(j), WithManagerStore(s))
	m1.Start("order_flow", "inst-1", "t", nil, Event{ID: "e1", Name: "submit"})

	// "崩溃"后:全新的 Manager(新执行器)从同一日志+摘要恢复并继续。
	exec2 := NewInMemorySideEffectExecutor("notify")
	m2 := NewManager(managerDefs(), exec2, WithManagerJournal(j), WithManagerStore(s))

	res, err := m2.Feed("inst-1", Event{ID: "e2", Name: "approve"})
	if err != nil || res.HasErrors() {
		t.Fatalf("recovered feed: %v / %v", err, res.Errors)
	}
	if rec, _ := m2.GetStatus("inst-1"); rec.Status != "completed" {
		t.Fatalf("expected completed after recovery, got %s", rec.Status)
	}
	// 幂等表随日志恢复:重放启动事件被拒绝(事件不被当前节点接受/重复消费)。
	if res, err := m2.Feed("inst-1", Event{ID: "e1", Name: "submit"}); err == nil && !res.HasErrors() {
		t.Fatal("stale event must be rejected after recovery")
	}
	// 恢复后的副作用在新执行器中执行(pack 节点的 notify 在恢复前已执行,
	// 此处无新副作用;幂等性由旧日志保证不会重复)。
	if outs := exec2.Outcomes(); len(outs) != 0 {
		t.Fatalf("expected no new side effects on replayed history, got %d", len(outs))
	}
}

func TestManager_OutboxRedispatchOnLoad(t *testing.T) {
	j := NewMemoryJournal()
	s := NewMemoryInstanceStore()
	exec := NewInMemorySideEffectExecutor("notify")
	m := NewManager(managerDefs(), exec, WithManagerJournal(j), WithManagerStore(s))
	m.Start("order_flow", "inst-1", "", nil, Event{ID: "e1", Name: "submit"})

	// 模拟崩溃:命令已落账(issued),结果(result)永远没有落账。
	j.Append(&Occurrence{
		InstanceID:  "inst-1",
		ExecutionID: "lost",
		Kind:        OccCommandIssued,
		Command:     &SideEffectCommand{ID: "lost-cmd", NodeID: "pack", Type: "notify", Target: "ops"},
	})

	// 下一次加载时 outbox 自动补投,并落账结果。
	m2 := NewManager(managerDefs(), exec, WithManagerJournal(j), WithManagerStore(s))
	if _, err := m2.Feed("inst-1", Event{ID: "e2", Name: "approve"}); err != nil {
		t.Fatalf("feed after redispatch: %v", err)
	}
	found := false
	for _, o := range exec.Outcomes() {
		if o.CommandID == "lost-cmd" && o.Status == "completed" {
			found = true
		}
	}
	if !found {
		t.Fatal("pending command must be re-dispatched on load")
	}
	// 补投后日志无未解决命令。
	if pending := PendingCommands(mustLoad(t, j, "inst-1")); len(pending) != 0 {
		t.Fatalf("expected clean outbox after redispatch, got %v", pending)
	}
}

func mustLoad(t *testing.T, j *MemoryJournal, instanceID string) []Occurrence {
	t.Helper()
	occs, err := j.LoadOccurrences(instanceID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return occs
}

func TestManager_WakeDueSweep(t *testing.T) {
	def := temporalDef() // timer 2h + approval deadline 24h
	j := NewMemoryJournal()
	s := NewMemoryInstanceStore()
	m := NewManager([]*ProcessDef{def}, NewInMemorySideEffectExecutor(), WithManagerJournal(j), WithManagerStore(s))
	if _, err := m.Start("temporal", "inst-1", "", nil, Event{ID: "e1", Name: "submit"}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// 到期前扫描:无推进。
	n, err := m.WakeDueSweep(time.Now().Add(time.Hour), 10)
	if err != nil || n != 0 {
		t.Fatalf("expected 0 advanced before due, got %d / %v", n, err)
	}

	// 到期后扫描:timer 触发,实例推进并重新停靠(approval deadline → 有新唤醒时刻)。
	n, err = m.WakeDueSweep(time.Now().Add(3*time.Hour), 10)
	if err != nil || n != 1 {
		t.Fatalf("expected 1 advanced, got %d / %v", n, err)
	}
	rec, _ := m.GetStatus("inst-1")
	if rec.CurrentNode != "approve" || rec.WakeUpAt == nil {
		t.Fatalf("expected parked at approval with next wakeup, got %+v", rec)
	}

	// 再扫:approval 未到期,无推进。
	n, _ = m.WakeDueSweep(time.Now().Add(3*time.Hour), 10)
	if n != 0 {
		t.Fatalf("expected 0 advanced, got %d", n)
	}
}

func TestManager_Compensate(t *testing.T) {
	exec := NewInMemorySideEffectExecutor("book_hotel", "charge_card", "cancel_hotel", "refund_card")
	m := NewManager([]*ProcessDef{sagaDef()}, exec, WithManagerAutoCompensate())
	if _, err := m.Start("saga", "i", "", nil, Event{ID: "s1", Name: "submit"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	if res, err := m.Feed("i", Event{ID: "s2", Name: "reject"}); err != nil || !res.HasErrors() {
		t.Fatalf("expected failure from broken node, got err=%v resErrs=%v", err, res.Errors)
	}
	out, err := m.Compensate("i")
	if err != nil {
		t.Fatalf("compensate: %v", err)
	}
	// 已随失败自动补偿过,显式 Compensate 应为空(幂等)。
	if len(out) != 0 {
		t.Fatalf("expected no additional compensations, got %d", len(out))
	}
}
