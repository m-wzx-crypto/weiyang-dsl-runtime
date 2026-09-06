package dsl

import (
	"fmt"
	"time"
)

// manager.go 实现阶段 2 的运行时门面:把 定义注册表 + Journal + InstanceStore
// 组装成可直接嵌入宿主应用的流程服务。
//
//	Manager
//	  ├── Start(defID, instanceID, ...)   启动实例(登记摘要 + 记账)
//	  ├── Feed(instanceID, event)         投递事件(先折叠恢复,再推进)
//	  ├── WakeDueSweep(now)               调度扫描:唤醒所有到期实例
//	  └── Load 时自动 outbox 重投         已派发未解决的命令安全补投
//
// 每次操作的固定节律:加载(fold 日志恢复状态)→ 执行 → 同步实例摘要 →
// 幂等表/undo 栈/等待槽已随日志恢复,无需额外持久化。这正是阶段 1 确定性
// 内核的兑现:宿主只需要可靠地存两样东西——日志和摘要。

// Manager 是流程运行时门面。
type Manager struct {
	// Defs 是可用定义注册表(key = ProcessDef.ID)。
	Defs map[string]*ProcessDef

	// Journal 承接事实追加;必须同时实现 JournalReader(内存实现天然满足,
	// 持久化实现需两者都实现)。
	Journal Journal
	// Store 持久化实例摘要;nil 时使用 MemoryInstanceStore(不推荐生产)。
	Store InstanceStore

	SideEffects    SideEffectExecutor
	Orchestrator   *CommandOrchestrator
	AutoCompensate bool
}

// ManagerOption 配置 Manager。
type ManagerOption func(*Manager)

// WithManagerJournal 绑定事实日志(需同时实现 JournalReader)。
func WithManagerJournal(j Journal) ManagerOption {
	return func(m *Manager) { m.Journal = j }
}

// WithManagerStore 绑定实例摘要存储。
func WithManagerStore(s InstanceStore) ManagerOption {
	return func(m *Manager) { m.Store = s }
}

// WithManagerOrchestrator 注入副作用编排器(重试语义)。
func WithManagerOrchestrator(o *CommandOrchestrator) ManagerOption {
	return func(m *Manager) { m.Orchestrator = o }
}

// WithManagerAutoCompensate 开启失败自动补偿。
func WithManagerAutoCompensate() ManagerOption {
	return func(m *Manager) { m.AutoCompensate = true }
}

// NewManager 创建运行时门面。sideEffects 为宿主的副作用执行器(业务语义:
// 发通知、扣库存、调用 LLM……)。
func NewManager(defs []*ProcessDef, sideEffects SideEffectExecutor, opts ...ManagerOption) *Manager {
	m := &Manager{
		Defs:        make(map[string]*ProcessDef, len(defs)),
		SideEffects: sideEffects,
	}
	for _, d := range defs {
		m.Defs[d.ID] = d
	}
	for _, o := range opts {
		o(m)
	}
	if m.Journal == nil {
		m.Journal = NewMemoryJournal()
	}
	if m.Store == nil {
		m.Store = NewMemoryInstanceStore()
	}
	return m
}

// load 折叠日志恢复出可继续执行的 Runtime,并补投 outbox 中未解决的命令
// (上次进程在"命令已发出、结果未落账"之间死亡的残余——命令幂等键保证
// 重投递安全,这正是 at-least-once + 幂等 = exactly-once 业务效果的落地)。
func (m *Manager) load(instanceID string) (*ProcessDef, *Runtime, *InstanceRecord, error) {
	reader, ok := m.Journal.(JournalReader)
	if !ok {
		return nil, nil, nil, ErrJournalNotReadable
	}
	rec, err := m.Store.GetInstance(instanceID)
	if err != nil {
		return nil, nil, nil, err
	}
	def, ok := m.Defs[rec.DefinitionID]
	if !ok {
		return nil, nil, nil, fmt.Errorf("%w: %q", ErrDefinitionUnknown, rec.DefinitionID)
	}
	occs, err := reader.LoadOccurrences(instanceID)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load occurrences: %w", err)
	}
	ctx, err := Fold(def, occs)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("fold journal: %w", err)
	}
	rt := NewRuntime(def, m.SideEffects,
		WithExecutionContext(ctx),
		WithJournal(m.Journal),
		WithOrchestrator(m.Orchestrator),
		WithAutoCompensate())
	// 租户身份属于实例摘要(不进事实日志),恢复时从记录带回。
	rt.Ctx.TenantID = rec.TenantID
	// outbox 补投(先于任何新事件,保证旧命令先于新状态变更被解决)。
	for _, cmd := range PendingCommands(occs) {
		rt.deliver(cmd)
	}
	return def, rt, rec, nil
}

// sync 把执行后的实例状态同步到摘要存储。
func (m *Manager) sync(rec *InstanceRecord, rt *Runtime) error {
	rec.Status = rt.Ctx.Status.String()
	rec.CurrentNode = rt.Ctx.CurrentNode
	if until, ok := rt.NextWakeup(); ok {
		w := until
		rec.WakeUpAt = &w
	} else {
		rec.WakeUpAt = nil
	}
	rec.UpdatedAt = time.Now()
	return m.Store.PutInstance(*rec)
}

// Start 启动一个新实例。
func (m *Manager) Start(defID, instanceID, tenantID string, vars map[string]interface{}, ev Event) (*ExecutionResult, error) {
	def, ok := m.Defs[defID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrDefinitionUnknown, defID)
	}
	if _, err := m.Store.GetInstance(instanceID); err == nil {
		return nil, fmt.Errorf("%w: %q", ErrInstanceExists, instanceID)
	}
	rec := InstanceRecord{
		InstanceID:   instanceID,
		DefinitionID: defID,
		TenantID:     tenantID,
		Status:       StatusPending.String(),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	rt := NewRuntime(def, m.SideEffects,
		WithJournal(m.Journal),
		WithOrchestrator(m.Orchestrator),
		WithAutoCompensate())
	if rt.Ctx != nil {
		rt.Ctx.TenantID = tenantID
	}
	executionID := fmt.Sprintf("%s#%d", instanceID, time.Now().UnixNano())
	res := rt.Start(instanceID, executionID, vars, ev)
	if err := m.sync(&rec, rt); err != nil {
		return res, err
	}
	return res, rt.JournalError()
}

// Feed 向实例投递事件。
func (m *Manager) Feed(instanceID string, ev Event) (*ExecutionResult, error) {
	_, rt, rec, err := m.load(instanceID)
	if err != nil {
		return nil, err
	}
	res := rt.Feed(ev)
	if err := m.sync(rec, rt); err != nil {
		return res, err
	}
	return res, rt.JournalError()
}

// GetStatus 返回实例当前状态摘要。
func (m *Manager) GetStatus(instanceID string) (*InstanceRecord, error) {
	return m.Store.GetInstance(instanceID)
}

// WakeDueSweep 是宿主定时器的调度入口:唤醒所有到期的实例(timer/deadline/
// scope 超时),返回实际推进的实例数。宿主按自己的节奏轮询(如每秒),
// 引擎保持零定时器依赖。
func (m *Manager) WakeDueSweep(now time.Time, limit int) (int, error) {
	recs, err := m.Store.ListWakeable(now, limit)
	if err != nil {
		return 0, err
	}
	advanced := 0
	for _, rec := range recs {
		_, rt, cur, err := m.load(rec.InstanceID)
		if err != nil {
			return advanced, fmt.Errorf("wake %q: %w", rec.InstanceID, err)
		}
		if until, ok := rt.NextWakeup(); ok && !until.After(now) {
			rt.WakeDue(now)
			advanced++
		}
		if err := m.sync(cur, rt); err != nil {
			return advanced, err
		}
	}
	return advanced, nil
}

// Compensate 对失败实例显式触发补偿(逆序 undo 栈)。
func (m *Manager) Compensate(instanceID string) ([]SideEffectResult, error) {
	_, rt, rec, err := m.load(instanceID)
	if err != nil {
		return nil, err
	}
	out := rt.Compensate()
	if err := m.sync(rec, rt); err != nil {
		return out, err
	}
	return out, rt.JournalError()
}
