package dsl

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// manager.go 实现阶段 2 的运行时门面:把 定义注册表 + Journal + InstanceStore
// 组装成可直接嵌入宿主应用的流程服务。
//
//	Manager
//	  ├── Start(defID, instanceID, ...)   启动实例(原子创建 + 记账)
//	  ├── Feed(instanceID, event)         投递事件(快照/折叠恢复,再推进)
//	  ├── WakeDueSweep(now)               调度扫描:唤醒所有到期实例
//	  ├── Load 时自动 outbox 重投         已派发未解决的命令安全补投
//	  └── 快照加速                        日志增量超阈值时保存快照,恢复
//	                                      时只折叠增量,避免全量回放
//
// 每次操作的固定节律:加载(快照+增量 或 fold 日志恢复状态)→ 执行 → 同步实例
// 摘要 → 幂等表/undo 栈/等待槽已随日志与快照恢复,无需额外持久化。宿主只需要
// 可靠地存三样东西——日志、摘要和(可选的)快照。
//
// 并发契约:Manager 内部用固定条带锁把同一实例的操作串行化(内化了 Runtime
// 并发契约),宿主无需自行按 InstanceID 分片投递;不同实例之间仍可并发。

// snapshotEveryN 是距上次快照累积多少笔事实后触发一次新快照的默认值。
const snapshotEveryN = 256

// Manager 是流程运行时门面。
type Manager struct {
	// Defs 是可用定义注册表(key = ProcessDef.ID)。
	Defs map[string]*ProcessDef

	// Journal 承接事实追加;必须同时实现 JournalReader(内存实现天然满足,
	// 持久化实现需两者都实现)。实现 JournalProgress 时快照机制可用。
	Journal Journal
	// Store 持久化实例摘要;nil 时使用 MemoryInstanceStore(不推荐生产)。
	Store InstanceStore
	// Snapshots 可选:实例上下文快照存储。配置后恢复走"快照 + 增量日志",
	// 长日志实例不再每次全量折叠;日志仍是真相,快照损坏自动回退。
	Snapshots SnapshotStore

	SideEffects    SideEffectExecutor
	Orchestrator   *CommandOrchestrator
	AutoCompensate bool
	// OnJournalError 透传给每个实例的上下文:WAL 断链(记账失败)时即时回调,
	// 供宿主告警;JournalErr 本身仍按实例保留首错。
	OnJournalError func(error)
	// SnapshotEveryN 是快照触发阈值(距上次快照累积的事实笔数),0 = 默认 256。
	SnapshotEveryN int

	// stripes 是固定条带的实例操作锁:同一实例的操作串行化,不同实例互不阻塞。
	// 锁表按实例 ID 哈希分片,无上限增长问题。
	stripes [managerLockStripes]sync.Mutex
}

// managerLockStripes 是实例操作锁的条带数。
const managerLockStripes = 64

// lockOf 返回实例所属条带锁。
func (m *Manager) lockOf(instanceID string) *sync.Mutex {
	return &m.stripes[fnv32a(instanceID)%managerLockStripes]
}

// fnv32a 计算 FNV-1a 32 位哈希(锁分片用,无需密码学强度)。
func fnv32a(s string) uint32 {
	const offset32 = 2166136261
	const prime32 = 16777619
	h := uint32(offset32)
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime32
	}
	return h
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

// WithManagerSnapshots 绑定实例快照存储(可选,长日志实例的恢复加速)。
func WithManagerSnapshots(s SnapshotStore) ManagerOption {
	return func(m *Manager) { m.Snapshots = s }
}

// WithManagerOrchestrator 注入副作用编排器(重试语义)。
func WithManagerOrchestrator(o *CommandOrchestrator) ManagerOption {
	return func(m *Manager) { m.Orchestrator = o }
}

// WithManagerAutoCompensate 开启失败自动补偿。
func WithManagerAutoCompensate() ManagerOption {
	return func(m *Manager) { m.AutoCompensate = true }
}

// WithManagerJournalError 绑定记账失败回调(WAL 断链即时告警)。
func WithManagerJournalError(fn func(error)) ManagerOption {
	return func(m *Manager) { m.OnJournalError = fn }
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

// load 折叠(或快照恢复)出可继续执行的 Runtime,并补投 outbox 中未解决的命令
// (上次进程在"命令已发出、结果未落账"之间死亡的残余——命令幂等键保证
// 重投递安全,这正是 at-least-once + 幂等 = exactly-once 业务效果的落地)。
// 调用方须已持有实例条带锁。
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

	// 快照加速:优先从最新快照 + 增量事实恢复;快照缺失/损坏时回退全量折叠
	// (日志即真相,快照只是加速结构,任何异常都不能阻断正确性)。
	var ctx *ExecutionContext
	fromSeq := int64(0)
	if m.Snapshots != nil {
		if snap, snapErr := m.Snapshots.LatestSnapshot(instanceID); snapErr == nil && snap.UptoSeq > 0 {
			if restored, restoreErr := RestoreExecutionContext(snap.Data); restoreErr == nil {
				ctx = restored
				fromSeq = snap.UptoSeq
			}
		}
	}
	if ctx == nil {
		ctx, err = Fold(def, occs)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("fold journal: %w", err)
		}
	} else if inc := occurrencesAfter(occs, fromSeq); len(inc) > 0 {
		if err := foldOnto(ctx, inc); err != nil {
			return nil, nil, nil, fmt.Errorf("fold incremental: %w", err)
		}
	}

	rt := NewRuntime(def, m.SideEffects,
		WithExecutionContext(ctx),
		WithJournal(m.Journal),
		WithOrchestrator(m.Orchestrator),
		WithAutoCompensate())
	if m.OnJournalError != nil {
		rt.Ctx.OnJournalError = m.OnJournalError
	}
	// 租户身份属于实例摘要(不进事实日志),恢复时从记录带回。
	rt.Ctx.TenantID = rec.TenantID
	// outbox 补投(先于任何新事件,保证旧命令先于新状态变更被解决)。
	for _, cmd := range PendingCommands(occs) {
		rt.deliver(cmd)
	}
	return def, rt, rec, nil
}

// sync 把执行后的实例状态同步到摘要存储,并在日志增量超阈值时保存快照。
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
	if err := m.Store.PutInstance(*rec); err != nil {
		return err
	}
	return m.maybeSnapshot(rec.InstanceID, rt)
}

// maybeSnapshot 在距上次快照累积超过快照阈值时保存一份新快照。
// Journal 未实现 JournalProgress(无法判定进度)时静默跳过,退化为全量折叠。
func (m *Manager) maybeSnapshot(instanceID string, rt *Runtime) error {
	if m.Snapshots == nil {
		return nil
	}
	progress, ok := m.Journal.(JournalProgress)
	if !ok {
		return nil
	}
	threshold := m.SnapshotEveryN
	if threshold <= 0 {
		threshold = snapshotEveryN
	}
	maxSeq, err := progress.MaxSeq(instanceID)
	if err != nil {
		return fmt.Errorf("snapshot: journal progress: %w", err)
	}
	if cur, err := m.Snapshots.LatestSnapshot(instanceID); err == nil {
		if maxSeq-cur.UptoSeq < int64(threshold) {
			return nil
		}
	} else if !errors.Is(err, ErrSnapshotNotFound) {
		return fmt.Errorf("snapshot: latest: %w", err)
	}
	data, err := rt.Ctx.MarshalJSON()
	if err != nil {
		return fmt.Errorf("snapshot: marshal context: %w", err)
	}
	if err := m.Snapshots.PutSnapshot(InstanceSnapshot{
		InstanceID: instanceID,
		UptoSeq:    maxSeq,
		Data:       data,
		CreatedAt:  time.Now(),
	}); err != nil {
		return fmt.Errorf("snapshot: put: %w", err)
	}
	return nil
}

// Start 启动一个新实例。
func (m *Manager) Start(defID, instanceID, tenantID string, vars map[string]interface{}, ev Event) (*ExecutionResult, error) {
	lock := m.lockOf(instanceID)
	lock.Lock()
	defer lock.Unlock()

	def, ok := m.Defs[defID]
	if !ok {
		return nil, fmt.Errorf("%w: %q", ErrDefinitionUnknown, defID)
	}
	rec := InstanceRecord{
		InstanceID:   instanceID,
		DefinitionID: defID,
		TenantID:     tenantID,
		Status:       StatusPending.String(),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	// 原子创建:并发同 ID 启动只有一个成功(消除"先查后写"的 TOCTOU 竞态)。
	if err := m.Store.CreateInstance(rec); err != nil {
		return nil, err
	}
	rt := NewRuntime(def, m.SideEffects,
		WithJournal(m.Journal),
		WithOrchestrator(m.Orchestrator),
		WithAutoCompensate())
	if rt.Ctx != nil {
		rt.Ctx.TenantID = tenantID
		if m.OnJournalError != nil {
			rt.Ctx.OnJournalError = m.OnJournalError
		}
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
	lock := m.lockOf(instanceID)
	lock.Lock()
	defer lock.Unlock()

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
// 引擎保持零定时器依赖。单个实例损坏只记录错误,不阻塞本轮其余实例。
func (m *Manager) WakeDueSweep(now time.Time, limit int) (int, error) {
	recs, err := m.Store.ListWakeable(now, limit)
	if err != nil {
		return 0, err
	}
	advanced := 0
	var errs []error
	for _, rec := range recs {
		lock := m.lockOf(rec.InstanceID)
		lock.Lock()
		_, rt, cur, err := m.load(rec.InstanceID)
		if err != nil {
			lock.Unlock()
			// 单个坏实例不阻塞整轮唤醒(否则它会永久堵住队列)。
			errs = append(errs, fmt.Errorf("wake %q: %w", rec.InstanceID, err))
			continue
		}
		if until, ok := rt.NextWakeup(); ok && !until.After(now) {
			rt.WakeDue(now)
			advanced++
		}
		if err := m.sync(cur, rt); err != nil {
			lock.Unlock()
			return advanced, err
		}
		lock.Unlock()
	}
	return advanced, errors.Join(errs...)
}

// Compensate 对失败实例显式触发补偿(逆序 undo 栈)。
func (m *Manager) Compensate(instanceID string) ([]SideEffectResult, error) {
	lock := m.lockOf(instanceID)
	lock.Lock()
	defer lock.Unlock()

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

// FailedCommands 返回实例日志的死信视图:已派发且最近一次结果为 failed 的命令
// (它们不会出现在 outbox 重投递里,但必须对宿主可见)。
func (m *Manager) FailedCommands(instanceID string) ([]FailedCommand, error) {
	reader, ok := m.Journal.(JournalReader)
	if !ok {
		return nil, ErrJournalNotReadable
	}
	occs, err := reader.LoadOccurrences(instanceID)
	if err != nil {
		return nil, fmt.Errorf("load occurrences: %w", err)
	}
	return FailedCommands(occs), nil
}
