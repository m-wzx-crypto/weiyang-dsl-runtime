package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ExecutionStatus 是实例生命周期的状态机状态（用户建议第 8 点）。
//
//	Pending → Running → Waiting ──(Event)──→ Resume → Running → Completed
//	   ↘ ... → Failed / Canceled / TimedOut
type ExecutionStatus int

const (
	StatusPending ExecutionStatus = iota
	StatusRunning
	StatusWaiting
	StatusSuspended
	StatusCompleted
	StatusFailed
	StatusCanceled
	StatusTimedOut
)

var statusNames = [...]string{
	"pending", "running", "waiting", "suspended",
	"completed", "failed", "canceled", "timed_out",
}

func (s ExecutionStatus) String() string {
	if int(s) >= 0 && int(s) < len(statusNames) {
		return statusNames[s]
	}
	return fmt.Sprintf("status(%d)", int(s))
}

// ParseExecutionStatus 把字符串状态解析为枚举，无法识别时返回 pending。
func ParseExecutionStatus(s string) ExecutionStatus {
	for i, n := range statusNames {
		if n == s {
			return ExecutionStatus(i)
		}
	}
	return StatusPending
}

// Event 是一次进入 Runtime 的领域事件。ID 用于幂等去重（用户建议第 7 点）。
type Event struct {
	ID      string
	Name    string
	Payload map[string]interface{}
	Time    time.Time
}

// ExecutionContext 是 Runtime 的统一上下文（用户建议第 1 点）。
//
// 它把 ProcessID / InstanceID / ExecutionID / CurrentNode / Variables / Event /
// Metadata 集中到一个对象，Executor / Runtime / SideEffect 全部从它取状态，避免
// 复杂的 DSL 把一堆参数到处透传。
//
// 并发契约:上下文不是并发安全的——内部互斥锁只保证 TryConsumeEvent /
// ReleaseEvent / Snapshot 等单项簿记操作的原子性;Variables / Scopes / Waitings
// 等可变状态的读取写入发生在 Runtime 的推进入口内,宿主必须对同一实例串行化
// 调用(见 Runtime 并发契约)。
type ExecutionContext struct {
	ProcessID    string
	DefinitionID string
	InstanceID   string
	ExecutionID  string
	TenantID     string

	CurrentNode  string
	Status       ExecutionStatus
	Variables    map[string]interface{}
	Metadata     map[string]string
	CurrentEvent *Event

	// Engine 是该上下文绑定的表达式引擎：Step / Runtime / 分支条件求值统一从
	// 上下文取引擎（单一事实源），保证校验与执行走同一套编译选项；nil 时回退
	// DefaultExpressionEngine。它不参与序列化，恢复时重置为默认引擎。
	Engine ExpressionEngine

	// Scopes 记录当前活跃的 parallel 作用域栈（支持嵌套 fork/join）。
	Scopes []*ParallelScope

	// Waitings 是 v2 时间契约的等待槽:key 为 "instance"(线性流程)或分支 ID。
	// 值记录该等待点的唤醒时间(timer 到期 / approval 超时),由宿主定时器
	// 依据 NextWakeup 调用 WakeDue 主动推进——超时不再依赖下一个事件的到来。
	Waitings map[string]*WaitingState

	// SideEffectResults 是已交付的全部副作用执行结果(原 Runtime 私有字段,
	// 移入上下文使其随日志可折叠、随 Savepoint 可持久化)。
	SideEffectResults []SideEffectResult

	// UndoStack 是 v2 行为契约的补偿栈:已成功执行的、声明了 Compensation 的
	// 副作用按完成顺序入栈,失败补偿时逆序发射。
	UndoStack []UndoEntry

	// VisitCounts 记录每个节点被执行的次数:用于派生幂等键,保证环路流程二次
	// 经过同一节点时副作用/唤醒命令不会被误去重。
	VisitCounts map[string]int

	// Journal 是确定性内核的记账出口(nil = 不记账,行为与既往完全一致)。
	// 见 journal.go 的记账契约。
	Journal Journal
	// JournalErr 记录最近一次记账失败(WAL 语义:失败不应被静默吞掉)。
	JournalErr error

	Attempt     int
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time

	mu              sync.Mutex
	processedEvents map[string]int64
}

// record 在绑定 Journal 时追加一笔事实;fill 在落库前填充载荷字段。
// 折叠路径(foldContext)的上下文 Journal 恒为 nil,因此 fold 不会二次记账。
func (c *ExecutionContext) record(kind OccKind, fill func(*Occurrence)) {
	if c.Journal == nil {
		return
	}
	occ := &Occurrence{
		Time:        time.Now(),
		InstanceID:  c.InstanceID,
		ExecutionID: c.ExecutionID,
		Kind:        kind,
	}
	if fill != nil {
		fill(occ)
	}
	if err := c.Journal.Append(occ); err != nil {
		c.JournalErr = err
	}
}

// engine 返回上下文绑定的表达式引擎(nil 时回退默认引擎)。
func (c *ExecutionContext) engine() ExpressionEngine {
	if c.Engine != nil {
		return c.Engine
	}
	return DefaultExpressionEngine
}

// NewExecutionContext 创建一次全新的执行上下文。v2 数据契约下,VarInit 中的
// 初始值先落入变量表(Start 注入的同名变量会覆盖它)。
func NewExecutionContext(def *ProcessDef, instanceID, executionID string) *ExecutionContext {
	ctx := &ExecutionContext{
		ProcessID:       def.ID,
		DefinitionID:    def.ID,
		InstanceID:      instanceID,
		ExecutionID:     executionID,
		Status:          StatusPending,
		Variables:       map[string]interface{}{},
		Metadata:        map[string]string{},
		Engine:          DefaultExpressionEngine,
		Waitings:        map[string]*WaitingState{},
		VisitCounts:     map[string]int{},
		processedEvents: map[string]int64{},
		StartedAt:       time.Now(),
	}
	for k, v := range def.VarInit {
		ctx.Variables[k] = v
	}
	return ctx
}

// WithTenant 设置租户并返回自身，便于链式构造。
func (c *ExecutionContext) WithTenant(tenantID string) *ExecutionContext {
	c.TenantID = tenantID
	return c
}

// WithVariables 注入初始流程变量并返回自身。
func (c *ExecutionContext) WithVariables(vars map[string]interface{}) *ExecutionContext {
	for k, v := range vars {
		c.Variables[k] = v
	}
	return c
}

func (c *ExecutionContext) setStatus(s ExecutionStatus) {
	c.mu.Lock()
	c.Status = s
	c.UpdatedAt = time.Now()
	if s == StatusCompleted || s == StatusFailed || s == StatusCanceled || s == StatusTimedOut {
		c.CompletedAt = time.Now()
	}
	c.mu.Unlock()
	c.record(OccStatus, func(o *Occurrence) { o.Status = s.String() })
}

// SetVariable 写入一个流程变量(记账:OccVariableSet)。
func (c *ExecutionContext) SetVariable(key string, value interface{}) {
	c.Variables[key] = value
	c.record(OccVariableSet, func(o *Occurrence) {
		o.VarKey = key
		o.VarValue = value
	})
}

// GetVariable 读取一个流程变量。
func (c *ExecutionContext) GetVariable(key string) interface{} {
	return c.Variables[key]
}

// TryConsumeEvent 以事件 ID 做幂等去重（用户建议第 7 点：消息重试后同一个事件不应
// 再次执行）。返回 false 表示该事件已被消费过，调用方应跳过本次副作用执行。
func (c *ExecutionContext) TryConsumeEvent(eventID string) bool {
	if eventID == "" {
		return true // 空事件 ID 视为不参与去重
	}
	c.mu.Lock()
	if _, seen := c.processedEvents[eventID]; seen {
		c.mu.Unlock()
		return false
	}
	c.processedEvents[eventID] = time.Now().UnixNano()
	c.mu.Unlock()
	c.record(OccEventConsumed, func(o *Occurrence) {
		o.Event = &Event{ID: eventID}
	})
	return true
}

// AcceptEvent 消费事件并把其设为当前事件(Start/Feed 的入口语义)。
// 返回 false 表示事件已被消费过(幂等拒绝),上下文不变。
// 空 ID 事件不参与去重,但同样落账(OccEventConsumed),保证折叠边界一致。
func (c *ExecutionContext) AcceptEvent(ev Event) bool {
	if ev.ID != "" {
		c.mu.Lock()
		if _, seen := c.processedEvents[ev.ID]; seen {
			c.mu.Unlock()
			return false
		}
		c.processedEvents[ev.ID] = time.Now().UnixNano()
		c.mu.Unlock()
	}
	e := ev
	c.record(OccEventConsumed, func(o *Occurrence) { o.Event = &e })
	c.CurrentEvent = &ev
	return true
}

// IsProcessedEvent 查询某事件是否已被消费过。
func (c *ExecutionContext) IsProcessedEvent(eventID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, seen := c.processedEvents[eventID]
	return seen
}

// ReleaseEvent 回滚一次事件消费:事件投递后没有驱动任何迁移时调用,
// 让同一事件的重投递仍有机会被处理(修复"事件被烧掉")。
func (c *ExecutionContext) ReleaseEvent(eventID string) {
	if eventID == "" {
		return
	}
	c.mu.Lock()
	_, existed := c.processedEvents[eventID]
	delete(c.processedEvents, eventID)
	c.mu.Unlock()
	if existed {
		c.record(OccEventReleased, func(o *Occurrence) {
			o.Event = &Event{ID: eventID}
		})
	}
}

// ActiveScope 返回当前最内层的 parallel 作用域；没有则为 nil。
func (c *ExecutionContext) ActiveScope() *ParallelScope {
	if len(c.Scopes) == 0 {
		return nil
	}
	return c.Scopes[len(c.Scopes)-1]
}

// PushScope 压入一个并行作用域(记账:压栈时刻的作用域快照)。
func (c *ExecutionContext) PushScope(s *ParallelScope) {
	c.Scopes = append(c.Scopes, s)
	if c.Journal != nil {
		snap := cloneScope(s)
		c.record(OccScopePushed, func(o *Occurrence) { o.Scope = snap })
	}
}

// PopScope 弹出最内层并行作用域并返回它；空栈返回 nil。
func (c *ExecutionContext) PopScope() *ParallelScope {
	if len(c.Scopes) == 0 {
		return nil
	}
	s := c.Scopes[len(c.Scopes)-1]
	c.Scopes = c.Scopes[:len(c.Scopes)-1]
	fork := s.ForkNode
	c.record(OccScopePopped, func(o *Occurrence) { o.NodeID = fork })
	return s
}

// IncrVisit 递增节点执行计数,返回本次序号(从 1 开始)。用于幂等键派生。
func (c *ExecutionContext) IncrVisit(nodeID string) int {
	c.VisitCounts[nodeID]++
	c.record(OccNodeVisited, func(o *Occurrence) { o.NodeID = nodeID })
	return c.VisitCounts[nodeID]
}

// VisitOf 返回节点已被执行的次数。
func (c *ExecutionContext) VisitOf(nodeID string) int {
	return c.VisitCounts[nodeID]
}

// Snapshot 返回当前上下文的一份快照（浅拷贝），用于读取/持久化。它不从持有锁的
// 结构体整体拷贝（避免复制 sync.Mutex），只搬运纯数据字段，内部可变 map 单独复制，
// Scopes 与 CurrentEvent 以引用共享。
func (c *ExecutionContext) Snapshot() *ExecutionContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &ExecutionContext{
		ProcessID:         c.ProcessID,
		DefinitionID:      c.DefinitionID,
		InstanceID:        c.InstanceID,
		ExecutionID:       c.ExecutionID,
		TenantID:          c.TenantID,
		CurrentNode:       c.CurrentNode,
		Status:            c.Status,
		Variables:         copyMap(c.Variables),
		Metadata:          copyStringMap(c.Metadata),
		CurrentEvent:      c.CurrentEvent,
		Engine:            c.Engine,
		Scopes:            c.Scopes,
		Waitings:          copyWaitings(c.Waitings),
		SideEffectResults: append([]SideEffectResult(nil), c.SideEffectResults...),
		UndoStack:         append([]UndoEntry(nil), c.UndoStack...),
		VisitCounts:       copyVisitCounts(c.VisitCounts),
		Attempt:           c.Attempt,
		StartedAt:         c.StartedAt,
		UpdatedAt:         c.UpdatedAt,
		CompletedAt:       c.CompletedAt,
		processedEvents:   copyIntMap(c.processedEvents),
	}
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyStringMap(m map[string]string) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyIntMap(m map[string]int64) map[string]int64 {
	out := make(map[string]int64, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

func copyWaitings(m map[string]*WaitingState) map[string]*WaitingState {
	if m == nil {
		return nil
	}
	out := make(map[string]*WaitingState, len(m))
	for k, v := range m {
		cp := *v
		out[k] = &cp
	}
	return out
}

func copyVisitCounts(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	out := make(map[string]int, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// executionContextJSON 是持久化用的影子结构：状态以字符串序列化，锁与引擎不落盘
// （引擎是运行时依赖，恢复时重置为默认引擎），幂等去重表随实例一起保存，保证
// 重启恢复后同一事件重放仍被拒绝（用户建议第 7/8 点的持久化闭环）。
type executionContextJSON struct {
	ProcessID         string                   `json:"processId,omitempty"`
	DefinitionID      string                   `json:"definitionId,omitempty"`
	InstanceID        string                   `json:"instanceId,omitempty"`
	ExecutionID       string                   `json:"executionId,omitempty"`
	TenantID          string                   `json:"tenantId,omitempty"`
	CurrentNode       string                   `json:"currentNode,omitempty"`
	Status            string                   `json:"status"`
	Variables         map[string]interface{}   `json:"variables,omitempty"`
	Metadata          map[string]string        `json:"metadata,omitempty"`
	CurrentEvent      *Event                   `json:"currentEvent,omitempty"`
	Scopes            []*ParallelScope         `json:"scopes,omitempty"`
	Waitings          map[string]*WaitingState `json:"waitings,omitempty"`
	SideEffectResults []sideEffectResultJSON   `json:"sideEffectResults,omitempty"`
	UndoStack         []UndoEntry              `json:"undoStack,omitempty"`
	VisitCounts       map[string]int           `json:"visitCounts,omitempty"`
	Attempt           int                      `json:"attempt,omitempty"`
	StartedAt         time.Time                `json:"startedAt,omitempty"`
	UpdatedAt         time.Time                `json:"updatedAt,omitempty"`
	CompletedAt       time.Time                `json:"completedAt,omitempty"`
	ProcessedEvents   map[string]int64         `json:"processedEvents,omitempty"`
}

// sideEffectResultJSON 是 SideEffectResult 的持久化影子(error 接口不可序列化,
// 以文本落盘,恢复后还原为 error)。
type sideEffectResultJSON struct {
	CommandID string                 `json:"commandId"`
	Status    string                 `json:"status"`
	ErrText   string                 `json:"errText,omitempty"`
	Outcome   map[string]interface{} `json:"outcome,omitempty"`
}

func resultsToJSON(in []SideEffectResult) []sideEffectResultJSON {
	if in == nil {
		return nil
	}
	out := make([]sideEffectResultJSON, len(in))
	for i, r := range in {
		out[i] = sideEffectResultJSON{CommandID: r.CommandID, Status: r.Status, Outcome: r.Outcome}
		if r.Error != nil {
			out[i].ErrText = r.Error.Error()
		}
	}
	return out
}

func resultsFromJSON(in []sideEffectResultJSON) []SideEffectResult {
	if in == nil {
		return nil
	}
	out := make([]SideEffectResult, len(in))
	for i, r := range in {
		out[i] = SideEffectResult{CommandID: r.CommandID, Status: r.Status, Outcome: r.Outcome}
		if r.ErrText != "" {
			out[i].Error = errors.New(r.ErrText)
		}
	}
	return out
}

// MarshalJSON 把上下文序列化为可持久化/可传输的 JSON（Savepoint 的底层实现）。
func (c *ExecutionContext) MarshalJSON() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return json.Marshal(&executionContextJSON{
		ProcessID:         c.ProcessID,
		DefinitionID:      c.DefinitionID,
		InstanceID:        c.InstanceID,
		ExecutionID:       c.ExecutionID,
		TenantID:          c.TenantID,
		CurrentNode:       c.CurrentNode,
		Status:            c.Status.String(),
		Variables:         c.Variables,
		Metadata:          c.Metadata,
		CurrentEvent:      c.CurrentEvent,
		Scopes:            c.Scopes,
		Waitings:          c.Waitings,
		SideEffectResults: resultsToJSON(c.SideEffectResults),
		UndoStack:         c.UndoStack,
		VisitCounts:       c.VisitCounts,
		Attempt:           c.Attempt,
		StartedAt:         c.StartedAt,
		UpdatedAt:         c.UpdatedAt,
		CompletedAt:       c.CompletedAt,
		ProcessedEvents:   c.processedEvents,
	})
}

// UnmarshalJSON 从 JSON 恢复上下文：恢复后的实例持有全新的锁与默认表达式引擎，
// 可直接交给 Runtime（WithExecutionContext）继续执行。
func (c *ExecutionContext) UnmarshalJSON(data []byte) error {
	var s executionContextJSON
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	c.ProcessID = s.ProcessID
	c.DefinitionID = s.DefinitionID
	c.InstanceID = s.InstanceID
	c.ExecutionID = s.ExecutionID
	c.TenantID = s.TenantID
	c.CurrentNode = s.CurrentNode
	c.Status = ParseExecutionStatus(s.Status)
	c.Variables = s.Variables
	if c.Variables == nil {
		c.Variables = map[string]interface{}{}
	}
	c.Metadata = s.Metadata
	if c.Metadata == nil {
		c.Metadata = map[string]string{}
	}
	c.CurrentEvent = s.CurrentEvent
	c.Scopes = s.Scopes
	c.Waitings = s.Waitings
	if c.Waitings == nil {
		c.Waitings = map[string]*WaitingState{}
	}
	c.SideEffectResults = resultsFromJSON(s.SideEffectResults)
	c.UndoStack = s.UndoStack
	c.VisitCounts = s.VisitCounts
	if c.VisitCounts == nil {
		c.VisitCounts = map[string]int{}
	}
	c.Attempt = s.Attempt
	c.StartedAt = s.StartedAt
	c.UpdatedAt = s.UpdatedAt
	c.CompletedAt = s.CompletedAt
	c.processedEvents = s.ProcessedEvents
	if c.processedEvents == nil {
		c.processedEvents = map[string]int64{}
	}
	c.Engine = DefaultExpressionEngine
	return nil
}
