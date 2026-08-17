package dsl

import (
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

	// Scopes 记录当前活跃的 parallel 作用域栈（支持嵌套 fork/join）。
	Scopes []*ParallelScope

	Attempt     int
	StartedAt   time.Time
	UpdatedAt   time.Time
	CompletedAt time.Time

	mu              sync.Mutex
	processedEvents map[string]int64
}

// NewExecutionContext 创建一次全新的执行上下文。
func NewExecutionContext(def *ProcessDef, instanceID, executionID string) *ExecutionContext {
	return &ExecutionContext{
		ProcessID:        def.ID,
		DefinitionID:     def.ID,
		InstanceID:       instanceID,
		ExecutionID:      executionID,
		Status:           StatusPending,
		Variables:        map[string]interface{}{},
		Metadata:         map[string]string{},
		processedEvents:  map[string]int64{},
		StartedAt:        time.Now(),
	}
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
}

// SetVariable 写入一个流程变量。
func (c *ExecutionContext) SetVariable(key string, value interface{}) {
	c.Variables[key] = value
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
	defer c.mu.Unlock()
	if _, seen := c.processedEvents[eventID]; seen {
		return false
	}
	c.processedEvents[eventID] = time.Now().UnixNano()
	return true
}

// IsProcessedEvent 查询某事件是否已被消费过。
func (c *ExecutionContext) IsProcessedEvent(eventID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, seen := c.processedEvents[eventID]
	return seen
}

// ActiveScope 返回当前最内层的 parallel 作用域；没有则为 nil。
func (c *ExecutionContext) ActiveScope() *ParallelScope {
	if len(c.Scopes) == 0 {
		return nil
	}
	return c.Scopes[len(c.Scopes)-1]
}

// PushScope 压入一个并行作用域。
func (c *ExecutionContext) PushScope(s *ParallelScope) {
	c.Scopes = append(c.Scopes, s)
}

// PopScope 弹出最内层并行作用域并返回它；空栈返回 nil。
func (c *ExecutionContext) PopScope() *ParallelScope {
	if len(c.Scopes) == 0 {
		return nil
	}
	s := c.Scopes[len(c.Scopes)-1]
	c.Scopes = c.Scopes[:len(c.Scopes)-1]
	return s
}

// Snapshot 返回当前上下文的一份快照（浅拷贝），用于读取/持久化。它不从持有锁的
// 结构体整体拷贝（避免复制 sync.Mutex），只搬运纯数据字段，内部可变 map 单独复制，
// Scopes 与 CurrentEvent 以引用共享。
func (c *ExecutionContext) Snapshot() *ExecutionContext {
	c.mu.Lock()
	defer c.mu.Unlock()
	return &ExecutionContext{
		ProcessID:       c.ProcessID,
		DefinitionID:    c.DefinitionID,
		InstanceID:      c.InstanceID,
		ExecutionID:     c.ExecutionID,
		TenantID:        c.TenantID,
		CurrentNode:     c.CurrentNode,
		Status:          c.Status,
		Variables:       copyMap(c.Variables),
		Metadata:        copyStringMap(c.Metadata),
		CurrentEvent:    c.CurrentEvent,
		Scopes:          c.Scopes,
		Attempt:         c.Attempt,
		StartedAt:       c.StartedAt,
		UpdatedAt:       c.UpdatedAt,
		CompletedAt:     c.CompletedAt,
		processedEvents: copyIntMap(c.processedEvents),
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