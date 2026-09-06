package dsl

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// journal.go 实现确定性内核(阶段 1:事件溯源)。
//
// 引擎的每一次状态变更都以不可变"事实"(Occurrence)追加进日志(Journal);
// 实例状态是"折叠日志"的产物:状态 = fold(日志)。由此一次拿到四样能力:
//
//   - 无损恢复:日志即真相,任意进程/机器可从日志重建可继续执行的上下文;
//   - 时间旅行:FoldTo 可重建任意历史时刻的实例状态;
//   - 完整审计:谁在何时消费了什么事件、走了哪条迁移、派发了什么命令、结果如何;
//   - Outbox 视图:PendingCommands 找出"已派发但未 resolved"的命令,配合命令
//     幂等键即得"至少一次 + 幂等"投递语义(持久化 Journal 是阶段 2)。
//
// 记账契约:
//   - Journal 为 nil 时引擎行为与既往完全一致(零开销、向后兼容);
//   - Append 由实例的执行 goroutine 调用(见 Runtime 并发契约),实现方需保证
//     跨实例并发安全(如同一个数据库);Append 失败视为致命(类似 WAL),
//     引擎记入 JournalErr 并在后续结果中可见,不静默吞掉。
//   - 折叠精确性由性质测试保证:任意流程执行后,从空日志折叠出的上下文与
//     活上下文逐字段一致(journal_test.go)。

// OccKind 标识一笔事实的类型。
type OccKind int

const (
	OccStarted            OccKind = iota // 实例/执行 ID 绑定(Start)
	OccEventConsumed                     // 事件被接受(含引擎自产的 wake 事件)
	OccEventReleased                     // 事件消费被回滚(未驱动任何迁移)
	OccNodeVisited                       // 节点执行计数 +1(幂等键的确定性来源)
	OccTransition                        // 前向迁移 from→to(节点与 running 状态随之确定)
	OccNodeSet                           // 直接落点(completeParallel 汇合后的 join 节点等)
	OccStatus                            // 生命周期状态变更
	OccVariableSet                       // 流程变量写入(output 映射、汇总变量、注入变量)
	OccScopePushed                       // parallel 作用域压栈(含压栈时刻的分支快照)
	OccScopePopped                       // parallel 作用域弹出
	OccBranchUpdated                     // 分支状态快照(移动/等待/失败/汇合/取消)
	OccWaitingSet                        // 等待槽登记(timer/deadline/scope 超时)
	OccWaitingCleared                    // 等待槽清除
	OccWoke                              // 时间驱动的唤醒生效(引擎自产迁移)
	OccCommandIssued                     // 副作用命令派发(outbox 的"已发出")
	OccCommandResult                     // 副作用命令结果(outbox 的"已解决")
	OccCompensationPushed                // 补偿入 undo 栈
	OccCompensationFired                 // 补偿发射(undo 条目标记 Done)
)

func (k OccKind) String() string {
	names := [...]string{
		"started", "event_consumed", "event_released", "node_visited",
		"transition", "node_set", "status", "variable_set",
		"scope_pushed", "scope_popped", "branch_updated",
		"waiting_set", "waiting_cleared", "woke",
		"command_issued", "command_result", "compensation_pushed", "compensation_fired",
	}
	if int(k) >= 0 && int(k) < len(names) {
		return names[k]
	}
	return fmt.Sprintf("occ(%d)", int(k))
}

// Occurrence 是日志中的一笔不可变事实。载荷字段按 Kind 取用,JSON 序列化时
// 未用字段全部省略——一个宽结构换取可持久化与可读的审计流。
type Occurrence struct {
	Seq         int64     `json:"seq"`
	Time        time.Time `json:"time"`
	InstanceID  string    `json:"instanceId,omitempty"`
	ExecutionID string    `json:"executionId,omitempty"`
	Kind        OccKind   `json:"kind"`

	// OccEventConsumed / OccEventReleased
	Event *Event `json:"event,omitempty"`

	// OccStarted(仅 ID)/ OccNodeVisited / OccNodeSet / OccBranchUpdated
	// (NodeID 为 fork 节点)/ OccWaitingSet/Cleared/Woke(Slot)
	NodeID string `json:"nodeId,omitempty"`
	Slot   string `json:"slot,omitempty"`

	// OccTransition
	FromNode        string `json:"fromNode,omitempty"`
	ToNode          string `json:"toNode,omitempty"`
	TransitionEvent string `json:"transitionEvent,omitempty"`

	// OccStatus
	Status string `json:"status,omitempty"`

	// OccVariableSet
	VarKey   string      `json:"varKey,omitempty"`
	VarValue interface{} `json:"varValue,omitempty"`

	// OccBranchUpdated:分支整体快照(含 ArrivedJoin)
	Branch *BranchState `json:"branch,omitempty"`

	// OccWaitingSet:等待槽快照
	Waiting *WaitingState `json:"waiting,omitempty"`

	// OccScopePushed:压栈时刻的作用域快照(深拷贝)
	Scope *ParallelScope `json:"scope,omitempty"`

	// OccCommandIssued
	Command *SideEffectCommand `json:"command,omitempty"`

	// OccCommandResult
	CommandID string                 `json:"commandId,omitempty"`
	Result    string                 `json:"result,omitempty"`
	Outcome   map[string]interface{} `json:"outcome,omitempty"`
	ErrText   string                 `json:"errText,omitempty"`

	// OccCompensationPushed / OccCompensationFired
	Undo *UndoEntry `json:"undo,omitempty"`
	Key  string     `json:"key,omitempty"`
}

// Journal 是事实的追加落点(阶段 2 提供持久化实现;内核只依赖此接口)。
type Journal interface {
	// Append 追加一笔事实;实现方负责分配 Seq(单调递增)。
	// 由实例执行 goroutine 串行调用,实现需跨实例并发安全。
	Append(occ *Occurrence) error
}

// MemoryJournal 是进程内的默认实现(测试与嵌入式场景)。
type MemoryJournal struct {
	mu   sync.Mutex
	occs []Occurrence
	next int64
}

func NewMemoryJournal() *MemoryJournal { return &MemoryJournal{} }

func (m *MemoryJournal) Append(occ *Occurrence) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.next++
	occ.Seq = m.next
	m.occs = append(m.occs, *occ)
	return nil
}

// Occurrences 返回日志的副本(读取安全)。
func (m *MemoryJournal) Occurrences() []Occurrence {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Occurrence, len(m.occs))
	copy(out, m.occs)
	return out
}

// LoadOccurrences 返回指定实例的事实(按 Seq 升序),实现 JournalReader。
func (m *MemoryJournal) LoadOccurrences(instanceID string) ([]Occurrence, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Occurrence
	for _, occ := range m.occs {
		if occ.InstanceID == instanceID {
			out = append(out, occ)
		}
	}
	return out, nil
}

// Len 返回当前事实条数。
func (m *MemoryJournal) Len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.occs)
}

// foldContext 按日志重建上下文。apply 只改状态、不再记账(折叠上下文的
// Journal 恒为 nil),因此 fold(日志) 幂等且不产生二次事实。
func foldContext(def *ProcessDef, occs []Occurrence) (*ExecutionContext, error) {
	ctx := NewExecutionContext(def, "", "")
	for i := range occs {
		occ := &occs[i]
		switch occ.Kind {
		case OccStarted:
			ctx.InstanceID = occ.InstanceID
			ctx.ExecutionID = occ.ExecutionID
		case OccEventConsumed:
			if occ.Event != nil {
				if occ.Event.ID != "" {
					ctx.processedEvents[occ.Event.ID] = occ.Time.UnixNano()
				}
				ctx.CurrentEvent = occ.Event
			}
		case OccEventReleased:
			if occ.Event != nil {
				delete(ctx.processedEvents, occ.Event.ID)
			}
		case OccNodeVisited:
			ctx.VisitCounts[occ.NodeID]++
		case OccTransition:
			ctx.CurrentNode = occ.ToNode
			applyStatus(ctx, StatusRunning, occ.Time)
		case OccNodeSet:
			ctx.CurrentNode = occ.NodeID
		case OccStatus:
			applyStatus(ctx, ParseExecutionStatus(occ.Status), occ.Time)
		case OccVariableSet:
			ctx.Variables[occ.VarKey] = occ.VarValue
		case OccScopePushed:
			if occ.Scope == nil {
				return nil, fmt.Errorf("occ#%d: scope_pushed without snapshot", occ.Seq)
			}
			ctx.Scopes = append(ctx.Scopes, cloneScope(occ.Scope))
		case OccScopePopped:
			if n := len(ctx.Scopes); n > 0 {
				ctx.Scopes = ctx.Scopes[:n-1]
			}
		case OccBranchUpdated:
			if occ.Branch == nil {
				return nil, fmt.Errorf("occ#%d: branch_updated without snapshot", occ.Seq)
			}
			scope := findScopeByFork(ctx, occ.NodeID)
			if scope == nil {
				return nil, fmt.Errorf("occ#%d: branch %q references unknown scope %q", occ.Seq, occ.Branch.ID, occ.NodeID)
			}
			b, ok := scope.Branches[occ.Branch.ID]
			if !ok {
				return nil, fmt.Errorf("occ#%d: unknown branch %q in scope %q", occ.Seq, occ.Branch.ID, occ.NodeID)
			}
			*b = *occ.Branch
			// branchReachedJoin 曾补填的汇合点由此恢复。
			if scope.JoinNode == "" && b.ArrivedJoin != "" {
				scope.JoinNode = b.ArrivedJoin
			}
		case OccWaitingSet:
			if occ.Waiting == nil {
				return nil, fmt.Errorf("occ#%d: waiting_set without snapshot", occ.Seq)
			}
			w := *occ.Waiting
			ctx.Waitings[occ.Slot] = &w
		case OccWaitingCleared:
			delete(ctx.Waitings, occ.Slot)
		case OccWoke:
			ctx.CurrentEvent = nil
			delete(ctx.Waitings, occ.Slot)
		case OccCommandIssued:
			// 纯审计/outbox 事实,不落上下文。
		case OccCommandResult:
			res := SideEffectResult{CommandID: occ.CommandID, Status: occ.Result, Outcome: occ.Outcome}
			if occ.ErrText != "" {
				res.Error = errors.New(occ.ErrText)
			}
			ctx.SideEffectResults = append(ctx.SideEffectResults, res)
		case OccCompensationPushed:
			if occ.Undo != nil {
				e := *occ.Undo
				ctx.UndoStack = append(ctx.UndoStack, e)
			}
		case OccCompensationFired:
			for j := range ctx.UndoStack {
				if ctx.UndoStack[j].Key == occ.Key {
					ctx.UndoStack[j].Done = true
				}
			}
		default:
			return nil, fmt.Errorf("occ#%d: unknown kind %s", occ.Seq, occ.Kind)
		}
	}
	return ctx, nil
}

// applyStatus 折叠路径的 setStatus(不记账、以事实时间为准)。
func applyStatus(ctx *ExecutionContext, s ExecutionStatus, at time.Time) {
	ctx.Status = s
	ctx.UpdatedAt = at
	if isTerminalStatus(s) {
		ctx.CompletedAt = at
	}
}

func findScopeByFork(ctx *ExecutionContext, forkNode string) *ParallelScope {
	for _, s := range ctx.Scopes {
		if s.ForkNode == forkNode {
			return s
		}
	}
	return nil
}

// cloneScope 深拷贝作用域(事实快照与折叠重建共用)。
func cloneScope(s *ParallelScope) *ParallelScope {
	if s == nil {
		return nil
	}
	cp := *s
	if s.Branches != nil {
		cp.Branches = make(map[string]*BranchState, len(s.Branches))
		for id, b := range s.Branches {
			bcp := *b
			cp.Branches[id] = &bcp
		}
	}
	return &cp
}

// Fold 从日志重建实例的当前状态。结果上下文未绑定 Journal(只读视图);
// 若要继续执行,使用 Resume。
func Fold(def *ProcessDef, occs []Occurrence) (*ExecutionContext, error) {
	return foldContext(def, occs)
}

// FoldTo 重建日志前 upto 条(含)事实时刻的状态——时间旅行。
func FoldTo(def *ProcessDef, occs []Occurrence, upto int64) (*ExecutionContext, error) {
	for i := range occs {
		if occs[i].Seq > upto {
			return foldContext(def, occs[:i])
		}
	}
	return foldContext(def, occs)
}

// Resume 把日志折叠为可继续执行的上下文并绑定 Runtime。宿主通过
// WithJournal 提供续写用的 Journal(幂等表/undo 栈/等待槽均已随日志恢复,
// 恢复后同一事件重放仍被拒绝、同一命令重放仍被去重)。
func Resume(def *ProcessDef, occs []Occurrence, sideEffects SideEffectExecutor, opts ...RuntimeOption) (*Runtime, error) {
	ctx, err := Fold(def, occs)
	if err != nil {
		return nil, err
	}
	all := append([]RuntimeOption{WithExecutionContext(ctx)}, opts...)
	return NewRuntime(def, sideEffects, all...), nil
}

// PendingCommands 返回"已派发但未 resolved"的命令——日志的 outbox 视图。
// 宿主的 worker 恢复时据此重投递;命令幂等键保证重投递安全。
func PendingCommands(occs []Occurrence) []SideEffectCommand {
	pending := map[string]*SideEffectCommand{}
	var order []string
	for i := range occs {
		switch occs[i].Kind {
		case OccCommandIssued:
			if occs[i].Command != nil {
				if _, seen := pending[occs[i].Command.ID]; !seen {
					order = append(order, occs[i].Command.ID)
				}
				cp := *occs[i].Command
				pending[cp.ID] = &cp
			}
		case OccCommandResult:
			delete(pending, occs[i].CommandID)
		}
	}
	out := make([]SideEffectCommand, 0, len(pending))
	for _, id := range order {
		if cmd, ok := pending[id]; ok {
			out = append(out, *cmd)
		}
	}
	return out
}

// marshalOccs 供测试与调试:把日志序列化为 JSON(审计留档格式)。
func marshalOccs(occs []Occurrence) ([]byte, error) {
	return json.Marshal(occs)
}
