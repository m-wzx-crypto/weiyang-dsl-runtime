package dsl

import (
	"fmt"
	"sync"
)

// SideEffectCommand 是 Executor 输出的"应该发生什么"的指令。
//
// 用户建议第 2 点：DSL Engine 决定"应该发生什么"（产生 SideEffectCommand），
// Runtime/Worker 决定"怎么让它发生"（通过 SideEffectExecutor 真正执行）。这样重试、
// 幂等、超时、异步、失败恢复、Dead Letter、事务边界都能在 Runtime 层做到，而 Executor
// 只需声明意图，绝不直接调用 sendNotification()/deductInventory() 这类业务副作用。
type SideEffectCommand struct {
	// ID 是命令级的幂等键，由 ExecutionID + 节点 + 节点执行序号 + 序号派生。
	// 包含执行序号是刻意的:环路流程二次经过同一节点时,两次执行的副作用是
	// 两次真实业务动作,不应被幂等去重误杀。
	ID string
	// NodeID 是声明该副作用的节点,补偿审计时用于追溯。
	NodeID string
	Type   string
	Target string
	// Payload 是命令载荷(业务语义由宿主的 SideEffectExecutor 解释)。
	Payload []byte
	// Critical 声明该副作用是业务关键动作(如扣库存):执行失败时所在节点/
	// 分支按失败处理,流程不会带着"扣款未发生"的假状态继续前进。默认 false
	// ——非关键副作用(如通知)失败仅落账,由死信视图/宿主对账兜底。
	Critical bool
	// Compensation 是 v2 行为契约:该副作用的逆操作声明。执行成功后进入
	// 实例 undo 栈(见 compensate.go)。
	Compensation *SideEffect
}

// ToCommand 把节点声明的一个 SideEffect 转成带幂等键的命令。
func ToCommand(se SideEffect, ctx *ExecutionContext, nodeID string, index int) SideEffectCommand {
	id := fmt.Sprintf("%s:%s:%d:%d", ctx.ExecutionID, nodeID, ctx.VisitOf(nodeID), index)
	return SideEffectCommand{
		ID:           id,
		NodeID:       nodeID,
		Type:         se.Type,
		Target:       se.Target,
		Payload:      se.Payload,
		Critical:     se.Critical,
		Compensation: se.Compensation,
	}
}

// SideEffectResult 是一次副作用执行的产物。
type SideEffectResult struct {
	CommandID string
	Status    string // completed | failed | skipped
	Error     error
	Outcome   map[string]interface{}
	// Permanent 标记失败为永久性(参数非法/类型不支持等,重试必然再失败)。
	// 编排器看到 Permanent 失败立即返回,不再重试;由死信视图/人工介入兜底。
	Permanent bool
}

// SideEffectExecutor 是真正执行副作用的抽象（Runtime 注入，业务侧实现）。
type SideEffectExecutor interface {
	// Handle 执行一个副作用命令。同一个 CommandID 若已成功执行过，实现方应幂等返回。
	Handle(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult
}

// SideEffectFunc 是 SideEffectExecutor 的函数式适配器。
type SideEffectFunc func(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult

func (f SideEffectFunc) Handle(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult {
	return f(ctx, cmd)
}

// InMemorySideEffectExecutor 是一个默认实现：它不做真实业务，只依据命令 Type 是否在
// knownTypes 中决定成功与失败，并把结果记录到 Out（供测试与观测）。它对已执行的
// CommandID 做幂等去重：同一命令再次交付时返回 skipped，模拟真实世界的消息重试。
type InMemorySideEffectExecutor struct {
	mu         sync.Mutex
	knownTypes map[string]bool
	executed   map[string]bool
	outcomes   []SideEffectResult
}

// NewInMemorySideEffectExecutor 创建内存实现；传入的 knownTypes 之外的命令会被判为失败。
func NewInMemorySideEffectExecutor(knownTypes ...string) *InMemorySideEffectExecutor {
	kt := make(map[string]bool, len(knownTypes))
	for _, t := range knownTypes {
		kt[t] = true
	}
	return &InMemorySideEffectExecutor{
		knownTypes: kt,
		executed:   make(map[string]bool),
	}
}

func (e *InMemorySideEffectExecutor) Handle(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	res := SideEffectResult{CommandID: cmd.ID}
	if e.executed[cmd.ID] {
		res.Status = "skipped"
		return res
	}
	if !e.knownTypes[cmd.Type] {
		// 类型不支持是永久性错误:宿主路由表没有该类型,重试不会改变结果。
		res.Status = "failed"
		res.Permanent = true
		res.Error = fmt.Errorf("side effect type %q is not supported", cmd.Type)
	} else {
		res.Status = "completed"
		e.executed[cmd.ID] = true
	}
	e.outcomes = append(e.outcomes, res)
	return res
}

// Outcomes 返回已执行过的命令结果（仅内存实现用于断言/观测）。
func (e *InMemorySideEffectExecutor) Outcomes() []SideEffectResult {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]SideEffectResult, len(e.outcomes))
	copy(out, e.outcomes)
	return out
}

// CommandOrchestrator 负责在 Runtime 上组织副作用执行的公共语义：限制重试次数、
// 并统一收集结果。业务侧真正的执行仍然委托给 SideEffectExecutor。
// 注意:不提供 Timeout 字段——放弃等待无法取消已发出的真实业务动作,只会留下
// 不确定状态;超时/异步语义属于 SideEffectExecutor 实现方的职责(它知道如何
// 安全地取消或对账)。
type CommandOrchestrator struct {
	Executor SideEffectExecutor
	MaxRetry int
}

func (o *CommandOrchestrator) Execute(ctx *ExecutionContext, cmd SideEffectCommand) SideEffectResult {
	attempts := 1
	if o.MaxRetry > 0 {
		attempts = o.MaxRetry + 1
	}
	var last SideEffectResult
	for i := 0; i < attempts; i++ {
		last = o.Executor.Handle(ctx, cmd)
		if last.Error == nil {
			return last
		}
		// 永久性失败(参数非法/类型不支持):重试必然再失败,立即返回,
		// 把错误交给死信视图与人工/对账路径,不放大故障。
		if last.Permanent {
			return last
		}
	}
	return SideEffectResult{CommandID: cmd.ID, Status: "failed", Error: fmt.Errorf(
		"side effect %q failed after %d attempts: %w", cmd.ID, attempts, last.Error)}
}
