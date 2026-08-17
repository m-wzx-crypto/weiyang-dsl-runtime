package dsl

import (
	"fmt"
	"time"
)

// SideEffectCommand 是 Executor 输出的"应该发生什么"的指令。
//
// 用户建议第 2 点：DSL Engine 决定"应该发生什么"（产生 SideEffectCommand），
// Runtime/Worker 决定"怎么让它发生"（通过 SideEffectExecutor 真正执行）。这样重试、
// 幂等、超时、异步、失败恢复、Dead Letter、事务边界都能在 Runtime 层做到，而 Executor
// 只需声明意图，绝不直接调用 sendNotification()/deductInventory() 这类业务副作用。
type SideEffectCommand struct {
	// ID 是命令级的幂等键，由 ExecutionID + 节点 + 序号派生，可安全重放。
	ID      string
	Type    string
	Target  string
	Payload []byte
}

// ToCommand 把节点声明的一个 SideEffect 转成带幂等键的命令。
func ToCommand(se SideEffect, ctx *ExecutionContext, nodeID string, index int) SideEffectCommand {
	id := fmt.Sprintf("%s:%s:%d", ctx.ExecutionID, nodeID, index)
	return SideEffectCommand{
		ID:      id,
		Type:    se.Type,
		Target:  se.Target,
		Payload: se.Payload,
	}
}

// SideEffectResult 是一次副作用执行的产物。
type SideEffectResult struct {
	CommandID string
	Status    string // completed | failed | skipped
	Error     error
	Outcome   map[string]interface{}
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
	res := SideEffectResult{CommandID: cmd.ID}
	if e.executed[cmd.ID] {
		res.Status = "skipped"
		return res
	}
	if !e.knownTypes[cmd.Type] {
		res.Status = "failed"
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
	out := make([]SideEffectResult, len(e.outcomes))
	copy(out, e.outcomes)
	return out
}

// CommandOrchestrator 负责在 Runtime 上组织副作用执行的公共语义：限制重试次数、超时、
// 并统一收集结果。业务侧真正的执行仍然委托给 SideEffectExecutor。
type CommandOrchestrator struct {
	Executor SideEffectExecutor
	MaxRetry int
	Timeout  time.Duration
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
	}
	return SideEffectResult{CommandID: cmd.ID, Status: "failed", Error: fmt.Errorf(
		"side effect %q failed after %d attempts: %w", cmd.ID, attempts, last.Error)}
}