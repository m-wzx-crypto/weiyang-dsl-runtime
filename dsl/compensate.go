package dsl

import (
	"fmt"
)

// compensate.go 实现 v2 行为契约:补偿(Saga)。
//
// 业务节点在 sideEffects[].compensation 中声明逆操作(如 deduct 的 refund);
// 副作用真正执行成功后,其补偿进入实例的 undo 栈。实例失败时(WithAutoCompensate
// 开启)或宿主显式调用 Compensate() 时,按完成顺序的逆序发射补偿命令。
// 补偿命令同样带幂等键,可安全重放;Done 标记保证重复 Compensate 不会二次执行。

// UndoEntry 是 undo 栈中的一笔可补偿副作用。
type UndoEntry struct {
	// NodeID 是发起副作用的节点 ID。
	NodeID string
	// CommandID 是原副作用的命令幂等键(审计可追溯)。
	CommandID string
	// Key 是补偿命令自身的幂等键。
	Key string
	// Effect 是要执行的逆操作声明。
	Effect SideEffect
	// Done 标记该补偿已发射,重复 Compensate 跳过。
	Done bool
}

// recordCompensation 在副作用命令成功完成后,把声明的补偿压入 undo 栈。
func (r *Runtime) recordCompensation(cmd SideEffectCommand, result SideEffectResult) {
	if cmd.Compensation == nil || result.Status != "completed" {
		return
	}
	r.Ctx.UndoStack = append(r.Ctx.UndoStack, UndoEntry{
		NodeID:    cmd.NodeID,
		CommandID: cmd.ID,
		Key:       fmt.Sprintf("undo:%d:%s", len(r.Ctx.UndoStack), cmd.ID),
		Effect:    *cmd.Compensation,
	})
}

// Compensate 按逆序对 undo 栈中未补偿的条目发射补偿命令,返回本次执行的结果。
// 可安全重复调用:已补偿条目自动跳过。
func (r *Runtime) Compensate() []SideEffectResult {
	before := len(r.results)
	r.compensate(&ExecutionResult{})
	out := make([]SideEffectResult, len(r.results)-before)
	copy(out, r.results[before:])
	return out
}

// compensate 是 Compensate 的内部形态:把补偿命令并入 res,结果记入 r.results,
// 供 onFailure 自动补偿复用。
func (r *Runtime) compensate(res *ExecutionResult) {
	for i := len(r.Ctx.UndoStack) - 1; i >= 0; i-- {
		entry := &r.Ctx.UndoStack[i]
		if entry.Done {
			continue
		}
		entry.Done = true
		cmd := SideEffectCommand{
			ID:      entry.Key,
			NodeID:  entry.NodeID,
			Type:    entry.Effect.Type,
			Target:  entry.Effect.Target,
			Payload: entry.Effect.Payload,
		}
		res.SideEffects = append(res.SideEffects, cmd)
		var result SideEffectResult
		if r.Orchestrator != nil {
			result = r.Orchestrator.Execute(r.Ctx, cmd)
		} else if r.SideEffect != nil {
			result = r.SideEffect.Handle(r.Ctx, cmd)
		} else {
			continue
		}
		r.results = append(r.results, result)
	}
}

// onFailure 在实例进入 failed 后触发自动补偿(需 WithAutoCompensate 开启)。
func (r *Runtime) onFailure(res *ExecutionResult) {
	if !r.AutoCompensate || r.Ctx.Status != StatusFailed {
		return
	}
	r.compensate(res)
}
