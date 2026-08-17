package dsl

import (
	"fmt"
)

// ExecutionOutput 是旧的单步执行输出，保留以兼容既有 API（executor_test 等直接引用）。
type ExecutionOutput struct {
	NextNode    string
	SideEffects []SideEffect
	Status      string
	Error       error
}

// StateTransition 记录一次从 From 到 To 的状态迁移。
type StateTransition struct {
	From   string
	To     string
	Event  string
	Status string
}

// NextAction 是 Executor 建议 Runtime 下一步"应该做什么"的指令。
//   - transition : 继续线性推进到 Target
//   - run_branch : 在并行作用域中运行一条分支 Target（BranchID 指示分支）
//   - complete   : 流程完成
//   - wait_event : 需要在 Target 等待外部事件
type NextAction struct {
	Type     string
	Target   string
	BranchID string
}

// ExecutionResult 是 Executor 的完整产物（用户建议第 1 点）。
//
// Executor 只决定"应该发生什么"，绝不真正执行业务副作用：
// StateTransition + SideEffects(命令级) + Events + Errors + NextActions 全部是意图声明，
// 由 Runtime / Worker 消费后才会落库、发通知、扣库存或调用 AI。
type ExecutionResult struct {
	Transition  *StateTransition
	Events      []Event
	SideEffects []SideEffectCommand
	Errors      []error
	NextActions []NextAction
}

func (r *ExecutionResult) HasErrors() bool { return len(r.Errors) > 0 }

// Execute 执行一次节点迁移（单步、向后兼容）。
// currentNodeID 为当前节点；event 为触发事件；variables 为流程变量，供 condition 节点
// 的 when 表达式求值使用。复杂的生命周期由 Runtime / Step 负责，这里保持简单。
func Execute(def *ProcessDef, currentNodeID string, event string, variables map[string]interface{}) ExecutionOutput {
	node, ok := def.Nodes[currentNodeID]
	if !ok {
		return ExecutionOutput{
			Status: "error",
			Error:  fmt.Errorf("current node %q not found in process definition", currentNodeID),
		}
	}

	if node.Type == "end" {
		return ExecutionOutput{
			NextNode:    "",
			SideEffects: node.SideEffects,
			Status:      "completed",
		}
	}

	// condition 节点分支选择（DSL-7）：
	// 1. 按声明顺序求值所有带 when 的 transition，命中即走该分支；
	// 2. 全部不匹配时，若有不带 when 的 transition，则第一个作为默认分支；
	// 3. 否则回退到按事件名匹配（兼容无 when 的旧 DSL）。
	if node.Type == "condition" {
		hasDefault := false
		defaultNext := ""
		for _, tr := range node.Transitions {
			if tr.When == "" {
				if !hasDefault {
					hasDefault = true
					defaultNext = tr.Next
				}
				continue
			}
			matched, err := DefaultExpressionEngine.Evaluate(tr.When, variables)
			if err != nil {
				return ExecutionOutput{
					Status: "error",
					Error:  fmt.Errorf("condition %q evaluation failed: %w", tr.When, err),
				}
			}
			if matched {
				return ExecutionOutput{
					NextNode:    tr.Next,
					SideEffects: node.SideEffects,
					Status:      "running",
				}
			}
		}
		if hasDefault {
			return ExecutionOutput{
				NextNode:    defaultNext,
				SideEffects: node.SideEffects,
				Status:      "running",
			}
		}
	}

	// 兜底：按事件名匹配（兼容无 when 的 condition 节点及其他节点类型）。
	for _, tr := range node.Transitions {
		if tr.Event == event {
			return ExecutionOutput{
				NextNode:    tr.Next,
				SideEffects: node.SideEffects,
				Status:      "running",
			}
		}
	}

	return ExecutionOutput{
		NextNode:    "",
		SideEffects: nil,
		Status:      "error",
		Error:       fmt.Errorf("event %q is not defined on node %q", event, currentNodeID),
	}
}

// Step 是新的运行器入口：输入 ExecutionContext，输出 ExecutionResult。
// 它在当前节点上完成"迁移决策 + 副作用声明"，并更新上下文状态。
func Step(def *ProcessDef, ctx *ExecutionContext) *ExecutionResult {
	res := &ExecutionResult{}
	node, ok := def.Nodes[ctx.CurrentNode]
	if !ok {
		res.Errors = append(res.Errors, fmt.Errorf("current node %q not found in process definition", ctx.CurrentNode))
		ctx.setStatus(StatusFailed)
		res.Transition = &StateTransition{From: ctx.CurrentNode, Status: "failed"}
		return res
	}

	// 副作用的处理方式：节点声明的副作用只被提升为带幂等键的命令，由 Runtime 交给
	// SideEffectExecutor 真正执行 —— DSL Engine 决定"应该发生什么"。
	for i, se := range node.SideEffects {
		res.SideEffects = append(res.SideEffects, ToCommand(se, ctx, node.ID, i))
	}

	switch node.Type {
	case "end":
		ctx.setStatus(StatusCompleted)
		res.Transition = &StateTransition{From: node.ID, Status: "completed"}
		res.NextActions = append(res.NextActions, NextAction{Type: "complete"})
		return res
	case "parallel":
		tr, actions, err := stepParallel(def, ctx, node)
		if err != nil {
			res.Errors = append(res.Errors, err)
			ctx.setStatus(StatusFailed)
			return res
		}
		res.Transition = tr
		res.NextActions = actions
		return res
	case "join":
		// join 的收敛判定由 Runtime 完成；这里只负责其前向迁移。
		return stepSelectTransition(def, ctx, node, res)
	default:
		return stepSelectTransition(def, ctx, node, res)
	}
}

// stepSelectTransition 完成普通/条件/汇合节点的迁移决策。节点副作用已在 Step 中发出。
func stepSelectTransition(def *ProcessDef, ctx *ExecutionContext, node *Node, res *ExecutionResult) *ExecutionResult {
	event := ""
	if ctx.CurrentEvent != nil {
		event = ctx.CurrentEvent.Name
	}
	variables := ctx.Variables

	if node.Type == "condition" {
		hasDefault := false
		defaultNext := ""
		for _, tr := range node.Transitions {
			if tr.When == "" {
				if !hasDefault {
					hasDefault = true
					defaultNext = tr.Next
				}
				continue
			}
			matched, err := DefaultExpressionEngine.Evaluate(tr.When, variables)
			if err != nil {
				res.Errors = append(res.Errors, fmt.Errorf("condition %q evaluation failed: %w", tr.When, err))
				ctx.setStatus(StatusFailed)
				res.Transition = &StateTransition{From: node.ID, Status: "failed", Event: event}
				return res
			}
			if matched {
				assignTransition(ctx, res, node.ID, tr.Next, event)
				return res
			}
		}
		if hasDefault {
			assignTransition(ctx, res, node.ID, defaultNext, event)
			return res
		}
	}

	for _, tr := range node.Transitions {
		if tr.Event == event {
			assignTransition(ctx, res, node.ID, tr.Next, event)
			return res
		}
	}

	res.Errors = append(res.Errors, fmt.Errorf("event %q is not defined on node %q", event, node.ID))
	ctx.setStatus(StatusFailed)
	res.Transition = &StateTransition{From: node.ID, Status: "failed", Event: event}
	return res
}

// assignTransition 记录一次前向迁移，更新上下文的当前节点。
func assignTransition(ctx *ExecutionContext, res *ExecutionResult, from, to, event string) {
	ctx.setStatus(StatusRunning)
	ctx.CurrentNode = to
	res.Transition = &StateTransition{From: from, To: to, Event: event, Status: "running"}
	res.NextActions = append(res.NextActions, NextAction{Type: "transition", Target: to})
}

// ExecuteFirstStep 启动流程的第一步。
func ExecuteFirstStep(def *ProcessDef, variables map[string]interface{}) ExecutionOutput {
	return Execute(def, def.StartNode, "submit", variables)
}

// ExecuteStep 执行一步（向后兼容包装）。
func ExecuteStep(def *ProcessDef, currentNode string, event string, variables map[string]interface{}) ExecutionOutput {
	return Execute(def, currentNode, event, variables)
}

// GetNextSteps 返回当前节点的可用迁移列表。
func GetNextSteps(def *ProcessDef, currentNodeID string) []Transition {
	node, ok := def.Nodes[currentNodeID]
	if !ok {
		return nil
	}
	return node.Transitions
}

// GetCurrentNode 返回当前节点定义。
func GetCurrentNode(def *ProcessDef, currentNodeID string) *Node {
	if def == nil {
		return nil
	}
	return def.Nodes[currentNodeID]
}

// IsEndNodeByDef 判断指定节点是否为终止节点。
func IsEndNodeByDef(def *ProcessDef, nodeID string) bool {
	if def == nil {
		return false
	}
	node, ok := def.Nodes[nodeID]
	return ok && node.Type == "end"
}