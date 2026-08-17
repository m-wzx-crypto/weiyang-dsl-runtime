package dsl

import (
	"fmt"
	"time"
)

// RuntimeOption 用于配置 Runtime。
type RuntimeOption func(*Runtime)

// WithExecutionContext 显式注入一次执行上下文（用于恢复/继续）。
func WithExecutionContext(ctx *ExecutionContext) RuntimeOption {
	return func(r *Runtime) { r.Ctx = ctx }
}

// WithExpressionEngine 覆盖默认的表达式引擎。
func WithExpressionEngine(engine ExpressionEngine) RuntimeOption {
	return func(r *Runtime) { r.Engine = engine }
}

// WithOrchestrator 注入副作用编排器（自带重试语义）。
func WithOrchestrator(o *CommandOrchestrator) RuntimeOption {
	return func(r *Runtime) { r.Orchestrator = o }
}

// maxBranchSteps / maxDrainSteps 限制自动推进的最大步数，防止死循环。
const (
	maxBranchSteps = 200
	maxDrainSteps  = 2000
)

// Runtime 是 DSL 真正的运行器（用户建议第 1 / 2 / 7 / 8 点的落地）。
//
// Executor（Step）只回答"应该发生什么"；Runtime 负责：
//   - 维护 ExecutionContext 与生命周期状态机（pending→running→waiting→resume→completed/failed）
//   - 通过 SideEffectExecutor 真正执行副作用（重试/幂等交给执行器与编排器）
//   - 用事件 ID 做幂等去重，容忍消息重试
//   - 编排 parallel fork/join 的分支收敛
type Runtime struct {
	Def          *ProcessDef
	Ctx          *ExecutionContext
	Engine       ExpressionEngine
	SideEffect   SideEffectExecutor
	Orchestrator *CommandOrchestrator

	results []SideEffectResult
}

// NewRuntime 创建绑定 def 的 Runtime。sideEffects 可为 nil（仅做迁移不落副作用）。
func NewRuntime(def *ProcessDef, sideEffects SideEffectExecutor, opts ...RuntimeOption) *Runtime {
	if sideEffects == nil {
		sideEffects = NewInMemorySideEffectExecutor()
	}
	engine := ExpressionEngine(DefaultExpressionEngine)
	r := &Runtime{
		Def:        def,
		Engine:     engine,
		SideEffect: sideEffects,
		Ctx:        NewExecutionContext(def, "", ""),
	}
	for _, o := range opts {
		o(r)
	}
	if r.Orchestrator == nil {
		r.Orchestrator = &CommandOrchestrator{Executor: sideEffects}
	}
	return r
}

// Start 启动流程：从 StartNode 起按给定事件推进，副作用由 Executor 声明、Runtime 执行。
func (r *Runtime) Start(instanceID, executionID string, vars map[string]interface{}, ev Event) *ExecutionResult {
	r.Ctx.InstanceID = instanceID
	r.Ctx.ExecutionID = executionID
	if vars != nil {
		for k, v := range vars {
			r.Ctx.Variables[k] = v
		}
	}
	if !r.Ctx.TryConsumeEvent(ev.ID) {
		res := &ExecutionResult{}
		res.Errors = append(res.Errors, fmt.Errorf("idempotency: duplicate start event %q ignored", ev.ID))
		return res
	}
	r.Ctx.CurrentEvent = &ev
	r.Ctx.CurrentNode = r.Def.StartNode
	r.Ctx.setStatus(StatusRunning)
	return r.drain(&ExecutionResult{})
}

// Run 推进当前上下文，直至到达等待点或终止节点（含 parallel fork/join）。
func (r *Runtime) Run() *ExecutionResult {
	return r.drain(&ExecutionResult{})
}

// drain 自动推进线性/分支迁移，直到遇到等待节点、完成或出错。
func (r *Runtime) drain(res *ExecutionResult) *ExecutionResult {
	for i := 0; i < maxDrainSteps; i++ {
		stepRes := Step(r.Def, r.Ctx)
		r.dispatchSideEffects(stepRes.SideEffects)
		mergeResult(res, stepRes)
		if len(stepRes.Errors) > 0 {
			return res
		}
		for _, a := range stepRes.NextActions {
			if a.Type == "run_branch" {
				return r.runParallelMode(res)
			}
		}
		if stepRes.Transition != nil && stepRes.Transition.Status == "completed" {
			return res
		}
		if isWaitingNode(r.Def.Nodes[r.Ctx.CurrentNode]) {
			r.Ctx.setStatus(StatusWaiting)
			return res
		}
	}
	res.Errors = append(res.Errors, fmt.Errorf("drain exceeded %d steps; possible runaway loop", maxDrainSteps))
	r.Ctx.setStatus(StatusFailed)
	return res
}

// Feed 以外部事件恢复一个 waiting 实例（线性或并行分支）。
func (r *Runtime) Feed(ev Event) *ExecutionResult {
	res := &ExecutionResult{}
	if !r.Ctx.TryConsumeEvent(ev.ID) {
		res.Errors = append(res.Errors, fmt.Errorf("idempotency: duplicate event %q ignored", ev.ID))
		return res
	}
	r.Ctx.CurrentEvent = &ev

	scope := r.Ctx.ActiveScope()
	if scope != nil && !scope.satisfied() {
		routed := false
		for _, b := range scope.Branches {
			if b.Status == StatusWaiting {
				if r.feedBranch(scope, b) {
					routed = true
				}
			}
		}
		if scope.satisfied() {
			return r.runParallelMode(res)
		}
		if r.enforceScopeTimeout(scope) {
			res.Errors = append(res.Errors, fmt.Errorf("parallel scope %q timed out", scope.ID))
			return res
		}
		if !routed {
			res.Errors = append(res.Errors, fmt.Errorf("event %q was not handled by any waiting branch", ev.Name))
		}
		r.Ctx.setStatus(StatusWaiting)
		return res
	}

	return r.drain(res)
}

// runParallelMode fork 之后推进所有分支，直至各分支等待/终止/汇合。
func (r *Runtime) runParallelMode(res *ExecutionResult) *ExecutionResult {
	scope := r.Ctx.ActiveScope()
	if scope == nil {
		res.Errors = append(res.Errors, fmt.Errorf("no active parallel scope"))
		r.Ctx.setStatus(StatusFailed)
		return res
	}
	for _, b := range scope.Branches {
		r.advanceBranch(scope, b, res)
	}
	if scope.satisfied() {
		if err := r.completeParallel(res); err == nil && r.Ctx.Status == StatusRunning {
			return r.drain(res)
		}
		return res
	}
	if r.enforceScopeTimeout(scope) {
		res.Errors = append(res.Errors, fmt.Errorf("parallel scope %q timed out", scope.ID))
		return res
	}
	r.Ctx.setStatus(StatusWaiting)
	return res
}

// advanceBranch 自动推进一条分支到等待点 / join / 终止节点。
func (r *Runtime) advanceBranch(scope *ParallelScope, branch *BranchState, res *ExecutionResult) {
	if branch.Done {
		return
	}
	for step := 0; step < maxBranchSteps; step++ {
		node := r.Def.Nodes[branch.CurrentNode]
		if node == nil {
			branch.Status = StatusFailed
			branch.Done = true
			res.Errors = append(res.Errors, fmt.Errorf("branch %q current node not found", branch.CurrentNode))
			return
		}
		switch node.Type {
		case "end":
			branch.Done = true
			branch.Status = StatusCompleted
			branch.FinishedAt = time.Now()
			return
		case "join":
			branchReachedJoin(r.Ctx, branch, node.ID)
			return
		}

		if isWaitingNode(node) {
			branch.Status = StatusWaiting
			return
		}

		if node.Type == "condition" {
			next, err := r.selectConditionNext(node)
			if err != nil {
				branch.Status = StatusFailed
				branch.Done = true
				res.Errors = append(res.Errors, err)
				return
			}
			if next == "" {
				branch.Status = StatusWaiting // 需事件驱动
				return
			}
			r.emitNodeSideEffects(node, res)
			branch.CurrentNode = next
			continue
		}

		// 自动节点（action/notification/start）：走第一个非空 next。
		next := ""
		for _, tr := range node.Transitions {
			if tr.Next != "" {
				next = tr.Next
				break
			}
		}
		if next == "" {
			branch.Done = true
			branch.Status = StatusWaiting // 结构性死路交由 Static Analyzer 报告
			return
		}
		r.emitNodeSideEffects(node, res)
		branch.CurrentNode = next
	}
}

// feedBranch 用当前事件推进一条处于 waiting 的分支；未匹配返回 false。
func (r *Runtime) feedBranch(scope *ParallelScope, b *BranchState) bool {
	node := r.Def.Nodes[b.CurrentNode]
	next := ""
	for _, tr := range node.Transitions {
		if tr.Event == r.Ctx.CurrentEvent.Name {
			next = tr.Next
			break
		}
	}
	if node.Type == "condition" && next == "" {
		n, err := r.selectConditionNext(node)
		if err != nil {
			return false
		}
		next = n
	}
	if next == "" {
		return false
	}
	r.emitNodeSideEffects(node, &ExecutionResult{})
	b.CurrentNode = next
	r.advanceBranch(scope, b, &ExecutionResult{})
	return true
}

// selectConditionNext 对 condition 节点按 when → 默认分支求值；无需事件时返回目标。
func (r *Runtime) selectConditionNext(node *Node) (string, error) {
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
		ok, err := r.Engine.Evaluate(tr.When, r.Ctx.Variables)
		if err != nil {
			return "", fmt.Errorf("condition %q evaluation failed: %w", tr.When, err)
		}
		if ok {
			return tr.Next, nil
		}
	}
	if hasDefault {
		return defaultNext, nil
	}
	return "", nil
}

// completeParallel 在收敛条件满足后弹出作用域，并从 join 节点继续前向迁移。
func (r *Runtime) completeParallel(res *ExecutionResult) error {
	scope := r.Ctx.PopScope()
	if scope == nil {
		return fmt.Errorf("no active parallel scope to complete")
	}
	joinNode := scope.JoinNode
	if joinNode == "" || joinNode == "join" {
		// 没有可抵达的 join 节点：各分支各自在 end 终止，整体视为完成。
		r.Ctx.setStatus(StatusCompleted)
		res.Transition = &StateTransition{From: scope.ForkNode, Status: "joined"}
		res.NextActions = append(res.NextActions, NextAction{Type: "complete"})
		return nil
	}

	r.Ctx.CurrentNode = joinNode
	r.Ctx.setStatus(StatusRunning)
	return nil
}

// applyWaitingState 依据当前节点类型决定实例是继续运行还是等待外部事件。
func (r *Runtime) applyWaitingState() {
	if r.Ctx.Status == StatusCompleted || r.Ctx.Status == StatusFailed {
		return
	}
	if isWaitingNode(r.Def.Nodes[r.Ctx.CurrentNode]) {
		r.Ctx.setStatus(StatusWaiting)
		return
	}
	r.Ctx.setStatus(StatusRunning)
}

// enforceScopeTimeout 在并行 scope 超过超时时将实例置为 timed_out。返回是否超时。
func (r *Runtime) enforceScopeTimeout(scope *ParallelScope) bool {
	if scope == nil || scope.Timeout <= 0 || scope.satisfied() {
		return false
	}
	if scope.elapsed() > scope.Timeout {
		r.Ctx.setStatus(StatusTimedOut)
		return true
	}
	return false
}

// emitNodeSideEffects 把节点声明的副作用提升为命令并交付给执行器。
func (r *Runtime) emitNodeSideEffects(node *Node, res *ExecutionResult) {
	if node == nil || len(node.SideEffects) == 0 {
		return
	}
	cmds := make([]SideEffectCommand, 0, len(node.SideEffects))
	for i, se := range node.SideEffects {
		cmds = append(cmds, ToCommand(se, r.Ctx, node.ID, i))
	}
	res.SideEffects = append(res.SideEffects, cmds...)
	r.dispatchSideEffects(cmds)
}

// dispatchSideEffects 交付命令给 SideEffectExecutor / Orchestrator 真正执行。
func (r *Runtime) dispatchSideEffects(cmds []SideEffectCommand) {
	for _, c := range cmds {
		if r.Orchestrator != nil {
			r.results = append(r.results, r.Orchestrator.Execute(r.Ctx, c))
		} else if r.SideEffect != nil {
			r.results = append(r.results, r.SideEffect.Handle(r.Ctx, c))
		}
	}
}

// SideEffectResults 返回已交付的所有副作用执行结果（观测用）。
func (r *Runtime) SideEffectResults() []SideEffectResult {
	out := make([]SideEffectResult, len(r.results))
	copy(out, r.results)
	return out
}

// Snapshot 返回当前上下文的一份快照。
func (r *Runtime) Snapshot() *ExecutionContext { return r.Ctx.Snapshot() }

// Status 返回实例当前状态。
func (r *Runtime) Status() ExecutionStatus { return r.Ctx.Status }

// isWaitingNode 判断该类型节点需要外部事件驱动（阻塞点）。
func isWaitingNode(node *Node) bool {
	if node == nil {
		return false
	}
	switch node.Type {
	case "approval", "subprocess":
		return true
	default:
		return false
	}
}

func mergeResult(dst, src *ExecutionResult) {
	if src == nil {
		return
	}
	if src.Transition != nil {
		dst.Transition = src.Transition
	}
	dst.Events = append(dst.Events, src.Events...)
	dst.SideEffects = append(dst.SideEffects, src.SideEffects...)
	dst.Errors = append(dst.Errors, src.Errors...)
	dst.NextActions = append(dst.NextActions, src.NextActions...)
}