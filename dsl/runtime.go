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
	// 引擎单一事实源：无论注入顺序如何，最终保证上下文持有可用引擎。
	if r.Engine == nil {
		r.Engine = DefaultExpressionEngine
	}
	if r.Ctx != nil && r.Ctx.Engine == nil {
		r.Ctx.Engine = r.Engine
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
				// 分支推进产生的副作用与错误全部并入本次结果，不再丢弃。
				if r.feedBranch(scope, b, res) {
					routed = true
				}
			}
		}
		// 收敛达成、全部分支结束（可能无一成功）、或 fail 策略触发，
		// 统一交给 runParallelMode 做收敛/失败判定。
		if scope.satisfied() ||
			scope.doneCount() == len(scope.Branches) ||
			(scope.OnFail == "fail" && scope.failedCount() > 0) {
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

	// 失败策略 onFail=fail：任一分支失败即取消其余分支、实例失败（fail-fast）。
	failed := scope.failedCount()
	if scope.OnFail == "fail" && failed > 0 {
		r.cancelPendingBranches(scope)
		r.Ctx.PopScope()
		r.Ctx.setStatus(StatusFailed)
		res.Errors = append(res.Errors, fmt.Errorf(
			"parallel scope %q failed (onFail=fail): %d branch(es) failed", scope.ID, failed))
		return res
	}

	// 全部分支已结束但无一成功：any / n_of_m 无法收敛，实例失败（不允许
	// "失败也算 partial success"）。
	if scope.doneCount() == len(scope.Branches) && !scope.satisfied() {
		r.Ctx.PopScope()
		r.Ctx.setStatus(StatusFailed)
		res.Errors = append(res.Errors, fmt.Errorf(
			"parallel scope %q cannot converge: no branch succeeded", scope.ID))
		return res
	}

	if scope.satisfied() {
		if err := r.completeParallel(res); err == nil && r.Ctx.Status == StatusRunning {
			// 汇合后从 join 节点继续前进（真正的 Join→Next，而非直接判定完成）。
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
			// continue 策略下分支失败被容忍（由 join 按 parallel_failed 路由补偿），
			// 只有 fail 策略才把失败上抛为顶层错误。
			if scope.OnFail == "fail" {
				res.Errors = append(res.Errors, fmt.Errorf("branch %q current node not found", branch.CurrentNode))
			}
			return
		}
		// 命中作用域的汇合点（显式声明或静态推导）即视为该分支完成；
		// 真实 join 节点 ID 与配置无关，不再依赖哨兵值。
		if scope.JoinNode != "" && node.ID == scope.JoinNode {
			branchReachedJoin(r.Ctx, branch, node.ID)
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
				if scope.OnFail == "fail" {
					res.Errors = append(res.Errors, err)
				}
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
// 副作用与错误并入 res（此前会被静默丢弃，违背副作用可见性）。
func (r *Runtime) feedBranch(scope *ParallelScope, b *BranchState, res *ExecutionResult) bool {
	node := r.Def.Nodes[b.CurrentNode]
	if node == nil {
		return false
	}
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
			// 条件求值失败：分支失败；是否上抛取决于作用域的失败策略。
			b.Status = StatusFailed
			b.Done = true
			if scope.OnFail == "fail" {
				res.Errors = append(res.Errors, err)
			}
			return true
		}
		next = n
	}
	if next == "" {
		return false
	}
	r.emitNodeSideEffects(node, res)
	b.CurrentNode = next
	r.advanceBranch(scope, b, res)
	return true
}

// selectConditionNext 对 condition 节点按 when → 默认分支求值；无需事件时返回目标。
// 引擎取自上下文（单一事实源），与 Step 的求值口径完全一致。
func (r *Runtime) selectConditionNext(node *Node) (string, error) {
	engine := r.Ctx.Engine
	if engine == nil {
		engine = DefaultExpressionEngine
	}
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
		ok, err := engine.Evaluate(tr.When, r.Ctx.Variables)
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

// completeParallel 在收敛条件满足后弹出作用域，并从真实 join 节点继续前向迁移：
//   - any / n_of_m 收敛时，仍在等待的分支不再需要，标记取消（partial success）；
//   - 失败分支数写入变量 parallel_failed，join 可用 when 路由到补偿分支；
//   - 无汇合点（各分支在各自终态结束）时整体判定完成。
func (r *Runtime) completeParallel(res *ExecutionResult) error {
	scope := r.Ctx.ActiveScope()
	if scope == nil {
		return fmt.Errorf("no active parallel scope to complete")
	}
	if scope.Mode == "any" || scope.Mode == "n_of_m" {
		r.cancelPendingBranches(scope)
	}
	failed := scope.failedCount()
	// 汇合摘要变量：join 节点可声明 { "when": "parallel_failed > 0", "next": "compensate" }。
	r.Ctx.SetVariable("parallel_failed", failed)

	popped := r.Ctx.PopScope()
	if popped == nil {
		return fmt.Errorf("no active parallel scope to complete")
	}
	joinNode := popped.JoinNode
	// 兼容历史哨兵值："join" 仅在确有同名节点时视为真实汇合点。
	if joinNode == "" || (joinNode == "join" && r.Def.Nodes["join"] == nil) {
		r.Ctx.setStatus(StatusCompleted)
		res.Transition = &StateTransition{From: popped.ForkNode, Status: "joined"}
		res.NextActions = append(res.NextActions, NextAction{Type: "complete"})
		return nil
	}

	r.Ctx.CurrentNode = joinNode
	r.Ctx.setStatus(StatusRunning)
	return nil
}

// cancelPendingBranches 把仍在等待/运行的分支标记为取消并结束（用于 any/n_of_m
// 收敛后的分支清理，以及 onFail=fail 的 fail-fast 取消）。
func (r *Runtime) cancelPendingBranches(scope *ParallelScope) {
	for _, b := range scope.Branches {
		if !b.Done {
			b.Done = true
			b.Status = StatusCanceled
			b.FinishedAt = time.Now()
		}
	}
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

// Savepoint 把当前执行上下文序列化为 JSON，用于崩溃恢复、跨进程迁移或审计留档。
// 与 Snapshot 的区别：Savepoint 产物可落盘/入队，恢复后幂等去重表仍在，
// 同一事件重放依旧被拒绝（真正闭合"暂停 → 等待事件 → 恢复"的生命周期）。
func (r *Runtime) Savepoint() ([]byte, error) { return r.Ctx.MarshalJSON() }

// RestoreExecutionContext 从 Savepoint 产物恢复一个可继续执行的上下文，
// 之后用 WithExecutionContext 注入新 Runtime 继续 Feed。
func RestoreExecutionContext(data []byte) (*ExecutionContext, error) {
	ctx := &ExecutionContext{}
	if err := ctx.UnmarshalJSON(data); err != nil {
		return nil, fmt.Errorf("restore execution context: %w", err)
	}
	return ctx, nil
}

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