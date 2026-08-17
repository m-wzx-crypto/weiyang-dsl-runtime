package dsl

import (
	"fmt"
	"time"
)

// ForkConfig 描述 parallel 节点的 fork 模式（用户建议第 4 点）。
type ForkConfig struct {
	// Mode: "all"（默认，等待全部分支）| "any"（任一分支满足即可）。
	Mode string
}

// JoinConfig 描述 join 节点的收敛策略。
type JoinConfig struct {
	// Mode: "all"（默认）| "any" | "n_of_m"。
	Mode string
	// Required 供 n_of_m 使用：满足的分支数量阈值。
	Required int
	// Timeout 是可选的收敛等待时长，例如 "30s"；空表示不设超时。
	Timeout string
}

// BranchState 记录 parallel 下某一条分支的运行时状态。
type BranchState struct {
	ID          string
	StartNode   string
	CurrentNode string
	Status      ExecutionStatus
	Done        bool
	// ArrivedJoin 记录该分支所汇聚到的 join 节点 ID（若经由 join 结束）。
	ArrivedJoin string
	FinishedAt  time.Time
}

// ParallelScope 表示一次 fork/join 的完整并行作用域。
type ParallelScope struct {
	ID        string
	ForkNode  string
	JoinNode  string
	Mode      string
	Required  int
	Timeout   time.Duration
	StartedAt time.Time
	Branches  map[string]*BranchState
	Status    ExecutionStatus
}

func defaultForkMode(mode string) string {
	if mode == "" {
		return "all"
	}
	return mode
}

func defaultJoinMode(mode string) string {
	if mode == "" {
		return "all"
	}
	return mode
}

// branchConfig 解析 fork/join 语义参数，供 Runtime 使用。
func (fs *ParallelScope) doneCount() int {
	n := 0
	for _, b := range fs.Branches {
		if b.Done {
			n++
		}
	}
	return n
}

// satisfied 判断当前作用域是否已达到收敛条件。
func (fs *ParallelScope) satisfied() bool {
	if fs == nil || len(fs.Branches) == 0 {
		return false
	}
	done := fs.doneCount()
	switch fs.Mode {
	case "any":
		return done >= 1
	case "n_of_m":
		if fs.Required <= 0 {
			fs.Required = 1
		}
		return done >= fs.Required
	default: // all
		return done == len(fs.Branches)
	}
}

// elapsed 返回自作用域创建以来的时长。
func (fs *ParallelScope) elapsed() time.Duration {
	return time.Since(fs.StartedAt)
}

// resolveJoinTimeout 解析 join 的超时字符串；非法或为空返回 0（表示不超时）。
func resolveJoinTimeout(raw string) time.Duration {
	if raw == "" {
		return 0
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0
	}
	return d
}

// stepParallel 执行 parallel 节点的 fork：
//   - 从节点的每条 transition 派生一条分支（分支入口为该 transition.Next）；
//   - 记录一个新的 ParallelScope；分支的具体推进交给 Runtime 回调推进。
//
// 它不阻塞、不做实际业务，只产出分支的 NextAction，把"并发怎么跑"交给 Runtime。
func stepParallel(def *ProcessDef, ctx *ExecutionContext, node *Node) (*StateTransition, []NextAction, error) {
	if len(node.Transitions) == 0 {
		return nil, nil, fmt.Errorf("parallel node %q must declare at least one branch transition", node.ID)
	}

	mode := "all"
	if node.Fork != nil {
		mode = defaultForkMode(node.Fork.Mode)
	}

	scope := &ParallelScope{
		ID:        node.ID,
		ForkNode:  node.ID,
		Mode:      mode,
		StartedAt: time.Now(),
		Status:    StatusRunning,
		Branches:  map[string]*BranchState{},
	}
	if node.Join != nil {
		scope.JoinNode = "join"
		scope.Required = node.Join.Required
		scope.Timeout = resolveJoinTimeout(node.Join.Timeout)
		if node.Join.Mode != "" {
			scope.Mode = defaultJoinMode(node.Join.Mode)
		}
	}

	actions := make([]NextAction, 0, len(node.Transitions))
	for i, tr := range node.Transitions {
		if tr.Next == "" {
			return nil, nil, fmt.Errorf("parallel node %q: branch %d has no next node", node.ID, i)
		}
		branchID := fmt.Sprintf("%s.b%d", node.ID, i)
		branch := &BranchState{
			ID:          branchID,
			StartNode:   tr.Next,
			CurrentNode: tr.Next,
			Status:      StatusRunning,
		}
		scope.Branches[branchID] = branch
		actions = append(actions, NextAction{Type: "run_branch", Target: tr.Next, BranchID: branchID})
	}

	ctx.PushScope(scope)
	ctx.CurrentNode = node.ID

	return &StateTransition{From: node.ID, Status: "forked"}, actions, nil
}

// branchReachedJoin 当分支推进到一个 join 节点时被 Runtime 调用：标记该分支完成并
// 记录到达的 join 节点，然后返回该分支所属的作用域。
func branchReachedJoin(ctx *ExecutionContext, branch *BranchState, joinNode string) *ParallelScope {
	branch.Done = true
	branch.ArrivedJoin = joinNode
	branch.Status = StatusCompleted
	branch.FinishedAt = time.Now()

	var scope *ParallelScope
	for _, s := range ctx.Scopes {
		if _, ok := s.Branches[branch.ID]; ok {
			scope = s
			break
		}
	}
	if scope != nil && scope.JoinNode == "" {
		scope.JoinNode = joinNode
	}
	return scope
}