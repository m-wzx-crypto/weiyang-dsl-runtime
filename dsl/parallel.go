package dsl

import (
	"fmt"
	"time"
)

// ForkConfig 描述 parallel 节点的 fork 模式（用户建议第 4 点）。
type ForkConfig struct {
	// Mode: "all"（默认，等待全部分支）| "any"（任一分支成功即可收敛）。
	Mode string
	// JoinNode 显式声明汇合节点 ID；为空时由引擎静态推导（各分支可达集的交集，
	// 取距离和最小者）。汇合后流程从该节点继续前进，而不是直接判定完成。
	JoinNode string
	// OnFail 分支失败策略："continue"（默认，失败分支随流汇合，可经 join 的
	// when 条件路由到补偿分支）| "fail"（任一分支失败即取消其余分支，实例失败）。
	OnFail string
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
	OnFail    string
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

// successCount 统计已成功（StatusCompleted）的分支数。收敛判定中 any / n_of_m
// 只数成功分支——失败分支不算 partial success。
func (fs *ParallelScope) successCount() int {
	n := 0
	for _, b := range fs.Branches {
		if b.Status == StatusCompleted {
			n++
		}
	}
	return n
}

// failedCount 统计已失败的分支数。
func (fs *ParallelScope) failedCount() int {
	n := 0
	for _, b := range fs.Branches {
		if b.Status == StatusFailed {
			n++
		}
	}
	return n
}

// satisfied 判断当前作用域是否已达到收敛条件：
//   - all    ：全部分支结束（成功或失败；失败按 continue 策略随流汇合）；
//   - any    ：至少一个分支成功（partial success）；
//   - n_of_m ：成功分支数达到 Required。
func (fs *ParallelScope) satisfied() bool {
	if fs == nil || len(fs.Branches) == 0 {
		return false
	}
	switch fs.Mode {
	case "any":
		return fs.successCount() >= 1
	case "n_of_m":
		if fs.Required <= 0 {
			fs.Required = 1
		}
		return fs.successCount() >= fs.Required
	default: // all
		return fs.doneCount() == len(fs.Branches)
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
//   - 汇合点取 fork.joinNode 的显式声明，缺省时静态推导各分支的公共可达节点；
//   - 记录一个新的 ParallelScope；分支的具体推进交给 Runtime 回调推进。
//
// 它不阻塞、不做实际业务，只产出分支的 NextAction，把"并发怎么跑"交给 Runtime。
func stepParallel(def *ProcessDef, ctx *ExecutionContext, node *Node) (*StateTransition, []NextAction, error) {
	if len(node.Transitions) == 0 {
		return nil, nil, fmt.Errorf("parallel node %q must declare at least one branch transition", node.ID)
	}

	mode := "all"
	onFail := "continue"
	joinNode := ""
	if node.Fork != nil {
		mode = defaultForkMode(node.Fork.Mode)
		if node.Fork.OnFail != "" {
			onFail = node.Fork.OnFail
		}
		joinNode = node.Fork.JoinNode
	}

	scope := &ParallelScope{
		ID:        node.ID,
		ForkNode:  node.ID,
		Mode:      mode,
		OnFail:    onFail,
		StartedAt: time.Now(),
		Status:    StatusRunning,
		Branches:  map[string]*BranchState{},
	}
	// 兼容把 join 收敛配置直接挂在 parallel 节点上的写法。
	if node.Join != nil {
		scope.Required = node.Join.Required
		scope.Timeout = resolveJoinTimeout(node.Join.Timeout)
		if node.Join.Mode != "" {
			scope.Mode = defaultJoinMode(node.Join.Mode)
		}
	}

	starts := make([]string, 0, len(node.Transitions))
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
		starts = append(starts, tr.Next)
		actions = append(actions, NextAction{Type: "run_branch", Target: tr.Next, BranchID: branchID})
	}

	if joinNode == "" {
		joinNode = resolveCommonJoin(def, node.ID, starts)
	}
	scope.JoinNode = joinNode

	ctx.PushScope(scope)
	ctx.CurrentNode = node.ID

	return &StateTransition{From: node.ID, Status: "forked"}, actions, nil
}

// resolveCommonJoin 静态推导汇合点：取所有分支可达节点集的交集（排除 fork 自身，
// 避免分支绕回 fork 造成二次 fork），选"各分支到该节点距离之和"最小者；并列时按
// 节点 ID 字典序保证确定性。无公共可达节点返回 ""（各分支独立到达各自终态）。
func resolveCommonJoin(def *ProcessDef, forkID string, starts []string) string {
	if len(starts) == 0 {
		return ""
	}
	candidates := make(map[string]int)
	for id, d := range bfsDistances(def, starts[0], forkID) {
		candidates[id] = d
	}
	for _, s := range starts[1:] {
		dist := bfsDistances(def, s, forkID)
		for id := range candidates {
			if d, ok := dist[id]; ok {
				candidates[id] += d
			} else {
				delete(candidates, id)
			}
		}
	}
	best, bestDist := "", -1
	for id, d := range candidates {
		if bestDist == -1 || d < bestDist || (d == bestDist && id < best) {
			best, bestDist = id, d
		}
	}
	return best
}

// bfsDistances 返回从 from 出发沿 transition 可达的每个节点的 BFS 距离。
// exclude（fork 自身）不会被穿过，保证推导的是"汇合"而非"回流"。
func bfsDistances(def *ProcessDef, from, exclude string) map[string]int {
	dist := map[string]int{from: 0}
	queue := []string{from}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		node, ok := def.Nodes[cur]
		if !ok {
			continue
		}
		for _, tr := range node.Transitions {
			next := tr.Next
			if next == "" || next == exclude {
				continue
			}
			if _, seen := dist[next]; seen {
				continue
			}
			if _, exists := def.Nodes[next]; !exists {
				continue
			}
			dist[next] = dist[cur] + 1
			queue = append(queue, next)
		}
	}
	return dist
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