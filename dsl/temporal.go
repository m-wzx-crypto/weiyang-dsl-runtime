package dsl

import (
	"fmt"
	"time"
)

// temporal.go 实现 v2 时间契约:引擎从"被动等事件"进化为"主动时间轴"。
//
//   - timer 节点与 waiting 节点的 deadline 在实例停靠时登记为等待槽(WaitingState);
//   - Runtime.NextWakeup 告诉宿主下一次需要唤醒的时刻,定时调度仍由宿主负责
//     (引擎保持零外部依赖);
//   - Runtime.WakeDue 在到达时刻把到期的等待槽转化为确定性的前向迁移,超时
//     因此成为"引擎自产的、带幂等键的事件",与外部事件先到先得。
//
// 唤醒命令的幂等键形如 wake:<slot>:<kind>:<visit>,环路流程二次经过同一
// timer/deadline 节点不会与上一轮混淆。

// WaitingState 描述一个等待槽:实例(或并行分支)正停在哪个节点、为什么等待、
// 何时到期。
type WaitingState struct {
	// Kind: "timer"(定时器节点到期前进) | "deadline"(等待节点超时升级)。
	Kind string
	// NodeID 是发起等待的节点。
	NodeID string
	// Until 是到期时刻。
	Until time.Time
	// Visit 是本次是该节点的第几次执行(用于幂等键)。
	Visit int
	// Next 仅 deadline 使用:超时后的升级路由目标。
	Next string
}

const (
	waitKindTimer    = "timer"
	waitKindDeadline = "deadline"
	// waitKindScope 是并行作用域的收敛超时:fork 时登记,到点把仍在等待的
	// scope 置为 timed_out——超时由时间轴主动触发,而非等下一个事件。
	waitKindScope = "scope_timeout"

	// instanceSlot 是线性流程实例级等待槽的固定 key;并行分支以分支 ID 为 key。
	instanceSlot = "instance"
)

// scopeSlotID 返回并行作用域超时等待槽的 key。
func scopeSlotID(forkNodeID string) string {
	return "scope:" + forkNodeID
}

// parseDurationStrict 严格解析时长字符串:非法或非正数报错(区别于
// resolveJoinTimeout 的静默归零——时间契约的配置错误必须在部署期暴露)。
func parseDurationStrict(raw, what string) (time.Duration, error) {
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%s %q is not a valid duration: %w", what, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s %q must be positive", what, raw)
	}
	return d, nil
}

// parkWaiting 在实例停靠到 waiting/timer 节点时登记等待槽。同一节点重复停靠
// (未离开)不重置到期时间;离开后再次到达则重新计时。
func (r *Runtime) parkWaiting(slotKey string, node *Node) {
	if r.Ctx.Waitings == nil {
		r.Ctx.Waitings = map[string]*WaitingState{}
	}
	if existing, ok := r.Ctx.Waitings[slotKey]; ok && existing.NodeID == node.ID {
		return // 已在该节点上等待,不重复计时
	}
	w := &WaitingState{Kind: waitKindDeadline, NodeID: node.ID, Visit: r.Ctx.VisitOf(node.ID)}
	switch node.Type {
	case "timer":
		d, err := parseDurationStrict(node.Duration, "timer duration")
		if err != nil {
			// parser/validator 已拦截非法配置;运行期兜底:不登记,由静态检查兜底。
			return
		}
		w.Kind = waitKindTimer
		w.Until = time.Now().Add(d)
	case "approval", "subprocess":
		if node.Deadline == nil {
			return
		}
		d, err := parseDurationStrict(node.Deadline.After, "deadline after")
		if err != nil {
			return
		}
		w.Kind = waitKindDeadline
		w.Until = time.Now().Add(d)
		w.Next = node.Deadline.Next
	default:
		return
	}
	r.Ctx.Waitings[slotKey] = w
}

// clearWaiting 清除指定等待槽(离开等待点时调用)。
func (r *Runtime) clearWaiting(slotKey string) {
	if r.Ctx.Waitings == nil {
		return
	}
	delete(r.Ctx.Waitings, slotKey)
}

// NextWakeup 返回当前最近一次等待槽的到期时刻;没有登记中的等待槽时 ok 为 false。
// 宿主据此设置定时器,到点后调用 WakeDue。
func (r *Runtime) NextWakeup() (time.Time, bool) {
	var best time.Time
	found := false
	for _, w := range r.Ctx.Waitings {
		if !found || w.Until.Before(best) {
			best = w.Until
			found = true
		}
	}
	return best, found
}

// WakeDue 推进所有在 now 时刻已到期的等待槽。返回值合并了全部由唤醒触发的
// 迁移与副作用。重复调用(对同一到期时刻)受幂等键保护,不会二次迁移。
func (r *Runtime) WakeDue(now time.Time) *ExecutionResult {
	res := &ExecutionResult{}
	for {
		slot, w := r.nextDueWaiting(now)
		if w == nil {
			break
		}
		// 幂等:同一等待轮次的唤醒只生效一次(消息重放安全)。
		wakeID := fmt.Sprintf("wake:%s:%s:%d", slot, w.Kind, w.Visit)
		if !r.Ctx.TryConsumeEvent(wakeID) {
			r.clearWaiting(slot)
			continue
		}
		r.fireWaiting(slot, w, res)
	}
	return res
}

// nextDueWaiting 取出最早到期且已到期的等待槽(确定性:同刻时按槽名字典序)。
func (r *Runtime) nextDueWaiting(now time.Time) (string, *WaitingState) {
	bestSlot := ""
	var best *WaitingState
	for slot, w := range r.Ctx.Waitings {
		if w.Until.After(now) {
			continue
		}
		if best == nil || w.Until.Before(best.Until) || (w.Until.Equal(best.Until) && slot < bestSlot) {
			bestSlot, best = slot, w
		}
	}
	return bestSlot, best
}

// fireWaiting 触发单个到期等待槽:实例级等待直接前向迁移并继续 drain;
// 分支级等待推进该分支后做汇合判定;scope 超时把仍在等待的作用域置为 timed_out。
func (r *Runtime) fireWaiting(slotKey string, w *WaitingState, res *ExecutionResult) {
	r.clearWaiting(slotKey)
	r.Ctx.CurrentEvent = nil // 唤醒是引擎自产迁移,不受残留外部事件影响

	// scope 收敛超时:作用域仍在(未弹出)且未收敛时生效;已收敛/已弹出的
	// 陈旧槽位自愈为 no-op。
	if w.Kind == waitKindScope {
		scope := r.scopeByFork(w.NodeID)
		if scope == nil || scope.satisfied() || scope.doneCount() == len(scope.Branches) {
			return
		}
		r.Ctx.setStatus(StatusTimedOut)
		res.Errors = append(res.Errors, fmt.Errorf("parallel scope %q timed out", scope.ID))
		return
	}

	node, ok := r.Def.Nodes[w.NodeID]
	if !ok {
		res.Errors = append(res.Errors, fmt.Errorf("waiting node %q not found", w.NodeID))
		r.Ctx.setStatus(StatusFailed)
		return
	}

	// deadline:超时升级路由是显式声明的 Next,直接迁移。
	if w.Kind == waitKindDeadline {
		if w.Next == "" || r.Def.Nodes[w.Next] == nil {
			res.Errors = append(res.Errors, fmt.Errorf("deadline on node %q has no valid next node", w.NodeID))
			r.Ctx.setStatus(StatusFailed)
			return
		}
		engine := r.Ctx.engine()
		view, err := enterNode(r.Def, r.Ctx, node, engine)
		if err == nil {
			err = leaveNode(r.Def, r.Ctx, node, view, engine)
		}
		if err != nil {
			res.Errors = append(res.Errors, err)
			r.Ctx.setStatus(StatusFailed)
			return
		}
		if slotKey == instanceSlot {
			assignTransition(r.Ctx, res, node.ID, w.Next, "deadline")
			// 升级目标可能是 waiting 节点(如升级审批人):直接停靠,不走 drain
			// (drain 会先 Step 该节点,破坏等待语义)。
			if target := r.Def.Nodes[w.Next]; isWaitingNode(target) {
				r.parkWaiting(instanceSlot, target)
				r.Ctx.setStatus(StatusWaiting)
				return
			}
			r.drain(res)
			return
		}
		// 分支级 deadline:推进该分支,再按作用域语义判定收敛。
		if scope := r.scopeOfBranch(slotKey); scope != nil {
			if b := scope.Branches[slotKey]; b != nil {
				b.CurrentNode = w.Next
				r.advanceBranch(scope, b, res)
				r.settleScope(scope, res)
			}
		}
		return
	}

	// timer:回到 timer 节点重新进入,由 Step 的 timer 语义(when 路由 + 自动兜底)
	// 决定去向,事件不参与(timer 的前进不需要外部事件)。
	if slotKey == instanceSlot {
		r.Ctx.CurrentNode = node.ID
		r.Ctx.setStatus(StatusRunning)
		r.drain(res)
		return
	}
	if scope := r.scopeOfBranch(slotKey); scope != nil {
		if b := scope.Branches[slotKey]; b != nil {
			b.CurrentNode = node.ID
			r.advanceBranch(scope, b, res)
			r.settleScope(scope, res)
		}
	}
}

// scopeOfBranch 根据分支 ID 找到所属并行作用域。
func (r *Runtime) scopeOfBranch(branchID string) *ParallelScope {
	for _, s := range r.Ctx.Scopes {
		if _, ok := s.Branches[branchID]; ok {
			return s
		}
	}
	return nil
}

// scopeByFork 根据 fork 节点 ID 找到当前活跃的作用域(嵌套时取最内层匹配)。
func (r *Runtime) scopeByFork(forkNodeID string) *ParallelScope {
	for i := len(r.Ctx.Scopes) - 1; i >= 0; i-- {
		if r.Ctx.Scopes[i].ForkNode == forkNodeID {
			return r.Ctx.Scopes[i]
		}
	}
	return nil
}
