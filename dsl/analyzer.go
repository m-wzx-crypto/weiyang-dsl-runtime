package dsl

import "fmt"

// Severity 表示静态分析诊断的严重程度。
type Severity int

const (
	// SeverityError 一定会导致运行时失败或不可达的流程。
	SeverityError Severity = iota
	// SeverityWarning 表示潜在问题，可能可行但不推荐。
	SeverityWarning
	// SeverityInfo 是信息性提示。
	SeverityInfo
)

func (s Severity) String() string {
	switch s {
	case SeverityError:
		return "error"
	case SeverityWarning:
		return "warning"
	default:
		return "info"
	}
}

// Diagnostic 是一条静态分析结果。
type Diagnostic struct {
	Severity Severity
	Code     string
	Path     string
	Message  string
}

// AnalysisReport 是 Analyze 的输出：Graph → Static Analysis（用户建议第 5 点）。
type AnalysisReport struct {
	IsClean     bool
	Diagnostics []Diagnostic
}

// MaxAnalysisPaths 上限，超过即报告 path_complexity（避免路径枚举爆炸）。
const MaxAnalysisPaths = 512

// Analyze 对流程做静态分析，覆盖：
//   - unreachable_node    不可达节点
//   - dead_end            非终止节点无出口
//   - invalid_terminal    终止节点定义错误（end 带出口 / 非 end 无出口）
//   - missing_transition  缺少迁移信息
//   - duplicate_transition 重复迁移
//   - duplicate_condition 重复条件分支（"不可能到达"的冗余分支）
//   - cycle               环（可能导致无限循环）
//   - path_complexity     路径复杂度过高
func Analyze(def *ProcessDef) AnalysisReport {
	report := AnalysisReport{IsClean: true}
	if def == nil {
		report.add(SeverityError, "nil_process", "", "process definition is nil")
		return report
	}
	if def.StartNode == "" {
		report.add(SeverityError, "missing_start", "", "start node is required")
		return report
	}

	// 1. 可达性（BFS）+ 死路。
	reachable := map[string]bool{def.StartNode: true}
	queue := []string{def.StartNode}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		node, ok := def.Nodes[cur]
		if !ok {
			continue
		}
		for _, tr := range node.Transitions {
			if tr.Next != "" && !reachable[tr.Next] {
				reachable[tr.Next] = true
				queue = append(queue, tr.Next)
			}
		}
	}

	// 2. 内部一致性：重复迁移 / 缺失迁移 / 终止状态 / 死路 / 重复条件。
	for id, node := range def.Nodes {
		slotA := make(map[string]int) // event 计数
		slotB := make(map[string]int) // when 计数
		for _, tr := range node.Transitions {
			if tr.Event == "" && tr.When == "" && tr.Next == "" {
				report.add(SeverityWarning, "missing_transition", "nodes["+id+"]",
					"transition has no event, when or next")
				continue
			}
			if tr.Event == "" && tr.When == "" {
				report.add(SeverityWarning, "missing_transition", "nodes["+id+"]",
					"transition has no trigger (event or when)")
			}
			if tr.Event != "" {
				slotA[tr.Event]++
				if slotA[tr.Event] > 1 {
					report.add(SeverityWarning, "duplicate_transition", "nodes["+id+"]",
						fmt.Sprintf("duplicate event %q", tr.Event))
				}
			}
			if tr.When != "" {
				slotB[tr.When]++
				if slotB[tr.When] > 1 {
					report.add(SeverityWarning, "duplicate_condition", "nodes["+id+"]",
						fmt.Sprintf("duplicate condition expression %q", tr.When))
				}
			}
		}

		// 终止状态检查。
		if node.Type == "end" {
			if len(node.Transitions) > 0 {
				report.add(SeverityError, "invalid_terminal", "nodes["+id+"]",
					"end node must not have outgoing transitions")
			}
		} else if len(node.Transitions) == 0 {
			report.add(SeverityError, "dead_end", "nodes["+id+"]",
				fmt.Sprintf("node %q of type %q has no transitions and is not an end node", id, node.Type))
		}

		// 不可达节点。
		if !reachable[id] {
			report.add(SeverityError, "unreachable_node", "nodes["+id+"]",
				fmt.Sprintf("node %q is not reachable from start node", id))
		}
	}

	// 3. 环检测（DFS 着色，仅报告一个代表性路径）。
	if cyc := detectCycleFirst(def); cyc != nil {
		report.add(SeverityWarning, "cycle", "nodes",
			fmt.Sprintf("detected cycle: %v", cyc))
	}

	// 4. 路径复杂度。
	if !reachableInGreatComplexity(def) {
		// 路径数超过阈值则给出告警。
		report.add(SeverityWarning, "path_complexity", "nodes",
			fmt.Sprintf("path search exceeded %d distinct paths", MaxAnalysisPaths))
	}

	report.IsClean = report.IsClean && len(report.Diagnostics) == 0
	return report
}

func (r *AnalysisReport) add(sev Severity, code, path, msg string) {
	d := Diagnostic{Severity: sev, Code: code, Path: path, Message: msg}
	r.Diagnostics = append(r.Diagnostics, d)
	r.IsClean = false
}

// detectCycleFirst 用迭代 DFS 返回任意出现的环（节点序列，首尾相同）；无环返回 nil。
func detectCycleFirst(def *ProcessDef) []string {
	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := make(map[string]int, len(def.Nodes))
	parent := make(map[string]string, len(def.Nodes))

	var visit func(nodeID string) []string
	visit = func(nodeID string) []string {
		color[nodeID] = gray
		stack := []string{nodeID}
		for len(stack) > 0 {
			u := stack[len(stack)-1]
			node := def.Nodes[u]
			advanced := false
			for _, tr := range node.Transitions {
				v := tr.Next
				if v == "" {
					continue
				}
				if _, ok := def.Nodes[v]; !ok {
					continue
				}
				if color[v] == white {
					color[v] = gray
					parent[v] = u
					stack = append(stack, v)
					advanced = true
					break
				} else if color[v] == gray && v != u {
					// 找到回边，重建所在环。
					cyc := []string{v, u}
					for p := u; p != v && p != ""; p = parent[p] {
						cyc = append(cyc, p)
					}
					cyc = append(cyc, v)
					for i, j := 0, len(cyc)-1; i < j; i, j = i+1, j-1 {
						cyc[i], cyc[j] = cyc[j], cyc[i]
					}
					return cyc
				}
			}
			if advanced {
				continue
			}
			stack = stack[:len(stack)-1]
			color[u] = black
		}
		return nil
	}

	for id := range def.Nodes {
		if color[id] == white {
			if path := visit(id); path != nil {
				return path
			}
		}
	}
	return nil
}

// reachableInGreatComplexity 判断路径枚举是否未超过阈值。返回 true 表示路径可控。
func reachableInGreatComplexity(def *ProcessDef) bool {
	if def.StartNode == "" {
		return true
	}
	count := 0
	type st struct {
		nodeID string
		vis    map[string]bool
	}
	start := st{nodeID: def.StartNode, vis: map[string]bool{def.StartNode: true}}
	queue := []st{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		node := def.Nodes[cur.nodeID]
		if node == nil {
			continue
		}
		if node.Type == "end" || len(node.Transitions) == 0 {
			count++
			if count > MaxAnalysisPaths {
				return false
			}
			continue
		}
		for _, tr := range node.Transitions {
			if tr.Next == "" {
				continue
			}
			nextID := tr.Next
			if cur.vis[nextID] {
				continue
			}
			// 复制 visited 以枚举不同路径。
			nv := make(map[string]bool, len(cur.vis)+1)
			for k, v := range cur.vis {
				nv[k] = v
			}
			nv[nextID] = true
			queue = append(queue, st{nodeID: nextID, vis: nv})
		}
	}
	return count <= MaxAnalysisPaths
}