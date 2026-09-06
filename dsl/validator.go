package dsl

import (
	"fmt"
	"strings"
)

var ValidNodeTypes = map[string]bool{
	"start":        true,
	"approval":     true,
	"condition":    true,
	"parallel":     true,
	"subprocess":   true,
	"action":       true,
	"notification": true,
	"timer":        true,
	"ai":           true,
	"end":          true,
}

type ValidationError struct {
	Path        string
	Description string
}

func (e ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Path, e.Description)
}

type ValidationResult struct {
	IsValid bool
	Errors  []ValidationError
}

func (r *ValidationResult) AddError(path, desc string) {
	r.Errors = append(r.Errors, ValidationError{Path: path, Description: desc})
	r.IsValid = false
}

func Validate(def *ProcessDef) ValidationResult {
	result := ValidationResult{IsValid: true}

	if def.ID == "" {
		result.AddError("id", "process id is required")
	}
	if def.Version == "" {
		result.AddError("version", "version is required")
	}
	if len(def.Nodes) == 0 {
		result.AddError("nodes", "process must have at least one node")
		return result
	}

	if def.StartNode == "" {
		result.AddError("nodes", "start node is required")
	} else if _, ok := def.Nodes[def.StartNode]; !ok {
		result.AddError("nodes", fmt.Sprintf("start node %q does not exist in nodes", def.StartNode))
	}

	for id, node := range def.Nodes {
		path := fmt.Sprintf("nodes[%s]", id)

		if node.ID == "" {
			result.AddError(path+".id", "node id is required")
		}
		if node.Type == "" {
			result.AddError(path+".type", "node type is required")
		} else if !ValidNodeTypes[node.Type] {
			result.AddError(path+".type", fmt.Sprintf("invalid node type %q, must be one of: start, approval, condition, parallel, subprocess, action, notification, timer, end", node.Type))
		}

		// v2 时间契约:timer 节点必须声明合法的正时长且有出口;waiting 节点的
		// deadline 必须声明合法的正时长且目标节点存在。
		if node.Type == "timer" {
			if _, err := parseDurationStrict(node.Duration, "timer duration"); err != nil {
				result.AddError(path+".duration", err.Error())
			}
			if len(node.Transitions) == 0 {
				result.AddError(path+".transitions", "timer node must declare at least one outgoing transition")
			}
		}

		// v2 AI 节点:prompt 必填;case 迁移仅 ai 节点可用;有界代理候选集与
		// case 一致;onError 目标存在。
		if node.Type == "ai" {
			if node.Ai == nil {
				result.AddError(path+".ai", "ai node requires an \"ai\" config block")
			} else {
				if strings.TrimSpace(node.Ai.Prompt) == "" {
					result.AddError(path+".ai.prompt", "ai node requires a non-empty prompt")
				}
				if node.Ai.OnError != "" {
					if _, ok := def.Nodes[node.Ai.OnError]; !ok {
						result.AddError(path+".ai.onError", fmt.Sprintf("onError target %q does not exist", node.Ai.OnError))
					}
				}
				if len(node.Ai.Choose) > 0 {
					if len(node.Transitions) == 0 {
						result.AddError(path+".transitions", "ai node with choose candidates must declare case transitions")
					}
					seen := map[string]bool{}
					for _, tr := range node.Transitions {
						if tr.Case != "" {
							seen[tr.Case] = true
						}
					}
					for _, tr := range node.Transitions {
						if tr.Case != "" && !containsStr(node.Ai.Choose, tr.Case) {
							result.AddError(fmt.Sprintf("%s.transitions", path),
								fmt.Sprintf("case %q is not in the declared choose candidates (bounded agency)", tr.Case))
						}
					}
					for _, c := range node.Ai.Choose {
						if !seen[c] {
							result.AddError(path+".ai.choose",
								fmt.Sprintf("choose candidate %q has no matching case transition", c))
						}
					}
				}
			}
		}
		if node.Type != "ai" && node.Ai != nil {
			result.AddError(path+".ai", "\"ai\" config is only valid on ai nodes")
		}
		if node.Deadline != nil {
			if node.Type != "approval" && node.Type != "subprocess" {
				result.AddError(path+".deadline", fmt.Sprintf("deadline is only supported on waiting nodes (approval, subprocess), got %q", node.Type))
			}
			if _, err := parseDurationStrict(node.Deadline.After, "deadline after"); err != nil {
				result.AddError(path+".deadline.after", err.Error())
			}
			if node.Deadline.Next == "" {
				result.AddError(path+".deadline.next", "deadline requires a next node for timeout escalation")
			} else if _, ok := def.Nodes[node.Deadline.Next]; !ok {
				result.AddError(path+".deadline.next", fmt.Sprintf("deadline next node %q does not exist", node.Deadline.Next))
			}
		}

		// v2 数据契约:input/output 表达式做语法校验(声明了变量 schema 时
		// 进一步做类型检查,见下方 when 的处理)。
		for name, exprStr := range node.Input {
			if err := validateValueExpr(def, exprStr); err != nil {
				result.AddError(fmt.Sprintf("%s.input[%s]", path, name), err.Error())
			}
		}
		for name, exprStr := range node.Output {
			if err := validateValueExpr(def, exprStr); err != nil {
				result.AddError(fmt.Sprintf("%s.output[%s]", path, name), err.Error())
			}
		}

		for j, tr := range node.Transitions {
			trPath := fmt.Sprintf("%s.transitions[%d]", path, j)
			if tr.Next != "" {
				if _, ok := def.Nodes[tr.Next]; !ok {
					result.AddError(trPath+".next", fmt.Sprintf("transition targets non-existent node %q", tr.Next))
				}
			}
			if tr.Case != "" && node.Type != "ai" {
				result.AddError(trPath+".case", fmt.Sprintf("\"case\" is only valid on ai nodes, node %q is %q", node.ID, node.Type))
			}
			if tr.When != "" {
				// 与 executor 共用同一个 ExpressionEngine（DSL-6），保证校验与执行的编译
				// 选项一致。Validate 使用 expr.Env(空 map) + AllowUndefinedVariables + AsBool。
				if err := DefaultExpressionEngine.Validate(tr.When); err != nil {
					result.AddError(trPath+".when", fmt.Sprintf("invalid condition expression %q: %v", tr.When, err))
				}
				// v2 数据契约:声明了变量 schema 时做静态类型检查,让
				// "amount > \"hello\"" 这类错误在部署期暴露而非运行期。
				if schema := def.TypeSchema(); schema != nil {
					for _, te := range DefaultExpressionEngine.TypeCheck(tr.When, schema) {
						result.AddError(trPath+".when", te.Message)
					}
				}
			}
		}

		if node.Type == "condition" {
			if len(node.Transitions) < 2 {
				result.AddError(path+".transitions", "condition node must have at least two transitions for different conditions")
			}
			// DSL-7：condition 节点必须提供至少一个不带 when 的默认 transition，
			// 避免表达式分支不穷尽时流程无法前进。
			hasDefault := false
			for _, tr := range node.Transitions {
				if tr.When == "" {
					hasDefault = true
					break
				}
			}
			if !hasDefault {
				result.AddError(path+".transitions", "condition_node_requires_default_transition")
			}
		}

		if node.Type == "parallel" {
			if len(node.Transitions) == 0 {
				result.AddError(path+".transitions", "parallel node must declare at least one branch transition")
			}
			if node.Fork != nil {
				switch node.Fork.Mode {
				case "", "all", "any":
				default:
					result.AddError(path+".fork.mode", fmt.Sprintf("invalid fork mode %q, must be all or any", node.Fork.Mode))
				}
				if node.Fork.JoinNode != "" {
					if _, ok := def.Nodes[node.Fork.JoinNode]; !ok {
						result.AddError(path+".fork.joinNode", fmt.Sprintf("fork joinNode %q does not exist in nodes", node.Fork.JoinNode))
					}
				}
				switch node.Fork.OnFail {
				case "", "continue", "fail":
				default:
					result.AddError(path+".fork.onFail", fmt.Sprintf("invalid onFail %q, must be continue or fail", node.Fork.OnFail))
				}
			}
			// parallel 节点可直挂 join 收敛配置(与 join 节点等效),校验规则一致:
			// 此前这里的非法值会静默失效或运行期悄悄纠偏。
			if node.Join != nil {
				switch node.Join.Mode {
				case "", "all", "any", "n_of_m":
				default:
					result.AddError(path+".join.mode", fmt.Sprintf("invalid join mode %q, must be all, any or n_of_m", node.Join.Mode))
				}
				if node.Join.Mode == "n_of_m" && node.Join.Required < 1 {
					result.AddError(path+".join.required", "n_of_m join requires required >= 1")
				}
				if node.Join.Timeout != "" {
					if _, err := parseDurationStrict(node.Join.Timeout, "join timeout"); err != nil {
						result.AddError(path+".join.timeout", err.Error())
					}
				}
			}
		}

		if node.Type == "join" {
			if node.Join != nil {
				switch node.Join.Mode {
				case "", "all", "any", "n_of_m":
				default:
					result.AddError(path+".join.mode", fmt.Sprintf("invalid join mode %q, must be all, any or n_of_m", node.Join.Mode))
				}
				if node.Join.Mode == "n_of_m" && node.Join.Required < 1 {
					result.AddError(path+".join.required", "n_of_m join requires required >= 1")
				}
				// 修复:join.timeout 此前非法值被静默归零(等于没有超时),现在部署期报错。
				if node.Join.Timeout != "" {
					if _, err := parseDurationStrict(node.Join.Timeout, "join timeout"); err != nil {
						result.AddError(path+".join.timeout", err.Error())
					}
				}
			}
			// join 是汇合后的前进枢纽：无出口则汇合即死路（dead end）。
			if len(node.Transitions) == 0 {
				result.AddError(path+".transitions", "join node must declare at least one outgoing transition")
			}
		}
	}

	referencedBy := make(map[string]int)
	for id, node := range def.Nodes {
		for _, tr := range node.Transitions {
			if tr.Next != "" {
				referencedBy[tr.Next]++
			}
		}
		// onError / deadline 升级目标也是真实的引用(否则会被误判孤儿节点)。
		if node.Ai != nil && node.Ai.OnError != "" {
			referencedBy[node.Ai.OnError]++
		}
		if node.Deadline != nil && node.Deadline.Next != "" {
			referencedBy[node.Deadline.Next]++
		}
		_ = id
	}

	for id, node := range def.Nodes {
		if node.Type == "start" {
			continue
		}
		if node.Type == "end" {
			continue
		}
		if referencedBy[id] == 0 {
			result.AddError(fmt.Sprintf("nodes[%s]", id), "orphan node: no other node transitions to this node")
		}
	}

	reachable := make(map[string]bool)
	queue := []string{def.StartNode}
	if _, ok := def.Nodes[def.StartNode]; ok {
		reachable[def.StartNode] = true
	}

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		node, ok := def.Nodes[current]
		if !ok {
			continue
		}
		edges := make([]string, 0, len(node.Transitions)+2)
		for _, tr := range node.Transitions {
			if tr.Next != "" {
				edges = append(edges, tr.Next)
			}
		}
		// onError / deadline 升级目标在运行期可达,同样算作图边。
		if node.Ai != nil && node.Ai.OnError != "" {
			edges = append(edges, node.Ai.OnError)
		}
		if node.Deadline != nil && node.Deadline.Next != "" {
			edges = append(edges, node.Deadline.Next)
		}
		for _, next := range edges {
			if _, exists := def.Nodes[next]; !exists {
				continue
			}
			if !reachable[next] {
				reachable[next] = true
				queue = append(queue, next)
			}
		}
	}

	for id := range def.Nodes {
		if !reachable[id] {
			result.AddError(fmt.Sprintf("nodes[%s]", id), "unreachable node: not reachable from start node")
		}
	}

	return result
}

// validateValueExpr 校验值表达式(input/output 映射):语法层面走引擎的
// ValidateValue(不要求布尔结果);引擎不支持值表达式时退化为条件语法校验。
func validateValueExpr(def *ProcessDef, exprStr string) error {
	if ve, ok := DefaultExpressionEngine.(ValueExpressionEngine); ok {
		return ve.ValidateValue(exprStr)
	}
	return DefaultExpressionEngine.Validate(exprStr)
}

func containsStr(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
