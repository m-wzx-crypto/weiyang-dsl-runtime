package dsl

import (
	"fmt"
)

var ValidNodeTypes = map[string]bool{
	"start":        true,
	"approval":     true,
	"condition":    true,
	"parallel":     true,
	"subprocess":   true,
	"action":       true,
	"notification": true,
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
			result.AddError(path+".type", fmt.Sprintf("invalid node type %q, must be one of: start, approval, condition, parallel, subprocess, action, notification, end", node.Type))
		}

		for j, tr := range node.Transitions {
			trPath := fmt.Sprintf("%s.transitions[%d]", path, j)
			if tr.Next != "" {
				if _, ok := def.Nodes[tr.Next]; !ok {
					result.AddError(trPath+".next", fmt.Sprintf("transition targets non-existent node %q", tr.Next))
				}
			}
			if node.Type == "condition" && tr.When != "" {
			// 与 executor 共用同一个 ExpressionEngine（DSL-6），保证校验与执行的编译
			// 选项一致。Validate 使用 expr.Env(空 map) + AllowUndefinedVariables + AsBool。
			if err := DefaultExpressionEngine.Validate(tr.When); err != nil {
				result.AddError(trPath+".when", fmt.Sprintf("invalid condition expression %q: %v", tr.When, err))
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
	reachable[def.StartNode] = true

	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		node, ok := def.Nodes[current]
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

	for id := range def.Nodes {
		if !reachable[id] {
			result.AddError(fmt.Sprintf("nodes[%s]", id), "unreachable node: not reachable from start node")
		}
	}

	return result
}
