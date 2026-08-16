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
				// 与 executor 的 evalCondition 共用 compileConditionExpr（DSL-6），
				// 保证校验与执行的编译选项一致（expr.Env(map) + expr.AsBool()）。
				if _, err := compileConditionExpr(tr.When, nil); err != nil {
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
