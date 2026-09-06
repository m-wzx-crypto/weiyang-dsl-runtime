package dsl

import (
	"fmt"
)

// contract.go 实现 v2 数据契约:
//   - 节点 Input 映射:进入节点时求值,产生节点局部作用域(局部名可遮蔽全局变量);
//   - 节点 Output 映射:离开节点时求值,写回流程变量,并按 VarSchema 做类型校验;
//   - 条件求值链(when → 默认分支)在此统一实现,executor 与 runtime 共用,
//     消除此前两处复制粘贴的漂移风险。
//
// 语义约定:
//   - Input 表达式基于全局 Variables 求值;
//   - when 条件与 Output 表达式基于 全局变量 + 本节点局部作用域 合并环境求值
//     (局部优先);
//   - 类型校验失败是"节点失败"而非静默写入,保证流程变量永远满足声明。

// nodeView 是一次节点执行期的数据视图。
type nodeView struct {
	local  map[string]interface{} // 节点局部作用域(Input 产出),可为 nil
	merged map[string]interface{} // 全局变量与局部作用域的合并视图
}

// enterNode 进入节点:求值 Input 映射,构建节点执行期的数据视图。
// 返回的 view 供 when 条件、Output 映射与副作用 payload 求值共用。
func enterNode(def *ProcessDef, ctx *ExecutionContext, node *Node, engine ExpressionEngine) (*nodeView, error) {
	if node == nil || len(node.Input) == 0 {
		return &nodeView{merged: ctx.Variables}, nil
	}
	ve, ok := engine.(ValueExpressionEngine)
	if !ok {
		return nil, fmt.Errorf("node %q declares input mapping but expression engine does not support value expressions", node.ID)
	}
	local := make(map[string]interface{}, len(node.Input))
	for name, exprStr := range node.Input {
		v, err := ve.EvaluateAny(exprStr, ctx.Variables)
		if err != nil {
			return nil, fmt.Errorf("node %q input %q: %w", node.ID, name, err)
		}
		local[name] = v
	}
	return &nodeView{local: local, merged: mergeEnv(ctx.Variables, local)}, nil
}

// leaveNode 离开节点:求值 Output 映射并写回流程变量。VarSchema 非空时逐项
// 做类型校验,任何失败都返回 error(由调用方把节点置为失败)。
func leaveNode(def *ProcessDef, ctx *ExecutionContext, node *Node, view *nodeView, engine ExpressionEngine) error {
	if node == nil || len(node.Output) == 0 {
		return nil
	}
	ve, ok := engine.(ValueExpressionEngine)
	if !ok {
		return fmt.Errorf("node %q declares output mapping but expression engine does not support value expressions", node.ID)
	}
	for name, exprStr := range node.Output {
		v, err := ve.EvaluateAny(exprStr, view.merged)
		if err != nil {
			return fmt.Errorf("node %q output %q: %w", node.ID, name, err)
		}
		if t, declared := def.VarSchema[name]; declared {
			if err := checkAssignable(name, t, v); err != nil {
				return fmt.Errorf("node %q output: %w", node.ID, err)
			}
		}
		ctx.SetVariable(name, v)
	}
	return nil
}

// evalWhenChain 按声明顺序求值 when 分支,命中即返回;全部不命中时回落到
// 第一个不带 when 的默认分支。没有 when 命中也没有默认分支时返回 ""。
func evalWhenChain(node *Node, env map[string]interface{}, engine ExpressionEngine) (string, error) {
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
		ok, err := engine.Evaluate(tr.When, env)
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

// mergeEnv 构造 全局 + 局部 的合并视图(局部遮蔽同名全局变量)。始终返回新 map,
// 不改动全局 Variables 本体。
func mergeEnv(global, local map[string]interface{}) map[string]interface{} {
	env := make(map[string]interface{}, len(global)+len(local))
	for k, v := range global {
		env[k] = v
	}
	for k, v := range local {
		env[k] = v
	}
	return env
}

// checkAssignable 校验运行期值是否符合声明的变量类型。JSON 反序列化产物的
// Go 类型是有限集合(number→float64、array→[]interface{}、object→map),这里
// 按该口径做宽容匹配,enum 额外校验取值成员。
func checkAssignable(name string, t *Type, v interface{}) error {
	if t == nil {
		return nil
	}
	switch t.Kind {
	case TypeAny:
		return nil
	case TypeString:
		return expectKind(name, "string", v, isString)
	case TypeEnum:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("variable %q: got %T, want string enum", name, v)
		}
		for _, allowed := range t.Enum {
			if s == allowed {
				return nil
			}
		}
		return fmt.Errorf("variable %q: value %q not in enum %v", name, s, t.Enum)
	case TypeNumber, TypeMoney:
		return expectKind(name, "number", v, isNumber)
	case TypeBoolean:
		return expectKind(name, "boolean", v, func(x interface{}) bool { _, ok := x.(bool); return ok })
	case TypeDateTime:
		return expectKind(name, "date string", v, isString)
	case TypeObject:
		return expectKind(name, "object", v, func(x interface{}) bool {
			_, ok := x.(map[string]interface{})
			return ok
		})
	case TypeArray:
		return expectKind(name, "array", v, func(x interface{}) bool {
			_, ok := x.([]interface{})
			return ok
		})
	default:
		return nil
	}
}

func expectKind(name, want string, v interface{}, pred func(interface{}) bool) error {
	if !pred(v) {
		return fmt.Errorf("variable %q: got %T, want %s", name, v, want)
	}
	return nil
}

func isString(v interface{}) bool {
	_, ok := v.(string)
	return ok
}

func isNumber(v interface{}) bool {
	switch v.(type) {
	case float64, float32, int, int32, int64:
		return true
	default:
		return false
	}
}
