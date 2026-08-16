package dsl

import (
	"fmt"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// exprEvalTimeout 限制单次条件表达式的执行时长，防止病态/超长表达式阻塞流程（DSL-8）。
const exprEvalTimeout = 3 * time.Second

type ExecutionOutput struct {
	NextNode    string
	SideEffects []SideEffect
	Status      string
	Error       error
}

// Execute 执行一次节点迁移。
// currentNodeID 为当前节点；event 为触发事件；variables 为流程变量，
// 供 condition 节点的 when 表达式求值使用。
func Execute(def *ProcessDef, currentNodeID string, event string, variables map[string]interface{}) ExecutionOutput {
	node, ok := def.Nodes[currentNodeID]
	if !ok {
		return ExecutionOutput{
			Status: "error",
			Error:  fmt.Errorf("current node %q not found in process definition", currentNodeID),
		}
	}

	if node.Type == "end" {
		return ExecutionOutput{
			NextNode:    "",
			SideEffects: node.SideEffects,
			Status:      "completed",
		}
	}

	// condition 节点分支选择（DSL-7）：
	// 1. 按声明顺序求值所有带 when 的 transition，命中即走该分支；
	// 2. 全部不匹配时，若有不带 when 的 transition，则第一个作为默认分支；
	// 3. 否则回退到按事件名匹配（兼容无 when 的旧 DSL）。
	if node.Type == "condition" {
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
			matched, err := evalCondition(tr.When, variables)
			if err != nil {
				return ExecutionOutput{
					Status: "error",
					Error:  fmt.Errorf("condition %q evaluation failed: %w", tr.When, err),
				}
			}
			if matched {
				return ExecutionOutput{
					NextNode:    tr.Next,
					SideEffects: node.SideEffects,
					Status:      "running",
				}
			}
		}
		if hasDefault {
			return ExecutionOutput{
				NextNode:    defaultNext,
				SideEffects: node.SideEffects,
				Status:      "running",
			}
		}
	}

	// 兜底：按事件名匹配（兼容无 when 的 condition 节点及其他节点类型）。
	for _, tr := range node.Transitions {
		if tr.Event == event {
			return ExecutionOutput{
				NextNode:    tr.Next,
				SideEffects: node.SideEffects,
				Status:      "running",
			}
		}
	}

	return ExecutionOutput{
		NextNode:    "",
		SideEffects: nil,
		Status:      "error",
		Error:       fmt.Errorf("event %q is not defined on node %q", event, currentNodeID),
	}
}

// compileConditionExpr 按统一选项编译条件表达式，evalCondition 与 validator 共用，
// 保证"校验通过的表达式在运行期以相同语义编译"（DSL-6）：
//   - expr.Env(map[string]interface{}) 与 executor 运行时 env 形状一致；
//   - expr.AllowUndefinedVariables()：expr v1.17 下空 env 中未知变量在编译期
//     即报 unknown name，放行后变量缺失/类型错误推迟到运行期暴露，
//     validator 无真实变量时也能做语法与布尔性校验；
//   - expr.AsBool() 强制表达式结果为布尔类型。
func compileConditionExpr(expression string, env map[string]interface{}) (*vm.Program, error) {
	if env == nil {
		env = map[string]interface{}{}
	}
	return expr.Compile(expression, expr.Env(env), expr.AllowUndefinedVariables(), expr.AsBool())
}

// evalCondition 使用 expr 库对条件表达式求值，返回布尔结果。
// 求值限时 exprEvalTimeout，超时返回 expression_timeout（DSL-8）。
func evalCondition(expression string, variables map[string]interface{}) (bool, error) {
	env := variables
	if env == nil {
		env = map[string]interface{}{}
	}
	program, err := compileConditionExpr(expression, env)
	if err != nil {
		return false, fmt.Errorf("compile: %w", err)
	}

	type runResult struct {
		value interface{}
		err   error
	}
	resultCh := make(chan runResult, 1)
	go func() {
		// expr.Run 是纯内存计算，不持有锁或外部资源；超时后外层放弃等待并丢弃
		// program，该 goroutine 会在计算自然结束后向带缓冲通道写入并退出，
		// 不会永久阻塞泄漏。
		value, runErr := expr.Run(program, env)
		resultCh <- runResult{value: value, err: runErr}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			return false, fmt.Errorf("evaluate: %w", res.err)
		}
		result, ok := res.value.(bool)
		if !ok {
			return false, fmt.Errorf("result is %T, want bool", res.value)
		}
		return result, nil
	case <-time.After(exprEvalTimeout):
		return false, fmt.Errorf("expression_timeout: condition %q exceeded %s", expression, exprEvalTimeout)
	}
}

func ExecuteFirstStep(def *ProcessDef, variables map[string]interface{}) ExecutionOutput {
	return Execute(def, def.StartNode, "submit", variables)
}

func ExecuteStep(def *ProcessDef, currentNode string, event string, variables map[string]interface{}) ExecutionOutput {
	return Execute(def, currentNode, event, variables)
}

func GetNextSteps(def *ProcessDef, currentNodeID string) []Transition {
	node, ok := def.Nodes[currentNodeID]
	if !ok {
		return nil
	}
	return node.Transitions
}

func GetCurrentNode(def *ProcessDef, currentNodeID string) *Node {
	return def.Nodes[currentNodeID]
}
