package dsl

import (
	"fmt"
	"time"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// exprEvalTimeout 限制单次条件表达式的执行时长，防止病态/超长表达式阻塞流程（DSL-8）。
const exprEvalTimeout = 3 * time.Second

// TypeError 是一次类型检查发现的诊断信息。
type TypeError struct {
	Expression string
	Message    string
}

func (e TypeError) Error() string { return e.Message }

// ExpressionEngine 是条件表达式的唯一抽象。
//
// 用户建议的第 3 点"Condition Expression 尽早抽象成独立模块"：Parser、Validator、
// Executor 与 Type Checker 全部依赖同一个 Expression Engine，统一 Parse / Compile /
// Validate / Evaluate / TypeCheck 语义，避免各处各自调用 expr 导致行为漂移。
type ExpressionEngine interface {
	// Validate 编译表达式并报告语法/布尔性错误，不依赖任何运行时变量。
	Validate(expression string) error
	// Evaluate 在运行期针对实际变量 map 对表达式求值为布尔结果。
	Evaluate(expression string, variables map[string]interface{}) (bool, error)
	// TypeCheck 基于变量类型 schema 做静态类型检查。
	TypeCheck(expression string, schema *TypeSchema) []TypeError
}

// ValueExpressionEngine 是对"值表达式"的可选扩展能力(接口协商,非强依赖):
// v2 数据契约的 input/output 映射需要把表达式求值为任意值而非布尔结果。
// 自定义引擎可以只实现 ExpressionEngine,此时依赖值表达式的特性不可用并会被
// 显式报错,而不是静默漂移。
type ValueExpressionEngine interface {
	// EvaluateAny 求值表达式并返回任意类型的值(不强制布尔)。
	EvaluateAny(expression string, variables map[string]interface{}) (interface{}, error)
	// ValidateValue 校验值表达式的语法(不要求结果为布尔)。
	ValidateValue(expression string) error
}

// exprEngine 是默认的、基于 expr 库的 ExpressionEngine 实现。
type exprEngine struct{}

// NewExpressionEngine 返回默认的 expr 后端引擎。
func NewExpressionEngine() ExpressionEngine { return exprEngine{} }

// DefaultExpressionEngine 是 executor / validator / type checker 共享的引擎实例。
var DefaultExpressionEngine ExpressionEngine = exprEngine{}

// compileExpr 使用统一选项编译条件表达式：
//   - expr.Env(env)：与运行期 env 形状一致；
//   - expr.AllowUndefinedVariables()：expr v1.17 下空 env 中未知变量在编译期即报
//     unknown name，放行后变量缺失/类型错误推迟到运行期暴露，validator 无真实变量
//     时也能做语法与布尔性校验；
//   - withBool：可选地强制结果为布尔。
func compileExpr(expression string, env map[string]interface{}, withBool bool) (*vm.Program, error) {
	if env == nil {
		env = map[string]interface{}{}
	}
	opts := []expr.Option{expr.Env(env), expr.AllowUndefinedVariables()}
	if withBool {
		opts = append(opts, expr.AsBool())
	}
	return expr.Compile(expression, opts...)
}

func (exprEngine) Validate(expression string) error {
	if _, err := compileExpr(expression, nil, true); err != nil {
		return fmt.Errorf("invalid condition expression %q: %w", expression, err)
	}
	return nil
}

func (exprEngine) ValidateValue(expression string) error {
	if _, err := compileExpr(expression, nil, false); err != nil {
		return fmt.Errorf("invalid value expression %q: %w", expression, err)
	}
	return nil
}

// runExpr 在独立 goroutine 中求值已编译表达式,超时后放弃等待(表达式为纯内存
// 计算,goroutine 会在自然结束后退出,不泄漏)。
func runExpr(program *vm.Program, env map[string]interface{}) (interface{}, error) {
	type runResult struct {
		value interface{}
		err   error
	}
	resultCh := make(chan runResult, 1)
	go func() {
		value, runErr := expr.Run(program, env)
		resultCh <- runResult{value: value, err: runErr}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			return nil, fmt.Errorf("evaluate: %w", res.err)
		}
		return res.value, nil
	case <-time.After(exprEvalTimeout):
		return nil, fmt.Errorf("expression_timeout: expression exceeded %s", exprEvalTimeout)
	}
}

func (exprEngine) Evaluate(expression string, variables map[string]interface{}) (bool, error) {
	env := variables
	if env == nil {
		env = map[string]interface{}{}
	}
	program, err := compileExpr(expression, env, true)
	if err != nil {
		return false, fmt.Errorf("compile: %w", err)
	}

	value, err := runExpr(program, env)
	if err != nil {
		return false, err
	}
	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("result is %T, want bool", value)
	}
	return result, nil
}

// EvaluateAny 求值值表达式:input/output 映射、变量派生等场景使用。
func (exprEngine) EvaluateAny(expression string, variables map[string]interface{}) (interface{}, error) {
	env := variables
	if env == nil {
		env = map[string]interface{}{}
	}
	program, err := compileExpr(expression, env, false)
	if err != nil {
		return nil, fmt.Errorf("compile: %w", err)
	}
	return runExpr(program, env)
}

func (exprEngine) TypeCheck(expression string, schema *TypeSchema) []TypeError {
	if schema == nil || len(schema.Vars) == 0 {
		// 没有可用的类型信息，退化为仅语法校验。
		if err := (exprEngine{}).Validate(expression); err != nil {
			return []TypeError{{Expression: expression, Message: err.Error()}}
		}
		return nil
	}
	envValue, err := buildTypedEnv(schema)
	if err != nil {
		return []TypeError{{Expression: expression, Message: err.Error()}}
	}
	// 使用强类型 env 编译：expr 会对标识符/成员访问/运算符做静态类型推断，
	// 因此 amount > "hello" 这类错误在编译期即可被发现。
	if _, err := expr.Compile(expression, expr.Env(envValue), expr.AllowUndefinedVariables()); err != nil {
		return []TypeError{{Expression: expression, Message: err.Error()}}
	}
	return nil
}