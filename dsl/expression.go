package dsl

import (
	"fmt"
	"sync"
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

// cachedProgram 是编译缓存条目(成功缓存程序,失败缓存错误——非法表达式在
// validator 循环里同样会被反复提交)。
type cachedProgram struct {
	program *vm.Program
	err     error
}

// exprProgramCache 缓存已编译的条件/值表达式。
//
// 编译成本是执行的几十到几百倍,环路流程与多实例并发下同一 when 会被反复编译。
// 编译期 env 统一为空 map:AllowUndefinedVariables 之下 map env 只提供"形状"提示,
// 标识符一律走运行期 map 取值,因此编译产物与实例变量集无关、全局可复用——
// 这同时消除了旧实现"同一表达式因实例变量不同而编译出不同程序"的漂移隐患。
var exprProgramCache sync.Map // string -> *cachedProgram

// compileExpr 使用统一选项编译表达式(带进程级缓存):
//   - expr.Env(空 map) + AllowUndefinedVariables:未知变量推迟到运行期暴露;
//   - withBool:可选地强制结果为布尔。
func compileExpr(expression string, withBool bool) (*vm.Program, error) {
	key := expression
	if withBool {
		key = "\x00bool\x00" + expression
	}
	if v, ok := exprProgramCache.Load(key); ok {
		cp := v.(*cachedProgram)
		if cp.err != nil {
			return nil, cp.err
		}
		return cp.program, nil
	}
	opts := []expr.Option{expr.Env(map[string]interface{}{}), expr.AllowUndefinedVariables()}
	if withBool {
		opts = append(opts, expr.AsBool())
	}
	program, err := expr.Compile(expression, opts...)
	exprProgramCache.Store(key, &cachedProgram{program: program, err: err})
	if err != nil {
		return nil, err
	}
	return program, nil
}

func (exprEngine) Validate(expression string) error {
	if _, err := compileExpr(expression, true); err != nil {
		return fmt.Errorf("invalid condition expression %q: %w", expression, err)
	}
	return nil
}

func (exprEngine) ValidateValue(expression string) error {
	if _, err := compileExpr(expression, false); err != nil {
		return fmt.Errorf("invalid value expression %q: %w", expression, err)
	}
	return nil
}

// runExpr 在独立 goroutine 中求值已编译表达式,超时后放弃等待。expr v1.17 的
// vm 尚不支持 context 取消,goroutine 是唯一的调用方保护(表达式为纯内存计算,
// goroutine 会在自然结束后退出,不泄漏)。用显式 Timer 替代 time.After:提前完成
// 时立即释放定时器,避免高频调用下 3s 定时器在队列中堆积。
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

	timer := time.NewTimer(exprEvalTimeout)
	defer timer.Stop()
	select {
	case res := <-resultCh:
		if res.err != nil {
			return nil, fmt.Errorf("evaluate: %w", res.err)
		}
		return res.value, nil
	case <-timer.C:
		return nil, fmt.Errorf("expression_timeout: expression exceeded %s", exprEvalTimeout)
	}
}

func (exprEngine) Evaluate(expression string, variables map[string]interface{}) (bool, error) {
	env := variables
	if env == nil {
		env = map[string]interface{}{}
	}
	program, err := compileExpr(expression, true)
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
	program, err := compileExpr(expression, false)
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
