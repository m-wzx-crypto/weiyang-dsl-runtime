package dsl

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// ai.go 实现阶段 4:AI 原生节点。
//
// 设计核心是"有界代理"(bounded agency):LLM 只能在 DSL **声明的候选集合**
// 中选择迁移(choose),永远不能发明新的节点或绕过路由;其结构化输出受
// schema 约束(复用 v2 类型系统);推理请求本身是一笔副作用命令——随日志
// 落账、随 outbox 重投,因此"AI 调用中崩溃"不会丢失或重复推理请求。
//
// 执行模型:
//
//	到达 ai 节点 → 发出推理命令(prompt 已插值 + 输出 schema + 候选集)→ 停靠
//	宿主执行 LLM → Feed(回调事件,载荷 {choice, output, error})
//	→ 校验输出 schema 写回变量 → 按 choice/when 路由(或 onError 升级)
//
// DSL 示例(v2):
//
//	{ "id": "triage", "type": "ai",
//	  "ai": {
//	    "prompt": "classify request {{ticket}}",
//	    "output": { "confidence": { "type": "number" } },
//	    "choose": ["billing", "technical", "other"],
//	    "onError": "manual_review"
//	  },
//	  "transitions": [
//	    { "case": "billing",   "next": "billing_flow" },
//	    { "case": "technical", "next": "tech_flow" },
//	    { "case": "other",     "next": "other_flow" }
//	  ] }
//
// 不声明 choose 时,ai 节点退化为"结构化富化"节点:输出写回变量,迁移走
// when 条件/默认分支——路由权仍然完全在 DSL 手里。

const (
	// DefaultAIEventType 是 ai 节点结果回调事件的默认名称。
	DefaultAIEventType = "ai_result"
	// DefaultAICommandType 是推理命令的默认类型。
	DefaultAICommandType = "ai_infer"
	// DefaultAITarget 是推理命令的默认目标。
	DefaultAITarget = "llm"

	// waitKindAI 是 ai 节点停靠槽的类型;无 deadline 时 Until 为零值——
	// 只承担"停靠登记/去重"职责,永不参与唤醒。
	waitKindAI = "ai_request"
)

// AIConfig 描述 ai 节点的推理契约。
type AIConfig struct {
	// Type 是推理命令类型(宿主 SideEffectExecutor 据此路由),默认 ai_infer。
	Type string
	// Target 是推理命令目标,默认 llm。
	Target string
	// Prompt 是提示词模板,支持 {{var}} 与 {{a.b}} 插值(来自流程变量)。
	Prompt string
	// Event 是结果回调事件名,默认 ai_result。
	Event string
	// OutputSpecs 是结构化输出的类型声明原文(进入命令 payload,供宿主约束
	// LLM 的结构化输出)。
	OutputSpecs map[string]json.RawMessage
	// OutputTypes 是解码后的输出 schema(引擎据此校验回传输出)。
	OutputTypes map[string]*Type
	// Choose 是有界代理的候选集合:声明后,AI 的 choice 只能在此集合中选择,
	// 迁移按 transition 的 case 匹配。
	Choose []string
	// OnError 是推理失败/越权选择/输出违约时的升级路由目标;未声明时这些
	// 情形使节点失败。
	OnError string
}

// AIResult 是宿主回传的推理结果(Event.Payload 的形状)。
type AIResult struct {
	// Choice 是有界代理下的候选选择(仅声明 choose 时有意义)。
	Choice string
	// Output 是结构化输出,键须覆盖 AIConfig.OutputTypes 声明的全部变量。
	Output map[string]interface{}
	// Error 非空表示推理失败(网络/超时/内容拒绝),走 onError 升级路由。
	Error string
	// RequestID 是回传的请求命令幂等键(可选)。并行分支场景下,回调载荷
	// 回带 request_id 可把结果精确投递给等待该请求的分支,避免串线。
	RequestID string
}

func aiEventName(node *Node) string {
	if node.Ai != nil && node.Ai.Event != "" {
		return node.Ai.Event
	}
	return DefaultAIEventType
}

func aiCommandType(node *Node) string {
	if node.Ai != nil && node.Ai.Type != "" {
		return node.Ai.Type
	}
	return DefaultAICommandType
}

func aiCommandTarget(node *Node) string {
	if node.Ai != nil && node.Ai.Target != "" {
		return node.Ai.Target
	}
	return DefaultAITarget
}

// parseAIResult 从事件载荷解析推理结果。
func parseAIResult(payload map[string]interface{}) AIResult {
	var r AIResult
	if payload == nil {
		return r
	}
	if c, ok := payload["choice"].(string); ok {
		r.Choice = c
	}
	if e, ok := payload["error"].(string); ok {
		r.Error = e
	}
	if rid, ok := payload["request_id"].(string); ok {
		r.RequestID = rid
	}
	if o, ok := payload["output"].(map[string]interface{}); ok {
		r.Output = o
	}
	return r
}

// resolveAINext 处理 AI 结果并给出迁移目标:
//  1. 任何 AI 侧失败(推理失败 / 输出缺字段 / schema 违约 / 有界代理越权)
//     → onError 升级(未声明则节点失败);
//  2. 结构化输出按 schema 校验后写回变量;
//  3. 声明 choose 时执行有界代理路由(choice ∉ 候选集 = 越权);
//  4. 未声明 choose 时按 when 条件路由 → 默认分支。
func resolveAINext(def *ProcessDef, ctx *ExecutionContext, node *Node, result AIResult) (string, error) {
	ai := node.Ai
	onError := func(reason string) (string, error) {
		if ai.OnError != "" && def.Nodes[ai.OnError] != nil {
			return ai.OnError, nil
		}
		return "", fmt.Errorf("ai node %q: %s", node.ID, reason)
	}

	if result.Error != "" {
		return onError("inference failed: " + result.Error)
	}

	for name, t := range ai.OutputTypes {
		v, ok := result.Output[name]
		if !ok {
			return onError(fmt.Sprintf("output %q missing in ai result", name))
		}
		if err := checkAssignable(name, t, v); err != nil {
			return onError(err.Error())
		}
		ctx.SetVariable(name, v)
	}

	if len(ai.Choose) > 0 {
		if result.Choice == "" {
			return onError("empty choice under bounded agency")
		}
		allowed := false
		for _, c := range ai.Choose {
			if c == result.Choice {
				allowed = true
				break
			}
		}
		if !allowed {
			return onError(fmt.Sprintf("choice %q outside declared candidates (bounded agency)", result.Choice))
		}
		for _, tr := range node.Transitions {
			if tr.Case == result.Choice {
				return tr.Next, nil
			}
		}
		return onError(fmt.Sprintf("no case transition for choice %q", result.Choice))
	}

	next, err := evalWhenChain(node, ctx.Variables, ctx.engine())
	if err != nil {
		return "", err
	}
	if next != "" {
		return next, nil
	}
	return "", fmt.Errorf("ai node %q: no route for ai result", node.ID)
}

var promptVarRe = regexp.MustCompile(`\{\{\s*([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)\s*\}\}`)

// renderPrompt 渲染提示词模板:{{var}} 与 {{a.b}} 取自流程变量;
// 无法解析的占位符保留原样(便于宿主排查)。对象/数组值按 JSON 渲染
// (%v 会输出 Go 语法的 map[k:v],喂给 LLM 既难读也易破坏其结构化输出)。
func renderPrompt(tpl string, vars map[string]interface{}) string {
	return promptVarRe.ReplaceAllStringFunc(tpl, func(m string) string {
		path := promptVarRe.FindStringSubmatch(m)[1]
		parts := strings.Split(path, ".")
		cur, ok := vars[parts[0]]
		if !ok {
			return m
		}
		for _, p := range parts[1:] {
			obj, ok := cur.(map[string]interface{})
			if !ok {
				return m
			}
			cur, ok = obj[p]
			if !ok {
				return m
			}
		}
		switch cur.(type) {
		case map[string]interface{}, []interface{}:
			b, err := json.Marshal(cur)
			if err != nil {
				return m
			}
			return string(b)
		default:
			return fmt.Sprintf("%v", cur)
		}
	})
}

// aiRequestCommandID 构造 ai 推理请求的命令幂等键:与 ToCommand 同构
// (ExecutionID:节点:访问序号:0),并行分支再追加分支槽后缀。请求派发与
// 回调关联(等待槽 RequestID)共用本构造,避免两处手写格式漂移。
func aiRequestCommandID(executionID, nodeID string, visit int, slotKey string) string {
	id := fmt.Sprintf("%s:%s:%d:0", executionID, nodeID, visit)
	if slotKey != instanceSlot {
		id += ":" + slotKey
	}
	return id
}

// buildAIRequest 构造推理命令载荷(命令本身由 Runtime 经 deliver 派发,
// 自动获得 outbox 语义)。
func buildAIRequest(node *Node, vars map[string]interface{}, requestID string) ([]byte, error) {
	payload := map[string]interface{}{
		"node":           node.ID,
		"prompt":         renderPrompt(node.Ai.Prompt, vars),
		"callback_event": aiEventName(node),
		"request_id":     requestID,
	}
	if len(node.Ai.OutputSpecs) > 0 {
		payload["output_schema"] = node.Ai.OutputSpecs
	}
	if len(node.Ai.Choose) > 0 {
		payload["candidates"] = node.Ai.Choose
	}
	return json.Marshal(payload)
}
