package dsl

import (
	"encoding/json"
	"fmt"
)

type Node struct {
	ID          string
	Type        string
	Label       string
	SideEffects []SideEffect
	Transitions []Transition

	// Optional parallel/join configuration. 仅当 type 为 "parallel" / "join" 时使用，
	// 用于承载 fork/join 的真正执行语义（用户建议第 4 点）。
	Fork *ForkConfig
	Join *JoinConfig

	// Input / Output 是 v2 数据契约的节点级映射：
	//   - Input  在进入节点时求值，产生节点局部作用域（供 when 条件与副作用引用）；
	//   - Output 在离开节点时求值，把结果写回流程变量（带 schema 类型校验）。
	Input  map[string]string
	Output map[string]string

	// Duration 仅 timer 节点使用：经过该时长后由 WakeDue 触发前向迁移。
	Duration string

	// Deadline 仅 waiting 节点（approval/subprocess）使用：等待超过 After 后
	// 迁移到 Next（超时升级路由），与外部事件先到先得。
	Deadline *DeadlineConfig
}

// DeadlineConfig 声明 waiting 节点的超时升级路由。
type DeadlineConfig struct {
	// After 是等待时长，例如 "24h"；由 WakeDue 主动检查。
	After string
	// Next 是超时后的迁移目标节点（如升级审批人、提醒节点）。
	Next string
}

type SideEffect struct {
	Type    string
	Target  string
	Payload []byte
	// Compensation 是 v2 行为契约:声明本副作用的逆操作。副作用执行成功后,
	// 逆操作进入实例的 undo 栈;实例失败(或显式 Compensate)时按逆序发射补偿命令。
	Compensation *SideEffect
}

type Transition struct {
	Event string
	When  string
	Next  string
}

type ProcessDef struct {
	ID        string
	Name      string
	Version   string
	Nodes     map[string]*Node
	StartNode string

	// VarSchema 是 v2 数据契约:流程变量的类型声明。非空时,validator 会对所有
	// when 表达式做静态类型检查,节点 output 写回会做运行期类型校验。
	VarSchema map[string]*Type
	// VarInit 是变量的初始值(在 NewExecutionContext 时应用,Start 注入的变量
	// 优先级更高)。
	VarInit map[string]interface{}
}

// TypeSchema 把声明转换为 TypeChecker 消费的 schema 对象;无声明时返回 nil。
func (d *ProcessDef) TypeSchema() *TypeSchema {
	if len(d.VarSchema) == 0 {
		return nil
	}
	schema := NewTypeSchema()
	for name, t := range d.VarSchema {
		schema.Declare(name, t)
	}
	return schema
}

type rawDSL struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Version string    `json:"version"`
	Nodes   []rawNode `json:"nodes"`
	// Variables 是 v2 数据契约:变量名 → 类型声明(字符串简写或对象形式),
	// 可附带 "init" 初始值。
	Variables map[string]json.RawMessage `json:"variables"`
}

type rawNode struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Label       string          `json:"label"`
	SideEffects []rawSideEffect `json:"sideEffects"`
	Transitions []rawTransition `json:"transitions"`
	Fork        *rawForkConfig  `json:"fork"`
	Join        *rawJoinConfig  `json:"join"`
	// v2 数据契约与时间契约:
	Input     map[string]string `json:"input"`
	Output    map[string]string `json:"output"`
	Duration  string            `json:"duration"`
	Deadline  *rawDeadline      `json:"deadline"`
}

type rawDeadline struct {
	After string `json:"after"`
	Next  string `json:"next"`
}

type rawForkConfig struct {
	Mode     string `json:"mode"`
	JoinNode string `json:"joinNode"`
	OnFail   string `json:"onFail"`
}

type rawJoinConfig struct {
	Mode     string `json:"mode"`
	Required int    `json:"required"`
	Timeout  string `json:"timeout"`
}

type rawSideEffect struct {
	Type    string          `json:"type"`
	Target  string          `json:"target"`
	Payload json.RawMessage `json:"payload"`
	// Compensation 声明逆操作(v2 行为契约),结构与副作用本身一致。
	Compensation *rawSideEffect `json:"compensation"`
}

type rawTransition struct {
	Event string `json:"event"`
	When  string `json:"when"`
	Next  string `json:"next"`
}

func ParseDSL(data []byte) (*ProcessDef, error) {
	var raw rawDSL
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse DSL JSON: %w", err)
	}

	if raw.Version == "" {
		return nil, fmt.Errorf("DSL version is required")
	}

	switch raw.Version {
	case "1", "1.0":
		return parseCore(&raw, false)
	case "2", "2.0":
		return parseCore(&raw, true)
	default:
		return nil, fmt.Errorf("unsupported DSL version: %s", raw.Version)
	}
}

// parseCore 是 v1/v2 共用的解析主体(单次 unmarshal,不再重复解码)。v2 在此之上
// 解锁:变量类型契约、节点 input/output 映射、timer 节点、waiting 节点 deadline、
// 副作用补偿声明。v1 语义保持逐字节不变:未知字段被忽略,不校验类型契约。
func parseCore(raw *rawDSL, v2 bool) (*ProcessDef, error) {
	if raw.ID == "" {
		return nil, fmt.Errorf("process id is required")
	}
	if len(raw.Nodes) == 0 {
		return nil, fmt.Errorf("process must have at least one node")
	}

	def := &ProcessDef{
		ID:      raw.ID,
		Name:    raw.Name,
		Version: raw.Version,
		Nodes:   make(map[string]*Node, len(raw.Nodes)),
	}

	if v2 && len(raw.Variables) > 0 {
		if err := decodeVarDecls(raw, def); err != nil {
			return nil, err
		}
	}

	// 全量 ID 索引:迁移目标的完整性检查 O(1)(此前为逐节点线性扫描 O(n²))。
	allIDs := make(map[string]bool, len(raw.Nodes))
	for _, rn := range raw.Nodes {
		allIDs[rn.ID] = true
	}

	nodeIDs := make(map[string]bool, len(raw.Nodes))

	for i, rn := range raw.Nodes {
		if rn.ID == "" {
			return nil, fmt.Errorf("nodes[%d]: node id is required", i)
		}
		if nodeIDs[rn.ID] {
			return nil, fmt.Errorf("nodes[%d]: duplicate node id %q", i, rn.ID)
		}
		nodeIDs[rn.ID] = true

		node := &Node{
			ID:          rn.ID,
			Type:        rn.Type,
			Label:       rn.Label,
			SideEffects: make([]SideEffect, 0, len(rn.SideEffects)),
			Transitions: make([]Transition, 0, len(rn.Transitions)),
		}

		if v2 {
			node.Input = rn.Input
			node.Output = rn.Output
			node.Duration = rn.Duration
			if rn.Deadline != nil {
				node.Deadline = &DeadlineConfig{After: rn.Deadline.After, Next: rn.Deadline.Next}
			}
		}

		if rn.Fork != nil {
			node.Fork = &ForkConfig{
				Mode:     rn.Fork.Mode,
				JoinNode: rn.Fork.JoinNode,
				OnFail:   rn.Fork.OnFail,
			}
		}
		if rn.Join != nil {
			node.Join = &JoinConfig{
				Mode:     rn.Join.Mode,
				Required: rn.Join.Required,
				Timeout:  rn.Join.Timeout,
			}
		}

		for _, se := range rn.SideEffects {
			var payload []byte
			if se.Payload != nil {
				payload = make([]byte, len(se.Payload))
				copy(payload, se.Payload)
			}
			effect := SideEffect{
				Type:    se.Type,
				Target:  se.Target,
				Payload: payload,
			}
			if v2 && se.Compensation != nil {
				comp := SideEffect{
					Type:    se.Compensation.Type,
					Target:  se.Compensation.Target,
				}
				if se.Compensation.Payload != nil {
					comp.Payload = make([]byte, len(se.Compensation.Payload))
					copy(comp.Payload, se.Compensation.Payload)
				}
				effect.Compensation = &comp
			}
			node.SideEffects = append(node.SideEffects, effect)
		}

		for k, tr := range rn.Transitions {
			if tr.Next != "" && !allIDs[tr.Next] {
				return nil, fmt.Errorf("nodes[%d].transitions[%d]: next node %q does not exist", i, k, tr.Next)
			}
			node.Transitions = append(node.Transitions, Transition{
				Event: tr.Event,
				When:  tr.When,
				Next:  tr.Next,
			})
		}

		if rn.Type == "start" {
			if def.StartNode != "" {
				return nil, fmt.Errorf("multiple start nodes found: %q and %q", def.StartNode, rn.ID)
			}
			def.StartNode = rn.ID
		}

		def.Nodes[rn.ID] = node
	}

	if def.StartNode == "" {
		return nil, fmt.Errorf("process must have a start node")
	}

	return def, nil
}

// decodeVarDecls 解析 v2 的变量声明块:类型 schema + 初始值。
func decodeVarDecls(raw *rawDSL, def *ProcessDef) error {
	def.VarSchema = make(map[string]*Type, len(raw.Variables))
	def.VarInit = make(map[string]interface{}, len(raw.Variables))
	for name, decl := range raw.Variables {
		var probe struct {
			Init json.RawMessage `json:"init"`
		}
		_ = json.Unmarshal(decl, &probe) // 字符串简写形式没有 init,忽略错误

		// 类型声明可能是整体一个字符串("money"),也可能是对象({"type":...,"init":...})。
		var probeMap map[string]json.RawMessage
		typeRaw := decl
		if err := json.Unmarshal(decl, &probeMap); err == nil {
			if t, ok := probeMap["type"]; ok {
				typeRaw = t
			}
			if _, ok := probeMap["fields"]; ok {
				// 完整对象类型直接整体解码。
				t, err := DecodeTypeSpec(decl)
				if err != nil {
					return fmt.Errorf("variables[%q]: %w", name, err)
				}
				def.VarSchema[name] = t
				if probe.Init != nil {
					var init interface{}
					if err := json.Unmarshal(probe.Init, &init); err != nil {
						return fmt.Errorf("variables[%q].init: %w", name, err)
					}
					def.VarInit[name] = init
				}
				continue
			}
		}

		t, err := DecodeTypeSpec(typeRaw)
		if err != nil {
			return fmt.Errorf("variables[%q]: %w", name, err)
		}
		def.VarSchema[name] = t
		if probe.Init != nil {
			var init interface{}
			if err := json.Unmarshal(probe.Init, &init); err != nil {
				return fmt.Errorf("variables[%q].init: %w", name, err)
			}
			def.VarInit[name] = init
		}
	}
	return nil
}
