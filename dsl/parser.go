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
}

type SideEffect struct {
	Type    string
	Target  string
	Payload []byte
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
}

type rawDSL struct {
	ID      string    `json:"id"`
	Name    string    `json:"name"`
	Version string    `json:"version"`
	Nodes   []rawNode `json:"nodes"`
}

type rawNode struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Label       string          `json:"label"`
	SideEffects []rawSideEffect `json:"sideEffects"`
	Transitions []rawTransition `json:"transitions"`
	Fork        *rawForkConfig  `json:"fork"`
	Join        *rawJoinConfig  `json:"join"`
}

type rawForkConfig struct {
	Mode string `json:"mode"`
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
		return parseV1(data)
	default:
		return nil, fmt.Errorf("unsupported DSL version: %s", raw.Version)
	}
}

func parseV1(data []byte) (*ProcessDef, error) {
	var raw rawDSL
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("failed to parse v1 DSL: %w", err)
	}

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

		if rn.Fork != nil {
			node.Fork = &ForkConfig{Mode: rn.Fork.Mode}
		}
		if rn.Join != nil {
			node.Join = &JoinConfig{
				Mode:     rn.Join.Mode,
				Required: rn.Join.Required,
				Timeout:  rn.Join.Timeout,
			}
		}

		for j, se := range rn.SideEffects {
			var payload []byte
			if se.Payload != nil {
				payload = make([]byte, len(se.Payload))
				copy(payload, se.Payload)
			}
			node.SideEffects = append(node.SideEffects, SideEffect{
				Type:    se.Type,
				Target:  se.Target,
				Payload: payload,
			})
			_ = j
		}

		for k, tr := range rn.Transitions {
			if tr.Next != "" && !nodeIDs[tr.Next] {
				exists := false
				for _, n := range raw.Nodes {
					if n.ID == tr.Next {
						exists = true
						break
					}
				}
				if !exists {
					return nil, fmt.Errorf("nodes[%d].transitions[%d]: next node %q does not exist", i, k, tr.Next)
				}
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
