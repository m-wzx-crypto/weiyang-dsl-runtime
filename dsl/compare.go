package dsl

import "fmt"

// compare.go — M3a(W6):同一定义两次执行的运行对比 v1。
//
// 成本在 M4 的真实效果预算落地前,以"被接受的模型决策数"为代理指标:统计
// OccEventConsumed 且 Actor.Kind=model 的事实,而不是 AI 命令派发次数,避免
// outbox 重试把一次推理重复计费。升级率的分母是被接受的领域事件决策数;
// deadline 迁移与 AI onError 迁移都计为一次升级。

// RunMetrics 是一次执行的可比较指标集。
type RunMetrics struct {
	InstanceID     string   `json:"instanceId"`
	ExecutionID    string   `json:"executionId"`
	Status         string   `json:"status"`
	Routes         []string `json:"routes"`
	Decisions      int      `json:"decisions"`
	Escalations    int      `json:"escalations"`
	EscalationRate float64  `json:"escalationRate"`
	ModelCalls     int      `json:"modelCalls"`
}

// RunComparison 是两次执行的指标与差值。RoutingAgreement 按路由序列相同位置
// 的一致比例计算,分母为较长序列;SameRoutes 要求长度与每一步都一致。
type RunComparison struct {
	A                   RunMetrics `json:"a"`
	B                   RunMetrics `json:"b"`
	SameRoutes          bool       `json:"sameRoutes"`
	RoutingAgreement    float64    `json:"routingAgreement"`
	EscalationRateDelta float64    `json:"escalationRateDelta"`
	ModelCallDelta      int        `json:"modelCallDelta"`
}

// CompareRuns 比较同一定义的两次实例日志。def 用于识别 AI onError 升级路由;
// 日志本身尚未记录定义 hash,调用方须保证两组日志确由该定义产生。
func CompareRuns(def *ProcessDef, a, b []Occurrence) (*RunComparison, error) {
	if def == nil {
		return nil, fmt.Errorf("compare runs: nil definition")
	}
	ma, err := metricsForRun(def, a)
	if err != nil {
		return nil, fmt.Errorf("compare run A: %w", err)
	}
	mb, err := metricsForRun(def, b)
	if err != nil {
		return nil, fmt.Errorf("compare run B: %w", err)
	}

	agreement := 1.0
	if len(ma.Routes) != 0 || len(mb.Routes) != 0 {
		longer := len(ma.Routes)
		if len(mb.Routes) > longer {
			longer = len(mb.Routes)
		}
		shorter := len(ma.Routes)
		if len(mb.Routes) < shorter {
			shorter = len(mb.Routes)
		}
		matched := 0
		for i := 0; i < shorter; i++ {
			if ma.Routes[i] == mb.Routes[i] {
				matched++
			}
		}
		agreement = float64(matched) / float64(longer)
	}
	return &RunComparison{
		A:                   ma,
		B:                   mb,
		SameRoutes:          len(ma.Routes) == len(mb.Routes) && agreement == 1,
		RoutingAgreement:    agreement,
		EscalationRateDelta: ma.EscalationRate - mb.EscalationRate,
		ModelCallDelta:      ma.ModelCalls - mb.ModelCalls,
	}, nil
}

func metricsForRun(def *ProcessDef, occs []Occurrence) (RunMetrics, error) {
	if len(occs) == 0 {
		return RunMetrics{}, fmt.Errorf("empty execution log")
	}
	m := RunMetrics{Status: StatusPending.String()}
	branchNodes := map[string]string{}
	for i := range occs {
		occ := &occs[i]
		if occ.InstanceID == "" {
			return RunMetrics{}, fmt.Errorf("occ#%d has no instance id", occ.Seq)
		}
		if m.InstanceID == "" {
			m.InstanceID = occ.InstanceID
			m.ExecutionID = occ.ExecutionID
		} else if occ.InstanceID != m.InstanceID || occ.ExecutionID != m.ExecutionID {
			return RunMetrics{}, fmt.Errorf("occ#%d mixes executions %s/%s and %s/%s",
				occ.Seq, m.InstanceID, m.ExecutionID, occ.InstanceID, occ.ExecutionID)
		}

		switch occ.Kind {
		case OccEventConsumed:
			if occ.Event != nil && occ.Event.Name != "" {
				m.Decisions++
			}
			if occ.Actor != nil && occ.Actor.Kind == PrincipalModel {
				m.ModelCalls++
			}
		case OccTransition:
			route := occ.FromNode + "->" + occ.ToNode
			if occ.TransitionEvent != "" {
				route += " (" + occ.TransitionEvent + ")"
			}
			m.Routes = append(m.Routes, route)
			if occ.TransitionEvent == "deadline" || isAIEscalation(def, occ.FromNode, occ.ToNode) {
				m.Escalations++
			}
		case OccBranchUpdated:
			if occ.Branch == nil {
				continue
			}
			id := occ.Branch.ID
			previous, seen := branchNodes[id]
			if !seen && occ.Branch.StartNode != "" {
				m.Routes = append(m.Routes, occ.NodeID+"->"+occ.Branch.StartNode+" (branch "+id+")")
				previous = occ.Branch.StartNode
			}
			if current := occ.Branch.CurrentNode; current != "" && current != previous {
				m.Routes = append(m.Routes, previous+"->"+current+" (branch "+id+")")
			}
			branchNodes[id] = occ.Branch.CurrentNode
		case OccStatus:
			m.Status = occ.Status
		}
	}
	if m.Decisions > 0 {
		m.EscalationRate = float64(m.Escalations) / float64(m.Decisions)
	}
	return m, nil
}

func isAIEscalation(def *ProcessDef, from, to string) bool {
	if def == nil || from == "" || to == "" {
		return false
	}
	node := def.Nodes[from]
	return node != nil && node.Ai != nil && node.Ai.OnError != "" && node.Ai.OnError == to
}
