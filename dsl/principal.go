package dsl

import (
	"errors"
)

// principal.go — M1(Principals)的基石:谁做了这个决策。
//
// ROADMAP §1 的判断:一旦行为主体可能是模型,"谁被允许做这件事"与"谁对此负责"
// 就成了引擎必须回答的问题。Principal 是行为主体的稳定身份,由宿主随事件传入,
// 以事实(Occurrence.Actor)的形式留在日志里,随折叠逐字段复现。
// 内核只记录与折叠归属,不获取、不校验身份(原则 3:身份是宿主的关切,记账是内核的关切)。
//
// W2(M1b)补全推理归属:AI 决策的行为主体是模型,携带"哪个模型、哪个模型版本、
// 哪个 prompt 版本"三元组,由宿主作为**数据**传入(见 ai.go 的回调载荷契约),
// 引擎固化为 Kind == PrincipalModel 的 principal 随事件入账。

// PrincipalKind 区分行为主体的类别。
type PrincipalKind string

const (
	PrincipalHuman  PrincipalKind = "human"  // 人:审批、驳回、人工介入
	PrincipalAgent  PrincipalKind = "agent"  // AI 代理以自身身份行动
	PrincipalSystem PrincipalKind = "system" // 引擎/宿主系统自身
	PrincipalModel  PrincipalKind = "model"  // 模型推理产出(模型/版本/prompt 归属随 ai 回调补全)
)

// Principal 是一次决策的行为主体。
// 人类决策至少携带 Kind + ID;稳定 ID 是跨执行、跨折叠复现归属的锚点。
// 模型决策(Kind == PrincipalModel)另携推理归属三元组 Model / ModelVersion /
// PromptVersion —— 回答"哪个模型、哪个 prompt 版本产出了这条决策"。
type Principal struct {
	Kind        PrincipalKind `json:"kind,omitempty"`
	ID          string        `json:"id,omitempty"`
	DisplayName string        `json:"displayName,omitempty"`

	// 推理归属(M1 W2):仅 Kind == PrincipalModel 时有意义,由宿主作为数据传入
	// (引擎经 ai 回调载荷提升,或宿主在 Event.Principal 上显式给出)。
	Model         string `json:"model,omitempty"`
	ModelVersion  string `json:"modelVersion,omitempty"`
	PromptVersion string `json:"promptVersion,omitempty"`
}

// ErrPrincipalRequired 是强制点(M1)的哨兵错误:要求归属的节点上,缺 principal
// (或归属不完整)的决策被结构性拒绝——不消费、不入账、实例保持等待。
// 宿主以 errors.Is 判别,补上归属后重投同一事件即可。
var ErrPrincipalRequired = errors.New("principal required")

// systemPrincipal 是引擎自产时间轴迁移的稳定归属,用于 deadline/timer
// 升级和其他无需外部 principal 的内部决策事实。
func systemPrincipal() *Principal {
	return &Principal{Kind: PrincipalSystem, ID: "weiyang-runtime", DisplayName: "Weiyang Runtime"}
}

// validDecisionPrincipal 判定事件携带的 principal 是否满足节点的归属要求。
// 通用要求:principal 存在且携带稳定身份(Kind + ID 齐备)。ai 节点进一步
// 要求**完整的模型归属**——推理决策必须能回答"哪个模型、哪个 prompt 版本"
// (ROADMAP M1),三元组缺一即不合规。
func validDecisionPrincipal(node *Node, p *Principal) bool {
	if p == nil || p.Kind == "" || p.ID == "" {
		return false
	}
	if node.Type == "ai" {
		return p.Kind == PrincipalModel &&
			p.Model != "" && p.ModelVersion != "" && p.PromptVersion != ""
	}
	return true
}

// principalRequirement 描述节点对决策归属的要求(强制点错误信息用)。
func principalRequirement(node *Node) string {
	if node.Type == "ai" {
		return "model attribution (principal of kind model with model, model_version and prompt_version)"
	}
	return "a principal (kind + id)"
}

// clone 返回独立副本。日志是 append-only 的真相,入账的 principal 不允许
// 与调用方共享可变指针——调用方事后改动自己的对象,不能改写已发生的账。
func (p *Principal) clone() *Principal {
	if p == nil {
		return nil
	}
	cp := *p
	return &cp
}
