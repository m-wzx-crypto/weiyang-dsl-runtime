package dsl

// principal.go — M1(Principals)的基石:谁做了这个决策。
//
// ROADMAP §1 的判断:一旦行为主体可能是模型,"谁被允许做这件事"与"谁对此负责"
// 就成了引擎必须回答的问题。Principal 是行为主体的稳定身份,由宿主随事件传入,
// 以事实(Occurrence.Actor)的形式留在日志里,随折叠逐字段复现。
// 内核只记录与折叠归属,不获取、不校验身份(原则 3:身份是宿主的关切,记账是内核的关切)。

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
type Principal struct {
	Kind        PrincipalKind `json:"kind,omitempty"`
	ID          string        `json:"id,omitempty"`
	DisplayName string        `json:"displayName,omitempty"`
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
