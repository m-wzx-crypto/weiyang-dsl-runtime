package dsl

// activation.go — M2(Attributed Rules)W5b:激活状态机 draft → active。
//
// ROADMAP §3.2:规则行为的变更要像代码变更一样可审、可问责。注册(W4)解决
// "改了什么"(不可变版本史 + 结构化 diff),激活解决"谁让它生效":
//
//   - 状态机只有两个状态:draft(已注册提案)→ active(生效规则)。
//   - 激活是问责门:proposer + owner 双字段齐备才能激活(缺一即
//     ErrAttributionRequired),执行激活的行为主体必须携带稳定身份
//     (ErrPrincipalRequired)。
//   - 身份不另造:hash、proposer、owner 全部复用注册表条目(W4 留下的接线点),
//     激活记录只是"哪一版 + 谁批的 + 何时"。
//   - 激活即事实:draft → active 的跃迁以 OccDefActivated 记入 Journal,与
//     实例级日志分账(DefID/DefVersion 携带定义键,InstanceID/ExecutionID 恒空)。
//     事实记不下来,激活就不成立(fact-first,同 WAL 语义)——状态不翻动。
//
// 折叠精确性同实例账口径:foldActivation(定义账) 复现的激活状态与活状态
// 逐字段一致(含 ActivatedAt 与 ActivatedBy)。

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// ActivationStatus 是定义版本的激活状态。
type ActivationStatus string

const (
	// ActivationDraft:已注册的提案,尚未生效。
	ActivationDraft ActivationStatus = "draft"
	// ActivationActive:生效中的规则。
	ActivationActive ActivationStatus = "active"
)

// ErrAttributionRequired 是激活问责门的哨兵错误:proposer 与 owner 必须齐备,
// 缺一即拒绝激活。宿主以 errors.Is 判别;补齐归属后重试即可。
var ErrAttributionRequired = errors.New("proposer and owner required for activation")

// ActivationState 是定义版本的激活状态快照。作为 OccDefActivated 的载荷
// 自带 hash 与归属,日志读者无需回查注册表即可回答"生效的是哪份内容、谁提案、
// 谁负责、谁让生效、何时生效"。
type ActivationState struct {
	DefID       string           `json:"defId"`
	Version     string           `json:"version"`
	Hash        string           `json:"hash"`
	Status      ActivationStatus `json:"status"`
	Proposer    string           `json:"proposer,omitempty"`
	Owner       string           `json:"owner,omitempty"`
	ActivatedBy *Principal       `json:"activatedBy,omitempty"`
	ActivatedAt time.Time        `json:"activatedAt,omitempty"`
}

// clone 返回独立副本:入账的快照不允许与调用方共享可变指针
// (调用方事后改对象不能改写已发生的账,同 Principal.clone 契约)。
func (s *ActivationState) clone() *ActivationState {
	if s == nil {
		return nil
	}
	cp := *s
	cp.ActivatedBy = s.ActivatedBy.clone()
	return &cp
}

// Activator 管理定义版本的激活状态机。
// 依赖注入 Registry(身份来源)与 Journal(事实落点),不另造 identity。
type Activator struct {
	mu     sync.RWMutex
	states map[registryKey]*ActivationState

	registry *DefinitionRegistry
	journal  Journal
}

// NewActivator 创建激活器。journal 允许为 nil(不入账,状态照常翻转);
// 非 nil 时事实记账失败会连带激活失败(fact-first)。
func NewActivator(registry *DefinitionRegistry, journal Journal) *Activator {
	return &Activator{
		states:   make(map[registryKey]*ActivationState),
		registry: registry,
		journal:  journal,
	}
}

// State 返回指定版本的激活状态;未注册版本返回 nil(没有状态机可言)。
// 注册而未激活的版本是 draft:状态的基线来自注册表条目(hash/归属),
// 未翻转时无需在激活器里另存一份。
func (a *Activator) State(id, version string) *ActivationState {
	a.mu.RLock()
	st := a.states[registryKey{id, version}]
	a.mu.RUnlock()
	if st != nil {
		return st.clone()
	}
	if a.registry == nil {
		return nil
	}
	entry := a.registry.GetVersion(id, version)
	if entry == nil {
		return nil
	}
	return &ActivationState{
		DefID:    id,
		Version:  version,
		Hash:     entry.Hash,
		Status:   ActivationDraft,
		Proposer: entry.Info.Proposer,
		Owner:    entry.Info.Owner,
	}
}

// Activate 把注册过的版本从 draft 推进到 active。
//
// 门禁(ROADMAP M2):
//   - 版本必须已在注册表中(激活不注册内容);
//   - 注册时的 proposer + owner 必须齐备,否则 ErrAttributionRequired;
//   - 执行激活的 actor 必须携带稳定身份,否则 ErrPrincipalRequired;
//   - 跃迁记为 OccDefActivated 事实;入账失败则激活失败、状态不翻动。
//
// 重复激活同一版本是幂等操作:状态不翻动(ActivatedAt/ActivatedBy 保持首次),
// 不追加第二笔事实。
func (a *Activator) Activate(id, version string, actor *Principal) (*ActivationState, error) {
	if actor == nil || actor.Kind == "" || actor.ID == "" {
		return nil, fmt.Errorf("%w: activation needs an acting principal (kind + id)", ErrPrincipalRequired)
	}
	if a.registry == nil {
		return nil, errors.New("nil registry")
	}
	entry := a.registry.GetVersion(id, version)
	if entry == nil {
		return nil, fmt.Errorf("unknown definition version %s@%s", id, version)
	}
	if entry.Info.Proposer == "" || entry.Info.Owner == "" {
		return nil, fmt.Errorf("%w: %s@%s registered without proposer/owner", ErrAttributionRequired, id, version)
	}

	key := registryKey{id, version}

	a.mu.Lock()
	defer a.mu.Unlock()

	if existing := a.states[key]; existing != nil && existing.Status == ActivationActive {
		// 幂等:同一版本重复激活不动状态、不重复入账。
		return existing.clone(), nil
	}

	now := time.Now()
	next := &ActivationState{
		DefID:       id,
		Version:     version,
		Hash:        entry.Hash,
		Status:      ActivationActive,
		Proposer:    entry.Info.Proposer,
		Owner:       entry.Info.Owner,
		ActivatedBy: actor.clone(),
		ActivatedAt: now,
	}

	// 事实先行:跃迁先入账,账落下来状态才成立(同 WAL 语义)。
	if a.journal != nil {
		occ := &Occurrence{
			Time:       now,
			Kind:       OccDefActivated,
			Actor:      actor.clone(),
			DefID:      id,
			DefVersion: version,
			Activation: next.clone(),
		}
		if err := a.journal.Append(occ); err != nil {
			return nil, fmt.Errorf("journal activation fact: %w", err)
		}
	}

	a.states[key] = next
	return next.clone(), nil
}

// foldActivation 从定义账重建激活状态(测试与审计视图共用)。
// 折叠口径与实例账一致:逐字段复现(含时间与行为主体),后写覆盖前写,
// 最后一笔 OccDefActivated 即该版本的当前激活状态。
func foldActivation(occs []Occurrence) map[registryKey]*ActivationState {
	out := make(map[registryKey]*ActivationState)
	for i := range occs {
		occ := &occs[i]
		if occ.Kind != OccDefActivated {
			continue
		}
		key := registryKey{occ.DefID, occ.DefVersion}
		if occ.Activation == nil {
			// 无快照的事实不可复现状态——跳过而非造出半截状态。
			continue
		}
		out[key] = occ.Activation.clone()
	}
	return out
}
