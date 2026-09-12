package dsl

// fold_property_test.go — 折叠精确性的性质测试(ROADMAP §6:"Fold exactness
// enforced by property tests, not convention")。
//
// journal.go 的契约:任意流程执行后,从空日志折叠出的上下文必须与活上下文
// 逐字段一致。TestFold_Exact_* 用五类手工场景固定该契约;本文件把它升级为
// 性质测试:按固定种子随机生成合法定义与随机事件驱动,在**每一次**
// Start/Feed 之后断言 fold == 活上下文——包括被拒绝、被消费又回滚的事件,
// 那条路径同样不允许绕过记账。
//
// 除全量折叠外,还验证两条恢复路径与全量折叠的一致性:
//   - Resume:把日志折叠为可继续执行的 Runtime(宿主崩溃恢复的入口);
//   - 增量折叠:先折前半段日志、再 foldOnto 追加余下事实(快照恢复的路径)。
//
// 随机源使用固定种子序列,CI 完全可复现;失败时 seed 即最小化凭据。

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

const propSeedCount = 40

// propFlow 是一个生成的随机用例:定义 + 声明的事件池 + 起始变量。
type propFlow struct {
	def    *ProcessDef
	events []string
	vars   map[string]interface{}
}

// genPropFlow 生成一个随机但合法、可通过部署期校验的流程定义:
//
//	start -submit-> 段1 -> 段2 -> ... -> end
//
// 每段随机取一种形态:
//   - approval:等一个声明事件后前进;
//   - condition:按 when 表达式路由,必须有无条件兜底(变量保证两条路可达);
//     一条 when 出口可能再插入一个 approval,增加"等事件点"的拓扑多样性;
//   - parallel:fork 2-3 个 approval 分支,分支以公共后继为自动 join
//     (与 runtime_test.go 的 parallelDef 同形)。
//
// 返回值 events 汇总定义声明的全部外部事件,供随机驱动器投递。
func genPropFlow(rng *rand.Rand, defID string) *propFlow {
	def := &ProcessDef{ID: defID, Version: "1.0", StartNode: "start", Nodes: map[string]*Node{}}

	amount := float64(100 + 200*rng.Intn(10)) // 100..1900
	conds := []string{
		"vip", "!vip",
		fmt.Sprintf("amount > %v", amount), fmt.Sprintf("amount <= %v", amount),
	}

	flow := &propFlow{
		def: def,
		vars: map[string]interface{}{
			"amount": amount,
			"vip":    rng.Intn(2) == 0,
		},
	}

	// 自 end 向 start 反向接线:entry 始终指向"下一段的入口或 end"。
	entry := "end"
	segCount := 1 + rng.Intn(4) // 1..4 段
	for i := segCount - 1; i >= 0; i-- {
		switch rng.Intn(3) {
		case 0: // approval
			id, ev := fmt.Sprintf("ap%d", i), fmt.Sprintf("approve%d", i)
			def.Nodes[id] = &Node{ID: id, Type: "approval",
				Transitions: []Transition{{Event: ev, Next: entry}}}
			flow.events = append(flow.events, ev)
			entry = id
		case 1: // condition(when 路由 + 无条件兜底)
			id := fmt.Sprintf("rt%d", i)
			branch := entry
			if rng.Intn(2) == 0 { // 一条 when 出口插入 approval,制造嵌套等待点
				wid, wEv := fmt.Sprintf("wt%d", i), fmt.Sprintf("waitev%d", i)
				def.Nodes[wid] = &Node{ID: wid, Type: "approval",
					Transitions: []Transition{{Event: wEv, Next: entry}}}
				flow.events = append(flow.events, wEv)
				branch = wid
			}
			def.Nodes[id] = &Node{ID: id, Type: "condition", Transitions: []Transition{
				{When: conds[rng.Intn(len(conds))], Next: branch},
				{Next: entry}, // 部署期校验要求:缺省兜底,表达式不穷尽也能前进
			}}
			entry = id
		default: // parallel:fork 2-3 个 approval 分支 → 公共 entry 自动 join
			fork := fmt.Sprintf("pa%d", i)
			var branches []Transition
			for b, n := 0, 2+rng.Intn(2); b < n; b++ {
				bid, bev := fmt.Sprintf("pa%d_b%d", i, b), fmt.Sprintf("pb%d_%d_ok", i, b)
				def.Nodes[bid] = &Node{ID: bid, Type: "approval",
					Transitions: []Transition{{Event: bev, Next: entry}}}
				branches = append(branches, Transition{Next: bid})
				flow.events = append(flow.events, bev)
			}
			def.Nodes[fork] = &Node{ID: fork, Type: "parallel", Transitions: branches}
			entry = fork
		}
	}

	def.Nodes["start"] = &Node{ID: "start", Type: "start",
		Transitions: []Transition{{Event: "submit", Next: entry}}}
	def.Nodes["end"] = &Node{ID: "end", Type: "end"}
	flow.events = append(flow.events, "submit")
	return flow
}

// assertResumeEq 断言:从日志 Resume 出的全新 Runtime 与活上下文逐字段一致
// (M1 门禁将复用同款断言:principal 必须原样穿越 fold)。
func assertResumeEq(t *testing.T, def *ProcessDef, r *Runtime) {
	t.Helper()
	j, ok := r.Journal.(*MemoryJournal)
	if !ok {
		t.Fatalf("expected *MemoryJournal, got %T", r.Journal)
	}
	r2, err := Resume(def, j.Occurrences(), NewInMemorySideEffectExecutor(), WithJournal(NewMemoryJournal()))
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := compareCtx(shadowOf(t, r.Ctx), shadowOf(t, r2.Ctx)); err != nil {
		t.Fatalf("resumed context diverges from live context: %v", err)
	}
}

// assertIncrementalFoldEq 断言:分段折叠(前半全量 + 后半增量)与全量折叠一致
// ——快照恢复(store-postgres 的 Savepoint 路径)依赖这一性质。
func assertIncrementalFoldEq(t *testing.T, def *ProcessDef, occs []Occurrence) {
	t.Helper()
	full, err := foldContext(def, occs)
	if err != nil {
		t.Fatalf("full fold: %v", err)
	}
	if len(occs) < 2 {
		return
	}
	half, err := foldContext(def, occs[:len(occs)/2])
	if err != nil {
		t.Fatalf("partial fold: %v", err)
	}
	if err := foldOnto(half, occs[len(occs)/2:]); err != nil {
		t.Fatalf("incremental fold: %v", err)
	}
	if err := compareCtx(shadowOf(t, full), shadowOf(t, half)); err != nil {
		t.Fatalf("incremental fold diverges from full fold: %v", err)
	}
}

func runPropSeed(t *testing.T, seed int64) {
	rng := rand.New(rand.NewSource(seed))
	flow := genPropFlow(rng, fmt.Sprintf("prop_%d", seed))

	if vr := Validate(flow.def); !vr.IsValid {
		t.Fatalf("generated definition failed deploy-time validation: %v", vr.Errors)
	}

	// 归属(M1 W1):每个事件都由随机的人类主体发出,principal 随每次
	// Start/Feed 入账,折叠精确性断言(含 compareCtx 的 principal 比较)全程生效。
	users := []Principal{
		{Kind: PrincipalHuman, ID: "u-alice", DisplayName: "Alice"},
		{Kind: PrincipalHuman, ID: "u-bob", DisplayName: "Bob"},
		{Kind: PrincipalHuman, ID: "u-carol", DisplayName: "Carol"},
	}
	someone := func() *Principal {
		p := users[rng.Intn(len(users))]
		return &p
	}

	j := NewMemoryJournal()
	r := NewRuntime(flow.def, NewInMemorySideEffectExecutor(), WithJournal(j))

	res := r.Start(fmt.Sprintf("inst-%d", seed), "exec-1", flow.vars,
		Event{ID: "s0", Name: "submit", Principal: someone()})
	if res.HasErrors() {
		t.Fatalf("start errors: %v", res.Errors)
	}
	requireFoldEq(t, flow.def, r)

	// 随机洗牌驱动:每轮把声明事件按随机顺序各投递一次(唯一 ID,重复名称合法)。
	// 顺序随机保留交织覆盖;全量覆盖保证活性——只要实例未终态,其等待中的
	// 事件必然在本轮洗牌内被投递,故每轮至少前进一步,终止性有界。
	// 不可投递的事件会被拒绝或消费后回滚——两条路径都必须保持折叠精确性。
	evSeq := 0
	for sweep := 0; sweep < len(flow.events)+4 && !isTerminalStatus(r.Status()); sweep++ {
		for _, idx := range rng.Perm(len(flow.events)) {
			if isTerminalStatus(r.Status()) {
				break
			}
			evSeq++
			r.Feed(Event{ID: fmt.Sprintf("ev-%d", evSeq),
				Name: flow.events[idx], Principal: someone()})
			requireFoldEq(t, flow.def, r)

			if evSeq%3 == 0 && !isTerminalStatus(r.Status()) {
				assertResumeEq(t, flow.def, r)
			}
		}
	}

	if !isTerminalStatus(r.Status()) {
		t.Fatalf("random drive stalled in status %s with events %v", r.Status(), flow.events)
	}
	assertResumeEq(t, flow.def, r)
	assertIncrementalFoldEq(t, flow.def, j.Occurrences())

	// 归属不变量:凡带 principal 的事件,其消费事实必有逐字段一致的 Actor
	// (W2 的强制点以此为地基)。
	for _, occ := range j.Occurrences() {
		if occ.Kind != OccEventConsumed || occ.Event == nil || occ.Event.Principal == nil {
			continue
		}
		if occ.Actor == nil || !reflect.DeepEqual(*occ.Actor, *occ.Event.Principal) {
			t.Fatalf("occ#%d: actor %+v != event principal %+v", occ.Seq, occ.Actor, occ.Event.Principal)
		}
	}
}

func TestProperty_FoldExactness_RandomFlows(t *testing.T) {
	for seed := int64(1); seed <= propSeedCount; seed++ {
		seed := seed
		t.Run(fmt.Sprintf("seed-%d", seed), func(t *testing.T) {
			runPropSeed(t, seed)
		})
	}
}
