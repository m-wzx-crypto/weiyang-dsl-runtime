package dsl

// m2a_gate_test.go — M2a(Attributed Rules W4):版本注册表 + 结构化 diff 门禁。
//
// 验收(PLAN W4 / ROADMAP M2a):
//   1. 版本注册表不可变、有序,每版带 proposer + accountable owner。
//   2. 结构化 diff:对 ProcessDef(节点/迁移/类型/时间契约)做稳定排序的 diff。
//   3. 黄金文件锁定输出格式:同输入必产出同 diff。
//
// 这些测试在实现完成前应全部失败。

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func w4OrderV1() *ProcessDef {
	return &ProcessDef{
		ID: "order_flow", Version: "1.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start":   {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":     {ID: "end", Type: "end"},
		},
	}
}

func w4OrderV2() *ProcessDef {
	return &ProcessDef{
		ID: "order_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start": {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "triage"}}},
			"triage": {ID: "triage", Type: "ai",
				RequirePrincipal: true,
				Ai: &AIConfig{
					Prompt:      "classify {{ticket}}",
					OutputTypes: map[string]*Type{"confidence": NumberType()},
					Choose:      []string{"fast", "manual"},
				},
				Transitions: []Transition{{Case: "fast", Next: "approve"}, {Case: "manual", Next: "manual"}},
			},
			"approve": {ID: "approve", Type: "approval", RequirePrincipal: true,
				Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"manual": {ID: "manual", Type: "approval", RequirePrincipal: true,
				Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end": {ID: "end", Type: "end"},
		},
	}
}

// ---- 注册表:不可变 + 有序 + 内容寻址 hash ----

func TestM2a_RegistryImmutableOrderAndHash(t *testing.T) {
	reg := NewVersionRegistry()

	if _, err := reg.RegisterVersion(w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001", Note: "init"}); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if _, err := reg.RegisterVersion(w4OrderV2(), &VersionInfo{Proposer: "u-1002", Owner: "u-2001", Note: "add ai triage"}); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	// 同版本号不同内容 → 不可覆盖(注册表不可变)。
	mutated := w4OrderV1()
	mutated.Nodes["approve"].Type = "action"
	if _, err := reg.RegisterVersion(mutated, &VersionInfo{Proposer: "hacker", Owner: "hacker"}); err == nil {
		t.Fatal("conflicting content for same version must be rejected (immutability)")
	} else if !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("expected conflict error, got %v", err)
	}

	v1 := reg.GetVersion("order_flow", "1.0")
	v2 := reg.GetVersion("order_flow", "2.0")
	if v1 == nil || v2 == nil {
		t.Fatalf("both versions must be registered")
	}
	if v1.Hash == v2.Hash {
		t.Fatal("different defs must produce different hashes")
	}
	if v1.Info.Proposer != "u-1001" || v2.Info.Proposer != "u-1002" {
		t.Fatalf("attribution must be preserved: v1=%+v v2=%+v", v1.Info, v2.Info)
	}

	// 按 ASCII 版本号排序。
	versions := reg.Versions("order_flow")
	if len(versions) != 2 || versions[0].Version != "1.0" || versions[1].Version != "2.0" {
		t.Fatalf("registry must be ordered ascending, got %v", versions)
	}

	// 同一内容重复注册返回同一 hash(不改变注册表状态)。
	v1b, err := reg.RegisterVersion(w4OrderV1(), &VersionInfo{Proposer: "u-1001", Owner: "u-2001"})
	if err != nil || v1b.Hash != v1.Hash {
		t.Fatalf("idempotent re-register must succeed with same hash, got %v / %q", err, v1b.Hash)
	}
	if got := len(reg.Versions("order_flow")); got != 2 {
		t.Fatalf("re-registration must not add entries, got %d", got)
	}

	// 未注册版本返回 nil。
	if missing := reg.GetVersion("order_flow", "9.9"); missing != nil {
		t.Fatalf("unknown version must be nil, got %+v", missing)
	}
}

// ---- Diff:同输入必产出同 diff;空 diff 表示等价 ----

func TestM2a_Diff_SameDefProducesNoChanges(t *testing.T) {
	d1 := w4OrderV1()
	d2 := w4OrderV1()
	diff := Diff(d1, d2)
	if diff == nil || len(diff.Changes) != 0 {
		t.Fatalf("identical defs must produce zero changes, got %v", diff)
	}
}

// 结构性变化的 diff 内容必须按节点/迁移稳定排序,且可追溯。
func TestM2a_Diff_ContentChanges(t *testing.T) {
	diff := Diff(w4OrderV1(), w4OrderV2())
	if diff == nil {
		t.Fatal("expected non-nil diff")
	}
	joined := strings.Join(diff.Changes, "\n")

	expectedSubstrings := []string{
		"node \"triage\": added type=\"ai\"",
		"node \"manual\": added type=\"approval\"",
		"node \"approve\" field \"requirePrincipal\": \"false\" -> \"true\"",
		"node \"start\" edge retarget: event[0]=\"submit\" -> \"approve\" => \"triage\"",
	}
	for _, want := range expectedSubstrings {
		if !strings.Contains(joined, want) {
			t.Errorf("missing expected diff line: %s\n---\nfull diff:\n%s", want, joined)
		}
	}
}

// ---- 黄金文件:同输入必产出同 diff ----
func TestM2a_Diff_Golden(t *testing.T) {
	d1 := w4OrderV1()
	d2 := w4OrderV2()
	got := strings.Join(Diff(d1, d2).Changes, "\n")

	goldenPath := "testdata/w4_diff_golden.txt"
	if os.Getenv("GOLDEN_UPDATE") != "" {
		if err := os.WriteFile(goldenPath, []byte(got+"\n"), 0o644); err != nil {
			t.Fatalf("write golden: %v", err)
		}
		t.Log("golden updated; rerun without GOLDEN_UPDATE to verify")
		return
	}
	want, err := os.ReadFile(goldenPath)
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	if got != strings.TrimRight(string(want), "\n") {
		t.Fatalf("diff mismatch\n--- got ---\n%s\n--- want ---\n%s", got, string(want))
	}
}

// ExampleDiff 演示 W4 周五交付物:同一规则的两个版本之间的结构化 diff。
func ExampleDiff() {
	v1 := &ProcessDef{ID: "order_flow", Version: "1.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start":   {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval", Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":     {ID: "end", Type: "end"},
		}}
	v2 := &ProcessDef{ID: "order_flow", Version: "2.0", StartNode: "start",
		Nodes: map[string]*Node{
			"start":   {ID: "start", Type: "start", Transitions: []Transition{{Event: "submit", Next: "approve"}}},
			"approve": {ID: "approve", Type: "approval", RequirePrincipal: true, Transitions: []Transition{{Event: "approve", Next: "end"}}},
			"end":     {ID: "end", Type: "end"},
		}}
	for _, line := range Diff(v1, v2).Changes {
		fmt.Println(line)
	}
	// Output:
	// node "approve" field "requirePrincipal": "false" -> "true"
}
