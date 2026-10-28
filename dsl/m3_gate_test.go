package dsl

// m3_gate_test.go — M3b(W7)与 ROADMAP M3 门禁。
//
// gate:导出的证据包能脱离 Registry/Runtime 自校验;篡改任一定义、版本元数据、
// 定义账或实例账条目均被检出。canonical JSON 另有重复序列化稳定性与大整数
// 不碰撞测试,防止 Go map 遍历顺序或 float64 精度破坏证据链。

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
)

func m3EvidenceFixture(t *testing.T) (*DefinitionRegistry, []*ProcessDef, []Occurrence, []Occurrence) {
	t.Helper()
	journal := NewMemoryJournal()
	registry := NewVersionRegistry()
	v1, v2 := w4OrderV1(), w4OrderV2()
	if _, err := registry.RegisterVersion(v1, &VersionInfo{Proposer: "u-proposer", Owner: "u-owner", Note: "initial"}); err != nil {
		t.Fatalf("register v1: %v", err)
	}
	if _, err := registry.RegisterVersion(v2, &VersionInfo{Proposer: "u-proposer-2", Owner: "u-owner", Note: "ai"}); err != nil {
		t.Fatalf("register v2: %v", err)
	}
	if _, err := NewActivator(registry, journal).Activate(v1.ID, v1.Version, w5Human("u-approver")); err != nil {
		t.Fatalf("activate: %v", err)
	}
	r := NewRuntime(v1, NewInMemorySideEffectExecutor(), WithJournal(journal))
	if res := r.Start("inst-evidence", "exec-evidence", map[string]interface{}{
		"large": int64(9007199254740993), // 2^53+1:证据协议不得经 float64 丢失精度
	}, Event{ID: "submit-evidence", Name: "submit", Principal: w5Human("u-starter")}); res.HasErrors() {
		t.Fatalf("start: %v", res.Errors)
	}

	var definitionLog, executionLog []Occurrence
	for _, occ := range journal.Occurrences() {
		if occ.InstanceID == "" {
			definitionLog = append(definitionLog, occ)
		} else {
			executionLog = append(executionLog, occ)
		}
	}
	// 故意逆序传入,导出器应产出稳定排序的版本史。
	return registry, []*ProcessDef{v2, v1}, definitionLog, executionLog
}

func TestM3Gate_ExportedBundleVerifiesOffline(t *testing.T) {
	registry, defs, definitionLog, executionLog := m3EvidenceFixture(t)
	bundle, err := ExportEvidence(defs, registry, definitionLog, executionLog)
	if err != nil {
		t.Fatalf("ExportEvidence: %v", err)
	}
	if bundle.Format != EvidenceFormatV1 || len(bundle.Definitions) != 2 {
		t.Fatalf("bundle header/history = %q/%d", bundle.Format, len(bundle.Definitions))
	}
	if bundle.Definitions[0].Definition.Version != "1.0" || bundle.Definitions[1].Definition.Version != "2.0" {
		t.Fatalf("version history must be stably ordered: %+v", bundle.Definitions)
	}
	if len(bundle.DefinitionLog) != 1 || len(bundle.ExecutionLog) == 0 || bundle.RootHash == "" {
		t.Fatalf("incomplete bundle: definition=%d execution=%d root=%q",
			len(bundle.DefinitionLog), len(bundle.ExecutionLog), bundle.RootHash)
	}

	// 离线口径:不访问 Registry、Runtime 或原日志,仅使用包内数据。
	if err := VerifyEvidence(bundle); err != nil {
		t.Fatalf("VerifyEvidence: %v", err)
	}
	again, err := ExportEvidence(defs, registry, definitionLog, executionLog)
	if err != nil {
		t.Fatalf("second ExportEvidence: %v", err)
	}
	if again.RootHash != bundle.RootHash {
		t.Fatalf("same input must produce same root: %s != %s", again.RootHash, bundle.RootHash)
	}

	// JSON 往返仍须通过,且大整数在解码中不得经 float64 被改写。
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal bundle: %v", err)
	}
	decoded, err := DecodeEvidence(raw)
	if err != nil {
		t.Fatalf("DecodeEvidence: %v", err)
	}
	if err := VerifyEvidence(decoded); err != nil {
		t.Fatalf("VerifyEvidence after JSON round-trip: %v", err)
	}
	if decoded.RootHash != bundle.RootHash {
		t.Fatalf("JSON round-trip changed root: %s != %s", decoded.RootHash, bundle.RootHash)
	}
}

func TestM3Gate_TamperingAnyEntryIsDetected(t *testing.T) {
	registry, defs, definitionLog, executionLog := m3EvidenceFixture(t)
	bundle, err := ExportEvidence(defs, registry, definitionLog, executionLog)
	if err != nil {
		t.Fatalf("ExportEvidence: %v", err)
	}
	raw, err := json.Marshal(bundle)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	mutations := []struct {
		name string
		fn   func(*EvidenceBundle)
	}{
		{"definition content", func(b *EvidenceBundle) { b.Definitions[0].Definition.Name = "tampered" }},
		{"version metadata", func(b *EvidenceBundle) { b.Definitions[0].Info.Owner = "hacker" }},
		{"definition fact", func(b *EvidenceBundle) { b.DefinitionLog[0].Occurrence.Activation.Hash = "forged" }},
		{"execution attribution", func(b *EvidenceBundle) {
			for i := range b.ExecutionLog {
				if b.ExecutionLog[i].Occurrence.Actor != nil {
					b.ExecutionLog[i].Occurrence.Actor.ID = "hacker"
					return
				}
			}
			panic("fixture has no actor")
		}},
		{"entry deletion", func(b *EvidenceBundle) { b.ExecutionLog = b.ExecutionLog[1:] }},
		{"entry reorder", func(b *EvidenceBundle) {
			b.ExecutionLog[0], b.ExecutionLog[1] = b.ExecutionLog[1], b.ExecutionLog[0]
		}},
		{"entry hash", func(b *EvidenceBundle) { b.ExecutionLog[0].Hash = "forged" }},
		{"root hash", func(b *EvidenceBundle) { b.RootHash = "forged" }},
	}
	for _, tc := range mutations {
		t.Run(tc.name, func(t *testing.T) {
			clone, err := DecodeEvidence(raw)
			if err != nil {
				t.Fatalf("clone: %v", err)
			}
			tc.fn(clone)
			if err := VerifyEvidence(clone); err == nil {
				t.Fatal("tampered bundle must fail offline verification")
			}
		})
	}
}

func TestM3_CanonicalJSONStableAndPreservesLargeIntegers(t *testing.T) {
	for seed := int64(0); seed < 100; seed++ {
		rng := rand.New(rand.NewSource(seed))
		value := map[string]interface{}{
			"z": rng.Int63(),
			"a": []interface{}{rng.Intn(100), fmt.Sprintf("seed-%d", seed), map[string]interface{}{
				"nested-b": rng.Float64(), "nested-a": rng.Int63(),
			}},
			"m": map[string]interface{}{"k3": true, "k1": nil, "k2": rng.Intn(1000)},
		}
		first, err := canonicalBytes(value)
		if err != nil {
			t.Fatalf("seed %d canonicalBytes: %v", seed, err)
		}
		for run := 0; run < 20; run++ {
			got, err := canonicalBytes(value)
			if err != nil {
				t.Fatalf("seed %d run %d: %v", seed, run, err)
			}
			if string(got) != string(first) {
				t.Fatalf("seed %d run %d: canonical JSON unstable\n%s\n%s", seed, run, first, got)
			}
		}
	}

	a := Occurrence{Seq: 1, Kind: OccVariableSet, VarKey: "n", VarValue: int64(9007199254740992)}
	b := a
	b.VarValue = int64(9007199254740993)
	ha, err := hashEvidenceEntry("", a)
	if err != nil {
		t.Fatalf("hash A: %v", err)
	}
	hb, err := hashEvidenceEntry("", b)
	if err != nil {
		t.Fatalf("hash B: %v", err)
	}
	if ha == hb {
		t.Fatal("distinct integers above 2^53 must not collide through float64 normalization")
	}
}
