package dsl

// evidence.go — M3b(W7):可离线自校验的证据包。
//
// 包同时提交(commit)四类数据:完整定义内容、注册版本元数据、定义级激活账、
// 实例级执行账。每条 occurrence 使用 canonical JSON + SHA256 形成 hash 链;
// 包根 hash 再绑定版本史、两条链的链头与条目数。因而修改、删除、插入或重排
// 任一条目,以及只改 proposer/owner 等版本元数据,都会使离线校验失败。
//
// 协议有显式版本号。canonical JSON 对对象键排序,数组保持原顺序,数字使用
// json.Number 保留精度;time.Time 沿用其标准 JSON 表示。VerifyEvidence 只读包内
// 数据,不依赖 Registry、Runtime、数据库或引擎内存状态。

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

const EvidenceFormatV1 = "weiyang-evidence/v1"

// EvidenceDefinition 是版本史中的一版:内容、内容地址与注册归属缺一不可。
type EvidenceDefinition struct {
	Definition *ProcessDef `json:"definition"`
	Hash       string      `json:"hash"`
	Info       VersionInfo `json:"info"`
}

// EvidenceEntry 把一笔事实接到所在账本的 hash 链上。
type EvidenceEntry struct {
	Occurrence Occurrence `json:"occurrence"`
	PrevHash   string     `json:"prevHash,omitempty"`
	Hash       string     `json:"hash"`
}

// EvidenceBundle 是一个定义版本史与一次实例执行的自校验证据包。
type EvidenceBundle struct {
	Format        string               `json:"format"`
	Definitions   []EvidenceDefinition `json:"definitions"`
	DefinitionLog []EvidenceEntry      `json:"definitionLog"`
	ExecutionLog  []EvidenceEntry      `json:"executionLog"`
	RootHash      string               `json:"rootHash"`
}

type evidenceLogSummary struct {
	Count int    `json:"count"`
	Head  string `json:"head"`
}

type evidenceRootPayload struct {
	Format        string               `json:"format"`
	Definitions   []EvidenceDefinition `json:"definitions"`
	DefinitionLog evidenceLogSummary   `json:"definitionLog"`
	ExecutionLog  evidenceLogSummary   `json:"executionLog"`
}

// ExportEvidence 导出证据包。definitions 应包含要交付的完整版本史;每版必须已
// 在 registry 注册,以复用 M2 的 proposer/owner 与内容身份。definitionLog 与
// executionLog 分账传入;导出器做深拷贝,调用方事后改对象不会改写证据包。
func ExportEvidence(definitions []*ProcessDef, registry *DefinitionRegistry, definitionLog, executionLog []Occurrence) (*EvidenceBundle, error) {
	if registry == nil {
		return nil, fmt.Errorf("export evidence: nil registry")
	}
	if len(definitions) == 0 {
		return nil, fmt.Errorf("export evidence: empty definition history")
	}

	history := make([]EvidenceDefinition, 0, len(definitions))
	for _, def := range definitions {
		if def == nil {
			return nil, fmt.Errorf("export evidence: nil definition")
		}
		entry := registry.GetVersion(def.ID, def.Version)
		if entry == nil {
			return nil, fmt.Errorf("export evidence: unregistered definition %s@%s", def.ID, def.Version)
		}
		clone, err := cloneEvidenceValue(def)
		if err != nil {
			return nil, fmt.Errorf("export evidence: clone %s@%s: %w", def.ID, def.Version, err)
		}
		hash, err := hashProcessDef(clone)
		if err != nil {
			return nil, fmt.Errorf("export evidence: hash %s@%s: %w", def.ID, def.Version, err)
		}
		history = append(history, EvidenceDefinition{Definition: clone, Hash: hash, Info: entry.Info})
	}
	sort.Slice(history, func(i, j int) bool {
		a, b := history[i].Definition, history[j].Definition
		if a.ID != b.ID {
			return a.ID < b.ID
		}
		return a.Version < b.Version
	})

	defChain, err := exportEvidenceChain(definitionLog)
	if err != nil {
		return nil, fmt.Errorf("export evidence definition log: %w", err)
	}
	execChain, err := exportEvidenceChain(executionLog)
	if err != nil {
		return nil, fmt.Errorf("export evidence execution log: %w", err)
	}
	bundle := &EvidenceBundle{
		Format:        EvidenceFormatV1,
		Definitions:   history,
		DefinitionLog: defChain,
		ExecutionLog:  execChain,
	}
	root, err := evidenceRootHash(bundle)
	if err != nil {
		return nil, fmt.Errorf("export evidence root: %w", err)
	}
	bundle.RootHash = root
	if err := VerifyEvidence(bundle); err != nil {
		return nil, fmt.Errorf("export evidence: %w", err)
	}
	return bundle, nil
}

// DecodeEvidence 解码 JSON 证据包并保留数字精度。调用方若直接用
// json.Unmarshal,interface{} 中的大整数会先变成 float64;离线协议应使用本入口。
func DecodeEvidence(data []byte) (*EvidenceBundle, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var bundle EvidenceBundle
	if err := dec.Decode(&bundle); err != nil {
		return nil, fmt.Errorf("decode evidence: %w", err)
	}
	var extra interface{}
	if err := dec.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("decode evidence: multiple JSON values")
		}
		return nil, fmt.Errorf("decode evidence trailing data: %w", err)
	}
	return &bundle, nil
}

// VerifyEvidence 重算定义 hash、两条 occurrence hash 链与包根 hash,并校验
// 分账边界、版本顺序、序号顺序和激活事实与定义内容的绑定。任何错误都表示
// 证据包不完整、格式不受支持或已被篡改。
func VerifyEvidence(bundle *EvidenceBundle) error {
	if bundle == nil {
		return fmt.Errorf("verify evidence: nil bundle")
	}
	if bundle.Format != EvidenceFormatV1 {
		return fmt.Errorf("verify evidence: unsupported format %q", bundle.Format)
	}
	if len(bundle.Definitions) == 0 {
		return fmt.Errorf("verify evidence: empty definition history")
	}

	known := make(map[registryKey]EvidenceDefinition, len(bundle.Definitions))
	previousKey := registryKey{}
	for i, version := range bundle.Definitions {
		def := version.Definition
		if def == nil || def.ID == "" || def.Version == "" {
			return fmt.Errorf("verify evidence: invalid definition at index %d", i)
		}
		key := registryKey{def.ID, def.Version}
		if i > 0 && (previousKey.id > key.id || (previousKey.id == key.id && previousKey.version >= key.version)) {
			return fmt.Errorf("verify evidence: definition history is not strictly ordered at %s@%s", key.id, key.version)
		}
		hash, err := hashProcessDef(def)
		if err != nil {
			return fmt.Errorf("verify evidence: hash %s@%s: %w", key.id, key.version, err)
		}
		if hash != version.Hash {
			return fmt.Errorf("verify evidence: definition hash mismatch at %s@%s", key.id, key.version)
		}
		known[key] = version
		previousKey = key
	}

	defHead, err := verifyEvidenceChain(bundle.DefinitionLog)
	if err != nil {
		return fmt.Errorf("verify evidence definition log: %w", err)
	}
	for i, entry := range bundle.DefinitionLog {
		occ := &entry.Occurrence
		if occ.Kind != OccDefActivated || occ.InstanceID != "" || occ.ExecutionID != "" {
			return fmt.Errorf("verify evidence definition log: invalid account boundary at entry %d", i)
		}
		key := registryKey{occ.DefID, occ.DefVersion}
		version, ok := known[key]
		if !ok || occ.Activation == nil {
			return fmt.Errorf("verify evidence definition log: unknown or incomplete activation at entry %d", i)
		}
		state := occ.Activation
		if state.DefID != key.id || state.Version != key.version || state.Hash != version.Hash ||
			state.Proposer != version.Info.Proposer || state.Owner != version.Info.Owner {
			return fmt.Errorf("verify evidence definition log: activation does not match definition at entry %d", i)
		}
	}

	execHead, err := verifyEvidenceChain(bundle.ExecutionLog)
	if err != nil {
		return fmt.Errorf("verify evidence execution log: %w", err)
	}
	if len(bundle.ExecutionLog) == 0 {
		return fmt.Errorf("verify evidence execution log: empty execution")
	}
	instanceID, executionID := "", ""
	started := false
	for i, entry := range bundle.ExecutionLog {
		occ := &entry.Occurrence
		if occ.Kind == OccDefActivated || occ.InstanceID == "" || occ.DefID != "" || occ.DefVersion != "" || occ.Activation != nil {
			return fmt.Errorf("verify evidence execution log: invalid account boundary at entry %d", i)
		}
		if i == 0 {
			instanceID, executionID = occ.InstanceID, occ.ExecutionID
		} else if occ.InstanceID != instanceID || occ.ExecutionID != executionID {
			return fmt.Errorf("verify evidence execution log: mixed executions at entry %d", i)
		}
		if occ.Kind == OccStarted {
			started = true
		}
	}
	if !started {
		return fmt.Errorf("verify evidence execution log: missing started fact")
	}

	root, err := evidenceRootHash(bundle)
	if err != nil {
		return fmt.Errorf("verify evidence root: %w", err)
	}
	if root != bundle.RootHash || defHead == "" && len(bundle.DefinitionLog) != 0 || execHead == "" {
		return fmt.Errorf("verify evidence: root hash mismatch")
	}
	return nil
}

func exportEvidenceChain(occs []Occurrence) ([]EvidenceEntry, error) {
	chain := make([]EvidenceEntry, 0, len(occs))
	previous := ""
	for i := range occs {
		clone, err := cloneEvidenceValue(occs[i])
		if err != nil {
			return nil, fmt.Errorf("clone entry %d: %w", i, err)
		}
		hash, err := hashEvidenceEntry(previous, clone)
		if err != nil {
			return nil, fmt.Errorf("hash entry %d: %w", i, err)
		}
		chain = append(chain, EvidenceEntry{Occurrence: clone, PrevHash: previous, Hash: hash})
		previous = hash
	}
	return chain, nil
}

func verifyEvidenceChain(chain []EvidenceEntry) (string, error) {
	previous := ""
	var previousSeq int64
	for i, entry := range chain {
		if entry.PrevHash != previous {
			return "", fmt.Errorf("previous hash mismatch at entry %d", i)
		}
		if i > 0 && entry.Occurrence.Seq <= previousSeq {
			return "", fmt.Errorf("non-increasing occurrence sequence at entry %d", i)
		}
		hash, err := hashEvidenceEntry(previous, entry.Occurrence)
		if err != nil {
			return "", fmt.Errorf("hash entry %d: %w", i, err)
		}
		if hash != entry.Hash {
			return "", fmt.Errorf("entry hash mismatch at entry %d", i)
		}
		previous = hash
		previousSeq = entry.Occurrence.Seq
	}
	return previous, nil
}

func hashEvidenceEntry(previous string, occ Occurrence) (string, error) {
	return canonicalHash(struct {
		Domain     string     `json:"domain"`
		Previous   string     `json:"previous"`
		Occurrence Occurrence `json:"occurrence"`
	}{Domain: EvidenceFormatV1 + "/occurrence", Previous: previous, Occurrence: occ})
}

func evidenceRootHash(bundle *EvidenceBundle) (string, error) {
	return canonicalHash(evidenceRootPayload{
		Format:        bundle.Format,
		Definitions:   bundle.Definitions,
		DefinitionLog: evidenceLogSummary{Count: len(bundle.DefinitionLog), Head: chainHead(bundle.DefinitionLog)},
		ExecutionLog:  evidenceLogSummary{Count: len(bundle.ExecutionLog), Head: chainHead(bundle.ExecutionLog)},
	})
}

func chainHead(chain []EvidenceEntry) string {
	if len(chain) == 0 {
		return ""
	}
	return chain[len(chain)-1].Hash
}

func canonicalHash(v interface{}) (string, error) {
	data, err := canonicalBytes(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func cloneEvidenceValue[T any](value T) (T, error) {
	var zero T
	raw, err := json.Marshal(value)
	if err != nil {
		return zero, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var clone T
	if err := dec.Decode(&clone); err != nil {
		return zero, err
	}
	return clone, nil
}
