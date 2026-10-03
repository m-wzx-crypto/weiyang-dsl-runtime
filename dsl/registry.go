package dsl

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// registry.go — M2(Attributed Rules)W4a:版本注册表。
//
// 规则变更被当成一次"版本注册":同一 ProcessDef+Version 只能注册一次——
// 覆盖视为非法(不可变)。每版带 proposer + accountable owner,以 hash 为
// 内容寻址锚点。定义序列化必须先做规范化(键排序),否则 Go map 的随机遍历
// 会让同一内容产生不同 hash。

// VersionInfo 是一次注册的版本元数据。
type VersionInfo struct {
	// Proposer 是变更提出人(必填)。
	Proposer string `json:"proposer,omitempty"`
	// Owner 是可问责的责任人(必填)。
	Owner string `json:"owner,omitempty"`
	// Note 是可选的变更说明。
	Note string `json:"note,omitempty"`

	// RegisteredAt 由注册表设置。
	RegisteredAt time.Time `json:"registeredAt,omitempty"`
}

// VersionEntry 是注册表中一行。
type VersionEntry struct {
	// ID/Version 是复合键。
	ID      string
	Version string
	// Hash 是规范化序列化的内容地址(sha256 前 32 字节 hex)。
	Hash string
	// Info 是注册时声明的元数据副本。
	Info VersionInfo
}

// registryKey 组合定义 ID 与版本。
type registryKey struct{ id, version string }

// DefinitionRegistry 是版本注册表:key 为 (id, version),按 ASCII
// 版本号排序。
// 不变性:已存在的 (id,version) 注册被视为冲突,不允许静默覆盖。
type DefinitionRegistry struct {
	mu       sync.RWMutex
	byID     map[string]map[string]*VersionEntry
	idOrders map[string][]string // 已排序的 version 列表,缓存
}

// NewVersionRegistry 创建空的版本注册表。
func NewVersionRegistry() *DefinitionRegistry {
	return &DefinitionRegistry{
		byID:     make(map[string]map[string]*VersionEntry),
		idOrders: make(map[string][]string),
	}
}

// RegisterVersion 注册一个定义版本。同一 (id, version) 已注册即返回错误。
// Hash 由规范化(canonical-key-sorted)JSON 计算。
func (r *DefinitionRegistry) RegisterVersion(def *ProcessDef, info *VersionInfo) (*VersionEntry, error) {
	if def == nil {
		return nil, fmt.Errorf("nil def")
	}
	if def.ID == "" {
		return nil, fmt.Errorf("def ID is required")
	}
	if def.Version == "" {
		return nil, fmt.Errorf("def version is required")
	}

	hash, err := hashProcessDef(def)
	if err != nil {
		return nil, fmt.Errorf("hash def: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	byVersion, ok := r.byID[def.ID]
	if !ok {
		byVersion = make(map[string]*VersionEntry)
		r.byID[def.ID] = byVersion
	}
	if existing, exists := byVersion[def.Version]; exists {
		// 幂等语义:同 (id, version) 同内容 → 返回已注册条目;
		// 同键不同内容 → 不可覆盖(注册表不可变)。
		if existing.Hash != hash {
			return nil, fmt.Errorf("registry conflict: %s@%s already registered with different content (hash %s)",
				def.ID, def.Version, existing.Hash)
		}
		return existing, nil
	}

	entry := &VersionEntry{
		ID:      def.ID,
		Version: def.Version,
		Hash:    hash,
	}
	if info != nil {
		cp := *info
		cp.RegisteredAt = time.Now()
		entry.Info = cp
	} else {
		entry.Info = VersionInfo{RegisteredAt: time.Now()}
	}
	byVersion[def.Version] = entry

	// 更新排序缓存。
	versions := r.idOrders[def.ID]
	versions = append(versions, def.Version)
	sort.Strings(versions)
	r.idOrders[def.ID] = versions

	return entry, nil
}

// GetVersion 取指定版本的注册信息;不存在返回 nil。
func (r *DefinitionRegistry) GetVersion(id, version string) *VersionEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byID[id][version]
}

// Versions 按 ASCII 版本号排序返回所有注册条目。
func (r *DefinitionRegistry) Versions(id string) []*VersionEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	byVersion := r.byID[id]
	versions := r.idOrders[id]
	out := make([]*VersionEntry, 0, len(versions))
	for _, v := range versions {
		out = append(out, byVersion[v])
	}
	return out
}

// hashProcessDef 计算规范化 JSON 的 SHA256。
// 规范化规则:所有 map 键按字典序重排后再序列化——Go map 遍历无序,
// 不排序则同一内容会产生不同 hash。
func hashProcessDef(def *ProcessDef) (string, error) {
	// 先走标准序列化,再解码为无序 map,最后按键排序重写。
	raw, err := json.Marshal(def)
	if err != nil {
		return "", err
	}
	var v interface{}
	if err := json.Unmarshal(raw, &v); err != nil {
		return "", err
	}
	data, err := canonicalJSON(v)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:32], nil
}

// canonicalJSON 把任意解码后的 JSON 值按键排序序列化。
func canonicalJSON(v interface{}) ([]byte, error) {
	switch t := v.(type) {
	case map[string]interface{}:
		keys := sortedKeys(t)
		var b strings.Builder
		b.WriteByte('{')
		for i, k := range keys {
			if i > 0 {
				b.WriteByte(',')
			}
			kb, _ := json.Marshal(k)
			vb, err := canonicalJSON(t[k])
			if err != nil {
				return nil, err
			}
			b.Write(kb)
			b.WriteByte(':')
			b.Write(vb)
		}
		b.WriteByte('}')
		return []byte(b.String()), nil
	case []interface{}:
		var b strings.Builder
		b.WriteByte('[')
		for i, e := range t {
			if i > 0 {
				b.WriteByte(',')
			}
			eb, err := canonicalJSON(e)
			if err != nil {
				return nil, err
			}
			b.Write(eb)
		}
		b.WriteByte(']')
		return []byte(b.String()), nil
	default:
		return json.Marshal(v)
	}
}
