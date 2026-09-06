package dsl

import (
	"errors"
	"sort"
	"sync"
	"time"
)

// store.go 实现阶段 2:持久化 SPI。
//
// 内核只定义接口与内存参考实现,保持零外部依赖;持久化实现(Postgres 等)
// 放独立 module(见 store-postgres/)。三块拼图:
//
//	Journal        事实日志(追加)          —— journal.go,阶段 1
//	JournalReader  事实日志(读取)          —— 本文件
//	InstanceStore  实例元数据(状态/唤醒时间) —— 本文件
//	Manager        三者之上的运行时门面      —— manager.go
//
// 实例的完整状态永远可以从 Journal 折叠重建(阶段 1 保证),InstanceStore 只是
// 可查询的"索引/摘要":调度扫描按 WakeUpAt 找到该唤醒的实例,观测按 Status
// 检索,真正的状态以 fold(Journal) 为准——存储损坏可随时由日志重建。

// JournalReader 读取实例的事实日志。持久化 Journal 实现应同时实现
// Journal 与 JournalReader。
type JournalReader interface {
	LoadOccurrences(instanceID string) ([]Occurrence, error)
}

// InstanceRecord 是实例在存储中的摘要行(索引,不是状态本体)。
type InstanceRecord struct {
	InstanceID   string
	DefinitionID string
	TenantID     string
	Status       string // ExecutionStatus 字符串
	CurrentNode  string
	// WakeUpAt 是下次需要唤醒的时刻(timer/deadline/scope 超时),无等待槽为 nil。
	WakeUpAt  *time.Time
	CreatedAt time.Time
	UpdatedAt time.Time
}

// 持久化 SPI 的哨兵错误:宿主可用 errors.Is 判别。
var (
	ErrInstanceNotFound   = errors.New("instance not found")
	ErrInstanceExists     = errors.New("instance already exists")
	ErrDefinitionUnknown  = errors.New("unknown process definition")
	ErrJournalNotReadable = errors.New("journal does not implement JournalReader")
	ErrSnapshotNotFound   = errors.New("snapshot not found")
)

// InstanceStore 持久化实例摘要。实现方需保证跨实例并发安全;同一实例的调用
// 由 Manager 串行化(与 Runtime 并发契约一致)。
type InstanceStore interface {
	// PutInstance 创建或更新实例摘要(upsert 语义)。
	PutInstance(rec InstanceRecord) error
	// CreateInstance 原子创建实例摘要:已存在时返回 ErrInstanceExists,
	// 不覆盖。Manager.Start 依赖它的原子性消除"先查后写"的 TOCTOU 竞态
	// (并发同 ID 启动只允许一个成功)。
	CreateInstance(rec InstanceRecord) error
	// GetInstance 返回实例摘要;不存在时返回 ErrInstanceNotFound。
	GetInstance(instanceID string) (*InstanceRecord, error)
	// ListWakeable 返回 WakeUpAt <= now 且仍在等待的实例(调度扫描入口),
	// 按 WakeUpAt 升序,最多 limit 条。
	ListWakeable(now time.Time, limit int) ([]InstanceRecord, error)
	// ListByStatus 按状态检索实例(观测/管理),最多 limit 条。
	ListByStatus(status string, limit int) ([]InstanceRecord, error)
}

// InstanceSnapshot 是实例上下文的一份持久化快照:状态 = 快照 + 其后增量日志,
// Manager 据此避免每次操作全量折叠长日志(日志仍是真相,快照损坏可随时弃用)。
type InstanceSnapshot struct {
	InstanceID string
	// UptoSeq 是快照已折叠到的日志序号(含);恢复时只重放 Seq 更大的事实。
	UptoSeq   int64
	Data      []byte // ExecutionContext 的 Savepoint JSON
	CreatedAt time.Time
}

// SnapshotStore 持久化实例快照(可选 SPI)。nil 时 Manager 退化为每次全量折叠。
type SnapshotStore interface {
	// PutSnapshot 保存快照(同实例覆盖旧快照即可——最新一份足够恢复)。
	PutSnapshot(snap InstanceSnapshot) error
	// LatestSnapshot 返回最新快照;不存在时返回 ErrSnapshotNotFound。
	LatestSnapshot(instanceID string) (*InstanceSnapshot, error)
}

// MemoryInstanceStore 是进程内的参考实现(测试与单机嵌入)。
type MemoryInstanceStore struct {
	mu   sync.RWMutex
	recs map[string]InstanceRecord
	// snaps 是 SnapshotStore 的内存实现(同对象双能力,测试与单机够用)。
	snaps map[string]InstanceSnapshot
}

func NewMemoryInstanceStore() *MemoryInstanceStore {
	return &MemoryInstanceStore{
		recs:  map[string]InstanceRecord{},
		snaps: map[string]InstanceSnapshot{},
	}
}

func (s *MemoryInstanceStore) PutInstance(rec InstanceRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.recs[rec.InstanceID] = rec
	return nil
}

// CreateInstance 原子创建实例摘要:已存在时返回 ErrInstanceExists,不覆盖。
func (s *MemoryInstanceStore) CreateInstance(rec InstanceRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.recs[rec.InstanceID]; exists {
		return ErrInstanceExists
	}
	s.recs[rec.InstanceID] = rec
	return nil
}

// PutSnapshot 保存实例快照(覆盖旧快照)。SnapshotStore 实现。
func (s *MemoryInstanceStore) PutSnapshot(snap InstanceSnapshot) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps[snap.InstanceID] = snap
	return nil
}

// LatestSnapshot 返回最新快照;不存在时返回 ErrSnapshotNotFound。SnapshotStore 实现。
func (s *MemoryInstanceStore) LatestSnapshot(instanceID string) (*InstanceSnapshot, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if snap, ok := s.snaps[instanceID]; ok {
		cp := snap
		return &cp, nil
	}
	return nil, ErrSnapshotNotFound
}

func (s *MemoryInstanceStore) GetInstance(instanceID string) (*InstanceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rec, ok := s.recs[instanceID]; ok {
		cp := rec
		return &cp, nil
	}
	return nil, ErrInstanceNotFound
}

func (s *MemoryInstanceStore) ListWakeable(now time.Time, limit int) ([]InstanceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.filter(func(r InstanceRecord) bool {
		return r.Status == StatusWaiting.String() &&
			r.WakeUpAt != nil && !r.WakeUpAt.After(now)
	}, limit)
}

func (s *MemoryInstanceStore) ListByStatus(status string, limit int) ([]InstanceRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.filter(func(r InstanceRecord) bool { return r.Status == status }, limit)
}

func (s *MemoryInstanceStore) filter(pred func(InstanceRecord) bool, limit int) ([]InstanceRecord, error) {
	var out []InstanceRecord
	for _, r := range s.recs {
		if pred(r) {
			out = append(out, r)
			if limit > 0 && len(out) >= limit {
				break
			}
		}
	}
	// 稳定排序便于测试与观测:按 InstanceID 字典序。
	sort.Slice(out, func(i, j int) bool { return out[i].InstanceID < out[j].InstanceID })
	return out, nil
}
