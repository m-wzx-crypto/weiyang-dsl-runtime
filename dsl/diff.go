package dsl

import (
	"fmt"
	"sort"
	"strings"
)

// diff.go — M2(Attributed Rules)W4b:结构化 diff。
//
// 目标:对 ProcessDef 按节点/迁移计算变化,输出**稳定排序**的行列表。
// 输出被黄金文件锁死:同输入必产出同 diff——黄金文件 diff 不符即红灯。

// ProcessDefDiff 是两个定义之间的结构化差异。
type ProcessDefDiff struct {
	FromID      string
	FromVersion string
	ToID        string
	ToVersion   string
	// Changes 按 node → field 稳定排序的可读行。
	Changes []string
}

// Diff 计算 a→b 的结构差异。b 为 nil 表示"删除整个定义";a 为 nil 表示"新增"。
func Diff(a, b *ProcessDef) *ProcessDefDiff {
	d := &ProcessDefDiff{}
	if a != nil {
		d.FromID = a.ID
		d.FromVersion = a.Version
	}
	if b != nil {
		d.ToID = b.ID
		d.ToVersion = b.Version
	}

	switch {
	case a == nil && b == nil:
		return d
	case a == nil:
		d.appendf("definition %q@%q: created", b.ID, b.Version)
		for _, id := range sortedKeys(b.Nodes) {
			describeNodeDiff(d, b, id, nil, "+")
		}
		return d
	case b == nil:
		d.appendf("definition %q@%q: removed", a.ID, a.Version)
		return d
	}

	if a.StartNode != b.StartNode {
		d.appendf("start_node: %q -> %q", a.StartNode, b.StartNode)
	}

	all := unionNodeIDs(a.Nodes, b.Nodes)
	for _, id := range all {
		na, nb := a.Nodes[id], b.Nodes[id]
		switch {
		case na == nil && nb == nil:
			// unreachable
		case na == nil:
			describeNodeDiff(d, b, id, nil, "+")
		case nb == nil:
			describeNodeDiff(d, a, id, nil, "-")
		default:
			diffNodeFields(d, id, na, nb)
			diffNodeEdges(d, id, na, nb)
		}
	}
	return d
}

// ---- 节点字段级 ----

var w4NodeFieldOrder = []struct {
	name string
	fn   func(*Node) string
}{
	{"type", func(n *Node) string { return n.Type }},
	{"label", func(n *Node) string { return n.Label }},
	{"requirePrincipal", func(n *Node) string { return boolStr(n.RequirePrincipal) }},
	{"duration", func(n *Node) string { return n.Duration }},
	{"deadline.after", func(n *Node) string {
		if n.Deadline == nil {
			return ""
		}
		return n.Deadline.After
	}},
	{"deadline.next", func(n *Node) string {
		if n.Deadline == nil {
			return ""
		}
		return n.Deadline.Next
	}},
	{"ai.prompt", func(n *Node) string {
		if n.Ai == nil {
			return ""
		}
		return n.Ai.Prompt
	}},
	{"ai.choose", func(n *Node) string {
		if n.Ai == nil {
			return ""
		}
		return strings.Join(n.Ai.Choose, ",")
	}},
	{"ai.onError", func(n *Node) string {
		if n.Ai == nil {
			return ""
		}
		return n.Ai.OnError
	}},
	{"archetype", func(n *Node) string { return "" }},
}

func diffNodeFields(d *ProcessDefDiff, id string, a, b *Node) {
	for _, f := range w4NodeFieldOrder {
		va, vb := f.fn(a), f.fn(b)
		if va != vb {
			d.appendf("node %q field %q: %s -> %s", id, f.name, displayNodeVal(va), displayNodeVal(vb))
		}
	}
}

func displayNodeVal(s string) string {
	if s == "" {
		return "∅"
	}
	return fmt.Sprintf("%q", s)
}

func describeNodeDiff(d *ProcessDefDiff, def *ProcessDef, id string, _ *Node, sign string) {
	n := def.Nodes[id]
	switch sign {
	case "+":
		d.appendf("node %q: added type=%q", id, n.Type)
	case "-":
		d.appendf("node %q: removed type=%q", id, n.Type)
	}
}

// ---- 迁移边级 ----

func diffNodeEdges(d *ProcessDefDiff, id string, a, b *Node) {
	aKeys := edgeMap(a.Transitions)
	bKeys := edgeMap(b.Transitions)

	for _, k := range sortedKeys(aKeys) {
		na := aKeys[k]
		nb, ok := bKeys[k]
		if !ok {
			d.appendf("node %q edge removed: %s -> %q", id, k, na)
			continue
		}
		if na != nb {
			d.appendf("node %q edge retarget: %s -> %q => %q", id, k, na, nb)
		}
	}
	for _, k := range sortedKeys(bKeys) {
		if _, ok := aKeys[k]; !ok {
			d.appendf("node %q edge added: %s -> %q", id, k, bKeys[k])
		}
	}
}

func edgeMap(trs []Transition) map[string]string {
	m := make(map[string]string, len(trs))
	for i, t := range trs {
		key := edgeKey(i, t)
		m[key] = t.Next
	}
	return m
}

func edgeKey(idx int, t Transition) string {
	switch {
	case t.Event != "":
		return fmt.Sprintf("event[%d]=%q", idx, t.Event)
	case t.When != "":
		return fmt.Sprintf("when[%d]=%q", idx, t.When)
	case t.Case != "":
		return fmt.Sprintf("case[%d]=%q", idx, t.Case)
	default:
		return fmt.Sprintf("auto[%d]", idx)
	}
}

// ---- helpers ----

func (d *ProcessDefDiff) appendf(format string, args ...interface{}) {
	d.Changes = append(d.Changes, fmt.Sprintf(format, args...))
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func unionNodeIDs(a, b map[string]*Node) []string {
	set := map[string]bool{}
	for id := range a {
		set[id] = true
	}
	for id := range b {
		set[id] = true
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}
