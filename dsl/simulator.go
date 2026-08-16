package dsl

import "fmt"

const MaxPathLength = 100

type PathResult struct {
	PathIndex     int
	NodeSequence  []string
	EventSequence []string
	IsComplete    bool
	IsAbnormal    bool
}

type SimulationResult struct {
	Paths          []PathResult
	CyclesDetected [][]string
}

func Simulate(def *ProcessDef) SimulationResult {
	result := SimulationResult{}

	if def.StartNode == "" {
		return result
	}

	type bfsState struct {
		currentNode string
		nodeSeq     []string
		eventSeq    []string
		visited     map[string]int
	}

	initialVisited := map[string]int{def.StartNode: 1}
	queue := []bfsState{{
		currentNode: def.StartNode,
		nodeSeq:     []string{def.StartNode},
		eventSeq:    []string{},
		visited:     initialVisited,
	}}

	pathIndex := 0

	for len(queue) > 0 {
		state := queue[0]
		queue = queue[1:]

		node, ok := def.Nodes[state.currentNode]
		if !ok {
			continue
		}

		if node.Type == "end" {
			result.Paths = append(result.Paths, PathResult{
				PathIndex:     pathIndex,
				NodeSequence:  state.nodeSeq,
				EventSequence: state.eventSeq,
				IsComplete:    true,
				IsAbnormal:    false,
			})
			pathIndex++
			continue
		}

		if len(node.Transitions) == 0 {
			result.Paths = append(result.Paths, PathResult{
				PathIndex:     pathIndex,
				NodeSequence:  state.nodeSeq,
				EventSequence: state.eventSeq,
				IsComplete:    false,
				IsAbnormal:    false,
			})
			pathIndex++
			continue
		}

		if len(state.nodeSeq) >= MaxPathLength {
			result.Paths = append(result.Paths, PathResult{
				PathIndex:     pathIndex,
				NodeSequence:  state.nodeSeq,
				EventSequence: state.eventSeq,
				IsComplete:    false,
				IsAbnormal:    true,
			})
			pathIndex++
			continue
		}

		for _, tr := range node.Transitions {
			if tr.Next == "" {
				result.Paths = append(result.Paths, PathResult{
					PathIndex:     pathIndex,
					NodeSequence:  state.nodeSeq,
					EventSequence: append(state.eventSeq, tr.Event),
					IsComplete:    false,
					IsAbnormal:    false,
				})
				pathIndex++
				continue
			}

			nextNode := def.Nodes[tr.Next]
			if nextNode == nil {
				continue
			}

			newVisited := make(map[string]int, len(state.visited))
			for k, v := range state.visited {
				newVisited[k] = v
			}
			newVisited[tr.Next]++

			if newVisited[tr.Next] > 2 {
				cycle := detectCycle(state.nodeSeq, tr.Next)
				if cycle != nil {
					result.CyclesDetected = append(result.CyclesDetected, cycle)
				}
				result.Paths = append(result.Paths, PathResult{
					PathIndex:     pathIndex,
					NodeSequence:  append(copySlice(state.nodeSeq), tr.Next),
					EventSequence: append(copySlice(state.eventSeq), tr.Event),
					IsComplete:    false,
					IsAbnormal:    true,
				})
				pathIndex++
				continue
			}

			newNodeSeq := make([]string, len(state.nodeSeq)+1)
			copy(newNodeSeq, state.nodeSeq)
			newNodeSeq[len(state.nodeSeq)] = tr.Next

			newEventSeq := make([]string, len(state.eventSeq)+1)
			copy(newEventSeq, state.eventSeq)
			newEventSeq[len(state.eventSeq)] = tr.Event

			queue = append(queue, bfsState{
				currentNode: tr.Next,
				nodeSeq:     newNodeSeq,
				eventSeq:    newEventSeq,
				visited:     newVisited,
			})
		}
	}

	return result
}

func detectCycle(path []string, repeatedNode string) []string {
	start := -1
	for i, n := range path {
		if n == repeatedNode {
			start = i
			break
		}
	}
	if start == -1 {
		return nil
	}
	cycle := make([]string, len(path)-start+1)
	copy(cycle, path[start:])
	cycle[len(cycle)-1] = repeatedNode
	return cycle
}

func copySlice(s []string) []string {
	c := make([]string, len(s))
	copy(c, s)
	return c
}

// FormatPath formats a path result for display.
func FormatPath(p PathResult) string {
	status := "incomplete"
	if p.IsComplete {
		status = "complete"
	}
	if p.IsAbnormal {
		status = "abnormal"
	}
	return fmt.Sprintf("Path#%d [%s]: %v (events: %v)", p.PathIndex, status, p.NodeSequence, p.EventSequence)
}
