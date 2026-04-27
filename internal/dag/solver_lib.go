package dag

import (
	"fmt"
	"iter"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
)

type lookback string

const CountLookback lookback = "CountLookback"
const TimedeltaLookback lookback = "TimedeltaLookback"

type Lookback struct {
	Count        int
	Timedelta    int
	GapCount     int
	GapTimedelta int
}

type AlgoDep struct {
	AlgoId   int64
	Lookback Lookback
}

func (d AlgoDep) NeedsLookback() bool {
	return d.Lookback.Count > 0 || d.Lookback.Timedelta > 0
}

func (d AlgoDep) LookbackType() (error, lookback) {
	if d.Lookback.Count > 0 {
		return nil, CountLookback
	}
	if d.Lookback.Timedelta > 0 {
		return nil, TimedeltaLookback
	}
	return fmt.Errorf("algodep with id %d does not require lookback", d.AlgoId), ""
}

// Node represents an algorithm in the DAG
type Node struct {
	id           int64
	algoId       int64
	procId       int64
	windowId     int64
	algoDeps     []AlgoDep
	selfLookback Lookback
	pathIdx      int
}

// ID satisfies the graph.Node interface.
func (n Node) ID() int64 {
	return n.id
}

func (n Node) AlgoId() int64 {
	return n.algoId
}
func (n Node) AlgoDeps() iter.Seq[AlgoDep] {
	return func(yield func(AlgoDep) bool) {
		for _, dep := range n.algoDeps {
			if !yield(dep) {
				return
			}
		}
	}
}
func (n Node) LenAlgoDeps() int {
	return len(n.algoDeps)
}

func (n Node) SelfLookback() Lookback {
	return n.selfLookback
}

// ProcessorTask represents a set of tasks (nodes) assigned to a single processor
type ProcessorTask struct {
	ProcId int64
	Nodes  []Node
}

// Stage represents a sequence of tasks. Each task in this stage can be executed
// in parallel
type Stage struct {
	Tasks []ProcessorTask
}

// Plan represents the full execution plan: a sequence of stages
type Plan struct {
	Stages             []Stage
	AffectedProcessors []int64
	NumAffectedAlgos   int64
}

// LayeredTopoSort returns the nodes of the directed graph g grouped into
// layers, where each layer contains nodes that can be processed in parallel
func LayeredTopoSort(g graph.Directed) ([][]graph.Node, error) {
	// calculate in-degrees
	inDegree := make(map[int64]int)
	nodes := g.Nodes()
	for nodes.Next() {
		node := nodes.Node()
		neighbors := g.From(node.ID())
		for neighbors.Next() {
			neighbor := neighbors.Node()
			inDegree[neighbor.ID()]++
		}
	}

	// find initial nodes (in-degree == 0)
	var currentLevel []graph.Node
	nodes = g.Nodes()
	for nodes.Next() {
		node := nodes.Node()
		if inDegree[node.ID()] == 0 {
			currentLevel = append(currentLevel, node)
		}
	}

	var layers [][]graph.Node
	processedCount := 0

	for len(currentLevel) > 0 {
		layers = append(layers, currentLevel)

		var nextLevel []graph.Node
		for _, node := range currentLevel {
			processedCount++
			neighbors := g.From(node.ID())
			for neighbors.Next() {
				neighbor := neighbors.Node()
				inDegree[neighbor.ID()]--
				if inDegree[neighbor.ID()] == 0 {
					nextLevel = append(nextLevel, neighbor)
				}
			}
		}
		currentLevel = nextLevel
	}

	if processedCount != g.Nodes().Len() {
		return nil, fmt.Errorf("cycle detected in graph: topological layering not possible")
	}

	return layers, nil
}

// BuildPlan builds a parallel execution Plan from the DAG represented by algoExecPaths,
// windowExecPaths, and procExecPaths.
func BuildPlan(
	algoExecPaths []string,
	windowExecPaths []string,
	procExecPaths []string,
	lookbackCounts []string,
	lookbackTimedeltas []string,
	lookbackGapCounts []string,
	lookbackGapTimedeltas []string,
	selfLookbackCounts []string,
	selfLookbackTimedeltas []string,
	selfLookbackGapCounts []string,
	selfLookbackGapTimedeltas []string,
	targetWindowId int64,
) (Plan, error) {
	if len(algoExecPaths) != len(windowExecPaths) ||
		len(windowExecPaths) != len(procExecPaths) ||
		len(procExecPaths) != len(lookbackCounts) ||
		len(lookbackCounts) != len(lookbackTimedeltas) ||
		len(lookbackTimedeltas) != len(lookbackGapCounts) ||
		len(lookbackGapCounts) != len(lookbackGapTimedeltas) ||
		len(lookbackGapTimedeltas) != len(selfLookbackCounts) ||
		len(selfLookbackCounts) != len(selfLookbackTimedeltas) ||
		len(selfLookbackTimedeltas) != len(selfLookbackGapCounts) ||
		len(selfLookbackGapCounts) != len(selfLookbackGapTimedeltas) {
		return Plan{}, fmt.Errorf(
			"number of graph paths do not match: algo=%d, window=%d, proc=%d, lbCounts=%d, lbTd=%d, lbGapCounts=%d, lbGapTd=%d, slbCounts=%d, slbTd=%d, slbGapCounts=%d, slbGapTd=%d",
			len(algoExecPaths),
			len(windowExecPaths),
			len(procExecPaths),
			len(lookbackCounts),
			len(lookbackTimedeltas),
			len(lookbackGapCounts),
			len(lookbackGapTimedeltas),
			len(selfLookbackCounts),
			len(selfLookbackTimedeltas),
			len(selfLookbackGapCounts),
			len(selfLookbackGapTimedeltas),
		)
	}

	g := simple.NewDirectedGraph()
	nodeMap := make(map[int64]Node) // map of algoIDs to nodes

	lookbackMap := make(map[string]Lookback) // map of edges (<algo_from_id>.<algo_to_id>) to lookback requirements
	var nextId int64 = 1

	for pathIdx, algoPath := range algoExecPaths {
		algoSegments := splitPath(algoPath)
		procSegments := splitPath(procExecPaths[pathIdx])
		windowSegments := splitPath(windowExecPaths[pathIdx])
		lookbackCountSegments := splitPath(lookbackCounts[pathIdx])
		lookbackTimedeltasSegments := splitPath(lookbackTimedeltas[pathIdx])
		lookbackGapCountSegments := splitPath(lookbackGapCounts[pathIdx])
		lookbackGapTimedeltaSegments := splitPath(lookbackGapTimedeltas[pathIdx])
		selfLookbackCountsSegments := splitPath(selfLookbackCounts[pathIdx])
		selfLookbackTimedeltasSegments := splitPath(selfLookbackTimedeltas[pathIdx])
		selfLookbackGapCountSegments := splitPath(selfLookbackGapCounts[pathIdx])
		selfLookbackGapTimedeltaSegments := splitPath(selfLookbackGapTimedeltas[pathIdx])

		if len(algoSegments) != len(windowSegments) ||
			len(windowSegments) != len(procSegments) ||
			len(procSegments) != len(lookbackCountSegments) ||
			len(lookbackCountSegments) != len(lookbackTimedeltasSegments) ||
			len(lookbackTimedeltasSegments) != len(lookbackGapCountSegments) ||
			len(lookbackGapCountSegments) != len(lookbackGapTimedeltaSegments) ||
			len(lookbackGapTimedeltaSegments) != len(selfLookbackCountsSegments) ||
			len(selfLookbackCountsSegments) != len(selfLookbackTimedeltasSegments) ||
			len(selfLookbackTimedeltasSegments) != len(selfLookbackGapCountSegments) ||
			len(selfLookbackGapCountSegments) != len(selfLookbackGapTimedeltaSegments) {
			return Plan{}, fmt.Errorf(
				"number of segments do not match in path %d", pathIdx,
			)
		}

		var pathWindowMap map[int64]int64
		var prevNode Node

		for ii, algoIdStr := range algoSegments {
			algoId := mustAtoi(algoIdStr)
			procId := mustAtoi(procSegments[ii])
			windowId := mustAtoi(windowSegments[ii])
			lookbackCount := mustAtoi(lookbackCountSegments[ii])
			lookbackTd := mustAtoi(lookbackTimedeltasSegments[ii])
			lookbackGapCount := mustAtoi(lookbackGapCountSegments[ii])
			lookbackGapTd := mustAtoi(lookbackGapTimedeltaSegments[ii])
			selfLookbackCount := mustAtoi(selfLookbackCountsSegments[ii])
			selfLookbackTd := mustAtoi(selfLookbackTimedeltasSegments[ii])
			selfLookbackGapCount := mustAtoi(selfLookbackGapCountSegments[ii])
			selfLookbackGapTd := mustAtoi(selfLookbackGapTimedeltaSegments[ii])

			if pathWindowMap == nil {
				pathWindowMap = make(map[int64]int64)
			}

			if prevWin, seen := pathWindowMap[int64(procId)]; seen {
				if prevWin != int64(windowId) {
					return Plan{}, fmt.Errorf(
						"window ID mismatch on processor %d in path %d: saw %d, then %d",
						procId, pathIdx, prevWin, windowId,
					)
				}
			} else {
				pathWindowMap[int64(procId)] = int64(windowId)
			}

			node, exists := nodeMap[int64(algoId)]
			if !exists {
				node = Node{
					id:       nextId,
					algoId:   int64(algoId),
					procId:   int64(procId),
					windowId: int64(windowId),
					selfLookback: Lookback{
						Count:        selfLookbackCount,
						Timedelta:    selfLookbackTd,
						GapCount:     selfLookbackGapCount,
						GapTimedelta: selfLookbackGapTd,
					},
					pathIdx: pathIdx,
				}
				nodeMap[int64(algoId)] = node
				g.AddNode(node)
				nextId++
			}

			if prevNode.id != 0 {
				_edgeStr := fmt.Sprintf("%d.%d", prevNode.algoId, node.algoId)
				if _, ok := lookbackMap[_edgeStr]; !ok {
					lookbackMap[_edgeStr] = Lookback{
						Count:        lookbackCount,
						Timedelta:    lookbackTd,
						GapCount:     lookbackGapCount,
						GapTimedelta: lookbackGapTd,
					}
				}

				edge := g.NewEdge(prevNode, node)
				g.SetEdge(edge)
			}
			prevNode = node
		}
	}

	layers, err := LayeredTopoSort(g)
	if err != nil {
		return Plan{}, fmt.Errorf("error during layered topological sort: %v", err)
	}

	var plan Plan
	var _edgeStr string
	for _, layer := range layers {
		taskMap := make(map[int64][]Node)

		for _, gn := range layer {
			node := gn.(Node)

			nodes := g.To(node.ID())
			for range nodes.Len() {
				nodes.Next()
				_currNode := nodes.Node()
				_currNode_v2, ok := _currNode.(Node)
				if !ok {
					panic(ok)
				}

				_edgeStr = fmt.Sprintf("%d.%d", _currNode_v2.algoId, node.algoId)
				lookbackConfig, ok := lookbackMap[_edgeStr]
				if !ok {
					return Plan{}, fmt.Errorf("could not find edge lookback settings between algoId: %d and algoId: %d", node.algoId, _currNode_v2.algoId)
				}
				if node.algoDeps == nil {
					node.algoDeps = []AlgoDep{{AlgoId: _currNode_v2.algoId, Lookback: lookbackConfig}}
				} else {
					node.algoDeps = append(node.algoDeps, AlgoDep{
						AlgoId: _currNode_v2.algoId, Lookback: lookbackConfig})
				}
			}
			slices.SortFunc(node.algoDeps, func(a, b AlgoDep) int {
				if a.AlgoId < b.AlgoId {
					return -1
				}
				if a.AlgoId > b.AlgoId {
					return 1
				}
				return 0
			})

			taskMap[node.procId] = append(taskMap[node.procId], node)
		}
		var stage Stage
		for procId, nodes := range taskMap {
			if !slices.Contains(plan.AffectedProcessors, procId) {
				plan.AffectedProcessors = append(plan.AffectedProcessors, procId)
			}
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].pathIdx < nodes[j].pathIdx
			})

			stage.Tasks = append(stage.Tasks, ProcessorTask{
				ProcId: procId,
				Nodes:  nodes,
			})
		}
		sort.Slice(stage.Tasks, func(i, j int) bool {
			return stage.Tasks[i].ProcId < stage.Tasks[j].ProcId
		})
		slices.Sort(plan.AffectedProcessors)

		plan.Stages = append(plan.Stages, stage)
	}

	plan.NumAffectedAlgos = int64(len(nodeMap))

	return plan, nil
}

func splitPath(path string) []string {
	return strings.Split(path, ".")
}

func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("invalid integer: %s", s))
	}
	return n
}
