package dag

import (
	"reflect"
	"testing"
)

func TestBuildPlan(t *testing.T) {
	tests := []struct {
		name                   string
		algoExecPath           []string
		windowExecPath         []string
		procExecPath           []string
		lookbackCounts         []string
		lookbackTimedeltas     []string
		selfLookbackCounts     []string
		selfLookbackTimedeltas []string
		targetWindowId         int64
		want                   Plan
		wantErr                bool
	}{
		{name: "simple straight line",
			algoExecPath:           []string{"1.2.3"},
			windowExecPath:         []string{"1.1.1"},
			procExecPath:           []string{"1.1.1"},
			lookbackCounts:         []string{"0.0.0"},
			lookbackTimedeltas:     []string{"0.0.0"},
			selfLookbackCounts:     []string{"0.0.0"},
			selfLookbackTimedeltas: []string{"0.0.0"},
			targetWindowId:         1,
			want: Plan{
				Stages: []Stage{
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{{algoId: 1, procId: 1, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{{algoId: 2, procId: 1, algoDeps: []AlgoDep{{AlgoId: 1}}, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{{algoId: 3, procId: 1, algoDeps: []AlgoDep{{AlgoId: 2}}, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
					}},
				},
				AffectedProcessors: []int64{1},
			},
			wantErr: false,
		},
		{
			name:                   "parallel roots",
			algoExecPath:           []string{"1", "2"},
			windowExecPath:         []string{"1", "1"},
			procExecPath:           []string{"1", "2"},
			lookbackCounts:         []string{"0", "0"},
			lookbackTimedeltas:     []string{"0", "0"},
			selfLookbackCounts:     []string{"0", "0"},
			selfLookbackTimedeltas: []string{"0", "0"},
			targetWindowId:         1,
			want: Plan{
				Stages: []Stage{
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{{algoId: 1, procId: 1, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
						{ProcId: 2, Nodes: []Node{{algoId: 2, procId: 2, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
					}},
				},
				AffectedProcessors: []int64{1, 2},
			},
			wantErr: false,
		},
		{
			name:                   "fork and join",
			algoExecPath:           []string{"1.2.4", "1.3.4"},
			windowExecPath:         []string{"1.1.1", "1.1.1"},
			procExecPath:           []string{"1.2.3", "1.2.3"},
			lookbackCounts:         []string{"0.0.0", "0.0.0"},
			lookbackTimedeltas:     []string{"0.0.0", "0.0.0"},
			selfLookbackCounts:     []string{"0.0.0", "0.0.0"},
			selfLookbackTimedeltas: []string{"0.0.0", "0.0.0"},
			targetWindowId:         1,
			want: Plan{
				Stages: []Stage{
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{{algoId: 1, procId: 1, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}}}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 2, Nodes: []Node{
							{algoId: 2, procId: 2, algoDeps: []AlgoDep{{AlgoId: 1}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
							{algoId: 3, procId: 2, algoDeps: []AlgoDep{{AlgoId: 1}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 3, Nodes: []Node{
							{algoId: 4, procId: 3, algoDeps: []AlgoDep{{AlgoId: 2}, {AlgoId: 3}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
					}},
				},
				AffectedProcessors: []int64{1, 2, 3},
			},
			wantErr: false,
		},
		{
			name:                   "cycle detection",
			algoExecPath:           []string{"1.2", "2.1"},
			windowExecPath:         []string{"1.1", "1.1"},
			procExecPath:           []string{"1.1", "1.1"},
			lookbackCounts:         []string{"0.0", "0.0"},
			lookbackTimedeltas:     []string{"0.0", "0.0"},
			selfLookbackCounts:     []string{"0.0", "0.0"},
			selfLookbackTimedeltas: []string{"0.0", "0.0"},
			targetWindowId:         1,
			want:                   Plan{},
			wantErr:                true,
		},
		{
			name:                   "empty inputs",
			algoExecPath:           []string{},
			windowExecPath:         []string{},
			procExecPath:           []string{},
			lookbackCounts:         []string{},
			lookbackTimedeltas:     []string{},
			selfLookbackCounts:     []string{},
			selfLookbackTimedeltas: []string{},
			targetWindowId:         1,
			want:                   Plan{Stages: nil},
			wantErr:                false,
		},
		{
			name: "complex DAG",
			algoExecPath: []string{
				"1.2.5",   // Path 1: Processor 1 -> Processor 2 -> Processor 3
				"3.4.5",   // Path 2: Processor 1 -> Processor 2 -> Processor 3
				"6.7.8.9", // Path 3: Processor 4 -> Processor 5 -> Processor 5 -> Processor 6
			},
			windowExecPath: []string{
				"1.1.1",
				"1.1.1",
				"1.1.1.1",
			},
			procExecPath: []string{
				"1.2.3",   // Node 1 (proc 1) -> Node 2 (proc 2) -> Node 5 (proc 3)
				"1.2.3",   // Node 3 (proc 1) -> Node 4 (proc 2) -> Node 5 (proc 3)
				"4.5.5.6", // Node 6 (proc 4) -> Node 7 (proc 5) -> Node 8 (proc 5) -> Node 9 (proc 6)
			},
			lookbackCounts:         []string{"0.0.0", "0.0.0", "0.0.0.0"},
			lookbackTimedeltas:     []string{"0.0.0", "0.0.0", "0.0.0.0"},
			selfLookbackCounts:     []string{"0.0.0", "0.0.0", "0.0.0.0"},
			selfLookbackTimedeltas: []string{"0.0.0", "0.0.0", "0.0.0.0"},
			targetWindowId:         1,
			want: Plan{
				Stages: []Stage{
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{
							{algoId: 1, procId: 1, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}},
							{algoId: 3, procId: 1, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
						{ProcId: 4, Nodes: []Node{
							{algoId: 6, procId: 4, algoDeps: nil, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 2, Nodes: []Node{
							{algoId: 2, procId: 2, algoDeps: []AlgoDep{{AlgoId: 1, Lookback: Lookback{Count: 0, Timedelta: 0}}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
							{algoId: 4, procId: 2, algoDeps: []AlgoDep{{AlgoId: 3, Lookback: Lookback{Count: 0, Timedelta: 0}}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
						{ProcId: 5, Nodes: []Node{
							{algoId: 7, procId: 5, algoDeps: []AlgoDep{{AlgoId: 6, Lookback: Lookback{Count: 0, Timedelta: 0}}}, selfLookback: Lookback{Count: 0, Timedelta: 0}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 3, Nodes: []Node{
							{algoId: 5, procId: 3, algoDeps: []AlgoDep{{AlgoId: 2, Lookback: Lookback{Count: 0, Timedelta: 0}}, {AlgoId: 4, Lookback: Lookback{Count: 0, Timedelta: 0}}}},
						}},
						{ProcId: 5, Nodes: []Node{
							{algoId: 8, procId: 5, algoDeps: []AlgoDep{{AlgoId: 7, Lookback: Lookback{Count: 0, Timedelta: 0}}}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 6, Nodes: []Node{
							{algoId: 9, procId: 6, algoDeps: []AlgoDep{{AlgoId: 8, Lookback: Lookback{Count: 0, Timedelta: 0}}}},
						}},
					}},
				},
				AffectedProcessors: []int64{1, 2, 3, 4, 5, 6},
			},
			wantErr: false,
		},
		{
			name: "complex DAG with lookback",
			windowExecPath: []string{
				"1.1.1",
				"1.1.1",
				"1.1.1.1",
			},
			algoExecPath: []string{
				"1.2.5",   // Path 1: Processor 1 -> Processor 2 -> Processor 3
				"3.4.5",   // Path 2: Processor 1 -> Processor 2 -> Processor 3
				"6.7.8.9", // Path 3: Processor 4 -> Processor 5 -> Processor 5 -> Processor 6
			},
			procExecPath: []string{
				"1.2.3",   // Node 1 (proc 1) -> Node 2 (proc 2) -> Node 5 (proc 3)
				"1.2.3",   // Node 3 (proc 1) -> Node 4 (proc 2) -> Node 5 (proc 3)
				"4.5.5.6", // Node 6 (proc 4) -> Node 7 (proc 5) -> Node 8 (proc 5) -> Node 9 (proc 6)
			},
			lookbackCounts: []string{
				"0.10.0",
				"0.0.103",
				"0.130.10.0",
			},
			lookbackTimedeltas: []string{
				"0.0.100",
				"0.13.0",
				"0.0.0.40",
			},
			selfLookbackCounts:     []string{"0.0.0", "0.0.0", "0.0.0.0"},
			selfLookbackTimedeltas: []string{"0.0.0", "0.0.0", "0.0.0.0"},
			targetWindowId:         1,
			want: Plan{
				Stages: []Stage{
					{Tasks: []ProcessorTask{
						{ProcId: 1, Nodes: []Node{
							{algoId: 1, procId: 1, algoDeps: nil},
							{algoId: 3, procId: 1, algoDeps: nil},
						}},
						{ProcId: 4, Nodes: []Node{
							{algoId: 6, procId: 4, algoDeps: nil},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 2, Nodes: []Node{
							{algoId: 2, procId: 2, algoDeps: []AlgoDep{{AlgoId: 1, Lookback: Lookback{Count: 10, Timedelta: 0}}}},
							{algoId: 4, procId: 2, algoDeps: []AlgoDep{{AlgoId: 3, Lookback: Lookback{Count: 0, Timedelta: 13}}}},
						}},
						{ProcId: 5, Nodes: []Node{
							{algoId: 7, procId: 5, algoDeps: []AlgoDep{{AlgoId: 6, Lookback: Lookback{Count: 130, Timedelta: 0}}}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 3, Nodes: []Node{
							{algoId: 5, procId: 3, algoDeps: []AlgoDep{{AlgoId: 2, Lookback: Lookback{Count: 0, Timedelta: 100}}, {AlgoId: 4, Lookback: Lookback{Count: 103, Timedelta: 0}}}},
						}},
						{ProcId: 5, Nodes: []Node{
							{algoId: 8, procId: 5, algoDeps: []AlgoDep{{AlgoId: 7, Lookback: Lookback{Count: 10, Timedelta: 0}}}},
						}},
					}},
					{Tasks: []ProcessorTask{
						{ProcId: 6, Nodes: []Node{
							{algoId: 9, procId: 6, algoDeps: []AlgoDep{{AlgoId: 8, Lookback: Lookback{Count: 0, Timedelta: 40}}}},
						}},
					}},
				},
				AffectedProcessors: []int64{1, 2, 3, 4, 5, 6},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildPlan(
				tt.algoExecPath,
				tt.windowExecPath,
				tt.procExecPath,
				tt.lookbackCounts,
				tt.lookbackTimedeltas,
				tt.selfLookbackCounts,
				tt.selfLookbackTimedeltas,
				tt.targetWindowId,
			)

			// Handling errors
			if (err != nil) != tt.wantErr {
				t.Errorf("BuildPlan() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				// If error was expected and occurred, move on
				return
			}

			// Now we need to *normalize* Nodes before comparing
			// because IDs (g.ID()) will differ in tests (they are auto-incremented)
			got = normalisePlan(got)
			want := normalisePlan(tt.want)

			if !reflect.DeepEqual(got, want) {
				t.Errorf("BuildPlan() = %#v, want %#v", got, want)
			}
		})
	}
}

// normalisePlan removes ID fields before comparison because they are generated at runtime.
func normalisePlan(plan Plan) Plan {
	for stageIdx := range plan.Stages {
		for taskIdx := range plan.Stages[stageIdx].Tasks {
			for nodeIdx := range plan.Stages[stageIdx].Tasks[taskIdx].Nodes {
				plan.Stages[stageIdx].Tasks[taskIdx].Nodes[nodeIdx].id = 0       // Reset generated id
				plan.Stages[stageIdx].Tasks[taskIdx].Nodes[nodeIdx].windowId = 0 // Reset windowId, not relevant to path
				plan.Stages[stageIdx].Tasks[taskIdx].Nodes[nodeIdx].pathIdx = 0  // Reset pathIdx
			}
		}
	}
	return plan
}
