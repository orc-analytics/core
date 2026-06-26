package internal

import (
	"sort"
	"testing"
)

// helper: normalise a pair slice so order doesn't affect comparison
func normalisePairs(pairs [][2]string) [][2]string {
	sorted := make([][2]string, len(pairs))
	copy(sorted, pairs)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i][0] != sorted[j][0] {
			return sorted[i][0] < sorted[j][0]
		}
		return sorted[i][1] < sorted[j][1]
	})
	return sorted
}

func assertPairs(t *testing.T, got, want [][2]string) {
	t.Helper()
	g := normalisePairs(got)
	w := normalisePairs(want)
	if len(g) != len(w) {
		t.Fatalf("length mismatch: got %d pairs, want %d\ngot:  %v\nwant: %v", len(g), len(w), g, w)
	}
	for i := range g {
		if g[i] != w[i] {
			t.Fatalf("pair mismatch at index %d:\ngot:  %v\nwant: %v", i, g, w)
		}
	}
}

// --- basic cases ---
func TestEmpty(t *testing.T) {
	got := transitivePairs(nil, func(s string) string { return s })
	assertPairs(t, got, nil)
}

func TestEmptySlice(t *testing.T) {
	got := transitivePairs([][2]string{}, func(s string) string { return s })
	assertPairs(t, got, nil)
}

func TestSingleEdge(t *testing.T) {
	edges := [][2]string{{"a", "b"}}
	want := [][2]string{{"a", "b"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- linear chains ---

func TestLinearChain(t *testing.T) {
	// a -> b -> c  =>  (a,b), (a,c), (b,c)
	edges := [][2]string{{"a", "b"}, {"b", "c"}}
	want := [][2]string{{"a", "b"}, {"a", "c"}, {"b", "c"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestLongerChain(t *testing.T) {
	// a -> b -> c -> d
	edges := [][2]string{{"a", "b"}, {"b", "c"}, {"c", "d"}}
	want := [][2]string{
		{"a", "b"}, {"a", "c"}, {"a", "d"},
		{"b", "c"}, {"b", "d"},
		{"c", "d"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- branching ---

func TestBranching(t *testing.T) {
	// a -> b, a -> c, b -> d
	// transitive: a reaches d via b
	edges := [][2]string{{"a", "b"}, {"a", "c"}, {"b", "d"}}
	want := [][2]string{
		{"a", "b"}, {"a", "c"}, {"a", "d"},
		{"b", "d"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestDiamond(t *testing.T) {
	//     a
	//    / \
	//   b   c
	//    \ /
	//     d
	edges := [][2]string{{"a", "b"}, {"a", "c"}, {"b", "d"}, {"c", "d"}}
	want := [][2]string{
		{"a", "b"}, {"a", "c"}, {"a", "d"},
		{"b", "d"},
		{"c", "d"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- cycles ---

func TestSimpleCycle(t *testing.T) {
	// a -> b -> a  (each node reaches the other)
	edges := [][2]string{{"a", "b"}, {"b", "a"}}
	want := [][2]string{{"a", "b"}, {"b", "a"}, {"a", "a"}, {"b", "b"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestLongerCycle(t *testing.T) {
	// a -> b -> c -> a
	// every node reaches every other node
	edges := [][2]string{{"a", "b"}, {"b", "c"}, {"c", "a"}}
	want := [][2]string{
		{"a", "b"}, {"a", "c"}, {"a", "a"},
		{"b", "c"}, {"b", "a"}, {"b", "b"},
		{"c", "a"}, {"c", "b"}, {"c", "c"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestCycleWithTail(t *testing.T) {
	// x -> a -> b -> a  (x reaches both a and b; cycle between a and b)
	edges := [][2]string{{"x", "a"}, {"a", "b"}, {"b", "a"}}
	want := [][2]string{
		{"x", "a"}, {"x", "b"},
		{"a", "b"}, {"a", "a"},
		{"b", "a"}, {"b", "b"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- disconnected components ---
func TestDisconnectedComponents(t *testing.T) {
	// a -> b   and   c -> d  (no cross-reachability)
	edges := [][2]string{{"a", "b"}, {"c", "d"}}
	want := [][2]string{{"a", "b"}, {"c", "d"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestDisconnectedWithChain(t *testing.T) {
	// a -> b -> c   and   x -> y (isolated)
	edges := [][2]string{{"a", "b"}, {"b", "c"}, {"x", "y"}}
	want := [][2]string{
		{"a", "b"}, {"a", "c"},
		{"b", "c"},
		{"x", "y"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- sink / source nodes ---

func TestSinkNode(t *testing.T) {
	// Multiple nodes pointing to one sink: a->c, b->c
	edges := [][2]string{{"a", "c"}, {"b", "c"}}
	want := [][2]string{{"a", "c"}, {"b", "c"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

func TestSourceNode(t *testing.T) {
	// One source fanning out: a->b, a->c, a->d
	edges := [][2]string{{"a", "b"}, {"a", "c"}, {"a", "d"}}
	want := [][2]string{{"a", "b"}, {"a", "c"}, {"a", "d"}}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}

// --- duplicate edges ---

func TestDuplicateEdges(t *testing.T) {
	// Same edge supplied twice — should not produce duplicate pairs
	edges := [][2]string{{"a", "b"}, {"a", "b"}}
	got := transitivePairs(edges, func(s string) string { return s })
	// Count occurrences of (a,b)
	count := 0
	for _, p := range got {
		if p == [2]string{"a", "b"} {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected pair (a,b) exactly once, got %d times", count)
	}
}

// --- self-loops ---

func TestSelfLoop(t *testing.T) {
	// a -> a: a self-loop; BFS from a visits a as a neighbour but a==src,
	// so whether it produces (a,a) depends on implementation.
	// This test documents current behaviour rather than asserting a preference.
	edges := [][2]string{{"a", "a"}}
	got := transitivePairs(edges, func(s string) string { return s })
	t.Logf("self-loop result: %v", got)
}

// --- realistic label formats ---

func TestUUIDs(t *testing.T) {
	edges := [][2]string{
		{"550e8400-e29b-41d4-a716-446655440000", "6ba7b810-9dad-11d1-80b4-00c04fd430c8"},
		{"6ba7b810-9dad-11d1-80b4-00c04fd430c8", "6ba7b814-9dad-11d1-80b4-00c04fd430c8"},
	}
	got := transitivePairs(edges, func(s string) string { return s })
	// Expect 3 pairs: direct a->b, b->c, and transitive a->c
	if len(got) != 3 {
		t.Fatalf("expected 3 pairs, got %d: %v", len(got), got)
	}
}

func TestNamespacedKeys(t *testing.T) {
	edges := [][2]string{
		{"service:auth", "service:users"},
		{"service:users", "db:postgres"},
	}
	want := [][2]string{
		{"service:auth", "service:users"},
		{"service:auth", "db:postgres"},
		{"service:users", "db:postgres"},
	}
	assertPairs(t, transitivePairs(edges, func(s string) string { return s }), want)
}
