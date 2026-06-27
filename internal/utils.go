package internal

import "github.com/jackc/pgx/v5/pgtype"

type transitiveEdge struct {
	uniqueName string
	name       string
	hash       string
	workerId   pgtype.UUID
}

// for a complete set of edges within a DAG, compute the transitive pairs
func transitivePairs[T any](edges [][2]T, key func(T) string) [][2]T {
	adj := map[string][]string{}
	nodes := map[string]struct{}{}
	lookup := map[string]T{}

	for _, e := range edges {
		k0, k1 := key(e[0]), key(e[1])
		adj[k0] = append(adj[k0], k1)
		nodes[k0] = struct{}{}
		nodes[k1] = struct{}{}
		lookup[k0] = e[0]
		lookup[k1] = e[1]
	}

	var pairs [][2]T
	for src := range nodes {
		visited := map[string]bool{}
		queue := []string{src}
		for len(queue) > 0 {
			n := queue[0]
			queue = queue[1:]
			for _, nb := range adj[n] {
				if !visited[nb] {
					visited[nb] = true
					pairs = append(pairs, [2]T{lookup[src], lookup[nb]})
					queue = append(queue, nb)
				}
			}
		}
	}
	return pairs
}
