/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"log"
	"sort"
)

type TransactionEdge struct {
	Sender string
	Receiver string
}

type Graph struct {
	Vertices map[string]bool     
	Edges map[string]map[string]int // adjs list with edge weights
}

func NewGraph() *Graph {
	return &Graph{
		Vertices: make(map[string]bool),
		Edges:    make(map[string]map[string]int),
	}
}

func (g *Graph) AddEdge(u, v string) {
	// Add vertices
	g.Vertices[u] = true
	g.Vertices[v] = true

	// Add edge (undirected, so add both directions)
	if g.Edges[u] == nil {
		g.Edges[u] = make(map[string]int)
	}
	if g.Edges[v] == nil {
		g.Edges[v] = make(map[string]int)
	}

	g.Edges[u][v]++
	g.Edges[v][u]++
}

func (g *Graph) GetDegree(v string) int {
	degree := 0
	if neighbors, exists := g.Edges[v]; exists {
		for _, weight := range neighbors {
			degree += weight
		}
	}
	return degree
}

type ReshardingResult struct {
	NewMapping map[string]int32 
	Moves []ReshardMove 
}

type ReshardMove struct {
	AccountID string
	FromCluster int32
	ToCluster int32
}


func BuildGraphFromTransactions(transactions []TransactionEdge) *Graph {
	graph := NewGraph()
	for _, tx := range transactions {
		if tx.Sender != tx.Receiver && tx.Sender != "" && tx.Receiver != "" {
			graph.AddEdge(tx.Sender, tx.Receiver)
		}
	}
	return graph
}

func PartitionGraph(graph *Graph, k int32, currentMapping map[string]int32) map[string]int32 {
	numClusters := int(k)
	newMapping := make(map[string]int32)
	
	// Initialize cluster sizes
	clusterSizes := make([]int, numClusters)
	
	// Calculate target size for balanced partitions
	totalVertices := len(graph.Vertices)
	targetSize := totalVertices / numClusters
	maxSize := targetSize + (targetSize / 10) // Allow 10% imbalance
	
	log.Printf("[Resharding] Total vertices: %d, Target size per cluster: %d, Max size: %d",
		totalVertices, targetSize, maxSize)
	
	// Step 1: Sort vertices by degree (high to low)
	type VertexDegree struct {
		Vertex string
		Degree int
	}
	
	vertexList := make([]VertexDegree, 0, len(graph.Vertices))
	for v := range graph.Vertices {
		vertexList = append(vertexList, VertexDegree{
			Vertex: v,
			Degree: graph.GetDegree(v),
		})
	}
	
	sort.Slice(vertexList, func(i, j int) bool {
		return vertexList[i].Degree > vertexList[j].Degree
	})
	
	// Step 2: Greedy assignment
	for _, vd := range vertexList {
		v := vd.Vertex
		
		// Calculate affinity to each cluster
		clusterAffinity := make([]int, numClusters)
		
		// Check edges to already-assigned vertices
		if neighbors, exists := graph.Edges[v]; exists {
			for neighbor, weight := range neighbors {
				if assignedCluster, assigned := newMapping[neighbor]; assigned {
					clusterAffinity[assignedCluster-1] += weight
				}
			}
		}
		
		// Find best cluster (highest affinity, respecting balance)
		bestCluster := int32(-1)
		bestScore := -1
		
		for c := 0; c < numClusters; c++ {
			// Skip if cluster is full
			if clusterSizes[c] >= maxSize {
				continue
			}
			
			// Prefer clusters with higher affinity
			// Tie-break by preferring current cluster (minimize moves)
			score := clusterAffinity[c] * 1000 // weight affinity heavily
			
			// Small bonus if this is the current cluster
			if currentCluster, exists := currentMapping[v]; exists && currentCluster == int32(c+1) {
				score += 10
			}
			
			// Prefer clusters that are smaller (for balance)
			score -= clusterSizes[c]
			
			if score > bestScore {
				bestScore = score
				bestCluster = int32(c + 1)
			}
		}
		
		// If no cluster found (all full), assign to smallest
		if bestCluster == -1 {
			minSize := clusterSizes[0]
			bestCluster = 1
			for c := 1; c < numClusters; c++ {
				if clusterSizes[c] < minSize {
					minSize = clusterSizes[c]
					bestCluster = int32(c + 1)
				}
			}
		}
		
		newMapping[v] = bestCluster
		clusterSizes[bestCluster-1]++
	}
	
	// Log cluster sizes
	for c := 0; c < numClusters; c++ {
		log.Printf("[Resharding] Cluster %d size: %d", c+1, clusterSizes[c])
	}
	
	return newMapping
}


func CalculateResharding(txHistory []TransactionEdge, currentMapping map[string]int32, numClusters int32) ReshardingResult {
	// Build graph
	graph := BuildGraphFromTransactions(txHistory)
	
	log.Printf("[Resharding] Built graph with %d vertices and %d edge entries",
		len(graph.Vertices), len(graph.Edges))
	
	// Partition graph
	newMapping := PartitionGraph(graph, numClusters, currentMapping)
	
	// Calculate moves
	moves := []ReshardMove{}
	for accountID, newCluster := range newMapping {
		oldCluster := currentMapping[accountID]
		if oldCluster != newCluster {
			moves = append(moves, ReshardMove{
				AccountID:   accountID,
				FromCluster: oldCluster,
				ToCluster:   newCluster,
			})
		}
	}
	
	log.Printf("[Resharding] Total moves required: %d", len(moves))
	
	return ReshardingResult{
		NewMapping: newMapping,
		Moves:      moves,
	}
}

func CalculateCutCost(graph *Graph, mapping map[string]int32) int {
	cost := 0
	visited := make(map[string]map[string]bool)
	
	for u, neighbors := range graph.Edges {
		for v, weight := range neighbors {
			if visited[u] == nil {
				visited[u] = make(map[string]bool)
			}
			if visited[u][v] {
				continue
			}
			
			if visited[v] == nil {
				visited[v] = make(map[string]bool)
			}
			visited[u][v] = true
			visited[v][u] = true
			
			if mapping[u] != mapping[v] {
				cost += weight
			}
		}
	}
	
	return cost
}