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

type ReshardingResult struct {
	NewMapping map[string]int32 
	Moves []ReshardMove 
}

type ReshardMove struct {
	AccountID string
	FromCluster int32
	ToCluster int32
}


func NewGraph() *Graph {
	return &Graph{
		Vertices: make(map[string]bool),
		Edges:    make(map[string]map[string]int),
	}
}

func (g *Graph) AddEdge(u, v string) {
	g.Vertices[u] = true
	g.Vertices[v] = true

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


func BuildGraphFromTransactions(transactions []TransactionEdge) *Graph {
	graph := NewGraph()
	for _, tx := range transactions {
		if tx.Sender != tx.Receiver && tx.Sender != "" && tx.Receiver != "" {
			graph.AddEdge(tx.Sender, tx.Receiver)
		}
	}
	return graph
}

func PartitionGraph(graph *Graph, numClusters int32, currentMapping map[string]int32) map[string]int32 {
    type Node struct {
        ID     string
        Degree int
    }
    nodes := make([]Node, 0, len(graph.Vertices))
    for v := range graph.Vertices {
        nodes = append(nodes, Node{ID: v, Degree: graph.GetDegree(v)})
    }
    sort.Slice(nodes, func(i, j int) bool {
        return nodes[i].Degree > nodes[j].Degree
    })

    newMapping := make(map[string]int32)
    clusterLoad := make(map[int32]int)
    
    totalNodes := len(nodes)
    targetLoad := float64(totalNodes) / float64(numClusters)

	if targetLoad < 1.0 { targetLoad = 1.0 }
    
    for _, node := range nodes {
        bestCluster := int32(1)
        bestScore := -9999999.0 // Start very low

        for c := int32(1); c <= numClusters; c++ {
            
            affinity := 0.0
            if neighbors, ok := graph.Edges[node.ID]; ok {
                for neighbor, weight := range neighbors {
                    if assignedTo, exists := newMapping[neighbor]; exists {
                        if assignedTo == c {
                            affinity += float64(weight) * 200.0 
                        }
                    } else if currentMapping[neighbor] == c {
                        affinity += float64(weight) * 100.0 
                    }
                }
            }

            stability := 0.0
            if currentMapping[node.ID] == c {
                stability = 50.0
            }

            currentCount := float64(clusterLoad[c])
            loadPenalty := 0.0
            
            if currentCount > (targetLoad * 1.2) { 
                excess := currentCount - targetLoad
                loadPenalty = (excess * excess) * 10.0 
            }

            score := affinity + stability - loadPenalty

            if score > bestScore {
                bestScore = score
                bestCluster = c
            }
        }

        newMapping[node.ID] = bestCluster
        clusterLoad[bestCluster]++
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