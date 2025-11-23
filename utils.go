/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"sort"
)

func getNodeCluster() []NodeConfig {
	return []NodeConfig{
		{NodeId: 1, PortNo: 8001},
		{NodeId: 2, PortNo: 8002},
		{NodeId: 3, PortNo: 8003},
		{NodeId: 4, PortNo: 8004},
		{NodeId: 5, PortNo: 8005},
	}
}


func getClientCluster() []ClientConfig {
    return []ClientConfig{
        {ClientId: 1, ClientName: "A", Port: 9000},
        {ClientId: 2, ClientName: "B", Port: 9001},
        {ClientId: 3, ClientName: "C", Port: 9002},
        {ClientId: 4, ClientName: "D", Port: 9003},
        {ClientId: 5, ClientName: "E", Port: 9004},
        {ClientId: 6, ClientName: "F", Port: 9005},
        {ClientId: 7, ClientName: "G", Port: 9006},
        {ClientId: 8, ClientName: "H", Port: 9007},
        {ClientId: 9, ClientName: "I", Port: 9008},
        {ClientId: 10, ClientName: "J", Port: 9009},
    }
}


func getAllNodeIDs() []int32 {
	var ids []int32

	for _, cfg := range getNodeCluster() {
		ids = append(ids, cfg.NodeId)
	}
	return ids
}


func (node *Node) PrintAcceptLogUtil() {
    // Get sorted list of sequence numbers
    node.muLog.RLock()
    keys := make([]int, 0, len(node.acceptLog))
    for k := range node.acceptLog {
        keys = append(keys, int(k))
    }
    node.muLog.RUnlock()

    sort.Ints(keys)

    fmt.Printf("\n[Node %d] Log (ordered by seq):\n", node.nodeId)
    if len(keys) == 0 {
        fmt.Println("  (no log entries)")
        fmt.Println("-------------------------------------------")
        return
    }

    // Process each entry
    for _, ki := range keys {
        seq := int32(ki)
        
        // Get entry pointer
        node.muLog.RLock()
        entry, exists := node.acceptLog[seq]
        node.muLog.RUnlock()

        if !exists {
            fmt.Printf("  Seq=%d | Phase=X | \n", seq)
            continue
        }

        // Lock entry and read all fields we need
        entry.mu.Lock()
        phase := entry.Phase
        sequenceNum := entry.SequenceNum
        ballotRound := entry.Ballot.RoundNumber
        ballotNodeId := entry.Ballot.NodeId
        req := entry.Request
        entry.mu.Unlock()

        status := "X"

        switch phase {
        case PhaseExecuted:
            status = "E"
        case PhaseCommitted:
            status = "C"
        case PhaseAccepted:
            status = "A"
		}

        // Format and print based on request type
        if req != nil {
			fmt.Printf("  Seq=%d | Ballot R=%d N=%d | Status=%s | (%s, %s, %d)\n",
				sequenceNum, ballotRound,ballotNodeId, status,
				req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)
        } else {
            fmt.Printf("  Seq=%d | Ballot R=%d N=%d | Status=%s | NO-OP\n",
                sequenceNum, ballotRound,ballotNodeId, status)
        }
    }
    
    fmt.Println("-------------------------------------------")
}