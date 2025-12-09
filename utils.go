/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	pb "transaction-processor/message"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// TO DO need to make this dynamic
func getNodeCluster() []NodeConfig {
	return []NodeConfig{
		{NodeId: 1, ClusterId: 1, PortNo: 8001},
        {NodeId: 2, ClusterId: 1, PortNo: 8002},
        {NodeId: 3, ClusterId: 1, PortNo: 8003},
		{NodeId: 4, ClusterId: 2, PortNo: 8004},
        {NodeId: 5, ClusterId: 2, PortNo: 8005},
        {NodeId: 6, ClusterId: 2, PortNo: 8006},
        {NodeId: 7, ClusterId: 3, PortNo: 8007},
        {NodeId: 8, ClusterId: 3, PortNo: 8008},
        {NodeId: 9, ClusterId: 3, PortNo: 8009},
	}
}

// TO DO need to make this dynamic
func getClusterId(id int32) int32 {
    if id >= 1 && id <= 3000 {
        return 1
    } else if id >= 3001 && id <= 6000 {
        return 2
    } else if id >= 6001 && id <= 9000 {
        return 3
    }
    
    // If data point is not within this limit then error should be thrown
    return -1
}

func (node *Node) getAllClusterNodes() []int32 {
    node.muCluster.Lock()
    defer node.muCluster.Unlock()
    
    targetNodeIds := node.clusterInfo[node.clusterId].NodeIds
	
	return targetNodeIds
}

func (node *Node) updateLeaderIfNeededForCluster(sender string,leaderIdFromMsg int32){
    senderIntVal, err := strconv.Atoi(sender) 
    if err != nil {
        log.Printf("[Node %d] Failed to convert value of client sender from string to int",node.nodeId)
        return
    }

    clusterId := getClusterId(int32(senderIntVal))

    node.muCluster.Lock()
    defer node.muCluster.Unlock()

    info,exists := node.clusterInfo[clusterId]

    if !exists {
        log.Printf("[[Node %d] Cluster info not present=%d",node.nodeId,clusterId)
        return
	}

    targetLeaderId := info.LeaderId

    if targetLeaderId != leaderIdFromMsg{
        // log.Printf("[Node %d] Updating leaderId to %d from %d for cluster=%d",node.nodeId,targetLeaderId,info.LeaderId,clusterId)
        info.LeaderId = leaderIdFromMsg
    }
}

func (node *Node) isLeader() bool {
	node.muBallot.RLock()
	defer node.muBallot.Unlock()

	return node.promisedBallotAccept.NodeId == node.nodeId
}


func makeRequestKey(clientId int32, timestamp *timestamppb.Timestamp) string {
	return fmt.Sprintf("%d-%d", clientId, timestamp.AsTime().UnixNano())
}

func (node *Node) requestsAreEqual(r1, r2 *pb.ClientRequest) bool {
    return r1.ClientId == r2.ClientId && r1.Timestamp.AsTime().Equal(r2.Timestamp.AsTime())
}

func (node *Node) areTwoPCPrepareEqual(m1, m2 *pb.TwoPCPrepareMessage) bool{
    return m1.NodeId == m2.NodeId && node.requestsAreEqual(m1.Request,m2.Request)
}

func (node *Node) areTwoPCAbortEqual(m1, m2 *pb.TwoPCAbortMessage) bool{
    return m1.NodeId == m2.NodeId && node.requestsAreEqual(m1.Request,m2.Request)
}

func (node *Node) computeStateDigest() (string, error) {
	iter,_ := node.state.NewIter(nil)
    defer iter.Close()

	hasher := sha256.New()

    for iter.First(); iter.Valid(); iter.Next() {
        if _, err := hasher.Write(iter.Key()); err != nil {
            return "", err
        }
        if _, err := hasher.Write(iter.Value()); err != nil {
            return "", err
        }
    }

    return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (node *Node) isBallotEqual(b1, b2 *pb.BallotNumber) bool {
    if b1 == nil || b2 == nil {
        return false 
    }
    return b1.RoundNumber == b2.RoundNumber && b1.NodeId == b2.NodeId
}

func (node *Node) isBallotGreaterThan(b1, b2 *pb.BallotNumber) bool {
    if b1 == nil || b2 == nil {
        return false
    }
    
    if b1.RoundNumber > b2.RoundNumber {
        return true
    }
    
    if b1.RoundNumber == b2.RoundNumber && b1.NodeId > b2.NodeId {
        return true
    }
    
    return false
}

func (node *Node) isBallotLessThan(b1, b2 *pb.BallotNumber) bool {
    if b1 == nil || b2 == nil {
        return false
    }

    if b1.RoundNumber < b2.RoundNumber {
        return true
    }

    if b1.RoundNumber == b2.RoundNumber && b1.NodeId < b2.NodeId {
        return true
    }

    return false
}


func isReadOnly(req *pb.ClientRequest) bool{
    if req == nil || req.Transaction == nil {
        return false
    }

    return req.Transaction.Sender == req.Transaction.Receiver
}

func isIntraShard(req *pb.ClientRequest) bool {
    if req == nil || req.Transaction == nil {
        return false
    }

    senderInt, _ := strconv.ParseInt(req.Transaction.Sender, 10, 0)
    receiverInt, _ := strconv.ParseInt(req.Transaction.Receiver, 10, 0)

    senderCluster := getClusterId(int32(senderInt))
    receiverCluster := getClusterId(int32(receiverInt))
    
    return senderCluster == receiverCluster
}

func datapointInShard(datapoint string,clusterId int32) bool{
    datapointInt, _ := strconv.ParseInt(datapoint, 10, 0)
    
    targetClusterId := getClusterId(int32(datapointInt))

    return targetClusterId == clusterId
}

func(node *Node) findTargetLeaderId(datapoint string) (int32) {
    datapointInt, _ := strconv.ParseInt(datapoint, 10, 0)
    
    targetClusterId := getClusterId(int32(datapointInt))
    // log.Printf("[Node %d] target cluster Id=%d",node.nodeId,targetClusterId)

    node.muCluster.RLock()
    targetLeaderId := node.clusterInfo[targetClusterId].LeaderId
    node.muCluster.RUnlock()
    // log.Printf("[Node %d] targetLeaderId=%d",node.nodeId,targetLeaderId)
    return targetLeaderId
}

func(node *Node) findTargetClusterIds(datapoint string) []int32 {
    datapointInt, _ := strconv.ParseInt(datapoint, 10, 0)
    
    targetClusterId := getClusterId(int32(datapointInt))
    // log.Printf("[Node %d] target cluster Id=%d",node.nodeId,targetClusterId)

    node.muCluster.RLock()
    targetNodes := node.clusterInfo[targetClusterId].NodeIds
    node.muCluster.RUnlock()
    // log.Printf("[Node %d] targetLeaderId=%d",node.nodeId,targetLeaderId)
    return targetNodes
}

func(node *Node) shouldKeepSendingAbort(req *pb.ClientRequest) bool {
    node.muCrossSharTxs.RLock()
    defer node.muCrossSharTxs.RUnlock()  

    reqKey := makeRequestKey(req.ClientId,req.Timestamp)

    tx,exists := node.crossSharTxs[reqKey]

    if !exists {
        return false
    }
    
    return tx.shouldKeepSendingAbort
}

func(node *Node) shouldKeepSendingCommmit(req *pb.ClientRequest) bool {
    node.muCrossSharTxs.RLock()
    defer node.muCrossSharTxs.RUnlock()  

    reqKey := makeRequestKey(req.ClientId,req.Timestamp)

    tx,exists := node.crossSharTxs[reqKey]

    if !exists {
        return false
    }
    
    return tx.shouldKeepSendingCommmit
}

func(node *Node) markAckReceived(req *pb.ClientRequest) {
    node.muCrossSharTxs.Lock()
    defer node.muCrossSharTxs.Unlock()  

    reqKey := makeRequestKey(req.ClientId,req.Timestamp)

    node.crossSharTxs[reqKey].shouldKeepSendingAbort = false
    node.crossSharTxs[reqKey].shouldKeepSendingCommmit = false
}

func (node *Node) Activate() {
    node.muStatus.Lock()
    defer node.muStatus.Unlock()
    node.status = NodeActive
    log.Printf("[Node %d] ACTIVATED", node.nodeId)
}

func (node *Node) Deactivate() {
    node.muStatus.Lock()
    defer node.muStatus.Unlock()
    node.status = NodeInactive
    log.Printf("[Node %d] DEACTIVATED", node.nodeId)

    if node.livenessTimer.IsRunning(){
        node.livenessTimer.Stop()
    }

    if node.prepareTimer.IsRunning(){
        node.prepareTimer.Stop()
    }
}

func (node *Node) isActive() bool {
    node.muStatus.RLock()
    defer node.muStatus.RUnlock()
    return node.status == NodeActive
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
        twoPCPhase:= entry.TwoPCPhase
        sequenceNum := entry.SequenceNum
        ballotRound := entry.Ballot.RoundNumber
        ballotNodeId := entry.Ballot.NodeId
        entryStatus := entry.Status
        req := entry.Request
        acceptType := entry.EntryAcceptType
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

        twoPCStatus := "X"
        switch twoPCPhase {
        case PhaseExecuted:
            twoPCStatus = "E"
        case PhaseCommitted:
            twoPCStatus = "C"
        case PhaseAccepted:
            twoPCStatus = "A"
		}

        // Format and print based on request type
        if req != nil {
			fmt.Printf("  Seq=%d | Ballot R=%d N=%d | Entry status=%s | AcceptType=%s | 1st R Status=%s | 2nd R Status=%s | (%s, %s, %d)\n",
				sequenceNum, ballotRound,ballotNodeId, entryStatus,acceptType,status,twoPCStatus,
				req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)
        } else {
            fmt.Printf("  Seq=%d | Ballot R=%d N=%d | Entry status=%s | AcceptType=%s | Request status=%s | NO-OP\n",
                sequenceNum, ballotRound,ballotNodeId,entryStatus,acceptType, status)
        }
    }
    
    fmt.Println("-------------------------------------------")
}


func CleanupPersistence() error {
	dirs := []string{"logs", "state", "wal"}

	for _, dir := range dirs {
		if _, err := os.Stat(dir); os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0755); err != nil {
				return fmt.Errorf("failed to create missing dir %s: %v", dir, err)
			}
			continue
		}

		entries, err := os.ReadDir(dir)
		if err != nil {
			return fmt.Errorf("failed to read directory %s: %v", dir, err)
		}

		for _, entry := range entries {
			fullPath := filepath.Join(dir, entry.Name())
			
			if err := os.RemoveAll(fullPath); err != nil {
				return fmt.Errorf("failed to delete %s: %v", fullPath, err)
			}
		}
		
		log.Printf("Cleaned up contents of: %s/", dir)
	}

    if err := os.RemoveAll("runnerlog.log"); err != nil {
        return fmt.Errorf("failed to delete runnerlog.log: %v", err)
    }
    log.Printf("Cleaned up file: runnerlog.log")

	return nil
}