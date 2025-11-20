/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"log"
	"sync"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type Phase int

const (
	PhaseNone        Phase = iota 
	PhaseAccepted             
	PhaseCommitted                
	PhaseExecuted				  
)


type LogEntry struct {
	mu sync.Mutex
	SequenceNum  int32
	Ballot *pb.BallotNumber
	Request *pb.ClientRequest
	AcceptedMessages map[int32] *pb.AcceptedMessage
	AcceptCount int32
	Phase Phase
}

type Node struct {
	pb.UnimplementedMessageServiceServer

	nodeId int32
	portNo int32
	N int32
	f int32

	muLeader sync.RWMutex
	ballot *pb.BallotNumber
	// inLeaderElection bool

	muLog sync.RWMutex
	currentSeqNo int32
	acceptLog map[int32]*LogEntry

	// ===== Duplicate request tracking =====
	muReplies sync.RWMutex
	replies map[string]*pb.ReplyMessage

	muPending sync.RWMutex
    pendingRequests map[string]int32

	peers map[int32]pb.MessageServiceClient

	muConn sync.Mutex
	clientConns map[int32]*grpc.ClientConn
}	


func NewNode(nodeId, portNo int32) (*Node, error) {
	// nodeIdStr := strconv.Itoa(int(nodeId))
	N := int32(len(getNodeCluster()))
	f := (N - 1) / 2

	ballot := &pb.BallotNumber{
		// Change later
		NodeId: 1,
		RoundNumber: 0,
	}

	newNode :=  &Node{
		nodeId: nodeId,
		N:N,
		f:f,
		portNo: portNo,
		ballot: ballot,
		currentSeqNo: 0,
		acceptLog:make(map[int32]*LogEntry),
		peers: make( map[int32]pb.MessageServiceClient),
		replies: make(map[string]*pb.ReplyMessage),
		pendingRequests: make(map[string]int32),
		clientConns:make(map[int32]*grpc.ClientConn),
	}

	return newNode,nil
}

func makeRequestKey(clientId int32, timestamp *timestamppb.Timestamp) string {
	return fmt.Sprintf("%d-%d", clientId, timestamp.AsTime().UnixNano())
}


func (node *Node) GetCachedReply(clientId int32, timestamp  *timestamppb.Timestamp) (*pb.ReplyMessage, bool) {
	key := makeRequestKey(clientId, timestamp)

	node.muReplies.RLock()
	defer node.muReplies.RUnlock()

	reply, exists := node.replies[key]
	return reply, exists
}

func (node *Node) getPendingSeqNum(clientId int32, timestamp *timestamppb.Timestamp) (int32, bool) {
    node.muPending.RLock()
    defer node.muPending.RUnlock()
    
    key := makeRequestKey(clientId, timestamp)
    seqNum, exists := node.pendingRequests[key]
    return seqNum, exists
}

func (node *Node) markRequestPending(clientId int32, timestamp *timestamppb.Timestamp, seqNum int32) {
    node.muPending.Lock()
    defer node.muPending.Unlock()
    
    key := makeRequestKey(clientId, timestamp)
    node.pendingRequests[key] = seqNum
    
    log.Printf("Node %d: Marked request from client %d as PENDING (seq %d)", 
        node.nodeId, clientId, seqNum)
}


func(node *Node) isBallotGreaterOrEqual(ballotFromMsg *pb.BallotNumber) bool {
	node.muLeader.RLock()	
	selfBallot := node.ballot
	node.muLeader.RUnlock()

	if selfBallot.RoundNumber != ballotFromMsg.RoundNumber {
		return ballotFromMsg.RoundNumber >= selfBallot.RoundNumber
	}

	return ballotFromMsg.NodeId >= selfBallot.NodeId
}

