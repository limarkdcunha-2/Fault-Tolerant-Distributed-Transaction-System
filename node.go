/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

	muExec        sync.Mutex
	lastExecSeqNo int32
	// catchUpExecSeqNo int32

	muState sync.RWMutex
    state   map[string]int32

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
		lastExecSeqNo:0,
		state:make(map[string]int32),
		acceptLog:make(map[int32]*LogEntry),
		peers: make( map[int32]pb.MessageServiceClient),
		replies: make(map[string]*pb.ReplyMessage),
		pendingRequests: make(map[string]int32),
		clientConns:make(map[int32]*grpc.ClientConn),
	}
	
	// Building peer connections
	for _, nodeConfig := range getNodeCluster() {
        if nodeConfig.NodeId == nodeId {
            continue
        }

        addr := ":" + strconv.Itoa(int(nodeConfig.PortNo))

        conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("Warning: Could not connect to peer %d on startup: %v", nodeConfig.NodeId, err)
        }

        client := pb.NewMessageServiceClient(conn)

        newNode.peers[nodeConfig.NodeId] = client
    }

	// Loading state TO DO update with SQLlite
	if err := newNode.loadState(); err != nil {
        return nil, fmt.Errorf("failed to load initial state: %w", err)
    }
    log.Printf("[Node %d] State successfully loaded into memory.", newNode.nodeId)

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


func(node *Node) isBallotGreaterOrEqual(ballotFromMsg *pb.BallotNumber,ballotFromNode *pb.BallotNumber,) bool {
	if ballotFromNode.RoundNumber != ballotFromMsg.RoundNumber {
		return ballotFromMsg.RoundNumber >= ballotFromNode.RoundNumber
	}

	return ballotFromMsg.NodeId >= ballotFromNode.NodeId
}

func (node *Node) RecordReply(reply *pb.ReplyMessage) {
	key := makeRequestKey(reply.ClientId, reply.ClientRequestTimestamp)

	node.muReplies.Lock()
	defer node.muReplies.Unlock()

	node.replies[key] = reply
}

func (node *Node) removePendingRequest(clientId int32, timestamp *timestamppb.Timestamp) {
    node.muPending.Lock()
    defer node.muPending.Unlock()
    
    key := makeRequestKey(clientId, timestamp)
    delete(node.pendingRequests, key)
    
    log.Printf("Node %d: Removed PENDING request from client %d", node.nodeId, clientId)
}


func (node *Node) cacheReplyAndClearPending(reply *pb.ReplyMessage) {
    // Remove from pending
    node.removePendingRequest(reply.ClientId, reply.ClientRequestTimestamp)
    
   node.RecordReply(reply)
}

func (node *Node) getConnForClient(targetClientId int32) (pb.ClientServiceClient, *grpc.ClientConn) {
	node.muConn.Lock()
	defer node.muConn.Unlock()

	if conn, ok := node.clientConns[targetClientId]; ok && conn != nil {
		return pb.NewClientServiceClient(conn), conn
	}

	var targetClient *ClientConfig
	for _, config := range getClientCluster() {
		if config.ClientId == int(targetClientId) {
			targetClient = &config
			break
		}
	}
	if targetClient == nil {
		log.Printf("[Node %d] Client configuration for ID %d not found.",node.nodeId , targetClientId)
		return nil, nil
	}

	targetAddr := fmt.Sprintf(":%d", targetClient.Port)
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("[NOde %d] Failed to connect to client %d at %s: %v", node.nodeId, targetClientId, targetAddr, err)
		return nil, nil
	}

	node.clientConns[targetClientId] = conn
	return pb.NewClientServiceClient(conn), conn
}
