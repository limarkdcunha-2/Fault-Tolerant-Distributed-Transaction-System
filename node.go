/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"
	pb "transaction-processor/message"

	"github.com/cockroachdb/pebble"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type NodeStatus int

const (
    NodeActive NodeStatus = iota
    NodeInactive
    NodeCrashed
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

type PrepareLog struct {
	log []*pb.PrepareMessage
	highestBallotPrepare *pb.PrepareMessage
}

type PromiseLog struct {
	log map[int32]*pb.PromiseMessage
	isPromiseQuorumReached bool
}

type Node struct {
	pb.UnimplementedMessageServiceServer

	nodeId int32
	portNo int32
	N int32
	f int32

	muBallot sync.RWMutex
	myBallot *pb.BallotNumber
	promisedBallotAccept  *pb.BallotNumber
	promisedBallotPrepare *pb.BallotNumber

	muLog sync.RWMutex
	currentSeqNo int32
	acceptLog map[int32]*LogEntry

	// ===== Duplicate request tracking =====
	muReplies sync.RWMutex
	replies map[string]*pb.ReplyMessage

	muPending sync.RWMutex
    pendingRequests map[string]bool

	muExec        sync.Mutex
	lastExecSeqNo int32
	// catchUpExecSeqNo int32

	muState sync.RWMutex
    state   *pebble.DB

	muReqQue sync.RWMutex
	requestsQueue []*pb.ClientRequest

	muLeader sync.RWMutex
	inLeaderElection bool
	isLeaderKnown bool
	livenessTimer *CustomTimer
	
	prepareTimer *CustomTimer

	muPreLog sync.RWMutex
	prepareLog PrepareLog

	muProLog sync.RWMutex
	promiseLog PromiseLog

	// Project helpers
	muStatus sync.RWMutex
    status   NodeStatus

	peers map[int32]pb.MessageServiceClient

	muConn sync.Mutex
	clientConns map[int32]*grpc.ClientConn
}	


func NewNode(nodeId, portNo int32) (*Node, error) {
	// nodeIdStr := strconv.Itoa(int(nodeId))
	N := int32(len(getNodeCluster()))
	f := (N - 1) / 2

	myBallot := &pb.BallotNumber{
		NodeId: nodeId,
		RoundNumber: 0,
	}

	defaultPromisedBallot := &pb.BallotNumber{
		NodeId: 0,
		RoundNumber: 0,
	}

	newNode :=  &Node{
		nodeId: nodeId,
		N:N,
		f:f,
		portNo: portNo,
		myBallot: myBallot,
		promisedBallotAccept:defaultPromisedBallot,
		promisedBallotPrepare: defaultPromisedBallot,
		currentSeqNo: 0,
		lastExecSeqNo:0,
		inLeaderElection:false,
		isLeaderKnown:false,
		// state:make(map[string]int32),
		acceptLog:make(map[int32]*LogEntry),
		peers: make( map[int32]pb.MessageServiceClient),
		replies: make(map[string]*pb.ReplyMessage),
		pendingRequests: make(map[string]bool),
		clientConns:make(map[int32]*grpc.ClientConn),
		requestsQueue:make([]*pb.ClientRequest, 0),
	}

	randomTime := time.Duration(rand.Intn(100)+100) * time.Millisecond
	newNode.livenessTimer = NewCustomTimer(randomTime,newNode.onLivenessTimerExpired)

	newNode.prepareTimer = NewCustomTimer(50 * time.Millisecond,newNode.doNothing)

	newNode.prepareLog = PrepareLog{
		log: make([]*pb.PrepareMessage,0),
		highestBallotPrepare: nil,
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

func (node *Node) isLeader() bool {
	node.muBallot.RLock()
	defer node.muBallot.Unlock()

	return node.promisedBallotAccept.NodeId == node.nodeId
}


func (node *Node) GetCachedReply(clientId int32, timestamp  *timestamppb.Timestamp) (*pb.ReplyMessage, bool) {
	key := makeRequestKey(clientId, timestamp)

	node.muReplies.RLock()
	defer node.muReplies.RUnlock()

	reply, exists := node.replies[key]
	return reply, exists
}

func (node *Node) checkIfPending(clientId int32, timestamp *timestamppb.Timestamp) bool {
    node.muPending.RLock()
    defer node.muPending.RUnlock()
    
    key := makeRequestKey(clientId, timestamp)
    _, exists := node.pendingRequests[key]
    return exists
}

func (node *Node) hasPendingWork() bool {
    node.muPending.RLock()
    pendingCount := len(node.pendingRequests)
    node.muPending.RUnlock()

    return pendingCount > 0 
}

func (node *Node) markRequestPending(clientId int32, timestamp *timestamppb.Timestamp) {
    node.muPending.Lock()
    defer node.muPending.Unlock()
    
    key := makeRequestKey(clientId, timestamp)
    node.pendingRequests[key] = true
    
    log.Printf("[Node %d] Marked request from client %d as PENDING", 
        node.nodeId, clientId)
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

func (node *Node) requestsAreEqual(r1, r2 *pb.ClientRequest) bool {
    return r1.ClientId == r2.ClientId && r1.Timestamp.AsTime().Equal(r2.Timestamp.AsTime())
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