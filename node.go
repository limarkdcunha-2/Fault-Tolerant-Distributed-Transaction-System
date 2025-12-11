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

type AbortAction struct {
	seqNum int32
	reqKey string
	sender string
	receiver string
	isStateRevertNeeded bool
}

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

type LogEntryStatus string

const (
    LogEntryPresent LogEntryStatus = "PRESENT"
    LogEntryDeleted LogEntryStatus = "DELETED"
)

type LockEntry struct {
    mu sync.Mutex
    holder string
}


type LogEntry struct {
	mu sync.RWMutex
	SequenceNum  int32
	Ballot *pb.BallotNumber
	Request *pb.ClientRequest
	AcceptedMessages map[int32] *pb.AcceptedMessage
	AcceptCount int32
	Phase Phase
	TwoPCAcceptedMessages map[int32] *pb.AcceptedMessage
	TwoPCAcceptCount int32
	TwoPCPhase Phase
	Status LogEntryStatus
	EntryAcceptType pb.AcceptType
}

type PrepareLog struct {
	log []*pb.PrepareMessage
	highestBallotPrepare *pb.PrepareMessage
}

type PromiseLog struct {
	log map[int32]*pb.PromiseMessage
	isPromiseQuorumReached bool
}

type CrossShardTrans struct {
	SequenceNum int32
	Ballot *pb.BallotNumber
	Request *pb.ClientRequest
	Timer *CustomTimer
	shouldKeepSendingAbort bool
	shouldKeepSendingCommmit bool
	isCommitOrAbortReceived bool
}

type lockReleasePair struct {
    sender   string
    receiver string
}

type Node struct {
	pb.UnimplementedMessageServiceServer

	nodeId int32
	portNo int32
	N int32
	f int32

	muBallot sync.RWMutex
	// myBallot *pb.BallotNumber
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

	// muReqQue sync.RWMutex

	requestQueue chan *pb.ClientRequest
	replyQueue chan *pb.ReplyMessage
	execSignal chan struct{}

	muPendingLeaderReq sync.RWMutex
	pendingLeaderRequests map[string]*pb.ClientRequest

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
	// muLocks sync.RWMutex
	locks [9001]*LockEntry

	muStatus sync.RWMutex
    status   NodeStatus

	muCheckpoint sync.RWMutex
	checkpointInterval int32
	latestCheckpointMessage *pb.CheckpointMessage
	lastStableSnapshot     *pebble.Snapshot

	muCluster sync.RWMutex
	clusterId int32
    clusterInfo map[int32]*ClusterInfo

	muTwoPcPrepareQueue sync.RWMutex
	twoPcPrepareQueue []*pb.TwoPCPrepareMessage

	muTwoPcAbortQueue sync.RWMutex
	twoPcAbortQueue []*pb.TwoPCAbortMessage

	muCrossSharTxs sync.RWMutex
	crossSharTxs map[string]*CrossShardTrans

	// Only leader of coordinator cluster has to maintain
	muPendingAckReplies sync.RWMutex
	pendingAckReplies map[string]*pb.ReplyMessage

	muTwoPcPreparedCache sync.RWMutex
	twoPCPreparedCache map[string]*pb.TwoPCPreparedMessage

	muAck sync.RWMutex
	ackReplies map[string]*pb.TwoPCAckMessage

	muFirstTimeAbortAck sync.RWMutex
	shouldSendAckForFirstTimeAbort map[string]bool

	// muReadOnly sync.RWMutex
	// readOnlyMap map[string]bool
	muShardMap  sync.RWMutex
    shardMap    map[string]int32

	wal *WriteAheadLog

	peers map[int32]pb.MessageServiceClient
	clientSideGrpcClient pb.ClientServiceClient

	muForce sync.RWMutex
	forceStopCommitAndAbort bool

	newViewTracker *NewViewTracker
}	


func NewNode(nodeId, portNo int32) (*Node, error) {
	// myBallot := &pb.BallotNumber{
	// 	NodeId: nodeId,
	// 	RoundNumber: 0,
	// }

	defaultPromisedBallot := &pb.BallotNumber{
		NodeId: 0,
		RoundNumber: 0,
	}

	newNode :=  &Node{
		nodeId: nodeId,
		portNo: portNo,
		// myBallot: myBallot,
		promisedBallotAccept:defaultPromisedBallot,
		promisedBallotPrepare: defaultPromisedBallot,
		currentSeqNo: 0,
		lastExecSeqNo:0,
		inLeaderElection:false,
		isLeaderKnown:false,
		checkpointInterval:100,
		latestCheckpointMessage: &pb.CheckpointMessage{
			SequenceNum: 0,
			Digest: "",
		},	
		acceptLog:make(map[int32]*LogEntry),
		peers: make( map[int32]pb.MessageServiceClient),
		replies: make(map[string]*pb.ReplyMessage),
		pendingRequests: make(map[string]bool),
		// This size is impt
		requestQueue:make(chan *pb.ClientRequest, 5000),
		replyQueue: make(chan *pb.ReplyMessage, 5000),
		execSignal: make(chan struct{}, 1),
		pendingLeaderRequests: make(map[string]*pb.ClientRequest),
		clusterInfo: make(map[int32]*ClusterInfo),
		crossSharTxs:make(map[string]*CrossShardTrans),
		pendingAckReplies:make(map[string]*pb.ReplyMessage),
		ackReplies:make(map[string]*pb.TwoPCAckMessage),
		twoPCPreparedCache:make(map[string]*pb.TwoPCPreparedMessage),
		shouldSendAckForFirstTimeAbort:make(map[string]bool),
		twoPcAbortQueue:make([]*pb.TwoPCAbortMessage, 0),
		twoPcPrepareQueue: make([]*pb.TwoPCPrepareMessage, 0),
		shardMap: make(map[string]int32),
		forceStopCommitAndAbort: false,
		// locks: make(map[string]string),
	}

	randomTime := time.Duration(rand.Intn(400)+200) * time.Millisecond
	newNode.livenessTimer = NewCustomTimer(randomTime,newNode.onLivenessTimerExpired)

	newNode.prepareTimer = NewCustomTimer(50 * time.Millisecond,newNode.doNothing)

	newNode.muCluster.Lock()
	newNode.clusterId = newNode.getMyClusterId()
	newNode.muCluster.Unlock()

	newNode.initializeShardMap()

	newNode.prepareLog = PrepareLog{
		log: make([]*pb.PrepareMessage,0),
		highestBallotPrepare: nil,
	}

	for i := 0; i < 9001; i++ {
        newNode.locks[i] = &LockEntry{
            holder: "", 
        }
    }

	dataDir := fmt.Sprintf("./wal/node_%d", nodeId)
	wal, _ := NewWriteAheadLog(nodeId, dataDir)
	newNode.wal = wal

	
	newNode.buildClusterMap()

	// Building gprc connections
	newNode.buildPeerGrpcConnections()
	newNode.registerClientSideGrpcConnection()

	lenNodes := len(newNode.getAllClusterNodes())
	newNode.N = int32(lenNodes)
	newNode.f = (newNode.N  - 1) / 2
	newNode.newViewTracker = NewNewViewTracker()

	// TO DO make this shard size (3000) dynamic
	if err := newNode.loadState(newNode.clusterId,int32(3000)); err != nil {
        return nil, fmt.Errorf("failed to load initial state: %w", err)
    }
    log.Printf("[Node %d] State successfully loaded into memory.", newNode.nodeId)

	go newNode.runRequestProcessor()
	go newNode.runExecutionLoop()

	for i := 0; i < 20; i++ {
        go newNode.runReplyWorker()
    }

	return newNode,nil
}

func (node *Node) runReplyWorker() {
    for reply := range node.replyQueue {
        node.sendReplyToClient(reply) 
    }
}


func (node *Node) initializeShardMap() {
    node.muShardMap.Lock()
    defer node.muShardMap.Unlock()
    
    for i := 1; i <= 9000; i++ {
        accountID := strconv.Itoa(i)
        clusterID := getClusterId(int32(i))
        node.shardMap[accountID] = clusterID
    }
    
    log.Printf("[Node %d] Initialized shard map with range partitioning", node.nodeId)
}

func(node *Node) buildPeerGrpcConnections(){
	// Building peer (grpc) connections
	for _, nodeConfig := range getNodeCluster() {
        if nodeConfig.NodeId == node.nodeId {
            continue
        }

        addr := ":" + strconv.Itoa(int(nodeConfig.PortNo))

        conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("Warning: Could not connect to peer %d on startup: %v", nodeConfig.NodeId, err)
        }

        client := pb.NewMessageServiceClient(conn)

        node.peers[nodeConfig.NodeId] = client
    }
}

func(node *Node) buildClusterMap(){
	node.muCluster.Lock()
    defer node.muCluster.Unlock()

	for _, nodeConfig := range getNodeCluster() {
		info, exists := node.clusterInfo[nodeConfig.ClusterId]

		if !exists {
			info = &ClusterInfo{
                ClusterId: nodeConfig.ClusterId,
                NodeIds:   []int32{},
                LeaderId:  (nodeConfig.ClusterId-1)*3+1,
            }

			node.clusterInfo[nodeConfig.ClusterId] = info
		}

		info.NodeIds = append(info.NodeIds, nodeConfig.NodeId)
	} 
}

func (node *Node) getMyClusterId() int32 {
    for _, cfg := range getNodeCluster() {
        if cfg.NodeId == node.nodeId {
            return cfg.ClusterId
        }
    }
    log.Fatalf("[Node %d] FATAL: cannot find own ClusterId in getNodeCluster()", node.nodeId)
    return -1
}

func (node *Node) getAccountCluster(accountID string) int32 {
    node.muShardMap.RLock()
    defer node.muShardMap.RUnlock()
    
    if cluster, exists := node.shardMap[accountID]; exists {
        return cluster
    }
    
    id, err := strconv.Atoi(accountID)
    if err != nil {
        return -1
    }
    return getClusterId(int32(id))
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

func (node *Node) hasPendingWorkBatchedVersion(pendingCount int,batchLen int) bool {
	return pendingCount > batchLen
}

func (node *Node) markRequestPending(clientId int32, timestamp *timestamppb.Timestamp) {
    node.muPending.Lock()
    defer node.muPending.Unlock()
    
    key := makeRequestKey(clientId, timestamp)
    node.pendingRequests[key] = true
    
    log.Printf("[Node %d] Marked request from client %d as PENDING", 
        node.nodeId, clientId)
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


func (node *Node) registerClientSideGrpcConnection() {
	if node.clientSideGrpcClient != nil {
		return
	}

	// 9000 is port for client grpcserver
	targetAddr := fmt.Sprintf(":%d", 9000)
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("[Ndde %d] Failed to connect to client at %s: %v", node.nodeId, targetAddr, err)
		return
	}

	node.clientSideGrpcClient = pb.NewClientServiceClient(conn)
}