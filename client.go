/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type PendingRequest struct {
	mu           sync.Mutex
	timestamp    *timestamppb.Timestamp   
	reply *pb.ReplyMessage
	isReadOnly bool    
	done         chan struct{}               
	completed    bool
	sender string
	receiver string 
	amount int32
	startTime *timestamppb.Timestamp   
	replyTime *timestamppb.Timestamp                     
}

type TransactionRecord struct {
    StartTime time.Time
    EndTime   time.Time
    IsSuccess bool
}

type Client struct {
	pb.UnimplementedClientServiceServer

	port   int

	muPending   sync.RWMutex
	pendingReqs map[string]*PendingRequest

	muConn sync.Mutex
	serverConns map[int32]*grpc.ClientConn

	muCluster sync.RWMutex
	clusterInfo map[int32]*ClusterInfo

	// for graceful shutdown
	muStop   sync.RWMutex
    stopChan chan struct{}
    stopped  bool

	replyTracker *ReplyTracker

	muPerf sync.RWMutex
	performanceMeasureMap map[string]*TransactionRecord
}


func NewClient(port int) (*Client, error) {
	client := &Client{
		port:        port,
		serverConns: make(map[int32]*grpc.ClientConn),
		pendingReqs: make(map[string]*PendingRequest),
		clusterInfo: make(map[int32]*ClusterInfo),
		performanceMeasureMap: make(map[string]*TransactionRecord),
		stopChan: make(chan struct{}),
        stopped: false, 
		replyTracker: NewReplyTracker(),
	}

	client.buildClientClusterMap()

	return client, nil
}

func (client *Client) startGrpcServer() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", client.port))
	if err != nil {
		return fmt.Errorf("client failed to listen on %d: %w", client.port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterClientServiceServer(grpcServer, client)

	go func() {
		log.Printf("[Client] listening for replies on :%d", client.port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[Client] gRPC serve error: %v", err)
		}
	}()
	return nil
}

func(client *Client) buildClientClusterMap(){
	client.muCluster.Lock()
    defer client.muCluster.Unlock()

	for _, nodeConfig := range getNodeCluster() {
		info, exists := client.clusterInfo[nodeConfig.ClusterId]

		if !exists {
			info = &ClusterInfo{
                ClusterId: nodeConfig.ClusterId,
                NodeIds:   []int32{},
                LeaderId:  1 + 3 * (nodeConfig.ClusterId-1),
            }

			client.clusterInfo[nodeConfig.ClusterId] = info
		}

		info.NodeIds = append(info.NodeIds, nodeConfig.NodeId)
	} 
}


func (client *Client) SendTransaction(tx Transaction) string {
	dataPointId :=client.getReqDataPoint(tx.Sender)

	req := &pb.ClientRequest{
        ClientId: dataPointId,
        Transaction: &pb.Transaction{
            Sender:   tx.Sender,
            Receiver: tx.Receiver,
            Amount:   tx.Amount,
        },
        Timestamp: timestamppb.Now(),
    }

	reqKey := client.getRequestKey(dataPointId,req.Timestamp)

	isReadOnly := isReadOnly(req)
    // Register pending request BEFORE sending
	pending := client.registerRequest(dataPointId,req.Timestamp,isReadOnly,tx.Sender,tx.Receiver,tx.Amount)
	defer client.cleanupRequest(reqKey)

	attemptCount := 0

	for {
		if client.IsStopped() {
            log.Printf("[Client] Stopping write request loop (stopped)")
            return ""
        }

		attemptCount++
        log.Printf("[Client] Write attempt #%d for request (%s, %s, %d)", attemptCount, 
		req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)

		client.muCluster.RLock()
		
		senderIntVal, err := strconv.Atoi(req.Transaction.Sender) 
		if err != nil {
			log.Printf("[Client] Failed to convert value of client sender from string to int %v",err)
			break
		}

		clusterId := getClusterId(int32(senderIntVal))
		log.Printf("[Client] Sending transaction to cluster=%d",clusterId)

		info,exists := client.clusterInfo[clusterId]

		if !exists {
			log.Printf("[Client] Cluster info not present=%d",clusterId)
		}
		log.Printf("[Client] cluster info presnt%v",info.NodeIds)

		targetLeaderId := client.clusterInfo[clusterId].LeaderId
		targetNodeIds := client.clusterInfo[clusterId].NodeIds
		log.Printf("[Client] Sending transaction to cluster=%d leaderId=%d",clusterId,targetLeaderId)

		client.muCluster.RUnlock()
		
		grpcClient, _ := client.getConnForNode(targetLeaderId)
		if grpcClient != nil {
			_, err := grpcClient.SendRequestMessage(context.Background(), req)
			
			if err != nil {
				log.Printf("[Client] Failed to send to leader %d: %v",  targetLeaderId, err)
			} else {
				log.Printf("[Client] Sent request to leader %d",  targetLeaderId)
			}
		}

		result,ok := client.waitForReply(pending, 500*time.Millisecond)
		if ok {
			pending.mu.Lock()	
			client.updateLeaderFromReply(pending.reply)
			pending.mu.Unlock()

			if req.Transaction.Sender == req.Transaction.Receiver {
				log.Printf("[Client] Received result=%s for (%s)",result,req.Transaction.Sender)
			} else {
				log.Printf("[Client] Received result=%s for (%s, %s, %d)",
				result,req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)
			}
			
			
			return result
		}

		if client.IsStopped() {
            log.Printf("[Client] Stopping write request loop (stopped)")
            return ""
        }

		client.broadcastToAllNodes(req,targetNodeIds)

		result,ok = client.waitForReply(pending, 600*time.Millisecond)
		if ok {
			pending.mu.Lock()
			client.updateLeaderFromReply(pending.reply)
			pending.mu.Unlock()
			
			if req.Transaction.Sender == req.Transaction.Receiver {
				log.Printf("[Client] Received result=%s for (%s)",result,req.Transaction.Sender)
			} else {
				log.Printf("[Client] Received result=%s for (%s, %s, %d)",
				result,req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)
			}

			return result
		}
	}

	// Returning garbage in case of some unexpected errors
	return ""
}

func (client *Client) updateLeaderFromReply(response *pb.ReplyMessage) {
    if response == nil {
        return
    }

    client.muCluster.Lock()
    defer client.muCluster.Unlock()

	leaderIdFromResp := response.Ballot.NodeId

	clusterId := getClusterId(response.ClientId)
	log.Printf("[Client] Checking leader update for cluster=%d for clientId=%d",clusterId,response.ClientId)
	existingLeaderId := client.clusterInfo[clusterId].LeaderId
	
    
    if leaderIdFromResp != existingLeaderId {
		log.Printf("[Client] Updating leaderID from %d to %d for cluster=%d",existingLeaderId,leaderIdFromResp,clusterId)
        client.clusterInfo[clusterId].LeaderId = leaderIdFromResp
    }
}

func (client *Client) broadcastToAllNodes(req *pb.ClientRequest,targetNodeIds []int32) {
	for _, nodeId := range targetNodeIds {
		go func(nid int32) {
			grpcClient, _ := client.getConnForNode(nid)
			if grpcClient == nil {
				log.Printf("[Client] No connection to node %d",  nid)
				return
			}
						
			_, err := grpcClient.SendRequestMessage(context.Background(), req)
			if err != nil {
				log.Printf("[Client] Failed to broadcast to node %d: %v",  nid, err)
			} else {
				log.Printf("[Client] Broadcasted request (%s, %s, %d) to node %d", 
				req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount, nid)
			}
		}(nodeId)
	}
}


func (client *Client) waitForReply(pending *PendingRequest, timeout time.Duration) (string, bool) {
	// Check if already completed before waiting
    pending.mu.Lock()
    if pending.completed {
		result := pending.reply.Status
		// pending.replyTime = timestamppb.Now()
        pending.mu.Unlock()
        return result, true
    }
    pending.mu.Unlock()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	
	select {
	case <-pending.done:
		pending.mu.Lock()
		result := pending.reply.Status
		pending.replyTime = timestamppb.Now()

		var status string

		if pending.isReadOnly {
			status = fmt.Sprintf("(%s) : %s",pending.sender,pending.reply.Status)
		} else {
			status = fmt.Sprintf("(%s, %s, %d) : %s",pending.sender,pending.receiver,pending.amount,pending.reply.Status)
		}
		
		client.replyTracker.Add(ReplyRecord{
			Timestamp: time.Now().UnixNano(),
			LeaderId: pending.reply.Ballot.NodeId,
			Status: status,
			ReqKey: makeRequestKey(pending.reply.ClientId,pending.reply.ClientRequestTimestamp),
		})
		pending.mu.Unlock()
		
		return result, true
	case <-timer.C:
		pending.mu.Lock()
		defer pending.mu.Unlock()
		
		log.Printf("[Client] Timeout waiting for reply from server")
		return "", false
	}
}

// Sender in transaction
func(client *Client) getReqDataPoint(dataPointStr string) int32{
	datapointClientId,_ := strconv.ParseInt(dataPointStr, 10, 0)

	return  int32(datapointClientId)
}


func (client *Client) getRequestKey(dataPoint int32,timestamp *timestamppb.Timestamp) string {
    return fmt.Sprintf("%d-%d-%d", dataPoint, timestamp.Seconds, timestamp.Nanos)
}

func (client *Client) createPendingRequest(timestamp *timestamppb.Timestamp,isReadOnly bool,sender string,receiver string,amount int32) *PendingRequest {
	return &PendingRequest{
		timestamp:    timestamp,
		reply:      nil,
		done:         make(chan struct{}),
		completed:    false,
		isReadOnly:isReadOnly,
		sender: sender,
		receiver: receiver,
		amount: amount,
		startTime: timestamppb.Now(),
	}
}

func (client *Client) registerRequest(datapoint int32, timestamp *timestamppb.Timestamp, isReadOnly bool,sender string,receiver string,amount int32) *PendingRequest {
	reqKey := client.getRequestKey(datapoint,timestamp)
	
	client.muPending.Lock()
	defer client.muPending.Unlock()
	
	// Check if already exists (shouldn't happen, but defensive)
	if existing, ok := client.pendingReqs[reqKey]; ok {
		return existing
	}
	
	pending := client.createPendingRequest(timestamp,isReadOnly,sender,receiver,amount)
	client.pendingReqs[reqKey] = pending
	return pending
}

func (client *Client) cleanupRequest(reqKey string) {
	client.muPending.Lock()
	client.muPerf.Lock()

	if pending, exists := client.pendingReqs[reqKey]; exists && pending.replyTime != nil{
		start := pending.startTime.AsTime()
		end := pending.replyTime.AsTime()

		record := &TransactionRecord{
            StartTime: start,
            EndTime:   end,
            IsSuccess: pending.completed,
        }

		client.performanceMeasureMap[reqKey] = record
	}

	client.muPerf.Unlock()

	delete(client.pendingReqs, reqKey)
	client.muPending.Unlock()
}

func (client *Client) getConnForNode(nodeId int32) (pb.MessageServiceClient, *grpc.ClientConn) {
	client.muConn.Lock()
	defer client.muConn.Unlock()

	if conn, ok := client.serverConns[nodeId]; ok && conn != nil {
		return pb.NewMessageServiceClient(conn), conn
	}

	var targetNode *NodeConfig
	for _, config := range getNodeCluster() {
		if config.NodeId == nodeId {
			targetNode = &config
			break
		}
	}
	if targetNode == nil {
		log.Printf("[Client] Node configuration for ID %d not found.",  nodeId)
		return nil, nil
	}

	targetAddr := fmt.Sprintf(":%d", targetNode.PortNo)
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("[Client] Failed to connect to node %d at %s: %v",  nodeId, targetAddr, err)
		return nil, nil
	}

	client.serverConns[nodeId] = conn
	return pb.NewMessageServiceClient(conn), conn
}

func (client *Client) HandleReply(ctx context.Context, reply *pb.ReplyMessage) (*emptypb.Empty, error) {
	// 1. VERIFY REQUEST TIMESTAMP MATCHES
	if reply.ClientRequestTimestamp == nil {
		log.Printf("[Client] Rejected reply: Missing request timestamp from Node %d", 
			 reply.Ballot.NodeId)
		return &emptypb.Empty{}, nil
	}

	// 2. GET PENDING REQUEST
    reqKey := client.getRequestKey(reply.ClientId,reply.ClientRequestTimestamp)

    client.muPending.RLock()
	pending, exists := client.pendingReqs[reqKey]
	client.muPending.RUnlock()
    
    if !exists {
		return &emptypb.Empty{}, nil
	}

	pending.mu.Lock()
	defer pending.mu.Unlock()
	
	if pending.completed {
		return &emptypb.Empty{}, nil
	}
	
	// Check if we already have a reply from this node
	if pending.reply != nil {
		return &emptypb.Empty{}, nil
	}
	
	// Store the reply
	pending.reply = reply
	pending.completed = true
	close(pending.done)
    
	return &emptypb.Empty{}, nil
}

func (client *Client) IsStopped() bool {
    client.muStop.RLock()
    defer client.muStop.RUnlock()
    return client.stopped
}

func (client *Client) Stop() {
    client.muStop.Lock()
    defer client.muStop.Unlock()
    
    if !client.stopped {
        close(client.stopChan)
        client.stopped = true
        log.Printf("[Client] STOPPED")
    }
}

func (client *Client) PrintReplyHistory() {
    client.replyTracker.PrintAll()
}


func (client *Client) closeAllConnections() {
	client.muConn.Lock()
	defer client.muConn.Unlock()
	for _, conn := range client.serverConns {
		if conn != nil {
			conn.Close()
		}
	}
}


func (client *Client) CalculatePerformance(txCount int) {
    client.muPerf.RLock()
    defer client.muPerf.RUnlock()

    count := len(client.performanceMeasureMap)
    if count == 0 {
        fmt.Println("\n[Performance] No completed requests found to calculate metrics.")
        return
    }

    var minStart, maxEnd time.Time
    var totalLatency time.Duration
    first := true

    for _, rec := range client.performanceMeasureMap {
        if first {
            minStart = rec.StartTime
            maxEnd = rec.EndTime
            first = false
        }

        if rec.StartTime.Before(minStart) {
            minStart = rec.StartTime
        }
        
        if rec.EndTime.After(maxEnd) {
            maxEnd = rec.EndTime
        }

        totalLatency += rec.EndTime.Sub(rec.StartTime)
    }

    experimentLength := maxEnd.Sub(minStart)

    throughput := 0.0
    if experimentLength.Seconds() > 0 {
        throughput = float64(txCount) / experimentLength.Seconds()
    }

    // Avg Latency = TotalLatency / Count
    avgLatency := totalLatency / time.Duration(txCount)

    fmt.Println("\n=============================================")
    fmt.Println("          PERFORMANCE METRICS                ")
    fmt.Println("=============================================")
    fmt.Printf("Total Completed Req: %d\n", count)
    fmt.Printf("Experiment Start:    %s\n", minStart.Format("15:04:05.000"))
    fmt.Printf("Experiment End:      %s\n", maxEnd.Format("15:04:05.000"))
    fmt.Printf("Experiment Length:   %v\n", experimentLength)
    fmt.Println("---------------------------------------------")
    fmt.Printf("THROUGHPUT:          %.2f Req/Sec\n", throughput)
    fmt.Printf("AVG LATENCY:         %v\n", avgLatency)
    fmt.Println("=============================================\n")
}