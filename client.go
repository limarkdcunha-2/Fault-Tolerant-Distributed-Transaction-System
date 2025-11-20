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
	"sync"
	"time"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClientConfig struct {
	ClientId   int
	ClientName string
	Port       int
}


type PendingRequest struct {
	mu           sync.Mutex
	timestamp    *timestamppb.Timestamp   
	reply *pb.ReplyMessage    
	done         chan struct{}               
	completed    bool                      
}

type Client struct {
	pb.UnimplementedClientServiceServer

	id     int
	name   string
	port   int

	muLeader sync.RWMutex
	leaderId int32

	muConn sync.Mutex
	serverConns map[int32]*grpc.ClientConn

	muPending   sync.RWMutex
	pendingReqs map[string]*PendingRequest

	// for graceful shutdown
	// muStop   sync.RWMutex
    // stopChan chan struct{}
    // stopped  bool
}


func NewClient(id int, name string, port int) (*Client, error) {
	
	return &Client{
		id:          id,
		name:        name,
		leaderId:      1,
		port:        port,
		serverConns: make(map[int32]*grpc.ClientConn),
		pendingReqs: make(map[string]*PendingRequest),
		// stopChan: make(chan struct{}),
        // stopped: false, 
		// replyTracker: NewReplyTracker(),
	}, nil
}

func (client *Client) startGrpcServer() error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", client.port))
	if err != nil {
		return fmt.Errorf("client %s failed to listen on %d: %w", client.name, client.port, err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterClientServiceServer(grpcServer, client)

	go func() {
		log.Printf("[Client %s] listening for replies on :%d", client.name, client.port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("[Client %s] gRPC serve error: %v", client.name, err)
		}
	}()

	// small warm-up pause tolerable
	time.Sleep(10 * time.Millisecond)
	return nil
}

func buildClients() map[string]*Client {
	clients := make(map[string]*Client)

    for _, cfg := range getClientCluster() {
		newClient,err := NewClient(cfg.ClientId, cfg.ClientName,cfg.Port)
        if err != nil {
            log.Fatalf("Failed to create client %s: %v", cfg.ClientName, err)
        }

        clients[cfg.ClientName] = newClient
    }

	return clients
}


func (client *Client) SendTransaction(tx Transaction) string {
	req := &pb.ClientRequest{
        ClientId: int32(client.id),
        Transaction: &pb.Transaction{
            Sender:   tx.Sender,
            Receiver: tx.Receiver,
            Amount:   int32(tx.Amount),
        },
        Timestamp: timestamppb.Now(),
    }

	reqKey := client.getRequestKey(req.Timestamp)

    // Register pending request BEFORE sending
	pending := client.registerRequest(req.Timestamp,false,false)
	defer client.cleanupRequest(reqKey)

	attemptCount := 0

	for {
		// if client.IsStopped() {
        //     log.Printf("[Client %d] Stopping write request loop (stopped)", client.id)
        //     return ""
        // }

		attemptCount++
        log.Printf("[Client %d] Write attempt #%d for request %s", client.id, attemptCount, reqKey)

		client.muLeader.RLock()
		leaderId := client.leaderId
		client.muLeader.RUnlock()
		
		grpcClient, _ := client.getConnForNode(leaderId)

		if grpcClient != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, err := grpcClient.SendRequestMessage(ctx, req)
			cancel()
			
			if err != nil {
				log.Printf("[Client %d] Failed to send to leader %d: %v", client.id, leaderId, err)
			} else {
				log.Printf("[Client %d] Sent request to leader %d", client.id, leaderId)
			}
		}

		result,ok := client.waitForReply(pending, 300*time.Millisecond)

		if ok {
			pending.mu.Lock()
			client.updateLeaderFromReply(pending.reply)
			pending.mu.Unlock()
			
			return result
		}

		client.broadcastToAllNodes(req)

		result,ok = client.waitForReply(pending, 300*time.Millisecond)

		if ok {
			pending.mu.Lock()
			client.updateLeaderFromReply(pending.reply)
			pending.mu.Unlock()
			
			return result
		}
	}
}

func (client *Client) updateLeaderFromReply(response *pb.ReplyMessage) {
    if response == nil {
        return
    }

    client.muLeader.Lock()
    defer client.muLeader.Unlock()

	leaderId := response.Ballot.NodeId
    
    if leaderId != client.leaderId {
		log.Printf("[Client %d] Updating leaderID from %d to %d",client.id,client.leaderId,leaderId)
        client.leaderId = leaderId
    }
}

func (client *Client) broadcastToAllNodes(req *pb.ClientRequest) {
	nodeIDs := getAllNodeIDs()
	
	for _, nodeId := range nodeIDs {
		go func(nid int32) {
			grpcClient, _ := client.getConnForNode(nid)
			if grpcClient == nil {
				log.Printf("[Client %d] No connection to node %d", client.id, nid)
				return
			}
			
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			
			_, err := grpcClient.SendRequestMessage(ctx, req)
			if err != nil {
				log.Printf("[Client %d] Failed to broadcast to node %d: %v", client.id, nid, err)
			} else {
				log.Printf("[Client %d] Broadcasted request to node %d", client.id, nid)
			}
		}(nodeId)
	}
}


func (client *Client) waitForReply(pending *PendingRequest, timeout time.Duration) (string, bool) {
	// Check if already completed before waiting
    pending.mu.Lock()
    if pending.completed {
		result := pending.reply.Status
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
		pending.mu.Unlock()
		
		return result, true
	case <-timer.C:
		pending.mu.Lock()
		defer pending.mu.Unlock()
		
		log.Printf("[Client %d] Timeout waiting for reply from client")
		return "", false
	}
}


// HandleReply

func (client *Client) getRequestKey(timestamp *timestamppb.Timestamp) string {
    return fmt.Sprintf("%d-%d-%d", client.id, timestamp.Seconds, timestamp.Nanos)
}

func (client *Client) createPendingRequest(timestamp *timestamppb.Timestamp,isReadOnly bool, isRetry bool) *PendingRequest {
	return &PendingRequest{
		timestamp:    timestamp,
		reply:      nil,
		done:         make(chan struct{}),
		completed:    false,
	}
}

func (client *Client) registerRequest(timestamp *timestamppb.Timestamp,isReadOnly bool, isRetry bool) *PendingRequest {
	reqKey := client.getRequestKey(timestamp)
	
	client.muPending.Lock()
	defer client.muPending.Unlock()
	
	// Check if already exists (shouldn't happen, but defensive)
	if existing, ok := client.pendingReqs[reqKey]; ok {
		return existing
	}
	
	pending := client.createPendingRequest(timestamp,isReadOnly,isRetry)
	client.pendingReqs[reqKey] = pending
	return pending
}

func (client *Client) cleanupRequest(reqKey string) {
	client.muPending.Lock()
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
		log.Printf("[Client %d] Node configuration for ID %d not found.", client.id, nodeId)
		return nil, nil
	}

	targetAddr := fmt.Sprintf(":%d", targetNode.PortNo)
	conn, err := grpc.NewClient(targetAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))

	if err != nil {
		log.Printf("[Client %d] Failed to connect to node %d at %s: %v", client.id, nodeId, targetAddr, err)
		return nil, nil
	}

	client.serverConns[nodeId] = conn
	return pb.NewMessageServiceClient(conn), conn
}

func (client *Client) HandleReply(ctx context.Context, reply *pb.ReplyMessage) (*emptypb.Empty, error) {
	// 1. VERIFY REQUEST TIMESTAMP MATCHES
	if reply.ClientRequestTimestamp == nil {
		log.Printf("[Client %d] Rejected reply: Missing request timestamp from Node %d", 
			client.id, reply.Ballot.NodeId)
		return &emptypb.Empty{}, nil
	}

	// 2. GET PENDING REQUEST
    reqKey := client.getRequestKey(reply.ClientRequestTimestamp)

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

func (client *Client) closeAllConnections() {
	client.muConn.Lock()
	defer client.muConn.Unlock()
	for _, conn := range client.serverConns {
		if conn != nil {
			conn.Close()
		}
	}
}