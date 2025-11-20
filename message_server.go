/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"context"
	"log"
	pb "transaction-processor/message"

	"google.golang.org/protobuf/types/known/emptypb"
)

func (node *Node) SendRequestMessage(ctx context.Context, req *pb.ClientRequest) (*emptypb.Empty, error) {
	// if !localNode.IsActive() {
	// 	// <-ctx.Done()
	// 	log.Printf("Node is inactive. Dropping REQUEST message.")
    //     return nil, errors.New("node inactive")
    // }

	// if inViewChange {
	// 	log.Printf("[Node %d] In VIEW CHANGE PHASE dropping CLIENT REQUEST message",node.nodeId)
    //     return &emptypb.Empty{}, nil
	// }

	log.Printf("[Node %d] Received client request from Client %d (ts=%s) [Sender=%s, Receiver=%s, Amount=%d]",
        node.nodeId, req.ClientId, req.Timestamp.AsTime(),
        req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)

	node.muLeader.RLock()
	leaderId := node.ballot.NodeId
	node.muLeader.RUnlock()
	
	// 1. Check if a leader exists
	if leaderId == 0 {
		// TO DO No leader present start leader election
	}

	// 2. Check if already processed this request before
	// if cachedReply, exists := node.GetCachedReply(req.ClientId, req.Timestamp); exists {
	// 	log.Printf("[Node %d] Duplicate request from client %d (ts=%s). Returning cached reply.",
	// 		node.nodeId, req.ClientId, req.Timestamp.AsTime())

    //     // go node.sendReplyToClient(cachedReply)
    //     return &emptypb.Empty{}, nil
    // }

	// 3. Check pending requests (assigned seq but not executed)
    if seqNum, isPending := node.getPendingSeqNum(req.ClientId, req.Timestamp); isPending {
        log.Printf("Node %d: Duplicate (pending seq %d) from client %d - ignoring retry",
            node.nodeId, seqNum, req.ClientId)
        return &emptypb.Empty{}, nil  // Don't process again
    }

	// 4. Reroute the request to leader
	if node.nodeId != leaderId {
		log.Printf("[Node %d] Not primary, routing client request to primary", node.nodeId)

		return node.peers[leaderId].SendRequestMessage(ctx,req)
	}

	// 5. Assign seq number
	node.muLog.Lock()
	node.currentSeqNo++
	seq := node.currentSeqNo
	node.muLog.Unlock()

	// 6. Mark this request in processing
	node.markRequestPending(req.ClientId, req.Timestamp, seq)

	acceptMessage := &pb.AcceptMessage{
		Ballot: &pb.BallotNumber{
			NodeId: node.ballot.NodeId,
			RoundNumber: node.ballot.RoundNumber,
		},
		SequenceNum: seq,
		Request:req,
	}

	// 7. Log self accepted message into accept log
	selfAcceptedMessage := &pb.AcceptedMessage{
		Ballot: acceptMessage.Ballot,
		SequenceNum: acceptMessage.SequenceNum,
		Request:acceptMessage.Request,
		NodeId: node.nodeId,
	}

	acceptedMessages := make(map[int32]*pb.AcceptedMessage)
	acceptedMessages[node.nodeId] = selfAcceptedMessage 
	acceptCount := int32(len(acceptedMessages))

	newEntry := &LogEntry{
		SequenceNum: acceptMessage.SequenceNum,
		Ballot: acceptMessage.Ballot,
		Request: acceptMessage.Request,
		AcceptedMessages: acceptedMessages,
		AcceptCount: acceptCount,
		Phase: PhaseNone,
	}

	
	node.muLog.Lock()
	node.acceptLog[acceptMessage.SequenceNum] = newEntry;
	node.muLog.Unlock()

	// 8. Broadcast accept message
	go node.broadcastAcceptMessage(acceptMessage)
	
	return &emptypb.Empty{}, nil
}

func (node *Node) broadcastAcceptMessage(msg *pb.AcceptMessage){
	allNodes := getAllNodeIDs()

	log.Printf("[Node %d] Broadcasting ACCEPT seq=%d", 
                node.nodeId, msg.SequenceNum)

	for _, nodeId := range allNodes {
 		if nodeId == node.nodeId {
            continue
        }

		peerClient, ok := node.peers[nodeId]
        if !ok {
            log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping Collected-prepare broadcast.", 
                node.nodeId, nodeId)
            continue
        }

		go func(id int32, client pb.MessageServiceClient) {
            
            
            _, err := client.HandleAccept(context.Background(), msg)
            
            if err != nil {
                log.Printf("[Node %d] FAILED to send ACCEPT seq=%d to Node %d: %v", 
                    node.nodeId, msg.SequenceNum, id, err)
            }
        }(nodeId, peerClient)
	}
}


func(node *Node) HandleAccept(ctx context.Context,msg *pb.AcceptMessage) (*emptypb.Empty, error) {
	// if !node.IsActive() {
    //     log.Printf("[Node %d] is inactive. Dropping ACCEPT message",node.nodeId)
    //     return &emptypb.Empty{}, nil
    // }

	log.Printf("[Node] Received ACCEPT: ballot=%d, seq=%d from leader=%d",
		msg.Ballot.RoundNumber, msg.SequenceNum,msg.Ballot.NodeId)

	// 1. If valid ballot number
	if !node.isBallotGreaterOrEqual(msg.Ballot) {
		log.Printf("[Node %d] Rejecting ACCEPT message with old ballot number",node.nodeId)
		
		return &emptypb.Empty{}, nil
	}

	// TO DO
	// 2. Timer logic
	// Timer logic will come here 

	// 3. Mark as in progress if not done before
	// if !node.isPendingRequest(prePrepareMsg.Request.ClientId, prePrepareMsg.Request.Timestamp) {
	// 		node.markRequestPending(prePrepareMsg.Request.ClientId, prePrepareMsg.Request.Timestamp, prePrepareMsg.SequenceNum)
	// }

	// 4. Check if already accepted this message
	node.muLog.Lock()
	existingEntry, exists := node.acceptLog[msg.SequenceNum];

	if exists {
		existingEntry.mu.Lock()
		node.muLog.Unlock()

		// Check if already acceped then send accepted message again
		if existingEntry.Phase >= PhaseAccepted {
			log.Printf("[Node %d] Seq=%d already in status %v, not overwriting to accepted",
                node.nodeId, msg.SequenceNum, existingEntry.Phase)

			acceptedMessage := &pb.AcceptedMessage{
				Ballot: existingEntry.Ballot,
				SequenceNum: existingEntry.SequenceNum,
				Request: existingEntry.Request,
				NodeId: node.nodeId,
			}

			existingEntry.mu.Unlock()

			go node.sendAcceptedMessage(acceptedMessage)
		}
		

	} else {
		node.muLog.Unlock()
	}

	// 5. If the msg is being seen for first time lets log it and send corresponding accepted message
	node.muLog.Lock()
	newEntry := &LogEntry{
        SequenceNum: msg.SequenceNum,
        Ballot:        msg.Ballot,
        Request:      msg.Request,
        Phase:       PhaseAccepted,
    }
	node.acceptLog[msg.SequenceNum] = newEntry
	node.muLog.Unlock()

	acceptedMessage := &pb.AcceptedMessage{
		Ballot: msg.Ballot,
		SequenceNum: msg.SequenceNum,
		Request: msg.Request,
		NodeId: node.nodeId,
	}

	go node.sendAcceptedMessage(acceptedMessage)

	return &emptypb.Empty{}, nil
}

func(node *Node) sendAcceptedMessage(msg *pb.AcceptedMessage){
	targetId := msg.Ballot.NodeId

	_, err := node.peers[targetId].HandleAccepted(context.Background(),msg)

	if err != nil {
		log.Printf("[Node %d] FAILED to send ACCEPTED message seq=%d to Node %d", 
			node.nodeId, msg.SequenceNum, targetId)
	}
}

func(node *Node) HandleAccepted(ctx context.Context,msg *pb.AcceptedMessage) (*emptypb.Empty, error) {
	log.Printf("[Node %d] [ACCEPTED] Received from Node %d | Seq=%d  | ballot round=%d",
		node.nodeId,msg.NodeId, msg.SequenceNum,msg.Ballot.RoundNumber)

	node.muLog.Lock()

	entry, exists := node.acceptLog[msg.SequenceNum]

	// 1. Retrieve the log entry for this sequence number (created during PrePrepare)
	if !exists {
		log.Printf("[Node %d] REJECTED ACCEPTED: No log entry for SeqNum %d.", node.nodeId, msg.SequenceNum)
		node.muLog.Unlock()
		return &emptypb.Empty{}, nil
	}

	entry.mu.Lock()
	node.muLog.Unlock()

	// 2. Check if valid ballot number 
	if entry.Ballot.NodeId != msg.Ballot.NodeId || entry.Ballot.RoundNumber != msg.Ballot.RoundNumber {
		log.Printf("[Node %d] REJECTED ACCEPTED: Ballot mismatch for (R:%d, S:%d).",
			node.nodeId, msg.Ballot.RoundNumber, msg.SequenceNum)
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}

	// 3. Quorum already reached so no need for new message
	if entry.Phase >= PhaseAccepted {
        log.Printf("[Node %d] Ignoring ACCEPTED for (R:%d, S:%d), already in Phase %v",
            node.nodeId, entry.Ballot.RoundNumber, entry.SequenceNum, entry.Phase)
       	entry.mu.Unlock()
        return &emptypb.Empty{}, nil
    }

	// 4. Check if already counted message from this node
	if _, exists := entry.AcceptedMessages[msg.NodeId]; exists {
		log.Printf("[Node %d] Duplicate ACCEPTED from node %d.", node.nodeId, msg.NodeId)
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}

	entry.AcceptedMessages[msg.NodeId] = msg
	entry.AcceptCount = int32(len(entry.AcceptedMessages))
	log.Printf("[Node %d] Accepted Count: %d",node.nodeId,entry.AcceptCount)

	threshold := node.f + 1

	if entry.AcceptCount >= threshold{
		log.Printf("[Node %d] Quorum reached for seq=%d, round=%d",node.nodeId,entry.SequenceNum,entry.Ballot.RoundNumber)
		entry.Phase = PhaseCommitted

		commitMsg := &pb.CommitMessage{
			Ballot: entry.Ballot,
			SequenceNum: entry.SequenceNum,
			Request: entry.Request,
		}

		entry.mu.Unlock()

		go node.broadcastCommitMessage(commitMsg)

		// go executeInorder()
		return &emptypb.Empty{}, nil
	}

	entry.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func(node *Node) broadcastCommitMessage(msg *pb.CommitMessage){
	allNodes := getAllNodeIDs()

	log.Printf("[Node %d] Broadcasting COMMIT seq=%d", 
                node.nodeId, msg.SequenceNum)

	for _, nodeId := range allNodes {
 		if nodeId == node.nodeId {
            continue
        }

		peerClient, ok := node.peers[nodeId]
        if !ok {
            log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping COMMIT broadcast.", 
                node.nodeId, nodeId)
            continue
        }

		go func(id int32, client pb.MessageServiceClient) {
            
            
            _, err := client.HandleCommit(context.Background(), msg)
            
            if err != nil {
                log.Printf("[Node %d] FAILED to send ACCEPT seq=%d to Node %d: %v", 
                    node.nodeId, msg.SequenceNum, id, err)
            }
        }(nodeId, peerClient)
	}
}


func(node *Node) HandleCommit(ctx context.Context,msg *pb.CommitMessage) (*emptypb.Empty, error) {
	log.Printf("[Node %d] Received COMMIT message from %d",node.nodeId,msg.Ballot.NodeId)

	return &emptypb.Empty{}, nil
}