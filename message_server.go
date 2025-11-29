/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"context"
	"fmt"
	"log"
	pb "transaction-processor/message"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/types/known/emptypb"
)

func (node *Node) SendRequestMessage(ctx context.Context, req *pb.ClientRequest) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping REQUEST message",node.nodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received client request from Client %d (ts=%s) [Sender=%s, Receiver=%s, Amount=%d]",
        node.nodeId, req.ClientId, req.Timestamp.AsTime(),
        req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)

	
	if !isIntraShard(req){
		log.Printf("[Node %d] CROSS SHARD flow",node.nodeId)
		// This return is temporary for now
		return &emptypb.Empty{}, nil
	}

	log.Printf("[Node %d] INTRA SHARD flow",node.nodeId)

	node.muBallot.RLock()
	leaderId := node.promisedBallotAccept.NodeId
	log.Printf("[Node %d] Ballot number for me: (R:%d,N:%d)",node.nodeId,node.promisedBallotAccept.RoundNumber,node.promisedBallotAccept.NodeId)
	node.muBallot.RUnlock()

	// 1. Check if a leader exists
	if leaderId == 0 {
		// 1a Log the requests to be processed after leader is elected
		// log.Printf("[Node %d] Leader doesnt exist so queuing the client request",node.nodeId)
		
		node.muReqQue.Lock()

		// 1b Check if already logged
		alreadyExists := false
		for _, existing := range node.requestsQueue {
			if node.requestsAreEqual(existing, req) {
				alreadyExists = true
				break
			}
		}

		if !alreadyExists {
			log.Printf("[Node %d] Adding req (%s, %s, %d) wiht timestamp=%vto queue since leader not known yet",node.nodeId,
		req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount,req.Timestamp)

			node.requestsQueue = append(node.requestsQueue, req)

			if !node.livenessTimer.IsRunning(){
				node.livenessTimer.Start()
			}
		}
		
		node.muReqQue.Unlock()

		return &emptypb.Empty{}, nil
	}

	node.handleRequest(req,leaderId)
	
	return &emptypb.Empty{}, nil
}

func(node *Node) handleRequest(req *pb.ClientRequest, leaderId int32){
	// 1. Check if already processed this request before
	if cachedReply, exists := node.GetCachedReply(req.ClientId, req.Timestamp); exists {
		log.Printf("[Node %d] Duplicate request from client %d (ts=%s). Returning cached reply.",
			node.nodeId, req.ClientId, req.Timestamp.AsTime())

        go node.sendReplyToClient(cachedReply)
        return
    }

	reqKey := makeRequestKey(req.ClientId, req.Timestamp)

	// 2. Check pending requests (assigned seq but not executed)
	node.muPending.Lock()
	_, isPending := node.pendingRequests[reqKey]

	// 2a If pending do nothing
    if isPending {	
		node.muPending.Unlock()
        log.Printf("[Node %d]: Duplicate (pending) from client %d - ignoring retry",
            node.nodeId, req.ClientId)
        return
    }

	// 3. Reroute the request to leader
	if node.nodeId != leaderId {
		node.muPending.Unlock()

		log.Printf("[Node %d] Not primary, routing client request to primary", node.nodeId)
		
		if !node.livenessTimer.IsRunning() {
			node.livenessTimer.Start()
		}
		
		node.peers[leaderId].SendRequestMessage(context.Background(),req)
		return
	}

	// 4. Check if data points are locked
	node.muState.Lock()
	if node.isLocked(req.Transaction.Sender) || node.isLocked(req.Transaction.Receiver) {
		// 4a If locked simply skip the transaction
		log.Printf("[Node %d] Rejecting request (%s, %s, %d) due to locks being held on datapoints",node.nodeId,
		req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)

		// Maintaining strict order of locking for these two
		node.muState.Unlock()
		node.muPending.Unlock()

		return
	}

	// 4b Else lock both the datapoints
	log.Printf("[Node %d] Acquiring locks on both the data points (%s, %s)",
	node.nodeId,req.Transaction.Sender,req.Transaction.Receiver)

	node.acquireLock(req.Transaction.Sender,reqKey)
	node.acquireLock(req.Transaction.Receiver,reqKey)

	node.muState.Unlock()

	// 4b Mark as pending
	node.pendingRequests[reqKey] = true
	node.muPending.Unlock()

	// 5. Assign seq number
	node.muLog.Lock()
	seq := node.currentSeqNo + 1
	
	if _, exists := node.acceptLog[seq]; exists {
		node.muLog.Unlock()
		log.Printf("[Node %d] CRITICAL: Sequence number %d already occupied!", node.nodeId, seq)
		return 
	}

	// Only increment if it is safe to increment
	node.currentSeqNo++

	node.muLog.Unlock()

	log.Printf("[Node %d] Assigning seq=%d to request (%s, %s, %d)",
	node.nodeId,seq,req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)

	// // 6. Mark this request in processing
	// node.markRequestPending(req.ClientId, req.Timestamp)

	acceptMessage := &pb.AcceptMessage{
		Ballot:node.myBallot,
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
		Phase: PhaseAccepted,
		Status: LogEntryPresent,
	}

	node.muLog.Lock()
	node.acceptLog[acceptMessage.SequenceNum] = newEntry;
	node.muLog.Unlock()

	// 8. Broadcast accept message
	go node.broadcastAcceptMessage(acceptMessage)
}

func (node *Node) broadcastAcceptMessage(msg *pb.AcceptMessage){
	allNodes := node.getAllClusterNodes()

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
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping ACCEPT message for seq=%d",node.nodeId,msg.SequenceNum)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node] Received ACCEPT: ballot=%d, seq=%d from leader=%d",
		msg.Ballot.RoundNumber, msg.SequenceNum,msg.Ballot.NodeId)

	// 1. If valid ballot number
	node.muBallot.Lock()
	promisedBallot := node.promisedBallotAccept
	
	// 1a. If less simply reject it
	if node.isBallotLessThan(msg.Ballot,promisedBallot) {
		log.Printf("[Node %d] Rejecting ACCEPT message with stale ballot number",node.nodeId)
		
		node.muBallot.Unlock()
		return &emptypb.Empty{}, nil
	}

	// 1b. Updated promisedBallot if higher
	isBallotGreater := false
	if node.isBallotGreaterThan(msg.Ballot,promisedBallot) {
		isBallotGreater = true
		node.promisedBallotAccept = msg.Ballot
	}

	node.muBallot.Unlock()

	// 3. Check if already accepted this message
	node.muLog.Lock()
	existingEntry, exists := node.acceptLog[msg.SequenceNum];

	if exists {
		existingEntry.mu.Lock()
		node.muLog.Unlock()

		if existingEntry.Phase >= PhaseCommitted {
			// If already committed or executed , do nothing
			log.Printf("[Node %d] Seq=%d already in status %v not overriding to Accepted",
                node.nodeId, msg.SequenceNum, existingEntry.Phase)

			existingEntry.mu.Unlock()

			return &emptypb.Empty{}, nil
		} else if existingEntry.Phase == PhaseAccepted {
			// Check if already acceped then send accepted message again
			log.Printf("[Node %d] Seq=%d already in status %v",
                node.nodeId, msg.SequenceNum, existingEntry.Phase)

			// If ballot number is higher then overrwrite in accept log and send accepted with new ballot number
			if isBallotGreater {
				existingEntry.Ballot = msg.Ballot
				existingEntry.Request = msg.Request

				acceptedMessage := &pb.AcceptedMessage{
					Ballot: existingEntry.Ballot,
					SequenceNum: existingEntry.SequenceNum,
					Request: existingEntry.Request,
					NodeId: node.nodeId,
				}

				existingEntry.mu.Unlock()
				
				node.markRequestPending(msg.Request.ClientId, msg.Request.Timestamp)

				// Impt: Start timer if not already running
				if !node.livenessTimer.IsRunning(){
					node.livenessTimer.Start()
				}

				go node.sendAcceptedMessage(acceptedMessage)
				return &emptypb.Empty{}, nil
			} else {
				// Ballot number will be equal
				acceptedMessage := &pb.AcceptedMessage{
					Ballot: existingEntry.Ballot,
					SequenceNum: existingEntry.SequenceNum,
					Request: existingEntry.Request,
					NodeId: node.nodeId,
				}
				existingEntry.mu.Unlock()

				node.markRequestPending(acceptedMessage.Request.ClientId, acceptedMessage.Request.Timestamp)

				// Impt: Start timer if not already running
				if !node.livenessTimer.IsRunning(){
					node.livenessTimer.Start()
				}

				go node.sendAcceptedMessage(acceptedMessage)
				return &emptypb.Empty{}, nil
			}			
		}  	
	}

	// 5. If the msg is being seen for first time lets log it and send corresponding accepted message
	newEntry := &LogEntry{
        SequenceNum: msg.SequenceNum,
        Ballot:        msg.Ballot,
        Request:      msg.Request,
        Phase:       PhaseAccepted,
		Status: LogEntryPresent,
    }
	node.acceptLog[msg.SequenceNum] = newEntry
	node.muLog.Unlock()

	acceptedMessage := &pb.AcceptedMessage{
		Ballot: msg.Ballot,
		SequenceNum: msg.SequenceNum,
		Request: msg.Request,
		NodeId: node.nodeId,
	}
	
	node.markRequestPending(msg.Request.ClientId, msg.Request.Timestamp)

	go node.sendAcceptedMessage(acceptedMessage)

	// Impt: Start timer if not already running
	if !node.livenessTimer.IsRunning(){
		node.livenessTimer.Start()
	}

	return &emptypb.Empty{}, nil
}

func(node *Node) sendAcceptedMessage(msg *pb.AcceptedMessage){
	log.Printf("[Node %d] Sending ACCEPTED for Ballot (R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
	targetId := msg.Ballot.NodeId

	_, err := node.peers[targetId].HandleAccepted(context.Background(),msg)

	if err != nil {
		log.Printf("[Node %d] FAILED to send ACCEPTED message seq=%d to Node %d", 
			node.nodeId, msg.SequenceNum, targetId)
	}
}

func(node *Node) HandleAccepted(ctx context.Context,msg *pb.AcceptedMessage) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping ACCEPTED message for seq=%d",node.nodeId,msg.SequenceNum)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] [ACCEPTED] Received for  Seq=%d  | from Node %d | ballot round=%d",
		node.nodeId,msg.SequenceNum,msg.NodeId, msg.Ballot.RoundNumber)

	node.muLog.Lock()

	// log.Printf("[Node %d] is accept log nil=%v",node.nodeId,node.acceptLog == nil)
	// if node.acceptLog != nil {
	// 	log.Printf("[Node %d] is accept log len=%d",node.nodeId,len(node.acceptLog))
	// }

	entry, exists := node.acceptLog[msg.SequenceNum]

	// 1. Retrieve the log entry for this sequence number (created during PrePrepare)
	if !exists {
		log.Printf("[Node %d] REJECTED ACCEPTED: No log entry for SeqNum %d.", node.nodeId, msg.SequenceNum)
		node.muLog.Unlock()
		return &emptypb.Empty{}, nil
	}

	// log.Printf("[Node %d] Entry exists",node.nodeId)

	entry.mu.Lock()
	node.muLog.Unlock()

	// 2. Check if valid ballot number 
	if !node.isBallotEqual(entry.Ballot,msg.Ballot) {
		log.Printf("[Node %d] REJECTED ACCEPTED: Ballot mismatch for (R:%d, S:%d).",
			node.nodeId, msg.Ballot.RoundNumber, msg.SequenceNum)
		log.Printf("[Node %d] Entry ballot (R:%d,N:%d) msg ballot (R:%d,N:%d)",
		node.nodeId,entry.Ballot.RoundNumber,entry.Ballot.NodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
		
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}
	
	// log.Printf("[Node %d] Ballot check complete",node.nodeId)

	// 3. Quorum already reached so no need for new message
	if entry.Phase >= PhaseCommitted {
        log.Printf("[Node %d] Ignoring ACCEPTED for (R:%d, S:%d), already in Phase %v",
            node.nodeId, entry.Ballot.RoundNumber, entry.SequenceNum, entry.Phase)
       	entry.mu.Unlock()
        return &emptypb.Empty{}, nil
    }

	// log.Printf("[Node %d] Quorum reach check complete",node.nodeId)

	// 4. Check if already counted message from this node
	if _, exists := entry.AcceptedMessages[msg.NodeId]; exists {
		log.Printf("[Node %d] Duplicate ACCEPTED from node %d.", node.nodeId, msg.NodeId)
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}

	// log.Printf("[Node %d] All checks completed",node.nodeId)

	entry.AcceptedMessages[msg.NodeId] = msg
	// log.Printf("[Node %d] Inserted accepted msg",node.nodeId)
	entry.AcceptCount = int32(len(entry.AcceptedMessages))
	// log.Printf("[Node %d] Accepted Count: %d",node.nodeId,entry.AcceptCount)

	threshold := node.f + 1

	if entry.AcceptCount >= threshold{
		log.Printf("[Node %d] Quorum reached for seq=%d, round=%d",node.nodeId,entry.SequenceNum,entry.Ballot.RoundNumber)
		entry.Phase = PhaseCommitted
		log.Printf("[Node %d] Seq=%d is now in phase=%d",node.nodeId,entry.SequenceNum,entry.Phase)
		
		commitMsg := &pb.CommitMessage{
			Ballot: entry.Ballot,
			SequenceNum: entry.SequenceNum,
			Request: entry.Request,
		}

		entry.mu.Unlock()

		go node.broadcastCommitMessage(commitMsg)

		go node.executeInOrder(true)

		return &emptypb.Empty{}, nil
	}

	entry.mu.Unlock()
	return &emptypb.Empty{}, nil
}

func(node *Node) broadcastCommitMessage(msg *pb.CommitMessage){
	allNodes := node.getAllClusterNodes()

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
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping COMMIT message for seq=%d",node.nodeId,msg.SequenceNum)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received COMMIT message for seq=%d from %d",node.nodeId,msg.SequenceNum,msg.Ballot.NodeId)

	// 1. Check for valid ballot number
	node.muBallot.RLock()
	selfBallot := node.promisedBallotAccept
	node.muBallot.RUnlock()

	if node.isBallotLessThan(msg.Ballot,selfBallot) {
		log.Printf("[Node %d] Rejecting COMMIT: stale ballot (R:%d,N:%d) < (R:%d,N:%d)",
            node.nodeId, msg.Ballot.RoundNumber, msg.Ballot.NodeId,
            selfBallot.RoundNumber, selfBallot.NodeId)
		
		return &emptypb.Empty{}, nil
	}

	// 2. Check if entry exists
	node.muLog.Lock()
	entry, exists := node.acceptLog[msg.SequenceNum]
	
	// 2a If it doesnt exist that means we have received COMMIT before its ACCEPT ie other nodes have reached quorum
	if !exists {
		log.Printf("[Node %d] WARNING: No log entry for COMMIT Seq=%d.Creating new entry",
			node.nodeId, msg.SequenceNum)

		newEntry := &LogEntry{
			SequenceNum: msg.SequenceNum,
			Ballot: msg.Ballot,
			Request: msg.Request,
			Phase: PhaseCommitted,
			Status: LogEntryPresent,
		}
		node.acceptLog[msg.SequenceNum] = newEntry

		node.muLog.Unlock()

		go node.executeInOrder(false)

		return &emptypb.Empty{}, nil
	}

	// 2b entry already exisits thus we simply need to check if already committed
	// If not update it to committed
	entry.mu.Lock()
	node.muLog.Unlock()
	
	// 3 Check if already COMMITTED
	if entry.Phase >= PhaseCommitted {
		log.Printf("[Node %d] INFO: Already committed/executed Seq=%d. Ignoring duplicate COMMIT.",
			node.nodeId, msg.SequenceNum)
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}

	// 4 Check if request mistmatch (can happen in split brain scenario where dead leader recovers)
	if msg.Request != nil && !node.requestsAreEqual(entry.Request, msg.Request) {
        log.Printf("[Node %d] SAFETY ERROR: COMMIT request mismatch for seq=%d!",
            node.nodeId, msg.SequenceNum)
        entry.mu.Unlock()
        return &emptypb.Empty{}, nil
    }

	// 5 Commit the entry
	entry.Phase = PhaseCommitted
	// Note this line is really critical for safety
	if node.isBallotGreaterThan(msg.Ballot, entry.Ballot) {
        entry.Ballot = msg.Ballot
    }
	
	entry.mu.Unlock()

	// 6 Execute
	go node.executeInOrder(false)

	return &emptypb.Empty{}, nil
}

func (node *Node) executeInOrder(isLeader bool) {
    node.muExec.Lock()
    defer node.muExec.Unlock()

	nextSeq := node.lastExecSeqNo + 1
	
	for {
		node.muLog.Lock()
		log.Printf("[Node %d] Running execution for seq=%d",node.nodeId,nextSeq)
		
		entry, exists := node.acceptLog[nextSeq]
		if !exists {
			log.Printf("[Node %d] Running execution no log entry for seq=%d",node.nodeId,nextSeq)
			node.muLog.Unlock()
			break;
		}

		entry.mu.Lock()
		node.muLog.Unlock()
	
		log.Printf("[Entry while in execution] AcceptCount: %d, phase=%v",entry.AcceptCount,entry.Phase)

		if entry.Phase == PhaseExecuted {
			log.Printf("[Node %d] Running execution phase=%v already executed for seq=%d",node.nodeId,entry.Phase,nextSeq)
            entry.mu.Unlock()
            continue
        }

		if entry.Phase != PhaseCommitted {
			log.Printf("[Node %d] Running execution phase=%v is not committed for seq=%d",node.nodeId,entry.Phase,nextSeq)
            entry.mu.Unlock()
            break
        }

		// IF NO-OP
		if entry.Request.Transaction.Sender == "noop" {
			log.Printf("[Node %d] EXECUTED NO-OP Seq=%d.", node.nodeId, nextSeq)
		} else {
			_, exists = node.GetCachedReply(entry.Request.ClientId, entry.Request.Timestamp)

			// Non executed new request
			if !exists {	
				log.Printf("[Node %d] Request for seq=%d has never been executed before",node.nodeId,nextSeq)

				// node.muState.Lock()

				var status string
				
				// Execute the request
				log.Printf("[Node %d] [Ballot Round=%d] EXECUTING: Seq=%d, Client=%d, Sender=%s, Receiver=%s, Amount=%d",
					node.nodeId, entry.Ballot.RoundNumber,entry.SequenceNum, entry.Request.ClientId,
					entry.Request.Transaction.Sender, entry.Request.Transaction.Receiver,
					entry.Request.Transaction.Amount)
				
				
				transaction := Transaction{
					Sender:   entry.Request.Transaction.Sender,
					Receiver: entry.Request.Transaction.Receiver,
					Amount:   entry.Request.Transaction.Amount,
				}

				response, err := node.processTransaction(transaction)

				if err != nil {
					log.Printf("[Node %d] Error in execution=%v",node.nodeId,err)
				}

				status = "failure"
				if response {
					status = "success"
				}
			
				// node.persistState()
				
				reply := &pb.ReplyMessage{
					ClientId:         entry.Request.ClientId,
					ClientRequestTimestamp: entry.Request.Timestamp,
					Status:           status,
					Ballot: entry.Ballot,
				}

				// Check if we should create a checkpoint
				if (nextSeq) % node.checkpointInterval == 0 {
					log.Printf("[Node %d] Seq=%d satifies checkpointing interval",node.nodeId,nextSeq)
				
					node.muCheckpoint.Lock()
					// Storing last stable state snapshot to send during catchup request
					if node.lastStableSnapshot != nil {
						node.lastStableSnapshot.Close()
					}
					node.lastStableSnapshot = node.state.NewSnapshot()
					log.Printf("[Node %d] Seq=%d snapshot installation complete",node.nodeId,nextSeq)

					if isLeader {
						stateDigest,err := node.computeStateDigest()

						if err != nil {
							log.Printf("[Node %d] CRITICAL Error in computing state digest for seq=%d",node.nodeId,nextSeq)
							node.muCheckpoint.Unlock()

							continue
						}

						log.Printf("[Node %d] Seq=%d state digest computation complete",node.nodeId,nextSeq)

						msg := &pb.CheckpointMessage{
							SequenceNum: nextSeq,
							Digest: stateDigest,
							NodeId: node.nodeId,
						}
						
						// Log highest checkpointing message
						node.latestCheckpointMessage = msg
						log.Printf("[Node %d] Seq=%d latest checkpoint message updated",node.nodeId,nextSeq)

						go node.broadcastCheckpointMessage(msg)
					}
					
					// Spawning this in go rotuntine here seems little risky
					// But since this is for past sequece numbers dont think there should be an issue
					// Since only checkpointing activity will be present for these sequence numbers
					go node.garbageCollectBeforeCheckpoint(nextSeq)


					node.muCheckpoint.Unlock()
					
				}

				node.cacheReplyAndClearPending(reply)

				if isLeader {
					// Release locks after execution
					node.muState.Lock()
					log.Printf("[Node %d] Releasing locks on data points (%s, %s)",node.nodeId,transaction.Sender,transaction.Receiver)
					node.releaseLock(transaction.Sender)
					node.releaseLock(transaction.Receiver)
					node.muState.Unlock()
				}
				
				log.Printf("[Node %d] EXECUTED: Seq=%d successfully. Status=%s", node.nodeId, entry.SequenceNum, status)

				if isLeader {
					go node.sendReplyToClient(reply)
				}
			}
		}

		entry.Phase = PhaseExecuted
		entry.mu.Unlock()

		node.lastExecSeqNo = nextSeq

		// Liveness timer 
		// When the request is executed, there are two cases:
		// (a) There is no pending (received but not executed) request on the node: the timer stops (and it is initiated when the node receives the next request)
		// (b) There are pending requests on the node: the timer resets
		if node.hasPendingWork() {
			log.Printf("[Node %d] Pending work present restarting liveness timer",node.nodeId)
			node.livenessTimer.Restart()
		} else {
			log.Printf("[Node %d] No pending work present stopping liveness timer",node.nodeId)
			node.livenessTimer.Stop()
		}

		nextSeq++
	}
}


func(node *Node) sendReplyToClient(reply *pb.ReplyMessage){
	log.Printf("[Node %d] Sending REPLY back to client",node.nodeId)
	
	node.clientSideGrpcClient.HandleReply(context.Background(),reply)
}


func (node *Node) HandlePrepare(ctx context.Context, msg *pb.PrepareMessage) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping PREPARE message for Ballot=(R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received PREPARE from node=%d for round=%d",node.nodeId,msg.Ballot.NodeId,msg.Ballot.RoundNumber)

	// 1. Check if prepare with valid ballot number
	node.muBallot.RLock()
	selfBallot := node.promisedBallotPrepare
	node.muBallot.RUnlock()

	if node.isBallotLessThan(msg.Ballot,selfBallot){
		log.Printf("[Node %d] Rejecting stale PREPARE with ballot (R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
		return &emptypb.Empty{},nil
	}

	// 2. Log prepare if timer still running
	if node.livenessTimer.IsRunning() {
		log.Printf("[Node %d] Timer running to logging PREPARE",node.nodeId)

		node.muPreLog.Lock()
		node.prepareLog.log =  append(node.prepareLog.log, msg)

		if node.prepareLog.highestBallotPrepare == nil {
			node.prepareLog.highestBallotPrepare = msg
		} else if node.isBallotGreaterThan(msg.Ballot,node.prepareLog.highestBallotPrepare.Ballot) {
			node.prepareLog.highestBallotPrepare = msg
		}
		node.muPreLog.Unlock()

		return &emptypb.Empty{}, nil
	}

	log.Printf("[Node %d] Timer expired for new PREPARE",node.nodeId)
	// 3. If liveness timer is expired, we can get highest ballot number from 2 sources
	// a. From previously logged prepare messages
	// b. Here
	if node.isBallotGreaterThan(msg.Ballot,selfBallot) {
		log.Printf("[Node %d] Latest received PREPARE ballot is higher than logged ones",node.nodeId)

		node.muBallot.Lock()
		node.promisedBallotPrepare = msg.Ballot
		node.muBallot.Unlock()
		
		// 4. Build send promise message
		node.buildAndSendPromise(msg.Ballot)

		// 5. Start tp timer
		if node.prepareTimer.IsRunning() {
			log.Printf("[Node %d] Restarting tp timer",node.nodeId)
			node.prepareTimer.Restart()
		} else {
			log.Printf("[Node %d] Starting tp timer",node.nodeId)
			node.prepareTimer.Start()
		}
	}

	return &emptypb.Empty{},nil
}


func (node *Node) buildAndSendPromise(ballot *pb.BallotNumber){
	log.Printf("[Node %d] Building promise message",node.nodeId)
	node.muCheckpoint.Lock()
	lastCheckpointSeq := node.latestCheckpointMessage.SequenceNum
	lastCheckpointDigest := node.latestCheckpointMessage.Digest 
	node.muCheckpoint.Unlock()
	
	promiseMsg := &pb.PromiseMessage {
		Ballot: ballot,
		AcceptLog: node.getAcceptedLogForPromise(lastCheckpointSeq),
		NodeId: node.nodeId,
		LastCheckpointSeq: lastCheckpointSeq,
		LastCheckpointDigest: lastCheckpointDigest,
	}

	log.Printf("[Node %d] Building promise complete",node.nodeId)
	go node.sendPromiseMessage(promiseMsg)
}

func (node *Node) getAcceptedLogForPromise(checkpointSeq int32) []*pb.AcceptedMessage {
    node.muLog.RLock()
    defer node.muLog.RUnlock()

    var acceptedEntries []*pb.AcceptedMessage
	log.Printf("[Node %d] Building accept log for promise message",node.nodeId)
    for _, entry := range node.acceptLog {
		entry.mu.RLock()
		Ballot := entry.Ballot
		SequenceNum:= entry.SequenceNum
		Request := entry.Request
		Phase := entry.Phase
		entry.mu.RUnlock()

		// Wanna ignore all log entries before last checkpoint
		if SequenceNum < checkpointSeq{
			continue
		}

        if  Phase >= PhaseAccepted {
            msg := &pb.AcceptedMessage{
                Ballot:      Ballot,
                SequenceNum: SequenceNum,
                Request:     Request,
                NodeId:      node.nodeId,
            }
            acceptedEntries = append(acceptedEntries, msg)
        }
    }

	log.Printf("[Node %d] Building accept log for promise message COMPLETE",node.nodeId)
    return acceptedEntries
}

func(node *Node) sendPromiseMessage(msg *pb.PromiseMessage){
	log.Printf("[Node %d] Sending PROMISE to %d with log len=%d",node.nodeId,msg.Ballot.NodeId,len(msg.AcceptLog))

	peerClient, ok := node.peers[msg.Ballot.NodeId]
	if !ok {
		log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping Collected-prepare broadcast.", 
			node.nodeId, msg.Ballot.NodeId)
		return
	}

	_, err := peerClient.HandlePromise(context.Background(), msg)
            
	if err != nil {
		log.Printf("[Node %d] FAILED to send PROMISE Round=%d to Node %d: %v", 
			node.nodeId, msg.Ballot.RoundNumber, msg.Ballot.NodeId, err)
	}
}


func(node *Node) HandlePromise(ctx context.Context,msg *pb.PromiseMessage) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping PROMISE message for Ballot=(R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received PROMISE for Ballot (R:%d) from node=%d",node.nodeId,msg.Ballot.RoundNumber,msg.NodeId)

	// 1. Valid ballot check
	node.muBallot.RLock()
	selfBallot := node.promisedBallotPrepare
	node.muBallot.RUnlock()

	if !node.isBallotEqual(msg.Ballot,selfBallot){
		log.Printf("[Node %d] Rejecting PROMISE with different ballot",node.nodeId)
		return &emptypb.Empty{},nil
	}

	log.Printf("[Node %d] PROMISE Ballot check complete (R:%d) from node=%d",node.nodeId,msg.Ballot.RoundNumber,msg.NodeId)

	// 2. Check if already logged the promise
	node.muProLog.Lock()
	_,exists := node.promiseLog.log[msg.NodeId]

	if exists {
		log.Printf("[Node %d] Rejecting PROMISE already logged",node.nodeId)
		node.muProLog.Unlock()
		return &emptypb.Empty{},nil
	}

	// 3. Check if quorum already reached
	if node.promiseLog.isPromiseQuorumReached {
		node.muProLog.Unlock()
		log.Printf("[Node %d] Rejecting PROMISE quorum already reached",node.nodeId)
		return &emptypb.Empty{},nil
	}

	log.Printf("[Node %d] PROMISE message logged (R:%d) from node=%d",node.nodeId,msg.Ballot.RoundNumber,msg.NodeId)
	node.promiseLog.log[msg.NodeId] = msg
	count := int32(len(node.promiseLog.log))
	log.Printf("[Node %d] Quorum count=%d for PROMISE with ballot (R:%d,N:%d)",node.nodeId,count,msg.Ballot.RoundNumber,msg.Ballot.NodeId)

	threshold := node.f+1

	// 4. Check for quorum
	if count >= threshold {
		log.Printf("[Node %d] Reached quorum for PROMISE with ballot (R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
		node.promiseLog.isPromiseQuorumReached = true

		// 4a Build merged accept log from promises
		// The minSeq from this output will the highest checkpoint seq from the promise quorum
		log.Printf("[Node %d] Building MERGED log for NEW-VIEW",node.nodeId)
		mergedLog,minSeq,maxSeq,maxCheckpointNodeId := node.buildMergedAcceptLog()
		log.Printf("[Node %d] Building MERGED log for NEW-VIE COMPLETE",node.nodeId)

		node.muProLog.Unlock()

		// 4b. Update self ballot number
		node.muBallot.Lock()
		// node.promisedBallotPrepare = msg.Ballot
		node.promisedBallotAccept = msg.Ballot
		node.myBallot = msg.Ballot
		node.muBallot.Unlock()

		// 4c. Updated election state pointers
		node.muLeader.Lock()
		node.isLeaderKnown = true
		node.inLeaderElection = false
		node.muLeader.Unlock()

		// 4d. Install new log unto self
		node.installMergedAcceptLog(mergedLog,msg.Ballot,minSeq,maxSeq,true)

		// This exec wont work inside muLog
		node.muExec.Lock()
		lastExeSeq := node.lastExecSeqNo
		node.muExec.Unlock()

		highestCheckpointNo := minSeq-1

		// 4f Leader is out of state and needs to catchup and thus lastExecSeqNo can only be updated in catcup flow
		if lastExeSeq < minSeq-1 {
			log.Printf("[Node %d] Initiating request for state transfer (in election) for seq=%d from lastExec=%d to node=%d",
			node.nodeId,highestCheckpointNo,lastExeSeq,maxCheckpointNodeId)

			go node.requestStateFromTarget(highestCheckpointNo,maxCheckpointNodeId)
		} else{
			// If leader has the latest state it can simply update
			// TO DO tp remember and note down why this was needed
			log.Printf("[Node %d] Updating lastExecSeqNo directly",node.nodeId)
			node.muExec.Lock()
			node.lastExecSeqNo = minSeq-1
			node.muExec.Unlock()
			log.Printf("[Node %d] Updating lastExecSeqNo directly COMPLETE",node.nodeId)
		}

		// 4e. Broadcast new view
		newViewMsg := &pb.NewViewMessage{
			Ballot: msg.Ballot,
			AcceptLog: mergedLog,
			NodeId: node.nodeId,
			MinSeq: minSeq,
			MaxSeq: maxSeq,
			MinSeqNodeId: maxCheckpointNodeId,
		}
		
		go node.broadcastNewView(newViewMsg)
		log.Printf("[Node %d] Broadcast NEW-VIEW fired off",node.nodeId)

		// 4f. Reset the pending cause role has been changed from follower to leader
		node.buildPendingRequestsQueue(highestCheckpointNo)
		log.Printf("[Node %d] Building pending requests complete",node.nodeId)

		// 4g. Start draining pending requests queue
		node.drainQueuedRequests(node.nodeId)
		log.Printf("[Node %d] Draining queued requests complete (as leader)",node.nodeId)

		return &emptypb.Empty{},nil
	}

	node.muProLog.Unlock()
	return &emptypb.Empty{},nil
}

// Lock for promise log has been held in parent
func(node *Node) buildMergedAcceptLog() ([]*pb.AcceptedMessage,int32,int32,int32) {
	// 1. Intializing minS and maxS
	var maxCheckpointSeq int32 = 0
	var maxCheckpointNodeId int32 = 0

    for _, promise := range node.promiseLog.log {
        if promise.LastCheckpointSeq > maxCheckpointSeq {
            maxCheckpointSeq = promise.LastCheckpointSeq
			maxCheckpointNodeId = promise.NodeId
        }
    }

	minS := maxCheckpointSeq + 1
    var maxSeq int32 = minS - 1
	
	merged := make(map[int32]*pb.AcceptedMessage)

	// 2. Merge log
	for _, promise := range node.promiseLog.log {
        for _, accepted := range promise.AcceptLog {
            seq := accepted.SequenceNum

			if seq < minS {
                continue
            }

            if seq > maxSeq {
                maxSeq = seq
            }

            existing, exists := merged[seq]

			// 2a Highest ballot wins
            if !exists || node.isBallotGreaterThan(accepted.Ballot, existing.Ballot) {
                merged[seq] = accepted
            }
        }
    }

	// 3 Gaps handling (NOOP)
	for seq := minS; seq <= maxSeq; seq++ {
        _, exists := merged[seq]
        if !exists {
            noopAccepted := &pb.AcceptedMessage{
                SequenceNum: seq,
                Ballot:      node.promisedBallotPrepare,
                Request: &pb.ClientRequest{
					ClientId: -1,
                    Transaction: &pb.Transaction{
                        Sender:   "noop",
                        Receiver: "noop",
                        Amount:   0,
                    },
                },
                NodeId: node.nodeId,
            }
            merged[seq] = noopAccepted
		}
	}

	// 4. Convert to slice
	sliceSize := maxSeq - minS + 1
    mergedSlice := make([]*pb.AcceptedMessage, sliceSize)

    for seq := minS; seq <= maxSeq; seq++ {
        mergedSlice[seq-minS] = merged[seq]
    }

    return mergedSlice,minS,maxSeq,maxCheckpointNodeId
}

func (node *Node) installMergedAcceptLog(mergedLog []*pb.AcceptedMessage,winningBallot *pb.BallotNumber,minSeq int32,maxSeq int32,isLeader bool) {
	node.muLog.Lock()
	
	// 1. Installing merged log
	for _, acceptedMsg := range mergedLog {
		seq := acceptedMsg.SequenceNum

		// Ignore old entries before checkpoint
		if seq < minSeq {
            continue
        }

		entry, exists := node.acceptLog[seq]

		if !exists {
			selfAcceptedMessage := &pb.AcceptedMessage{
				Ballot: winningBallot,
				SequenceNum: seq,
				Request: acceptedMsg.Request,
				NodeId: node.nodeId,
			}

			newAcceptMsgLog := make(map[int32] *pb.AcceptedMessage)
			newAcceptMsgLog[node.nodeId] = selfAcceptedMessage

            node.acceptLog[seq] = &LogEntry{
                SequenceNum: seq,
                Ballot:      winningBallot,
                Request:     acceptedMsg.Request,
                Phase:       PhaseAccepted,
				AcceptedMessages:newAcceptMsgLog,
				AcceptCount:1,
				Status: LogEntryPresent,
            }
        } else {
			entry.mu.Lock()

			if node.isBallotGreaterThan(winningBallot,entry.Ballot){
				log.Printf("[Node %d] Updating ballot in seq=%d to (R:%d,N:%d)",node.nodeId,seq,winningBallot.RoundNumber,winningBallot.NodeId)
				entry.Ballot = winningBallot
				entry.Request = acceptedMsg.Request
			}
			
			if isLeader {
				selfAcceptedMessage := &pb.AcceptedMessage{
					Ballot: entry.Ballot,
					SequenceNum: seq,
					Request: acceptedMsg.Request,
					NodeId: node.nodeId,
				}

				newAcceptMsgLog := make(map[int32] *pb.AcceptedMessage)
				newAcceptMsgLog[node.nodeId] = selfAcceptedMessage

				entry.Phase = PhaseAccepted
				entry.AcceptedMessages = newAcceptMsgLog
				entry.AcceptCount = 1
			} 
			
			entry.mu.Unlock()
		}
	}


	//2. Clear any acceptLog entries beyond maxSeq to remove stale data
	for _, entry := range node.acceptLog{
		entry.mu.RLock()
		seq := entry.SequenceNum
		entry.mu.RUnlock()

		log.Printf("[Node %d] Attempting delete at seq=%d",node.nodeId,seq)
		if seq > maxSeq {
			log.Printf("[Node %d] Deleted at seq=%d",node.nodeId,seq)
			delete(node.acceptLog, seq)
		}
	}
	
	// 3. Updating curentSeq
	node.currentSeqNo = maxSeq
	node.muLog.Unlock()

	log.Printf("[Node %d] installMergedAcceptLog complete: minSeq=%d maxSeq=%d, cleared entries beyond max", node.nodeId,minSeq, maxSeq)
}

func(node *Node) broadcastNewView(msg *pb.NewViewMessage){
	allNodes := node.getAllClusterNodes()

	log.Printf("[Node %d] Broadcasting NEW-VIEW Round=%d", 
                node.nodeId, msg.Ballot.RoundNumber)

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
            _, err := client.HandleNewView(context.Background(), msg)
            
            if err != nil {
                log.Printf("[Node %d] FAILED to send NEW-VIEW Round=%d to Node %d: %v", 
                    node.nodeId, msg.Ballot.RoundNumber, id, err)
            }
        }(nodeId, peerClient)
	}
}

func (node *Node) buildPendingRequestsQueue(highestCheckpointSeq int32){
	node.muLog.RLock()

	logCopy := make(map[int32]*LogEntry, len(node.acceptLog))
	for seq, entry := range node.acceptLog {
		logCopy[seq] = entry
	}

	node.muLog.RUnlock()

	pendingRequests := make(map[string]bool)

	for _, entry := range logCopy {
		// log.Printf("[Node %d] [PendingQueue] ... seq=%d has NULL digest. Skipping.",node.nodeId,)
		entry.mu.Lock()

		SequenceNum := entry.SequenceNum
		ClientId := entry.Request.ClientId
		Timestamp := entry.Request.Timestamp
		
		entry.mu.Unlock()
		
		// IMPT Need to trust that state transfer will eventually update the last executed seq number
		if SequenceNum <= highestCheckpointSeq{
			continue
		}

		if ClientId == -1 {
			log.Printf("[Node %d] [PendingQueue] Seq=%d has NOOP Skipping.", node.nodeId, SequenceNum)
			continue
		}

		reqId := makeRequestKey(ClientId,Timestamp)
		_, exists := node.GetCachedReply(ClientId, Timestamp)

		log.Printf("[Node %d] [PendingQueue] ... checking reply cache for reqId=%s", node.nodeId, reqId)
		// This means its not already executed before and thus can be added to pending queue
		if !exists {
			log.Printf("[Node %d] [PendingQueue] ... NOT EXECUTED. Added reqId=%s (seq=%d) to new pending list.", node.nodeId, reqId, SequenceNum)
			pendingRequests[reqId] = true
		}
	}

	log.Printf("[Node %d] [PendingQueue] Acquiring muPending lock to update node.pendingRequests...", node.nodeId)
	// Actual update in the memory
	node.muPending.Lock()
	node.pendingRequests = pendingRequests
	node.muPending.Unlock()
	log.Printf("[Node %d] [PendingQueue] Released muPending lock. Node's pendingRequests map updated to size %d.", node.nodeId, len(pendingRequests))
}

func(node *Node) drainQueuedRequests(leaderId int32){
	node.muReqQue.Lock()
    queued := node.requestsQueue
    node.requestsQueue = make([]*pb.ClientRequest, 0)
    node.muReqQue.Unlock()

    if len(queued) == 0 {
        return
    }

	log.Printf("[Node %d] Draining %d queued client requests", node.nodeId, len(queued))

    for _, req := range queued {
        go node.handleRequest(req, leaderId)
    }
}

func(node *Node) HandleNewView(ctx context.Context,msg *pb.NewViewMessage) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping NEW-VIEW message for Ballot=(R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received NEW-VIEW from node=%d for Round=%d",node.nodeId,msg.NodeId,msg.Ballot.RoundNumber)
	
	// 1. Check for ballot
	node.muBallot.RLock()
	selfBallot := node.promisedBallotPrepare
	node.muBallot.RUnlock()

	if node.isBallotLessThan(msg.Ballot,selfBallot) {
		log.Printf("[Node %d] Ignoring stale NEW-VIEW with Ballot (R:%d,N:%d)",
            node.nodeId, msg.Ballot.RoundNumber,  msg.Ballot.NodeId)

		return &emptypb.Empty{},nil
	}

	// 2. Stop timers
	if node.prepareTimer.IsRunning(){
		node.prepareTimer.Stop()
	}

	if node.livenessTimer.IsRunning(){
		node.livenessTimer.Stop()
	}

	// 3. Update the ballot and the flags
	node.muBallot.Lock()
	node.promisedBallotAccept = msg.Ballot
	node.promisedBallotPrepare = msg.Ballot
	node.muBallot.Unlock()

	node.muLeader.Lock()
	node.isLeaderKnown = true
	node.inLeaderElection = false
	node.muLeader.Unlock()

	node.muPreLog.Lock()
	node.prepareLog = PrepareLog{
		log: make([]*pb.PrepareMessage,0),
		highestBallotPrepare: nil,
	}
	node.muPreLog.Unlock()

	node.muExec.Lock()
	lastExecSeqNo := node.lastExecSeqNo 
	node.muExec.Unlock()

	// 4. If behind initiate catchup mechanism
	highestCheckpointSeq :=  msg.MinSeq-1
	if lastExecSeqNo < highestCheckpointSeq {
		log.Printf("[Node %d] Initiating request for state transfer (new view receipt) for seq=%d, from node=%d",
		node.nodeId,highestCheckpointSeq,msg.MinSeqNodeId)
		go node.requestStateFromTarget(highestCheckpointSeq,msg.MinSeqNodeId)
	}

	// 5. Install log
	node.installMergedAcceptLog(msg.AcceptLog,msg.Ballot,msg.MinSeq,msg.MaxSeq,false)

	// 6. Build pending queue from new log
	node.buildPendingRequestsQueue(highestCheckpointSeq)

	// 7. Drain queued requests
	node.drainQueuedRequests(msg.Ballot.NodeId)
	log.Printf("[Node %d] Draining queued requests complete (as backup)",node.nodeId)

	// 8. Send accepted back for each entry
	node.sendBackAcceptedForEntireMergedLog(msg,highestCheckpointSeq)

	// 9. Start the timer
	if node.hasPendingWork(){
		node.livenessTimer.Start()
	}

	return &emptypb.Empty{},nil
}

func(node *Node) sendBackAcceptedForEntireMergedLog(msg *pb.NewViewMessage,highestCheckpointSeq int32){
	log.Printf("[Node %d] Sending ACCEPTED (entire ballot) back for ballot(R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
	node.muLog.RLock()
	defer node.muLog.RUnlock()

	for _, entry := range node.acceptLog {
		entry.mu.Lock()
		
		Ballot := entry.Ballot
		SequenceNum := entry.SequenceNum
		Request := entry.Request

		entry.mu.Unlock()

		// No need to send accepted for already checkpointed seq numbers
		if SequenceNum <= highestCheckpointSeq{
			continue
		}

		acceptedMsg := &pb.AcceptedMessage{
			Ballot:      Ballot,     
			SequenceNum: SequenceNum,
			Request:     Request,  
			NodeId:      node.nodeId,
		}

		go node.sendAcceptedMessage(acceptedMsg)
	}
}


func(node *Node) broadcastCheckpointMessage(msg *pb.CheckpointMessage){
	allNodes := node.getAllClusterNodes()

	log.Printf("[Node %d] Broadcasting CHECKPOINT for seq=%d", 
                node.nodeId, msg.SequenceNum)

	for _, nodeId := range allNodes {
 		if nodeId == node.nodeId {
            continue
        }

		peerClient, ok := node.peers[nodeId]
        if !ok {
            log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping CHECKPOINT broadcast.", 
                node.nodeId, nodeId)
            continue
        }

		go func(id int32, client pb.MessageServiceClient) {
            _, err := client.HandleCheckpoint(context.Background(), msg)
            
            if err != nil {
                log.Printf("[Node %d] FAILED to send CHECKPOINT for seq=%d to Node %d: %v", 
                    node.nodeId, msg.SequenceNum, id, err)
            }
        }(nodeId, peerClient)
	}
}


func(node *Node) HandleCheckpoint(ctx context.Context,msg *pb.CheckpointMessage) (*emptypb.Empty, error) {
	if !node.isActive() {
		log.Printf("[Node %d] Inactive dropping CHECKPOINT message",node.nodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d]: Received CHECKPOINT from Node %d, seq=%d, digest=%s", 
        node.nodeId, msg.NodeId, msg.SequenceNum, msg.Digest[:16])

	// 1. Check executing lag
	node.muExec.Lock()
	lastExecSeq := node.lastExecSeqNo
	node.muExec.Unlock()

	// Lagging in state so need to catchup
	if lastExecSeq < msg.SequenceNum {
		log.Printf("[Node %d] Initiating request for state transfer for seq=%d",node.nodeId,msg.SequenceNum)
		go node.requestStateFromTarget(msg.SequenceNum,msg.NodeId)
	} else{
		// state upto date
		node.muCheckpoint.Lock()
		if node.latestCheckpointMessage == nil || msg.SequenceNum > node.latestCheckpointMessage.SequenceNum {
			log.Printf("[Node %d] Updating stable checkpoint metadata to seq=%d", node.nodeId, msg.SequenceNum)
            node.latestCheckpointMessage = msg

			go node.garbageCollectBeforeCheckpoint(msg.SequenceNum)
		}
		node.muCheckpoint.Unlock()
	}

	return &emptypb.Empty{}, nil
}

func (node *Node) garbageCollectBeforeCheckpoint(stableSeq int32) {
	log.Printf("[Node %d] Seq=%d garbage collecting start",node.nodeId,stableSeq)

    node.muLog.Lock()
    defer node.muLog.Unlock()
    
    markedCount := 0
    for seq, entry := range node.acceptLog {
        if seq <= stableSeq {
            entry.mu.Lock()
            entry.Status = LogEntryDeleted
            entry.mu.Unlock()
            markedCount++
        }
    }
    
    log.Printf("Node %d: Marked %d log entries as DELETED before seq=%d", 
        node.nodeId, markedCount, stableSeq)
}

func (node *Node) requestStateFromTarget(seq int32,targetId int32) {
	req := &pb.StateTranserRequest{
		NodeId: node.nodeId,
		TargetSeq: seq,
	}

	_,err := node.peers[targetId].MakeStateTransferRequest(context.Background(),req)

	if err != nil {
		log.Printf("[Node %d] Failed to send to state transfer request to node=%d",node.nodeId,targetId)
	} else {
		log.Printf("[Node %d] Successfully sent state transfer request to node=%d",node.nodeId,targetId)
	}
}

func (node *Node) MakeStateTransferRequest(ctx context.Context, req *pb.StateTranserRequest) (*emptypb.Empty, error) {
    if !node.isActive() {
        log.Printf("[Node %d] Inactive dropping STATE TRANSFER request",node.nodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received State Transfer Request from Node %d", node.nodeId, req.NodeId)
	
	node.muCheckpoint.RLock()
	lastStableCheckpoint := node.latestCheckpointMessage.SequenceNum
	node.muCheckpoint.RUnlock()

	// Should not happen as it is leader
	if req.TargetSeq < lastStableCheckpoint{
		log.Printf("[Node %d] Cant send state as I am myself lagging",node.nodeId)
		return &emptypb.Empty{}, nil
	}

    go node.sendStateSnapshot(req.NodeId)

    return &emptypb.Empty{}, nil
}

func (node *Node) sendStateSnapshot(targetNodeId int32) {
	log.Printf("[Node %d] Preparing snapshot for Node %d", node.nodeId, targetNodeId)

    node.muCheckpoint.RLock()
    
    if node.lastStableSnapshot == nil {
        node.muCheckpoint.RUnlock()
        log.Printf("[Node %d] ERROR: No stable snapshot to send!", node.nodeId)
        return
    }

	snapshotSeq := node.latestCheckpointMessage.SequenceNum
	snapshotMap := make(map[string]int32)

	iter,err := node.lastStableSnapshot.NewIter(nil)

	node.muCheckpoint.RUnlock()

	if err != nil {
		log.Printf("[Node %d] Error in iterating snapshot",node.nodeId)
		return
	}

    for iter.First(); iter.Valid(); iter.Next() {
        balance, _ := deserializeBalance(iter.Value())
        snapshotMap[string(iter.Key())] = balance
    }
    iter.Close()

	response := &pb.StateTransferResponse{
        LatestCheckpointSeqNum: snapshotSeq, 
        State:                  snapshotMap,
	}

	log.Printf("[Node %d] Pushing STABLE Snapshot (Seq: %d) to Node %d", node.nodeId, snapshotSeq, targetNodeId)
    _, err = node.peers[targetNodeId].HandleStateTransferResponse(context.Background(), response)

    if err != nil {
        log.Printf("[Node %d] Failed to send state snapshot to node=%d",node.nodeId,targetNodeId)
    }
}

func (node *Node) HandleStateTransferResponse(ctx context.Context,response *pb.StateTransferResponse)  (*emptypb.Empty, error){
	node.muExec.Lock()
	defer node.muExec.Unlock()

	if response.LatestCheckpointSeqNum <= node.lastExecSeqNo {
        log.Printf("[Node %d] IGNORING Snapshot (Seq %d) <= Current Exec (Seq %d)", 
            node.nodeId, response.LatestCheckpointSeqNum, node.lastExecSeqNo)
        return &emptypb.Empty{}, nil
    }

	start := []byte("account:")
    end := []byte("account;")

	node.muState.Lock()

	if err := node.state.DeleteRange(start, end, pebble.NoSync); err != nil {
		node.muState.Unlock()

        log.Printf("[Node %d] CRITICAL: Failed to wipe DB: %v", node.nodeId, err)
        return &emptypb.Empty{}, nil
    }

	batch := node.state.NewBatch()
    defer batch.Close()

	for name, balance := range response.State {
        val, _ := serializeBalance(balance)
        if err := batch.Set(accountKey(name), val, nil); err != nil {
			node.muState.Unlock()

			log.Printf("[Node %d] Error while setting key in state repair",node.nodeId)
            return &emptypb.Empty{}, nil
        }
    }

	if err := batch.Commit(pebble.NoSync); err != nil {
		node.muState.Unlock()

        log.Printf("[Node %d] CRITICAL: Failed to commit snapshot: %v", node.nodeId, err)
        return &emptypb.Empty{}, fmt.Errorf("commit failed")
    }

	node.muState.Unlock()

	node.lastExecSeqNo = response.LatestCheckpointSeqNum

	node.muCheckpoint.Lock()
	if node.latestCheckpointMessage == nil || response.LatestCheckpointSeqNum > node.latestCheckpointMessage.SequenceNum {
		log.Printf("[Node %d] Updating stable checkpoint metadata to seq=%d", node.nodeId, response.LatestCheckpointSeqNum)
		node.latestCheckpointMessage = &pb.CheckpointMessage{
			SequenceNum: response.LatestCheckpointSeqNum,
			Digest: "",// TO DO update this with apt value
		}
		go node.garbageCollectBeforeCheckpoint(response.LatestCheckpointSeqNum)
	}
	node.muCheckpoint.Unlock()


	log.Printf("[Node %d] STATE TRANSFER COMPLETE. Jumped to Seq %d.", 
        node.nodeId, response.LatestCheckpointSeqNum)

	if node.hasPendingWork() {
		log.Printf("[Node %d] Pending work present restarting liveness timer",node.nodeId)
		node.livenessTimer.Restart()
	} else {
		log.Printf("[Node %d] No pending work present stopping liveness timer",node.nodeId)
		node.livenessTimer.Stop()
	}

	return &emptypb.Empty{},nil
} 


func (node *Node) PrintAcceptLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	node.PrintAcceptLogUtil()
	
	return &emptypb.Empty{},nil
}

func(node *Node) PrintBalance(ctx context.Context,req *pb.PrintBalanceReq ) (*emptypb.Empty, error) {
	balance,err := node.getBalance(req.Datapoint)

	if err == nil {
		fmt.Printf("[Node %d] %s=%d \n",node.nodeId,req.Datapoint,balance)
	} else {
		log.Printf("[Node %d] Error while reading balance for datapoint=%s err=%v",node.nodeId,req.Datapoint,err)
	}

	return &emptypb.Empty{},nil
}

func(node *Node) FailNode(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	node.Deactivate()

	return &emptypb.Empty{},nil
}

func(node *Node) RecoverNode(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	node.Activate()

	return &emptypb.Empty{},nil
}