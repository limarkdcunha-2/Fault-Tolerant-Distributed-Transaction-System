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
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping REQUEST message",node.nodeId)
        return &emptypb.Empty{}, nil
    }

	log.Printf("[Node %d] Received client request from Client %d (ts=%s) [Sender=%s, Receiver=%s, Amount=%d]",
        node.nodeId, req.ClientId, req.Timestamp.AsTime(),
        req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)

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
		// 2. Check if already processed this request before
	if cachedReply, exists := node.GetCachedReply(req.ClientId, req.Timestamp); exists {
		log.Printf("[Node %d] Duplicate request from client %d (ts=%s). Returning cached reply.",
			node.nodeId, req.ClientId, req.Timestamp.AsTime())

        go node.sendReplyToClient(cachedReply)
        return
    }

	// 3. Check pending requests (assigned seq but not executed)
    if isPending := node.checkIfPending(req.ClientId, req.Timestamp); isPending {
        log.Printf("Node %d: Duplicate (pending) from client %d - ignoring retry",
            node.nodeId, req.ClientId)
        return
    }

	// 4. Reroute the request to leader
	if node.nodeId != leaderId {
		log.Printf("[Node %d] Not primary, routing client request to primary", node.nodeId)
		
		node.markRequestPending(req.ClientId, req.Timestamp)

		if !node.livenessTimer.IsRunning() {
			node.livenessTimer.Start()
		}
		
		node.peers[leaderId].SendRequestMessage(context.Background(),req)
		return
	}

	// 5. Assign seq number
	node.muLog.Lock()
	node.currentSeqNo++
	seq := node.currentSeqNo
	node.muLog.Unlock()

	log.Printf("[Node %d] Assigning seq=%d to request (%s, %s, %d)",
	node.nodeId,seq,req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)

	// 6. Mark this request in processing
	node.markRequestPending(req.ClientId, req.Timestamp)

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
		Phase: PhaseNone,
	}

	
	node.muLog.Lock()
	node.acceptLog[acceptMessage.SequenceNum] = newEntry;
	node.muLog.Unlock()

	// 8. Broadcast accept message
	go node.broadcastAcceptMessage(acceptMessage)
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
				existingEntry.Ballot =msg.Ballot

				acceptedMessage := &pb.AcceptedMessage{
					Ballot: existingEntry.Ballot,
					SequenceNum: existingEntry.SequenceNum,
					Request: existingEntry.Request,
					NodeId: node.nodeId,
				}

				existingEntry.mu.Unlock()

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
    }
	node.acceptLog[msg.SequenceNum] = newEntry
	node.muLog.Unlock()

	acceptedMessage := &pb.AcceptedMessage{
		Ballot: msg.Ballot,
		SequenceNum: msg.SequenceNum,
		Request: msg.Request,
		NodeId: node.nodeId,
	}

	// Impt: Start timer if not already running
	if !node.livenessTimer.IsRunning(){
		node.livenessTimer.Start()
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
	if !node.isActive() {
		log.Printf("[Node %d] Inactive. Dropping ACCEPTED message for seq=%d",node.nodeId,msg.SequenceNum)
        return &emptypb.Empty{}, nil
    }

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
		log.Printf("[Node %d] Rejecting COMMIT message with stale ballot number (R:%d,N:%d)",node.nodeId,msg.Ballot.RoundNumber,msg.Ballot.NodeId)
		
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
	
	if entry.Phase >= PhaseCommitted {
		log.Printf("[Node %d] INFO: Already committed/executed Seq=%d. Ignoring duplicate COMMIT.",
			node.nodeId, msg.SequenceNum)
		entry.mu.Unlock()
		return &emptypb.Empty{}, nil
	}

	entry.Phase = PhaseCommitted
	// Note this line is really critical for safety
	entry.Ballot = msg.Ballot 
	
	entry.mu.Unlock()

	go node.executeInOrder(false)

	return &emptypb.Empty{}, nil
}

func (node *Node) executeInOrder(shouldSendReply bool) {
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

		if entry.Phase != PhaseCommitted {
			log.Printf("[Node %d] Running execution phase=%v is not committed for seq=%d",node.nodeId,entry.Phase,nextSeq)
            entry.mu.Unlock()
            break
        }

		// IF NO-OP
		if entry.Request.ClientId == -1 {
			log.Printf("[Node %d] EXECUTED NO-OP Seq=%d.", node.nodeId, nextSeq)
		} else {

			_, exists = node.GetCachedReply(entry.Request.ClientId, entry.Request.Timestamp)

			// Non executed new request
			if !exists {	
				log.Printf("[Node %d] Request for seq=%d has never been executed before",node.nodeId,nextSeq)

				node.muState.Lock()

				var status string
				
				// Execute the request
				log.Printf("[Node %d] [Ballot %d] EXECUTING: Seq=%d, Client=%d, Sender=%s, Receiver=%s, Amount=%d",
					node.nodeId, entry.Ballot.RoundNumber,entry.SequenceNum, entry.Request.ClientId,
					entry.Request.Transaction.Sender, entry.Request.Transaction.Receiver,
					entry.Request.Transaction.Amount)
				
				
				transaction := Transaction{
					Sender:   entry.Request.Transaction.Sender,
					Receiver: entry.Request.Transaction.Receiver,
					Amount:   entry.Request.Transaction.Amount,
				}

				response, _ := node.processTransaction(transaction)

				status = "failure"
				if response {
					status = "success"
				}
			
				node.persistState()
				

				reply := &pb.ReplyMessage{
					ClientId:         entry.Request.ClientId,
					ClientRequestTimestamp: entry.Request.Timestamp,
					Status:           status,
					Ballot: entry.Ballot,
				}

				// Check if we should create a checkpoint
				// if nextSeq % node.checkpointInterval == 0{
				// 	stateDigest := node.computeStateDigest()
				// 	go node.createAndBroadcastCheckpoint(nextSeq,stateDigest,entry.View)
				// }

				node.muState.Unlock()

				node.cacheReplyAndClearPending(reply)
				
				if shouldSendReply {
					go node.sendReplyToClient(reply)
				}
				
				log.Printf("[Node %d] EXECUTED: Seq=%d successfully. Status=%s", node.nodeId, entry.SequenceNum, status)
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
	grpcClient, _ := node.getConnForClient(reply.ClientId)

	grpcClient.HandleReply(context.Background(),reply)
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
	promiseMsg := &pb.PromiseMessage {
		Ballot: ballot,
		AcceptLog: node.getAcceptedLogForPromise(),
		NodeId: node.nodeId,
	}
	
	go node.sendPromiseMessage(promiseMsg)
}

func (node *Node) getAcceptedLogForPromise() []*pb.AcceptedMessage {
    node.muLog.RLock()
    defer node.muLog.RUnlock()

    var acceptedEntries []*pb.AcceptedMessage

    for _, entry := range node.acceptLog {
        if entry.Phase == PhaseAccepted {
            amsg := &pb.AcceptedMessage{
                Ballot:      entry.Ballot,
                SequenceNum: entry.SequenceNum,
                Request:     entry.Request,
                NodeId:      node.nodeId,
            }
            acceptedEntries = append(acceptedEntries, amsg)
        }
    }
    return acceptedEntries
}

func(node *Node) sendPromiseMessage(msg *pb.PromiseMessage){
	log.Printf("[Node %d] Sending PROMISE to %d",node.nodeId,msg.Ballot.NodeId)

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
		mergedLog := node.buildMergedAcceptLog()

		node.muProLog.Unlock()

		// 4b. Update self ballot number
		node.muBallot.Lock()
		node.promisedBallotAccept = node.promisedBallotPrepare
		node.myBallot = node.promisedBallotPrepare
		node.muBallot.Unlock()

		// 4c. Updated election state pointers
		node.muLeader.Lock()
		node.isLeaderKnown = true
		node.inLeaderElection = false
		node.muLeader.Unlock()

		// 4d. Install new log unto self
		node.installMergedAcceptLog(mergedLog)

		// 4e. Broadcast new view
		newViewMsg := &pb.NewViewMessage{
			Ballot: selfBallot,
			AcceptLog: mergedLog,
			NodeId: node.nodeId,
		}

		go node.broadcastNewView(newViewMsg)

		// 4f. Start draining pending requests queue
		node.drainQueuedRequests(node.nodeId)

		return &emptypb.Empty{},nil
	}

	node.muProLog.Unlock()
	return &emptypb.Empty{},nil
}

// Lock for promise log has been held in parent
func(node *Node) buildMergedAcceptLog() []*pb.AcceptedMessage {
	// update minS with checkpointing interval
	var minS int32 = 1
    var maxSeq int32 = 0
	
	merged := make(map[int32]*pb.AcceptedMessage)

	for _, promise := range node.promiseLog.log {
        for _, accepted := range promise.AcceptLog {
            seq := accepted.SequenceNum
            if seq > maxSeq {
                maxSeq = seq
            }
            existing, exists := merged[seq]
            if !exists || node.isBallotGreaterThan(accepted.Ballot, existing.Ballot) {
                merged[seq] = accepted
            }
        }
    }

	for seq := minS; seq <= maxSeq; seq++ {
        _, exists := merged[seq]
        if !exists {
            noopAccepted := &pb.AcceptedMessage{
                SequenceNum: seq,
                Ballot:      node.promisedBallotPrepare,
                Request: &pb.ClientRequest{
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

	mergedSlice := make([]*pb.AcceptedMessage, maxSeq)
    for seq := minS; seq <= maxSeq; seq++ {
        mergedSlice[seq-1] = merged[seq]
    }

    return mergedSlice
}

func (node *Node) installMergedAcceptLog(mergedLog []*pb.AcceptedMessage) {
	node.muLog.Lock()
    defer node.muLog.Unlock()

	maxSeq := int32(len(mergedLog))

	for _, acceptedMsg := range mergedLog {
		seq := acceptedMsg.SequenceNum
        if seq > maxSeq {
            maxSeq = seq
        }

		entry, exists := node.acceptLog[seq]

		if !exists {
            node.acceptLog[seq] = &LogEntry{
                SequenceNum: seq,
                Ballot:      acceptedMsg.Ballot,
                Request:     acceptedMsg.Request,
                Phase:       PhaseAccepted,
            }
        } else {
			entry.mu.Lock()

			if node.isBallotGreaterThan(acceptedMsg.Ballot,entry.Ballot){
				entry.Ballot = acceptedMsg.Ballot
			}
			entry.Phase = PhaseAccepted

			entry.mu.Unlock()
		}
	}

	// Clear any acceptLog entries beyond maxSeq to remove stale data
	for seq := range node.acceptLog {
        if seq > maxSeq {
            delete(node.acceptLog, seq)
        }
    }

	log.Printf("[Node %d] installMergedAcceptLog complete: maxSeq=%d, cleared entries beyond max", node.nodeId, maxSeq)
}

func(node *Node) broadcastNewView(msg *pb.NewViewMessage){
	allNodes := getAllNodeIDs()

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
                log.Printf("[Node %d] FAILED to send PREPARE Round=%d to Node %d: %v", 
                    node.nodeId, msg.Ballot.RoundNumber, id, err)
            }
        }(nodeId, peerClient)
	}
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

	// 2. Update the ballot and the flags
	node.muBallot.Lock()
	node.promisedBallotAccept = msg.Ballot
	node.promisedBallotPrepare = msg.Ballot
	node.muBallot.Unlock()

	node.muLeader.Lock()
	node.isLeaderKnown = true
	node.inLeaderElection = false
	node.muLeader.Unlock()

	// 3. Install log
	node.installMergedAcceptLog(msg.AcceptLog)

	// TO DO 4. Send accepted back for each entry
	

	return &emptypb.Empty{},nil
}


func (node *Node) PrintAcceptLog(ctx context.Context, req *emptypb.Empty) (*emptypb.Empty, error) {
	node.PrintAcceptLogUtil()
	
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