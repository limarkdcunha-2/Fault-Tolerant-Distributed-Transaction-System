/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"context"
	"log"
	pb "transaction-processor/message"
)

func (node *Node) doNothing() {}

func(node *Node) onLivenessTimerExpired(){
	log.Printf("[Node %d] Liveness timer expired",node.nodeId)
	node.muLeader.Lock()
	node.isLeaderKnown = false
	// log.Printf("[Node %d] isLeaderKnown=%v FIRST",node.nodeId,node.isLeaderKnown)
	node.muLeader.Unlock()

	// 1. Need to wait here until it expires
	if node.prepareTimer.IsRunning(){
		log.Printf("[Node %d] Prepare timer is running so WAITING",node.nodeId)
		
		for node.prepareTimer.IsRunning() {
			// DO nothing just wait here
		} 

		log.Printf("[Node %d] Prepare timer WAITING over",node.nodeId)
	}

	// 2. Once timer expires check if any prepare messages were logged
	node.muPreLog.RLock()
	isPrepareLogNotEmpty := len(node.prepareLog.log) > 0
	hightesBallotPrepare := node.prepareLog.highestBallotPrepare
	node.muPreLog.RUnlock()

	// 2a Check if prepare messages are logged
	if isPrepareLogNotEmpty {
		log.Printf("[Node %d] Processing loggged PREPAREs",node.nodeId)
		node.muBallot.Lock()
		selfBallot := node.promisedBallotPrepare

		// 2b Check if the highest ballot prepare is still the highest
		if !node.isBallotGreaterThan(hightesBallotPrepare.Ballot,selfBallot) {
			log.Printf("[Node %d] Logged PREPAPRE is not the highest",node.nodeId)
			node.muBallot.Unlock()
			return
		}

		log.Printf("[Node %d] Logged PREPAPRE is the highest",node.nodeId)
		// 2c If this logged prepare is with highest ballot then update self ballot
		// And send Promise message
		node.promisedBallotPrepare = hightesBallotPrepare.Ballot
		node.muBallot.Unlock()

		node.buildAndSendPromise(hightesBallotPrepare.Ballot)

		// 5. Start tp timer
		if node.prepareTimer.IsRunning() {
			log.Printf("[Node %d] Restarting tp timer",node.nodeId)
			node.prepareTimer.Restart()
		} else {
			log.Printf("[Node %d] Starting tp timer",node.nodeId)
			node.prepareTimer.Start()
		}

		return
	} 
	
	// Else check for sending a prepare message
	// Check if leader election is over
	node.muLeader.RLock()
	isLeaderKnown := node.isLeaderKnown
	node.muLeader.RUnlock()

	// log.Printf("[Node %d] isLeaderKnown=%v SECOND",node.nodeId,isLeaderKnown)

	if !isLeaderKnown {
		// If leader not known yet send prepare message to trigger leader election
		node.startLeaderElection()
	}
}

func (node *Node) startLeaderElection(){
	log.Printf("[Node %d] Starting leader election",node.nodeId)

	node.muLeader.Lock()
	node.inLeaderElection = true
	node.isLeaderKnown = false
	node.muLeader.Unlock()

	node.muBallot.Lock()
	updatedBallot := &pb.BallotNumber{
		NodeId: node.nodeId,
		RoundNumber: node.promisedBallotAccept.RoundNumber+1,
	}
	// node.myBallot = updatedBallot
	node.promisedBallotPrepare = updatedBallot

	node.muBallot.Unlock()

	prepareMsg := &pb.PrepareMessage{
		Ballot: updatedBallot,
	}

	node.muCheckpoint.Lock()
	lastCheckpointSeq := node.latestCheckpointMessage.SequenceNum
	lastCheckpointDigest := node.latestCheckpointMessage.Digest 
	node.muCheckpoint.Unlock()

	// Log own PROMISE message
	selfPromiseMsg := &pb.PromiseMessage {
		Ballot: updatedBallot,
		AcceptLog: node.getAcceptedLogForPromise(lastCheckpointSeq),
		NodeId: node.nodeId,
		LastCheckpointSeq: lastCheckpointSeq,
		LastCheckpointDigest: lastCheckpointDigest,
	}
	
	node.muProLog.Lock()
	node.promiseLog.log = make(map[int32]*pb.PromiseMessage)
	node.promiseLog.log[node.nodeId] = selfPromiseMsg
	node.promiseLog.isPromiseQuorumReached = false
	node.muProLog.Unlock()

	go node.broadcastPrepareMessage(prepareMsg)
}

func (node *Node) broadcastPrepareMessage(msg *pb.PrepareMessage){
	allNodes := node.getAllClusterNodes()

	log.Printf("[Node %d] Broadcasting PREPARE Round=%d", 
                node.nodeId, msg.Ballot.RoundNumber)

	for _, nodeId := range allNodes {
 		if nodeId == node.nodeId {
            continue
        }

		peerClient, ok := node.peers[nodeId]
        if !ok {
            log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping PREPARE broadcast.", 
                node.nodeId, nodeId)
            continue
        }

		go func(id int32, client pb.MessageServiceClient) {
            _, err := client.HandlePrepare(context.Background(), msg)
            
            if err != nil {
                log.Printf("[Node %d] FAILED to send PREPARE Round=%d to Node %d: %v", 
                    node.nodeId, msg.Ballot.RoundNumber, id, err)
            }
        }(nodeId, peerClient)
	}
}