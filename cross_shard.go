/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
	pb "transaction-processor/message"
)

type WALRole string

const (
    WALRoleSenderSide   WALRole = "senderSide"
    WALRoleReceiverSide WALRole = "receiverSide"
)

type WALEntry struct {
	ReqId string
    SequenceNum int32 
    Sender string
    Receiver string
    Amount int32 
	Role WALRole 
}

type WriteAheadLog struct {
	mu sync.RWMutex
	logFile *os.File
	logPath string
	encoder *json.Encoder
	nodeId int32
}

func NewWriteAheadLog(nodeId int32, dataDir string) (*WriteAheadLog, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}
	
	logPath := filepath.Join(dataDir, fmt.Sprintf("node_%d_wal.log", nodeId))
	
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	
	wal := &WriteAheadLog{
		logFile: logFile,
		logPath: logPath,
		encoder: json.NewEncoder(logFile),
		nodeId: nodeId,
	}
	
	log.Printf("[Node %d] WAL initialized at %s", nodeId, logPath)
	return wal, nil
}

func(wal *WriteAheadLog) appendToLog(req *pb.ClientRequest, seqNum int32,role WALRole) error {
	reqKey := makeRequestKey(req.ClientId,req.Timestamp)

	entry := &WALEntry{
		ReqId:       reqKey,
		SequenceNum: seqNum,
		Sender:      req.Transaction.Sender,
		Receiver:    req.Transaction.Receiver,
		Amount:      req.Transaction.Amount,
		Role: role,
	}

	wal.mu.Lock()
	defer wal.mu.Unlock()

	if err := wal.encoder.Encode(entry); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// This sync can be time consuming
	if err := wal.logFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	log.Printf("[Node %d] WAL: Appended Seq=%d, (%s, %s,%d)", 
		wal.nodeId, seqNum, req.Transaction.Sender, req.Transaction.Receiver, req.Transaction.Amount)
	
	return nil
}

func(wal *WriteAheadLog) getFromLog(reqId string) *WALEntry {
	wal.mu.RLock()
	defer wal.mu.RUnlock()

	file, err := os.Open(wal.logPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("[Node %d] WAL: File doesn't exist yet", wal.nodeId)
			return nil
		}
		log.Printf("[Node %d] WAL: Failed to open file for reading: %v", wal.nodeId, err)
		return nil
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	var lastMatchingEntry *WALEntry

	for scanner.Scan() {
		line := scanner.Text()
		
		var entry WALEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			log.Printf("[Node %d] WAL: Failed to parse line: %v", wal.nodeId, err)
			continue
		}
		
		if entry.ReqId == reqId {
			lastMatchingEntry = &entry
		}
	}

	if err := scanner.Err(); err != nil {
		log.Printf("[Node %d] WAL: Error reading file: %v", wal.nodeId, err)
		return nil
	}
	
	if lastMatchingEntry != nil {
	return lastMatchingEntry
	}
	
	log.Printf("[Node %d] WAL: No entry found for %s", wal.nodeId, reqId)
	return nil
}


func(node *Node) handleAbortActions(reqKey string){
	log.Printf("[Node %d] Handling ABORT actions for request %s", node.nodeId, reqKey)

	walEntry := node.wal.getFromLog(reqKey)
    if walEntry == nil {
        log.Printf("[Node %d] No WAL entry found for %s - nothing to undo", node.nodeId, reqKey)
        return
    }

	log.Printf("[Node %d] Found WAL entry: Seq=%d, Role=%s, Sender=%s, Receiver=%s, Amount=%d",
        node.nodeId, walEntry.SequenceNum, walEntry.Role, walEntry.Sender, walEntry.Receiver, walEntry.Amount)

	var success bool
    var err error

	if walEntry.Role == WALRoleSenderSide {
		log.Printf("[Node %d] UNDO: Crediting sender %s by %d (reversing debit)", 
            node.nodeId, walEntry.Sender, walEntry.Amount)

		success, err = node.undoSenderDebit(walEntry.Sender, walEntry.Amount)

		if err != nil {
            log.Printf("[Node %d] ERROR undoing sender debit: %v", node.nodeId, err)
            return
        }

		log.Printf("[Node %d] UNDO COMPLETE: Sender %s credited back %d", 
            node.nodeId, walEntry.Sender, walEntry.Amount)

		node.releaseLock(walEntry.Sender)

		log.Printf("[Node %d] Released lock on datapoint (%s)",node.nodeId,walEntry.Sender)
	} else if walEntry.Role == WALRoleReceiverSide {
		log.Printf("[Node %d] UNDO: Debiting receiver %s by %d (reversing credit)",
            node.nodeId, walEntry.Receiver, walEntry.Amount)

		success, err = node.undoReceiverCredit(walEntry.Receiver, walEntry.Amount)
        
        if err != nil {
            log.Printf("[Node %d] ERROR undoing receiver credit: %v", node.nodeId, err)
            return
        }
        
        log.Printf("[Node %d] UNDO COMPLETE: Receiver %s debited back %d",
            node.nodeId, walEntry.Receiver, walEntry.Amount)

		node.releaseLock(walEntry.Receiver)

		log.Printf("[Node %d] Released lock on datapoint (%s)",node.nodeId,walEntry.Receiver)
	}

	if !success {
        log.Printf("[Node %d] ABORT actions failed for %s", node.nodeId, reqKey)
    } else {
        log.Printf("[Node %d] ABORT actions complete for %s", node.nodeId, reqKey)
    }
}


func (wal *WriteAheadLog) Close() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	
	if wal.logFile != nil {
		log.Printf("[Node %d] WAL: Closing file", wal.nodeId)
		return wal.logFile.Close()
	}
	return nil
}

func(node *Node) on2PCTimerExpired(req *pb.ClientRequest){
	log.Printf("[Node %d] Timer expired for 2PC protocol for req (%s, %s, %d)",
	node.nodeId,req.Transaction.Sender,req.Transaction.Receiver,req.Transaction.Amount)

	node.muBallot.RLock()
	roundNumber := node.promisedBallotAccept.RoundNumber
    leaderId := node.promisedBallotAccept.NodeId
    node.muBallot.RUnlock()

    if leaderId != node.nodeId {
        log.Printf("[Node %d] Not leader anymore; ignoring timer expiry", node.nodeId)
        return
    }

	reqKey := makeRequestKey(req.ClientId, req.Timestamp)

	node.muCrossSharTxs.Lock()
    tx, exists := node.crossSharTxs[reqKey]
    if !exists {
        node.muCrossSharTxs.Unlock()
        log.Printf("[Node %d] No transaction state for %s; likely already completed", node.nodeId, reqKey)
        return
    }
    seq := tx.SequenceNum
	if tx.shouldKeepSendingCommmit {
		tx.shouldKeepSendingCommmit = false
	}
    node.muCrossSharTxs.Unlock()

	node.muLog.Lock()
    entry, exists := node.acceptLog[seq]
   
	if !exists {
		node.muLog.Unlock()
        log.Printf("[Node %d] No log entry for seq=%d during timeout", node.nodeId, seq)
        return
    }

	entry.mu.Lock()
	node.muLog.Unlock()

	if entry.TwoPCPhase == PhaseCommitted {
        log.Printf("[Node %d] Seq=%d already in second round phase (committed), ignoring timeout", 
            node.nodeId, seq)
        entry.mu.Unlock()
        return
    }

	if entry.Phase <= PhaseCommitted {
        log.Printf("[Node %d] Seq=%d PREPARE not yet committed, cannot timeout yet", 
            node.nodeId, seq)
        entry.mu.Unlock()
        return
    }

	// This wont happen but just to be sure
	if entry.EntryAcceptType != pb.AcceptType_PREPARE {
        log.Printf("[Node %d] Seq=%d is not PREPARE (type=%v), cannot timeout", 
            node.nodeId, seq, entry.EntryAcceptType)
        entry.mu.Unlock()
        return
    }

	entry.TwoPCPhase = PhaseAccepted
    entry.EntryAcceptType = pb.AcceptType_ABORT

	selfAcceptedMessage := &pb.AcceptedMessage{
        Ballot:      entry.Ballot,
        SequenceNum: seq,
        Request:     entry.Request,
        NodeId:      node.nodeId,
        AcceptType:  pb.AcceptType_ABORT,
    }

	if entry.TwoPCAcceptedMessages == nil {
        entry.TwoPCAcceptedMessages = make(map[int32]*pb.AcceptedMessage)
    }
    entry.TwoPCAcceptedMessages[node.nodeId] = selfAcceptedMessage
    entry.TwoPCAcceptCount = 1

    entry.mu.Unlock()

	log.Printf("[Node %d] Seq=%d timeout confirmed, starting ABORT actions and consensus", 
        node.nodeId, seq)

	node.handleAbortActions(reqKey)

	abortAccept := &pb.AcceptMessage{
        Ballot:      selfAcceptedMessage.Ballot,
        SequenceNum: seq,
        Request:     selfAcceptedMessage.Request,
        AcceptType:  pb.AcceptType_ABORT,
    }

	go node.broadcastAcceptMessage(abortAccept)

	abortMsg := &pb.TwoPCAbortMessage {
		Request: selfAcceptedMessage.Request,
		NodeId: node.nodeId,
	}

	go node.sendTwoPCAbortToParticipant(abortMsg)

	// Send reply to client
	abortReply := &pb.ReplyMessage{
		Ballot: &pb.BallotNumber{
			NodeId: leaderId,
			RoundNumber: roundNumber,
		},
		ClientRequestTimestamp: abortMsg.Request.Timestamp,
		ClientId: abortMsg.Request.ClientId,
		Status: "failure",
	}

	node.muReplies.Lock()
	node.replies[reqKey] = abortReply
	node.muReplies.Unlock()

	go node.sendReplyToClient(abortReply)
}


func(node *Node) sendTwoPCAbortToParticipant(msg *pb.TwoPCAbortMessage){
	targetClusterNodes  := node.findTargetClusterIds(msg.Request.Transaction.Receiver)

	for {
		if !node.shouldKeepSendingAbort(msg.Request){
			break
		}

		for _, nodeId := range targetClusterNodes {
			peerClient, ok := node.peers[nodeId]

			log.Printf("[Node %d] Sending 2PC ABORT(%s, %s, %d) to node=%d",node.nodeId,
			msg.Request.Transaction.Sender,msg.Request.Transaction.Receiver,msg.Request.Transaction.Amount,nodeId)

			if !ok {
				log.Printf("[Node %d] ERROR: No peer client connection found for Node %d. Skipping 2PC ABORT broadcast.", 
					node.nodeId, nodeId)
				continue
			}

			go func(id int32, client pb.MessageServiceClient) {
				_, err := client.HandleTwoPCAbortAsParticipant(context.Background(),msg)
				if err != nil {
					log.Printf("[Node %d] Failed to 2PC ABORT(%s, %s, %d) to node=%d",node.nodeId,
					msg.Request.Transaction.Sender,msg.Request.Transaction.Receiver,msg.Request.Transaction.Amount,id)
				}
			}(nodeId, peerClient)
		}

		time.Sleep(10*time.Millisecond)
	}
}