/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"sync"
	pb "transaction-processor/message"
)


type LogEntry struct {
	mu sync.Mutex
	SequenceNum  int32
	
}
 
type Node struct {
	pb.UnimplementedMessageServiceServer

	nodeId int32
	portNo int32
	N int32
	f int32

	muLeader sync.RWMutex
	leaderId int32

	muLog sync.RWMutex
	currentSeqNo int32
	log map[int32]*LogEntry
}	


func NewNode(nodeId, portNo int32) (*Node, error) {
	// nodeIdStr := strconv.Itoa(int(nodeId))
	N := int32(len(getNodeCluster()))
	f := (N - 1) / 2


	newNode :=  &Node{
		nodeId: nodeId,
		N:N,
		f:f,
		portNo: portNo,
		leaderId: 0,
		currentSeqNo: 0,
		log:make(map[int32]*LogEntry),
	}

	return newNode,nil
}






