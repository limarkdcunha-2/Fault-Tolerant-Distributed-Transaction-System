/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import "log"


func(node *Node) on2PCTimerExpired(){
	log.Printf("[Node %d] Timer expired for 2PC protocol",node.nodeId)
}