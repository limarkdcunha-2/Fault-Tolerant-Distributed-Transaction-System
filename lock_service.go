/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"log"
	"strconv"
)


func (node *Node) getLockIndex(datapoint string) int {
	idx, err := strconv.Atoi(datapoint)
	if err != nil || idx < 1 || idx > 9000 {
		log.Printf("[Node %d] CRITICAL WARNING: Invalid lock key received: '%s'", node.nodeId, datapoint)
		return 0
	}
	return idx
}

func (node *Node) isLocked(datapoint string) bool {
	idx := node.getLockIndex(datapoint)
	if idx == 0 { return false }

	entry := node.locks[idx]

	entry.mu.Lock()
	defer entry.mu.Unlock()

	return entry.holder != ""
}
// needs to be called with muLocks held
func (node *Node) acquireLock(datapoint string, reqKey string) {
	idx := node.getLockIndex(datapoint)
	if idx == 0 { return }

	entry := node.locks[idx]

	entry.mu.Lock()
	entry.holder = reqKey
	entry.mu.Unlock()
}

func (node *Node) releaseLock(datapoint string) {
	idx := node.getLockIndex(datapoint)
	if idx == 0 { return }

	entry := node.locks[idx]

	entry.mu.Lock()
	entry.holder = "" 
	entry.mu.Unlock()
}

func (node *Node) tryAcquireTransactionLocks(sender, receiver, reqKey string) bool {
    idx1 := node.getLockIndex(sender)
    idx2 := node.getLockIndex(receiver)

    if idx1 == 0 || idx2 == 0 { return false }

    first, second := idx1, idx2
    if first > second {
        first, second = second, first
    }

    entry1 := node.locks[first]
    entry2 := node.locks[second]

    entry1.mu.Lock()
    if first != second {
        entry2.mu.Lock()
    }

    defer func() {
        if first != second {
            entry2.mu.Unlock()
        }
        entry1.mu.Unlock()
    }()

    // Check Sender
    if entry1.holder != "" && entry1.holder != reqKey {
        return false // Fail: Sender is locked
    }
    // Check Receiver
    if entry2.holder != "" && entry2.holder != reqKey {
        return false // Fail: Receiver is locked
    }

    // 4. Success: We own both now.
    entry1.holder = reqKey
    entry2.holder = reqKey

    log.Printf("[Node %d] locks acquired for (%s, %s) reqkey=%s",node.nodeId,sender,receiver,reqKey)

    return true
}

func (node *Node) releaseTransactionLocks(sender, receiver string) {
    idx1 := node.getLockIndex(sender)
    idx2 := node.getLockIndex(receiver)

    if idx1 == 0 || idx2 == 0 { return }

    // 1. Sort indices to match the Acquire order (Crucial for Deadlock prevention)
    first, second := idx1, idx2
    if first > second {
        first, second = second, first
    }

    entry1 := node.locks[first]
    entry2 := node.locks[second]

    // 2. Lock Mutexes
    entry1.mu.Lock()
    if first != second {
        entry2.mu.Lock()
    }

    

    // 3. RELEASE: Set holder to empty string
    entry1.holder = ""
    entry2.holder = ""

    log.Printf("[Node %d] locks released for (%s, %s)",node.nodeId,sender,receiver)

    // 4. Unlock Mutexes
    if first != second {
        entry2.mu.Unlock()
    }
    entry1.mu.Unlock()
}

// Helper to check lock status without acquiring
func (node *Node) areDatapointsLocked(sender, receiver string) bool {
    // Check Sender
    idx1 := node.getLockIndex(sender)
    if idx1 != 0 {
        entry1 := node.locks[idx1]
        entry1.mu.Lock()
        isLocked := entry1.holder != ""
        entry1.mu.Unlock()
        
        if isLocked { return true }
    }

    // Check Receiver
    if receiver != sender {
        idx2 := node.getLockIndex(receiver)
        if idx2 != 0 {
            entry2 := node.locks[idx2]
            entry2.mu.Lock()
            isLocked := entry2.holder != ""
            entry2.mu.Unlock()
            
            if isLocked { return true }
        }
    }

    return false
}

func keyUpperBound(b []byte) []byte {
	end := make([]byte, len(b))
	copy(end, b)
	for i := len(end) - 1; i >= 0; i-- {
		end[i] = end[i] + 1
		if end[i] != 0 {
			return end[:i+1]
		}
	}
	return nil
}

// func (node *Node) releaseAllLocks() {
//     node.muState.Lock()
//     defer node.muState.Unlock()

//     if node.state == nil {
//         return
//     }

//     prefix := []byte("lock:")
    
//     iterOptions := &pebble.IterOptions{
//         LowerBound: prefix,
//         UpperBound: keyUpperBound(prefix),
//     }

//     iter, err := node.state.NewIter(iterOptions)
//     if err != nil {
//         log.Printf("[Node %d] Error creating iterator for releasing locks: %v", node.nodeId, err)
//         return
//     }
//     defer iter.Close()

//     // Use a Batch for atomic, efficient deletion
//     batch := node.state.NewBatch()
//     defer batch.Close()

//     count := 0
//     for iter.First(); iter.Valid(); iter.Next() {
//         key := make([]byte, len(iter.Key()))
//         copy(key, iter.Key())
        
//         batch.Delete(key, pebble.NoSync)
//         count++
//     }

//     if count > 0 {
//         if err := batch.Commit(pebble.NoSync); err != nil {
//             log.Printf("[Node %d] Failed to commit batch release of locks: %v", node.nodeId, err)
//         } else {
//             log.Printf("[Node %d] Released all %d locks due to leadership change/timeout", node.nodeId, count)
//         }
//     } else {
//         log.Printf("[Node %d] No locks found to release.", node.nodeId)
//     }
// }