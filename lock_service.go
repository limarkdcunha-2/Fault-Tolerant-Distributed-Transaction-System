/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"log"

	"github.com/cockroachdb/pebble"
)


func (node *Node) getLockKey(datapoint string) []byte {
	return []byte("lock:" + datapoint)
}

// needs to be called with muState held
func (node *Node) isLocked(datapoint string) bool {
	key := node.getLockKey(datapoint)
	
	_, closer, err := node.state.Get(key)
	
	if err == pebble.ErrNotFound {
		return false
	}
	if err != nil {
		return false
	}
	defer closer.Close()

	return true
}

// needs to be called with muState held
func (node *Node) acquireLock(datapoint string, reqKey string) error {
	key := node.getLockKey(datapoint)
	
	if err := node.state.Set(key, []byte(reqKey), pebble.NoSync); err != nil {
		log.Printf("failed to acquire lock for %s: %v", datapoint, err)
		return nil
	}
	
	return nil
}

// needs to be called with muState held
func (node *Node) releaseLock(datapoint string) error {
	key := node.getLockKey(datapoint)
	
	if err := node.state.Delete(key, pebble.NoSync); err != nil {
		log.Printf("failed to release lock for %s: %v", datapoint, err)
		return nil
	}
	
	return nil
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

func (node *Node) releaseAllLocks() {
    node.muState.Lock()
    defer node.muState.Unlock()

    if node.state == nil {
        return
    }

    prefix := []byte("lock:")
    
    iterOptions := &pebble.IterOptions{
        LowerBound: prefix,
        UpperBound: keyUpperBound(prefix),
    }

    iter, err := node.state.NewIter(iterOptions)
    if err != nil {
        log.Printf("[Node %d] Error creating iterator for releasing locks: %v", node.nodeId, err)
        return
    }
    defer iter.Close()

    // Use a Batch for atomic, efficient deletion
    batch := node.state.NewBatch()
    defer batch.Close()

    count := 0
    for iter.First(); iter.Valid(); iter.Next() {
        key := make([]byte, len(iter.Key()))
        copy(key, iter.Key())
        
        batch.Delete(key, pebble.NoSync)
        count++
    }

    if count > 0 {
        if err := batch.Commit(pebble.NoSync); err != nil {
            log.Printf("[Node %d] Failed to commit batch release of locks: %v", node.nodeId, err)
        } else {
            log.Printf("[Node %d] Released all %d locks due to leadership change/timeout", node.nodeId, count)
        }
    } else {
        log.Printf("[Node %d] No locks found to release.", node.nodeId)
    }
}