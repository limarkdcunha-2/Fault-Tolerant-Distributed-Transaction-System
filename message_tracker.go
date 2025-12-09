/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// For client side
// ReplyRecord represents a single reply received by the client
type ReplyRecord struct {
    Timestamp int64
    LeaderId      int32
    Status    string
    ReqKey    string
}

// ReplyTracker tracks all replies received by a client
type ReplyTracker struct {
    mu      sync.RWMutex
    replies []ReplyRecord
}

// NewReplyTracker creates a new reply tracker
func NewReplyTracker() *ReplyTracker {
    return &ReplyTracker{
        replies: make([]ReplyRecord, 0),
    }
}

// Add records a new reply
func (rt *ReplyTracker) Add(record ReplyRecord) {
    rt.mu.Lock()
    defer rt.mu.Unlock()
    rt.replies = append(rt.replies, record)
}

// Clear removes all replies
func (rt *ReplyTracker) Clear() {
    rt.mu.Lock()
    defer rt.mu.Unlock()
    rt.replies = make([]ReplyRecord, 0)
}

// PrintAll prints all replies in formatted output
func (rt *ReplyTracker) PrintAll() {
    rt.mu.RLock()
    defer rt.mu.RUnlock()

    fmt.Printf("\n[Client] REPLY History (%d replies)\n", len(rt.replies))
    fmt.Println(strings.Repeat("-", 80))

    if len(rt.replies) == 0 {
        fmt.Println("  (no replies received)")
        fmt.Println(strings.Repeat("-", 80))
        return
    }

    for _, reply := range rt.replies {
        ts := time.Unix(0, reply.Timestamp).Format("15:04:05.000")
        fmt.Printf("[%s] [REPLY] LeaderId=%d Status %s ReqKey=%s\n",
            ts, reply.LeaderId, reply.Status, reply.ReqKey)
    }
    
    fmt.Println(strings.Repeat("-", 80))
}

// GetCount returns the number of replies tracked
func (rt *ReplyTracker) GetCount() int {
    rt.mu.RLock()
    defer rt.mu.RUnlock()
    return len(rt.replies)
}