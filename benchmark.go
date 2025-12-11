/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"math/rand"
	"sync"
)

type BenchmarkConfig struct {
	NumAccounts     int32
	NumClusters     int32
	NumTransactions int
	ReadPct         float64
	IntraPct        float64

	//0.9 - 90% of transactions go to the hot set.
	Skew float64

	// HotDataPct represents the size of the hot set (0.0 - 1.0).
	// 0.1 - 10% of accounts considered hot.
	HotDataPct float64
}

type Benchmark struct {
	config BenchmarkConfig
	mu     sync.Mutex
    rrCounter int32
}

func NewBenchmark(cfg BenchmarkConfig) *Benchmark {
	// Set default hot data percentage if not specified but Skew is used
	if cfg.Skew > 0 && cfg.HotDataPct == 0 {
		cfg.HotDataPct = 0.10 
	}

	return &Benchmark{
		config: cfg,
	}
}

func (b *Benchmark) GenerateWorkload() []Transaction {
	txns := make([]Transaction, 0, b.config.NumTransactions)

	for i := 0; i < b.config.NumTransactions; i++ {
		txns = append(txns, b.generateSingleTransaction())
	}

	return txns
}

func (b *Benchmark) generateSingleTransaction() Transaction {
	isRead := rand.Float64() < b.config.ReadPct

	if isRead {
		account := b.pickAccountInRange(1, b.config.NumAccounts)
		return Transaction{
			Sender:   fmt.Sprintf("%d", account),
			Receiver: fmt.Sprintf("%d", account),
			Amount: 0,
		}
	}

	isIntra := rand.Float64() < b.config.IntraPct

	if isIntra {
		sender, receiver := b.pickIntraShardPair()
		return Transaction{
			Sender:   fmt.Sprintf("%d", sender),
			Receiver: fmt.Sprintf("%d", receiver),
			Amount: int32(rand.Intn(10) + 1),
		}
	}

	sender, receiver := b.pickCrossShardPair()
	return Transaction{
		Sender:   fmt.Sprintf("%d", sender),
		Receiver: fmt.Sprintf("%d", receiver),
		Amount: int32(rand.Intn(10) + 1),
	}
}

func (b *Benchmark) pickIntraShardPair() (int32, int32) {
    var clusterId int32

    if b.config.Skew == 0 {
        b.mu.Lock()
        clusterId = (b.rrCounter % b.config.NumClusters) + 1
        b.rrCounter++
        b.mu.Unlock()
    } else {
        clusterId = rand.Int31n(b.config.NumClusters) + 1
    }

    itemsPerShard := b.config.NumAccounts / b.config.NumClusters
    startId := (clusterId-1)*itemsPerShard + 1
    endId := clusterId * itemsPerShard

    if clusterId == b.config.NumClusters {
        endId = b.config.NumAccounts
    }

    sender := b.pickAccountInRange(startId, endId)
    receiver := b.pickAccountInRange(startId, endId)

    for receiver == sender {
        receiver = b.pickAccountInRange(startId, endId)
    }

    return sender, receiver
}

func (b *Benchmark) pickCrossShardPair() (int32, int32) {
    var c1 int32

    if b.config.Skew == 0 {
        b.mu.Lock()
        c1 = (b.rrCounter % b.config.NumClusters) + 1
        b.rrCounter++
        b.mu.Unlock()
    } else {
        c1 = rand.Int31n(b.config.NumClusters) + 1
    }

    c2 := rand.Int31n(b.config.NumClusters) + 1
    for c2 == c1 {
        c2 = rand.Int31n(b.config.NumClusters) + 1
    }

    itemsPerShard := b.config.NumAccounts / b.config.NumClusters

    start1 := (c1-1)*itemsPerShard + 1
    end1 := c1 * itemsPerShard
    if c1 == b.config.NumClusters { end1 = b.config.NumAccounts }
    sender := b.pickAccountInRange(start1, end1)

    start2 := (c2-1)*itemsPerShard + 1
    end2 := c2 * itemsPerShard
    if c2 == b.config.NumClusters { end2 = b.config.NumAccounts }
    receiver := b.pickAccountInRange(start2, end2)

    return sender, receiver
}

func (b *Benchmark) pickAccountInRange(start, end int32) int32 {
    rangeSize := end - start + 1
    if rangeSize <= 0 {
        return start
    }

    if b.config.Skew <= 0.0 {
        return start + rand.Int31n(rangeSize)
    }

    numHot := int32(float64(rangeSize) * b.config.HotDataPct)
    if numHot < 1 {
        numHot = 1
    }

    isHotTx := rand.Float64() < b.config.Skew

    if isHotTx {
        offset := rand.Int31n(numHot)
        return start + offset
    } else {
        numCold := rangeSize - numHot
        if numCold < 1 {
            offset := rand.Int31n(numHot)
            return start + offset
        }
        offset := rand.Int31n(numCold)
        return start + numHot + offset
    }
}