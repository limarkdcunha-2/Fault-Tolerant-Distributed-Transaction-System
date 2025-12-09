/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
)


type BenchmarkConfig struct {
    NumAccounts int32
    NumClusters int32
    NumTransactions int
    ReadPct float64
    IntraPct float64
    Skew float64
}

type Benchmark struct {
    config BenchmarkConfig
    zipfGen *ZipfGenerator
    mu sync.Mutex
}

func NewBenchmark(cfg BenchmarkConfig) *Benchmark {
    zipf := NewZipfGenerator(int(cfg.NumAccounts), cfg.Skew)
    return &Benchmark{
        config:  cfg,
        zipfGen: zipf,
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
        account := b.pickAccount()
        return Transaction{
            Sender:   fmt.Sprintf("%d", account),
            Receiver: fmt.Sprintf("%d", account),
            Amount:   0,                         
        }
    }

    isIntra := rand.Float64() < b.config.IntraPct

    if isIntra {
        sender, receiver := b.pickIntraShardPair()
        return Transaction{
            Sender:   fmt.Sprintf("%d", sender),
            Receiver: fmt.Sprintf("%d", receiver),
            Amount:   int32(rand.Intn(100) + 1),
        }
    }

    sender, receiver := b.pickCrossShardPair()
    return Transaction{
        Sender:   fmt.Sprintf("%d", sender),
        Receiver: fmt.Sprintf("%d", receiver),
        Amount:   int32(rand.Intn(100) + 1),
    }
}

func (b *Benchmark) pickAccount() int32 {
    b.mu.Lock()
    defer b.mu.Unlock()
    return int32(b.zipfGen.Next()) + 1
}

func (b *Benchmark) pickIntraShardPair() (int32, int32) {
    clusterId := rand.Int31n(b.config.NumClusters) + 1

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
    c1 := rand.Int31n(b.config.NumClusters) + 1
    c2 := rand.Int31n(b.config.NumClusters) + 1

    for c2 == c1 {
        c2 = rand.Int31n(b.config.NumClusters) + 1
    }

    itemsPerShard := b.config.NumAccounts / b.config.NumClusters

    start1 := (c1-1)*itemsPerShard + 1
    end1 := c1 * itemsPerShard
    if c1 == b.config.NumClusters {
        end1 = b.config.NumAccounts
    }
    sender := b.pickAccountInRange(start1, end1)

    start2 := (c2-1)*itemsPerShard + 1
    end2 := c2 * itemsPerShard
    if c2 == b.config.NumClusters {
        end2 = b.config.NumAccounts
    }
    receiver := b.pickAccountInRange(start2, end2)

    return sender, receiver
}

func (b *Benchmark) pickAccountInRange(start, end int32) int32 {
    rangeSize := end - start + 1

    if b.config.Skew == 0.0 {
        return start + rand.Int31n(rangeSize)
    }

    b.mu.Lock()
    localZipf := NewZipfGenerator(int(rangeSize), b.config.Skew)
    offset := int32(localZipf.Next())
    b.mu.Unlock()

    return start + offset
}


type ZipfGenerator struct {
    n int
    theta float64 
    zetan float64 
    alpha float64
    eta float64
}

func NewZipfGenerator(n int, theta float64) *ZipfGenerator {
    zg := &ZipfGenerator{
        n:     n,
        theta: theta,
        alpha: 1.0 / (1.0 - theta),
    }

    zg.zetan = zg.zeta(n, theta)
    zg.eta = (1 - math.Pow(2.0/float64(n), 1-theta)) / (1 - zg.zeta(2, theta)/zg.zetan)

    return zg
}

func (zg *ZipfGenerator) zeta(n int, theta float64) float64 {
    sum := 0.0
    for i := 1; i <= n; i++ {
        sum += 1.0 / math.Pow(float64(i), theta)
    }
    return sum
}

func (zg *ZipfGenerator) Next() int {
    if zg.theta == 0.0 {
        return rand.Intn(zg.n)
    }

    u := rand.Float64()
    uz := u * zg.zetan

    if uz < 1.0 {
        return 0
    }

    if uz < 1.0+math.Pow(0.5, zg.theta) {
        return 1
    }

    result := int(float64(zg.n) * math.Pow(zg.eta*u-zg.eta+1, zg.alpha))
    
    if result >= zg.n {
        result = zg.n - 1
    }
    if result < 0 {
        result = 0
    }

    return result
}