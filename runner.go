/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
)

type Runner struct {
	nodeConfigs  []NodeConfig
	testCases    map[int]TestCase
	nodeClients  map[int32]pb.MessageServiceClient
	client *Client
}

type TransactionBatch struct {
	transactions    []TestTransaction
	isControl    bool
}

func NewRunner() *Runner {
	runner:= &Runner{
        nodeConfigs: getNodeCluster(),
        nodeClients: make(map[int32]pb.MessageServiceClient),
	}

    for _, nodeConfig := range runner.nodeConfigs {
        conn, err := grpc.NewClient(fmt.Sprintf(":%d", nodeConfig.PortNo),grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("[Runner] Failed to dial node %d : %v", nodeConfig.NodeId ,err)
            continue
        }

        runner.nodeClients[nodeConfig.NodeId] = pb.NewMessageServiceClient(conn)
    }

    // Extra code just placing it here
    logFile, err := os.OpenFile("runnerlog.log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
    if err != nil {
        log.Fatalf("Failed to open clientlog.log: %v", err)
    }

    // Set log output to file
    log.SetOutput(logFile)
    // log.SetOutput(io.Discard)

    return runner
}

func (r *Runner) RunAllTestSets() {
    r.testCases = getAllTestCases()

    if len(r.testCases) == 0 {
        log.Println("[Runner] no test cases found")
        return
    }

    // 1. Build client
    r.client,_ = NewClient(9000)
    r.client.startGrpcServer()

    reader := bufio.NewReader(os.Stdin)
    
	for setNum := 1; setNum <= len(r.testCases); setNum++ {
        if setNum != 1 {
            continue
        }

        tc := r.testCases[setNum]
        fmt.Printf("\n=========================================\n")
        fmt.Printf("[Runner] Running Set %d — live nodes %v\n", tc.SetNumber, tc.LiveNodes)
        fmt.Printf("=========================================\n")

        fmt.Print("\nUse benchmark workload instead of CSV? (y/n): ")
        choice, _ := reader.ReadString('\n')
        choice = strings.TrimSpace(strings.ToLower(choice))

        var transactions []Transaction

        if choice == "y" || choice == "yes" {
            // Generate benchmark workload
            transactions = r.GenerateBenchmarkWorkload(reader)
            if transactions == nil {
                fmt.Println("Failed to generate benchmark, skipping test set.")
                continue
            }

            tc.Transactions = r.convertToTestTransactions(transactions)
        }

        // r.CleanupStateAndWAL()

        r.UpdateActiveNodes(tc.LiveNodes)

        // Execute transactions
        start := time.Now()
        r.ExecuteTestCase(tc)
        end := time.Since(start)
        log.Printf("[Runner] Total time elapsed %v",end)
        
        r.showInteractiveMenu()
        // r.PrintStatusAll()
    }

    log.Printf("All test sets complete")

    // Client connection cleanup
    if r.client != nil {
        r.client.closeAllConnections()
    }
    log.Println("[Runner] All sets complete.")
}

func (r *Runner) ExecuteTestCase(tc TestCase) {
	log.Printf("[Runner] Executing %d transactions for Set %d...", len(tc.Transactions), tc.SetNumber)

	batches := r.splitTransactionsIntoBatches(tc.Transactions)

   for _, batch := range batches {
        if batch.isControl {
            cmd := batch.transactions[0]
            // This delay is impt
            time.Sleep(1 * time.Second)

			if cmd.IsFailNode {
				log.Printf("[Runner] Executing Fail Node %d...", cmd.TargetNodeId)
				r.FailNodeCommand(int32(cmd.TargetNodeId))
			} else if cmd.IsRecoverNode {
				log.Printf("[Runner] Executing Recover Node %d...", cmd.TargetNodeId)
				r.RecoverNodeCommand(int32(cmd.TargetNodeId))
			}
        } else {
            var wg sync.WaitGroup

            for i, tx := range batch.transactions {
                wg.Add(1)

				// Launch a goroutine for EVERY transaction
				go func(t TestTransaction, idx int) {
					defer wg.Done()

					// Uses the single gateway client for everything
                    log.Printf("[Runner] Sending transaction (%s, %s, %d)",t.Sender,t.Receiver,t.Amount)
					go r.client.SendTransaction(Transaction{
						Sender:   t.Sender,
						Receiver: t.Receiver,
						Amount:   int32(t.Amount),
					})
				}(tx, i)
            }
            wg.Wait()
        }
    }

    log.Printf("[Runner] Finished all transactions for Set %d.", tc.SetNumber)
}

func (r *Runner) splitTransactionsIntoBatches(transactions []TestTransaction) []TransactionBatch {
    var batches []TransactionBatch
    currentBatch := []TestTransaction{}

    for _, tx := range transactions {
        if tx.IsFailNode || tx.IsRecoverNode {
            // Save current batch and add LF as separate batch
            if len(currentBatch) > 0 {
				batches = append(batches, TransactionBatch{
					transactions: currentBatch,
					isControl:    false,
				})
				currentBatch = []TestTransaction{} // Start fresh
			}

			// 2. Add the control command as its own batch
			batches = append(batches, TransactionBatch{
				transactions: []TestTransaction{tx},
				isControl:    true,
			})
        } else {
            currentBatch = append(currentBatch, tx)
        }
    }

    if len(currentBatch) > 0 {
		batches = append(batches, TransactionBatch{
			transactions: currentBatch,
			isControl:    false,
		})
	}
    
    return batches
}

func (r *Runner) GenerateBenchmarkWorkload(reader *bufio.Reader) []Transaction {
    fmt.Println("\n╔════════════════════════════════════════╗")
    fmt.Println("║     BENCHMARK CONFIGURATION            ║")
    fmt.Println("╚════════════════════════════════════════╝")

    // Number of transactions
    fmt.Print("Number of transactions: ")
    numTxnsStr, _ := reader.ReadString('\n')
    numTxns, err := strconv.Atoi(strings.TrimSpace(numTxnsStr))
    if err != nil || numTxns <= 0 {
        fmt.Println("Invalid input, using default: 100")
        numTxns = 100
    }

    // Read percentage
    fmt.Print("Read percentage (0.0-1.0, e.g., 0.5 for 50%): ")
    readPctStr, _ := reader.ReadString('\n')
    readPct, err := strconv.ParseFloat(strings.TrimSpace(readPctStr), 64)
    if err != nil || readPct < 0 || readPct > 1 {
        fmt.Println("Invalid input, using default: 0.3")
        readPct = 0.3
    }

    // Intra-shard percentage
    fmt.Print("Intra-shard percentage (0.0-1.0, e.g., 0.7 for 70%): ")
    intraPctStr, _ := reader.ReadString('\n')
    intraPct, err := strconv.ParseFloat(strings.TrimSpace(intraPctStr), 64)
    if err != nil || intraPct < 0 || intraPct > 1 {
        fmt.Println("Invalid input, using default: 0.7")
        intraPct = 0.7
    }

    // Skew
    fmt.Print("Data skew (0.0=uniform, 0.99=highly skewed): ")
    skewStr, _ := reader.ReadString('\n')
    skew, err := strconv.ParseFloat(strings.TrimSpace(skewStr), 64)
    if err != nil || skew < 0 || skew > 1 {
        fmt.Println("Invalid input, using default: 0.0")
        skew = 0.0
    }

    numClusters := int32(3) // default
    totalAccounts := int32(9000)

    benchConfig := BenchmarkConfig{
        NumAccounts:     totalAccounts,
        NumClusters:     numClusters,
        NumTransactions: numTxns,
        ReadPct:         readPct,
        IntraPct:        intraPct,
        Skew:            skew,
    }

    fmt.Printf("\n✓ Generating %d transactions...\n", numTxns)
    bench := NewBenchmark(benchConfig)
    txns := bench.GenerateWorkload()

    fmt.Printf("✓ Workload: %.0f%% reads, %.0f%% intra-shard, skew=%.2f\n",
        readPct*100, intraPct*100, skew)

    return txns
}

func (r *Runner) convertToTestTransactions(txns []Transaction) []TestTransaction {
    testTxns := make([]TestTransaction, len(txns))
    
    for i, tx := range txns {
        testTxns[i] = TestTransaction{
            Sender:        tx.Sender,
            Receiver:      tx.Receiver,
            Amount:        int(tx.Amount),
            IsFailNode:    false,
            IsRecoverNode: false,
            TargetNodeId:  0,
        }
    }
    
    return testTxns
}

func (r *Runner) showInteractiveMenu() {
    reader := bufio.NewReader(os.Stdin)
    
    for {
        fmt.Println("\n========================================")
        fmt.Println("         INTERACTIVE MENU")
        fmt.Println("========================================")
        // fmt.Println("1. PrintLog")
        fmt.Println("2. PrintBalance ")
        // fmt.Println("3. PrintStatus (single sequence number)")
        fmt.Println("4. PrintStatus (all sequence numbers)")
        // fmt.Println("5. PrintView")
        fmt.Println("6. Stop client retries")
        // fmt.Println("7. Stop server timers")
        fmt.Println("8. View client side replies")
        fmt.Println("9. Proceed to next batch")
        fmt.Println("========================================")
        fmt.Print("Enter choice (1-9): ")

        input, _ := reader.ReadString('\n')
        input = strings.TrimSpace(input)

        switch input {
        // case "1":
        //     r.PrintLogsForAllNodes()
        case "2":
                fmt.Print("Enter datapoint number: ")
            seqInput, _ := reader.ReadString('\n')
            seqInput = strings.TrimSpace(seqInput)

            r.PrintBalanceAll(seqInput)
        // case "3":
        //     fmt.Print("Enter sequence number: ")
        //     seqInput, _ := reader.ReadString('\n')
        //     seqInput = strings.TrimSpace(seqInput)
        //     seqNum, err := strconv.Atoi(seqInput)
        //     if err != nil {
        //         fmt.Printf("Invalid sequence number %d",seqNum)
        //         continue
        //     }
        //     r.PrintStatusForSequence(int32(seqNum))
        case "4":
            r.PrintStatusAll()
        // case "5":
        //     r.PrintViewForAllNodes()
        case "6":
            r.StopClient()
        // case "7":
        //     r.StopAllNodeTimers()
        case "8":
            r.client.PrintReplyHistory()
        case "9":
            fmt.Println("\nProceeding to next batch...")
            return

        default:
            fmt.Println("Invalid choice. Please enter 1-9.")
        }
    }
}

func (r *Runner) FailNodeCommand(targetNodeId int32){
    client, exists := r.nodeClients[targetNodeId]
    if !exists {
        log.Printf("[Runner] No connection to node %d", targetNodeId)
        return
    }

    _, err := client.FailNode(context.Background(),  &emptypb.Empty{})

    if err != nil {
        log.Printf("[Runner] Failed to send FAIL signal for node %d: %v", targetNodeId, err)
    }
}


func (r *Runner) RecoverNodeCommand(targetNodeId int32){
    client, exists := r.nodeClients[targetNodeId]
    if !exists {
        log.Printf("[Runner] No connection to node %d", targetNodeId)
        return
    }

    _, err := client.RecoverNode(context.Background(),  &emptypb.Empty{})

    if err != nil {
        log.Printf("[Runner] Failed to send RECOVER signal for node %d: %v", targetNodeId, err)
    }
}


func (r *Runner) UpdateActiveNodes(liveNodes []int) {
    for _, cfg := range r.nodeConfigs {
        client, exists := r.nodeClients[int32(cfg.NodeId)]
        if !exists {
            log.Printf("[Runner] No connection to node %d", cfg.NodeId)
            continue
        }

        if !slices.Contains(liveNodes, int(cfg.NodeId)) {
            _, err := client.FailNode(context.Background(),  &emptypb.Empty{})

            if err != nil {
                log.Printf("[Runner] Failed to send FAIL signal for node %d: %v", cfg.NodeId, err)
            }
        }
    }
}

func (r *Runner) PrintStatusAll() {
    for _, nodeCfg := range getNodeCluster() {
        client, exists := r.nodeClients[int32(nodeCfg.NodeId)]
        if !exists {
            log.Printf("[Runner] No connection to node %d", nodeCfg.NodeId)
            continue
        }

        _, err := client.PrintAcceptLog(context.Background(),  &emptypb.Empty{})

        if err != nil {
            log.Printf("[Runner] Failed to print log for node %d: %v", nodeCfg.NodeId, err)
        }
    }
}

func (r *Runner) PrintBalanceAll(datapoint string){
    datapointInt, err := strconv.ParseInt(datapoint, 10, 0)
	if err != nil {
		log.Printf("failed to convert string to int32: %v", err)
        return
	}

    targetClusterId := getClusterId(int32(datapointInt))

    r.client.muCluster.RLock()
    targetNodeIds := r.client.clusterInfo[targetClusterId].NodeIds
    r.client.muCluster.RUnlock()

    for _, nodeId := range targetNodeIds {
        client, exists := r.nodeClients[nodeId]
        if !exists {
            log.Printf("[Runner] No connection to node %d", nodeId)
            continue
        }

        _, err := client.PrintBalance(context.Background(),  &pb.PrintBalanceReq{Datapoint: datapoint})

        if err != nil {
            log.Printf("[Runner] Failed to print log for node %d: %v",nodeId, err)
        }
    }
}

func (r *Runner) StopClient() {
    log.Printf("[Runner] Stopping all clients...")
    r.client.Stop()
    log.Printf("[Runner] All clients stopped")
}
