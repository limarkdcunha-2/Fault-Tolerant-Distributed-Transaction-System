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
	"sort"
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

    muTxHistory      sync.RWMutex
	txHistory        []TransactionEdge
	maxTxHistory     int
	currentShardMap  map[string]int32
}

type TransactionBatch struct {
	transactions    []TestTransaction
	isControl    bool
}

func NewRunner() *Runner {
	runner:= &Runner{
        nodeConfigs: getNodeCluster(),
        nodeClients: make(map[int32]pb.MessageServiceClient),
        txHistory:   make([]TransactionEdge, 0),
		maxTxHistory: 10000,
        currentShardMap: make(map[string]int32),
	}

    runner.initializeShardMap()

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

func (r *Runner) initializeShardMap() {
	r.muTxHistory.Lock()
	defer r.muTxHistory.Unlock()
	
	for i := 1; i <= 9000; i++ {
		accountID := fmt.Sprintf("%d", i)
		clusterID := getClusterId(int32(i))
		r.currentShardMap[accountID] = clusterID
	}
	
	log.Printf("[Runner] Initialized shard map with %d accounts", len(r.currentShardMap))
}

func (r *Runner) RecordTransaction(sender, receiver string) {
	if receiver == "" || sender == receiver {
		return
	}
	
	r.muTxHistory.Lock()
	defer r.muTxHistory.Unlock()
	
	r.txHistory = append(r.txHistory, TransactionEdge{
		Sender:   sender,
		Receiver: receiver,
	})

	if len(r.txHistory) > r.maxTxHistory {
		r.txHistory = r.txHistory[len(r.txHistory)-r.maxTxHistory:]
	}
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

            allLiveNodes := make([]int, 0, len(r.nodeConfigs))
            for _, cfg := range r.nodeConfigs {
                allLiveNodes = append(allLiveNodes, int(cfg.NodeId))
            }
            tc.LiveNodes = allLiveNodes

            r.UpdateActiveNodes(tc.LiveNodes)
        } else {
            r.UpdateActiveNodes(tc.LiveNodes)
        }

        countFired := r.ExecuteTestCase(tc)
            
        r.showInteractiveMenu(countFired)
    }

    log.Printf("All test sets complete")

    // Client connection cleanup
    if r.client != nil {
        r.client.closeAllConnections()
    }
    log.Println("[Runner] All sets complete.")
}

func (r *Runner) ExecuteTestCase(tc TestCase) (int) {
	log.Printf("[Runner] Executing transactions for Set %d...", tc.SetNumber)
    count :=0
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
                count++
                wg.Add(1)

				// Launch a goroutine for EVERY transaction
				go func(t TestTransaction, idx int) {
					defer wg.Done()

					// Uses the single gateway client for everything
                    r.RecordTransaction(t.Sender, t.Receiver)
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
    return count
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

    // 1. Number of transactions
    fmt.Print("Number of transactions: ")
    numTxnsStr, _ := reader.ReadString('\n')
    numTxns, err := strconv.Atoi(strings.TrimSpace(numTxnsStr))
    if err != nil || numTxns <= 0 {
        fmt.Println("Invalid input, using default: 100")
        numTxns = 100
    }

    // 2. Read percentage
    fmt.Print("Read percentage (0.0-1.0, e.g., 0.5 for 50%): ")
    readPctStr, _ := reader.ReadString('\n')
    readPct, err := strconv.ParseFloat(strings.TrimSpace(readPctStr), 64)
    if err != nil || readPct < 0 || readPct > 1 {
        fmt.Println("Invalid input, using default: 0.3")
        readPct = 0.3
    }

    // 3. Intra-shard percentage
    fmt.Print("Intra-shard percentage (0.0-1.0, e.g., 0.7 for 70%): ")
    intraPctStr, _ := reader.ReadString('\n')
    intraPct, err := strconv.ParseFloat(strings.TrimSpace(intraPctStr), 64)
    if err != nil || intraPct < 0 || intraPct > 1 {
        fmt.Println("Invalid input, using default: 0.7")
        intraPct = 0.7
    }

    // 4. Skew (Probability of hitting the Hot Set)
    fmt.Print("Traffic Skew Probability (0.0-1.0, e.g., 0.9 = 90% traffic to hot set): ")
    skewStr, _ := reader.ReadString('\n')
    skew, err := strconv.ParseFloat(strings.TrimSpace(skewStr), 64)
    if err != nil || skew < 0 || skew > 1 {
        fmt.Println("Invalid input, using default: 0.0 (Uniform)")
        skew = 0.0
    }

    // 5. Hot Data Percentage (Size of the Hot Set) -- NEW ADDITION
    hotPct := 0.10 // default
    if skew > 0 {
        fmt.Print("Hot Data Size (0.0-1.0, e.g., 0.1 = 10% of accounts are hot): ")
        hotPctStr, _ := reader.ReadString('\n')
        val, err := strconv.ParseFloat(strings.TrimSpace(hotPctStr), 64)
        if err == nil && val > 0 && val <= 1 {
            hotPct = val
        } else {
            fmt.Println("Invalid input, using default: 0.1 (10%)")
        }
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
        HotDataPct:      hotPct, // Pass the new value here
    }

    fmt.Printf("\n✓ Generating %d transactions...\n", numTxns)
    bench := NewBenchmark(benchConfig)
    txns := bench.GenerateWorkload()

    fmt.Printf("✓ Workload: %.0f%% reads, %.0f%% intra-shard\n", readPct*100, intraPct*100)
    if skew > 0 {
        fmt.Printf("✓ Skew: %.0f%% of traffic targets %.0f%% of accounts (Hot/Cold)\n", skew*100, hotPct*100)
    } else {
        fmt.Println("✓ Skew: Uniform distribution")
    }

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

func (r *Runner) showInteractiveMenu(transactionCount int) {
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
        fmt.Println("10. Performance metrics")
        fmt.Println("11. Print Reshard")
        fmt.Println("12. Apply Reshard")
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
        case "10":
            r.client.CalculatePerformance(transactionCount)
        case "11":
            r.PrintReshard()
        case "12:":
            r.ApplyReshard()
        default:
            fmt.Println("Invalid choice. Please enter 1-15.")
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


// PrintReshard computes and displays resharding plan
func (r *Runner) PrintReshard() {
	r.muTxHistory.RLock()
	txHistoryCopy := make([]TransactionEdge, len(r.txHistory))
	copy(txHistoryCopy, r.txHistory)
	currentMapCopy := make(map[string]int32)
	for k, v := range r.currentShardMap {
		currentMapCopy[k] = v
	}
	r.muTxHistory.RUnlock()
	
	log.Printf("[PrintReshard] Computing resharding based on %d transactions", len(txHistoryCopy))
	
	result := CalculateResharding(txHistoryCopy, currentMapCopy, 3)
	
	fmt.Println("\n========================================")
	fmt.Println("         RESHARDING PLAN")
	fmt.Println("========================================")
	fmt.Printf("Total moves required: %d\n\n", len(result.Moves))
	
	if len(result.Moves) == 0 {
		fmt.Println("No resharding needed - current mapping is optimal")
		return
	}
	
	// Sort moves for consistent output
	sort.Slice(result.Moves, func(i, j int) bool {
		return result.Moves[i].AccountID < result.Moves[j].AccountID
	})
	
	// Print first 50 moves (or all if fewer)
	maxPrint := 50
	if len(result.Moves) < maxPrint {
		maxPrint = len(result.Moves)
	}
	
	for i := 0; i < maxPrint; i++ {
		move := result.Moves[i]
		fmt.Printf("%s, c%d, c%d\n", move.AccountID, move.FromCluster, move.ToCluster)
	}
	
	if len(result.Moves) > maxPrint {
		fmt.Printf("... and %d more moves\n", len(result.Moves)-maxPrint)
	}
	
	fmt.Println("========================================")
	
	// Calculate and print metrics
	graph := BuildGraphFromTransactions(txHistoryCopy)
	oldCost := CalculateCutCost(graph, currentMapCopy)
	newCost := CalculateCutCost(graph, result.NewMapping)
	
	fmt.Printf("\nCross-shard edges (old): %d\n", oldCost)
	fmt.Printf("Cross-shard edges (new): %d\n", newCost)
	if oldCost > 0 {
		improvement := float64(oldCost-newCost) / float64(oldCost) * 100
		fmt.Printf("Improvement: %.1f%%\n", improvement)
	}
	fmt.Println()
}

func (r *Runner) ApplyReshard() {
	r.muTxHistory.RLock()
    txHistoryCopy := make([]TransactionEdge, len(r.txHistory))
    copy(txHistoryCopy, r.txHistory)
    currentMapCopy := make(map[string]int32)
    for k, v := range r.currentShardMap {
        currentMapCopy[k] = v
    }
    r.muTxHistory.RUnlock()
    
    log.Printf("[ApplyReshard] Starting resharding process...")
    fmt.Println("\n========================================")
    fmt.Println("      APPLYING RESHARDING")
    fmt.Println("========================================")
    
    // Compute resharding
    result := CalculateResharding(txHistoryCopy, currentMapCopy, 3)
    
    if len(result.Moves) == 0 {
        log.Printf("[ApplyReshard] No moves needed")
        fmt.Println("No resharding needed - current mapping is optimal")
        return
    }
    
    fmt.Printf("Moving %d accounts...\n", len(result.Moves))
    
    // Step 1: Apply new shard map to all nodes
    log.Printf("[ApplyReshard] Step 1: Broadcasting new shard map to all nodes")
    if err := r.broadcastShardMap(result.NewMapping); err != nil {
        log.Printf("[ApplyReshard] ERROR: Failed to broadcast shard map: %v", err)
        fmt.Printf("ERROR: Failed to update shard map on nodes: %v\n", err)
        return
    }
    fmt.Println("✓ Updated shard map on all nodes")
    
    // Step 2: Move data for each account
    log.Printf("[ApplyReshard] Step 2: Moving %d accounts", len(result.Moves))
    successCount := 0
    errorCount := 0
    
    for i, move := range result.Moves {
        if i%100 == 0 && i > 0 {
            fmt.Printf("  Progress: %d/%d accounts moved\n", i, len(result.Moves))
        }
        
        if err := r.moveAccount(move); err != nil {
            log.Printf("[ApplyReshard] ERROR moving account %s: %v", move.AccountID, err)
            errorCount++
        } else {
            successCount++
        }
    }
    
    fmt.Printf("✓ Completed: %d successful, %d errors\n", successCount, errorCount)
    
    // Step 3: Update local shard map
    r.muTxHistory.Lock()
    r.currentShardMap = result.NewMapping
    r.muTxHistory.Unlock()
    
    log.Printf("[ApplyReshard] Resharding complete: %d moves executed", successCount)
    fmt.Println("========================================")
    fmt.Println("Resharding complete!")
    fmt.Println("========================================")

}

func (r *Runner) broadcastShardMap(newMapping map[string]int32) error {
    // Convert map to protobuf entries
    entries := make([]*pb.ReshardEntry, 0, len(newMapping))
    for accountID, clusterID := range newMapping {
        entries = append(entries, &pb.ReshardEntry{
            AccountId: accountID,
            ClusterId: clusterID,
        })
    }
    
    req := &pb.ApplyShardMapRequest{
        Entries: entries,
    }
    
    // Send to all nodes
    var wg sync.WaitGroup
    errChan := make(chan error, len(r.nodeClients))
    
    for nodeID, client := range r.nodeClients {
        wg.Add(1)
        go func(id int32, c pb.MessageServiceClient) {
            defer wg.Done()
            _, err := c.ApplyShardMap(context.Background(), req)
            if err != nil {
                log.Printf("[ApplyReshard] Failed to update shard map on node %d: %v", id, err)
                errChan <- fmt.Errorf("node %d: %v", id, err)
            }
        }(nodeID, client)
    }
    
    wg.Wait()
    close(errChan)
    
    // Check for errors
    var errs []error
    for err := range errChan {
        errs = append(errs, err)
    }
    
    if len(errs) > 0 {
        return fmt.Errorf("failed to update %d nodes: %v", len(errs), errs[0])
    }
    
    return nil
}

// moveAccount moves a single account from source to destination cluster
func (r *Runner) moveAccount(move ReshardMove) error {
    // Step 1: Read balance from source cluster
    balance, err := r.getBalanceFromCluster(move.AccountID, move.FromCluster)
    if err != nil {
        return fmt.Errorf("failed to read balance: %v", err)
    }
    
    // Step 2: Write to all nodes in destination cluster
    if err := r.writeToCluster(move.AccountID, move.ToCluster, balance); err != nil {
        return fmt.Errorf("failed to write to destination: %v", err)
    }
    
    // Step 3: Delete from all nodes in source cluster
    if err := r.deleteFromCluster(move.AccountID, move.FromCluster); err != nil {
        // Log but don't fail - data is already in destination
        log.Printf("[ApplyReshard] Warning: failed to delete %s from source cluster %d: %v",
            move.AccountID, move.FromCluster, err)
    }
    
    log.Printf("[ApplyReshard] Moved account %s from c%d to c%d (balance=%d)",
        move.AccountID, move.FromCluster, move.ToCluster, balance)
    
    return nil
}

// getBalanceFromCluster reads balance from any node in the cluster
func (r *Runner) getBalanceFromCluster(accountID string, clusterID int32) (int32, error) {
    // Get cluster info
    r.client.muCluster.RLock()
    nodeIDs := r.client.clusterInfo[clusterID].NodeIds
    r.client.muCluster.RUnlock()
    
    if len(nodeIDs) == 0 {
        return 0, fmt.Errorf("no nodes found in cluster %d", clusterID)
    }
    
    // Try each node until we get a successful response
    for _, nodeID := range nodeIDs {
        client, exists := r.nodeClients[nodeID]
        if !exists {
            continue
        }
        
        resp, err := client.GetBalance(context.Background(), &pb.GetBalanceRequest{
            AccountId: accountID,
        })
        
        if err != nil {
            log.Printf("[ApplyReshard] Failed to get balance from node %d: %v", nodeID, err)
            continue
        }
        
        if resp.Success {
            return resp.Balance, nil
        }
    }
    
    return 0, fmt.Errorf("failed to read balance from any node in cluster %d", clusterID)
}

// writeToCluster writes account to all nodes in destination cluster
func (r *Runner) writeToCluster(accountID string, clusterID int32, balance int32) error {
    // Get cluster info
    r.client.muCluster.RLock()
    nodeIDs := r.client.clusterInfo[clusterID].NodeIds
    r.client.muCluster.RUnlock()
    
    if len(nodeIDs) == 0 {
        return fmt.Errorf("no nodes found in cluster %d", clusterID)
    }
    
    // Write to all nodes in parallel
    var wg sync.WaitGroup
    errChan := make(chan error, len(nodeIDs))
    
    for _, nodeID := range nodeIDs {
        client, exists := r.nodeClients[nodeID]
        if !exists {
            continue
        }
        
        wg.Add(1)
        go func(id int32, c pb.MessageServiceClient) {
            defer wg.Done()
            
            _, err := c.MoveDatapoint(context.Background(), &pb.MoveDatapointRequest{
                AccountId:  accountID,
                SrcCluster: clusterID, // Not used but included for logging
                DstCluster: clusterID,
                Balance:    balance,
            })
            
            if err != nil {
                errChan <- fmt.Errorf("node %d: %v", id, err)
            }
        }(nodeID, client)
    }
    
    wg.Wait()
    close(errChan)
    
    // Check for errors
    var errs []error
    for err := range errChan {
        errs = append(errs, err)
    }
    
    if len(errs) > 0 {
        return fmt.Errorf("failed to write to %d nodes: %v", len(errs), errs[0])
    }
    
    return nil
}

// deleteFromCluster deletes account from all nodes in source cluster
func (r *Runner) deleteFromCluster(accountID string, clusterID int32) error {
    // Get cluster info
    r.client.muCluster.RLock()
    nodeIDs := r.client.clusterInfo[clusterID].NodeIds
    r.client.muCluster.RUnlock()
    
    if len(nodeIDs) == 0 {
        return fmt.Errorf("no nodes found in cluster %d", clusterID)
    }
    
    // Delete from all nodes in parallel
    var wg sync.WaitGroup
    errChan := make(chan error, len(nodeIDs))
    
    for _, nodeID := range nodeIDs {
        client, exists := r.nodeClients[nodeID]
        if !exists {
            continue
        }
        
        wg.Add(1)
        go func(id int32, c pb.MessageServiceClient) {
            defer wg.Done()
            
            _, err := c.DeleteDatapoint(context.Background(), &pb.DeleteDatapointRequest{
                AccountId: accountID,
            })
            
            if err != nil {
                errChan <- fmt.Errorf("node %d: %v", id, err)
            }
        }(nodeID, client)
    }
    
    wg.Wait()
    close(errChan)
    
    // Check for errors
    var errs []error
    for err := range errChan {
        errs = append(errs, err)
    }
    
    if len(errs) > 0 {
        return fmt.Errorf("failed to delete from %d nodes: %v", len(errs), errs[0])
    }
    
    return nil
}
