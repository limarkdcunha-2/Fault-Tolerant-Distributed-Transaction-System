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
	localClients map[string]*Client
}

type TransactionBatch struct {
	transactions    []TestTransaction
	isLeaderFailure bool
}

func NewRunner() *Runner {
	runner:= &Runner{
        nodeConfigs: getNodeCluster(),
        nodeClients: make(map[int32]pb.MessageServiceClient),
        localClients: make(map[string]*Client),
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

    // 1. Build all 10 clients
    clients := buildClients()
    for _, c := range clients {
		// start local client gRPC server so replicas can call HandleReply
		if err := c.startGrpcServer(); err != nil {
			log.Fatalf("[Runner] client %s failed to start server: %v", c.name, err)
		}
		// keep clients map for use by ExecuteTestCase
		r.localClients[c.name] = c
	}

    log.Printf("[Runner] Loaded config for %d nodes and %d clients\n", len(r.nodeConfigs), len(r.localClients))

	for setNum := 1; setNum <= len(r.testCases); setNum++ {
        // if setNum > 3 {
        //     break
        // }

        tc := r.testCases[setNum]
        fmt.Printf("\n=========================================\n")
        fmt.Printf("[Runner] Running Set %d — live nodes %v\n", tc.SetNumber, tc.LiveNodes)
        fmt.Printf("=========================================\n")

        // Reset last set before new one starts
        // if setNum > 1 {
        //     r.CleanupAfterSet()
        //     r.ResetAllClients(r.localClients)
        // }

        r.UpdateActiveNodes(tc.LiveNodes)

        // Execute transactions
        start := time.Now()
        r.ExecuteTestCase(tc,r.localClients)
        end := time.Since(start)

        log.Printf("[Runner] Total time elapsed %v",end)
        
        r.showInteractiveMenu()
        // r.PrintStatusAll()
    }

    log.Printf("All test sets complete")

    // Client connection cleanup
    for _, client := range clients {
        client.closeAllConnections()
    }
    log.Println("[Runner] All sets complete.")
}

func (r *Runner) ExecuteTestCase(tc TestCase, clients map[string]*Client) {
	log.Printf("[Runner] Executing %d transactions for Set %d...", len(tc.Transactions), tc.SetNumber)

    batches := r.splitTransactionsByLF(tc.Transactions)

    for _, batch := range batches {
        if batch.isLeaderFailure {
            r.ExecuteLeaderFailure()
        } else {
            // Group transactions by client (sender)
            clientTxMap := make(map[string][]TestTransaction)
            for _, tx := range batch.transactions {
                clientTxMap[tx.Sender] = append(clientTxMap[tx.Sender], tx)
            }

            var wg sync.WaitGroup

            for clientName, txList := range clientTxMap {
                client, ok := clients[clientName]
                if !ok {
                    log.Printf("[Runner] Warning: No client found with name '%s'. Skipping.", clientName)
                    continue
                }

                wg.Add(1)
                go func(c *Client, transactions []TestTransaction, cName string) {
                    defer wg.Done()

                    // Execute this client's transactions SERIALLY
                    for i, tx := range transactions {
                        log.Printf("[Runner] ---> Sending Tx %d/%d: (%s, %s, %d) via Client %s",
                            i+1, len(transactions), tx.Sender, tx.Receiver, tx.Amount, cName)

                        c.SendTransaction(Transaction{
                            Sender: tx.Sender,	
                            Receiver: tx.Receiver,	
                            Amount: int32(tx.Amount),	
                        })

                        // Small delay between this client's transactions
                        // time.Sleep(20 * time.Millisecond)
                    }

                    log.Printf("[Runner] Client %s completed all %d transactions for Set %d", 
                        cName, len(transactions), tc.SetNumber)
                }(client, txList, clientName)
            }

            // WAIT for all clients to finish their transactions
            wg.Wait()
        }
    }

    

    log.Printf("[Runner] Finished all transactions for Set %d.", tc.SetNumber)
}

func (r *Runner) splitTransactionsByLF(transactions []TestTransaction) []TransactionBatch {
    var batches []TransactionBatch
    currentBatch := []TestTransaction{}

    for _, tx := range transactions {
        if tx.IsLeaderFailure {
            // Save current batch and add LF as separate batch
            if len(currentBatch) > 0 {
                batches = append(batches, TransactionBatch{
                    transactions:    currentBatch,
                    isLeaderFailure: false,
                })
                currentBatch = nil
            }
            
            batches = append(batches, TransactionBatch{
                isLeaderFailure: true,
            })
        } else {
            currentBatch = append(currentBatch, tx)
        }
    }

    if len(currentBatch) > 0 {
        batches = append(batches, TransactionBatch{
            transactions:    currentBatch,
            isLeaderFailure: false,
        })
        currentBatch = nil
    }
    
    return batches
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
        // fmt.Println("6. Stop clients")
        // fmt.Println("7. Stop server timers")
        // fmt.Println("8. View client side replies")
        fmt.Println("9. Proceed to next batch")
        fmt.Println("========================================")
        fmt.Print("Enter choice (1-9): ")

        input, _ := reader.ReadString('\n')
        input = strings.TrimSpace(input)

        switch input {
        // case "1":
        //     r.PrintLogsForAllNodes()
        case "2":
                fmt.Print("Enter sequence number: ")
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
        // case "6":
        //     r.StopAllClients()
        // case "7":
        //     r.StopAllNodeTimers()
        // case "8":
        //     r.PrintClientReplyHistory()
        case "9":
            fmt.Println("\nProceeding to next batch...")
            return

        default:
            fmt.Println("Invalid choice. Please enter 1-9.")
        }
    }
}


func (r *Runner) ExecuteLeaderFailure(){
    leaderId := r.localClients["B"].leaderId
    log.Printf("[Runner] Sending LF command to Node=%d",leaderId)

    client, exists := r.nodeClients[leaderId]
    if !exists {
        log.Printf("[Runner] No connection to node %d", leaderId)
        return
    }

    _, err := client.FailNode(context.Background(),  &emptypb.Empty{})

    if err != nil {
        log.Printf("[Runner] Failed to send FAIL signal for node %d: %v", leaderId, err)
    }
}

func (r *Runner) UpdateActiveNodes(liveNodes []int) {
    for _, cfg := range r.nodeConfigs {
        client, exists := r.nodeClients[int32(cfg.NodeId)]
        if !exists {
            log.Printf("[Runner] No connection to node %d", cfg.NodeId)
            continue
        }

        if slices.Contains(liveNodes, int(cfg.NodeId)) {
            _, err := client.RecoverNode(context.Background(),  &emptypb.Empty{})

            if err != nil {
                log.Printf("[Runner] Failed to send RECOVER signal for node %d: %v", cfg.NodeId, err)
            }
        } else {
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

func (r *Runner) PrintBalanceAll(clientName string){
    for _, nodeCfg := range getNodeCluster() {
        client, exists := r.nodeClients[int32(nodeCfg.NodeId)]
        if !exists {
            log.Printf("[Runner] No connection to node %d", nodeCfg.NodeId)
            continue
        }

        _, err := client.PrintBalance(context.Background(),  &pb.PrintBalanceReq{ClientName: clientName})

        if err != nil {
            log.Printf("[Runner] Failed to print log for node %d: %v", nodeCfg.NodeId, err)
        }
    }
}