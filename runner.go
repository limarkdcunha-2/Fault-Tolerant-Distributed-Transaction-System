/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Runner struct {
	nodeConfigs  []NodeConfig
	testCases    map[int]TestCase
	nodeClients  map[int32]pb.MessageServiceClient
	localClients map[string]*Client
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
        tc := r.testCases[setNum]
        fmt.Printf("\n=========================================\n")
        fmt.Printf("[Runner] Running Set %d — live nodes %v\n", tc.SetNumber, tc.LiveNodes)
        fmt.Printf("=========================================\n")

        // Reset last set before new one starts
        // if setNum > 1 {
        //     r.CleanupAfterSet()
        //     r.ResetAllClients(r.localClients)
        // }

        // r.ConfigureNodesForSet(tc)

        // Execute transactions
        r.ExecuteTestCase(tc,r.localClients)

        // r.showInteractiveMenu()
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

    // Group transactions by client (sender)
    clientTxMap := make(map[string][]TestTransaction)
    for _, tx := range tc.Transactions {
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
                log.Printf("[Runner] ---> Sending Tx %d/%d: (%s -> %s, $%d) via Client %s",
                    i+1, len(transactions), tx.Sender, tx.Receiver, tx.Amount, cName)

                c.SendTransaction(Transaction{
					Sender: tx.Sender,	
					Receiver: tx.Receiver,	
					Amount: int32(tx.Amount),	
				})

                // Small delay between this client's transactions
                time.Sleep(20 * time.Millisecond)
            }

            log.Printf("[Runner] Client %s completed all %d transactions for Set %d", 
                cName, len(transactions), tc.SetNumber)
        }(client, txList, clientName)
    }

    // WAIT for all clients to finish their transactions
    wg.Wait()

    log.Printf("[Runner] Finished all transactions for Set %d.", tc.SetNumber)
}