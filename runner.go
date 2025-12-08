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
	"os/exec"
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
	testCases map[int]TestCase

	nodeClients map[int32]pb.MessageServiceClient
	client *Client

    muConfig          sync.RWMutex
    currentConfig     *ClusterConfiguration
    nodeProcesses     []*exec.Cmd
}

type TransactionBatch struct {
	transactions []TestTransaction
	isControl bool
}

type ClusterConfiguration struct {
    NumClusters     int32
    NodesPerCluster int32
    TotalDataItems  int32
    BasePort        int32
    NodeConfigs     []NodeConfig
}

func NewRunner() *Runner {
	runner:= &Runner{
        nodeClients: make(map[int32]pb.MessageServiceClient),
        nodeProcesses:  make([]*exec.Cmd, 0),
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


func(r *Runner) buildNodeConnections(){
    for _, nodeConfig := range r.currentConfig.NodeConfigs {
        conn, err := grpc.NewClient(fmt.Sprintf(":%d", nodeConfig.PortNo),grpc.WithTransportCredentials(insecure.NewCredentials()))
        if err != nil {
            log.Printf("[Runner] Failed to dial node %d : %v", nodeConfig.NodeId ,err)
            continue
        }

        r.nodeClients[nodeConfig.NodeId] = pb.NewMessageServiceClient(conn)
    }
}


func (r *Runner) configureAllNodes(config *ClusterConfiguration) error {
    log.Println("[Runner] Configuring all nodes...")
    pbNodeConfigs := make([]*pb.NodeConfig, len(config.NodeConfigs))

    for i, cfg := range config.NodeConfigs {
        pbNodeConfigs[i] = &pb.NodeConfig{
            NodeId: cfg.NodeId,
            ClusterId: cfg.ClusterId,
            Port: cfg.PortNo,
        }
    }

    configReq := &pb.ConfigureClusterRequest{
        NumOfClusters: config.NumClusters,
        NodesPerCluster: config.NodesPerCluster,
        TotalDataPoints: config.TotalDataItems,
        NodeConfigs: pbNodeConfigs,
    }

    for _, cfg := range config.NodeConfigs {
        grpcClient, exists := r.nodeClients[cfg.NodeId]  // Use nodeClients, not controlClients
        if !exists {
            log.Printf("[Runner] No client for node %d", cfg.NodeId)
            return fmt.Errorf("missing client for node %d", cfg.NodeId)
        }

        _, err := grpcClient.ConfigureCluster(context.Background(), configReq)

        if err != nil {
            log.Printf("[Runner] Failed to configure node %d: %v", cfg.NodeId, err)
            return err
        }
    }
    log.Println("[Runner] Configuring all nodes complete")
    return nil
}

func (r *Runner) killAllNodes() {
    fmt.Println("\nShutting down all nodes...")

    r.muConfig.RLock()
    config := r.currentConfig
    r.muConfig.RUnlock()

    if config != nil {
        for _, cfg := range config.NodeConfigs {
            grpcClient, exists := r.nodeClients[cfg.NodeId] 
            if grpcClient != nil && exists {
                grpcClient.Shutdown(context.Background(), &emptypb.Empty{}) 
            }
        }

        time.Sleep(1 * time.Second)

        // This is not working
        exec.Command("osascript", "-e", `
            tell application "Terminal"
                repeat with w in windows
                    if name of w contains "go run" then
                        do script "exit" in w
                    end if
                end repeat
            end tell
        `).Run()
    }

    

    r.nodeProcesses = make([]*exec.Cmd, 0)
    r.nodeClients = make(map[int32]pb.MessageServiceClient)
}

func (r *Runner) RunAllTestSets() {
    r.testCases = getAllTestCases()

    if len(r.testCases) == 0 {
        log.Println("[Runner] no test cases found")
        return
    }

    reader := bufio.NewReader(os.Stdin)
    
	for setNum := 1; setNum <= len(r.testCases); setNum++ {
        // if setNum > 2 {
        //     break
        // }

        tc := r.testCases[setNum]
        fmt.Printf("\n=========================================\n")
        fmt.Printf("[Runner] Running Set %d — live nodes %v\n", tc.SetNumber, tc.LiveNodes)
        fmt.Printf("=========================================\n")

        config := r.askForClusterConfig(reader)
        r.spawnNodes(config)

        // Waiting for terminal to start
        time.Sleep(time.Duration(config.NodesPerCluster) * time.Second)

        log.Printf("[Runner] Building server grpc connections")
        r.buildNodeConnections()
        log.Printf("[Runner] Building server grpc connections complete")

        // Building client
        log.Printf("[Runner] Building client")
        r.client, _ = NewClient(9000)
        r.client.startGrpcServer()
        r.client.SetClusterConfig(config.NumClusters, config.NodesPerCluster, 
            config.TotalDataItems, config.NodeConfigs)
        log.Printf("[Runner] Building client complete")

        r.configureAllNodes(config)

         // Make this active after a while
        // r.UpdateActiveNodes(tc.LiveNodes)

        // Execute transactions
        r.ExecuteTestCase(tc)
        
        r.showInteractiveMenu()

        r.killAllNodes() 
        r.cleanup()
    }

    log.Printf("All test sets complete")

    // Client connection cleanup
    if r.client != nil {          // ✅ guard
        r.client.closeAllConnections()
    }
    log.Println("[Runner] All sets complete.")
}


func (r *Runner) spawnNodes(config *ClusterConfiguration) {
    projectDir := "/Users/limarkdcunha/Desktop/dist-sys/project3/code"

    for _, cfg := range config.NodeConfigs {
		cmd := exec.Command("osascript", "-e",
			fmt.Sprintf(`tell application "Terminal" to do script "cd %s && go run . -s node %d %d"`,
			projectDir, cfg.NodeId, cfg.PortNo))

		err := cmd.Run()
		if err != nil {
			log.Println("Failed to spawn Node", cfg.NodeId, ":", err)
		} else {
			log.Println("Spawned Node ", cfg.NodeId, "on port", cfg.PortNo)
		}
	}
}

func (r *Runner) askForClusterConfig(reader *bufio.Reader) *ClusterConfiguration {
    fmt.Println("     CLUSTER CONFIGURATION INPUT        ")
    
    var numClusters, nodesPerCluster int

    for {
        fmt.Print("\nEnter number of clusters: ") 
        clustersStr, _ := reader.ReadString('\n')
        clustersStr = strings.TrimSpace(clustersStr)
        
        var err error
        numClusters, err = strconv.Atoi(clustersStr)
        
        if err != nil || numClusters <= 0 || numClusters > 10 {
            fmt.Println("Invalid input. Please enter a number between 1 and 10.")
            continue
        }
        break
    }

    for {
        fmt.Print("Enter nodes per cluster: ")
        nodesStr, _ := reader.ReadString('\n')
        nodesStr = strings.TrimSpace(nodesStr)
        
        var err error
        nodesPerCluster, err = strconv.Atoi(nodesStr)
        
        if err != nil || nodesPerCluster <= 0 || nodesPerCluster > 15 {
            fmt.Println("Invalid input. Please enter a number between 1 and 15.")
            continue
        }
        break
    }

    totalDataItems := int32(9000)
    basePort := int32(8001)

    nodeConfigs := r.generateNodeConfigs(int32(numClusters), int32(nodesPerCluster), basePort)

    config := &ClusterConfiguration{
        NumClusters:     int32(numClusters),
        NodesPerCluster: int32(nodesPerCluster),
        TotalDataItems:  totalDataItems,
        BasePort:        basePort,
        NodeConfigs:     nodeConfigs,
    }

    r.muConfig.Lock()
    r.currentConfig = config
    r.muConfig.Unlock()

    return config
}


func (r *Runner) generateNodeConfigs(numClusters, nodesPerCluster, basePort int32) []NodeConfig {
    totalNodes := numClusters * nodesPerCluster
    configs := make([]NodeConfig, 0, totalNodes)

    nodeId := int32(1)
    portNo := basePort

    for clusterId := int32(1); clusterId <= numClusters; clusterId++ {
        for i := int32(0); i < nodesPerCluster; i++ {
            configs = append(configs, NodeConfig{
                NodeId:    nodeId,
                ClusterId: clusterId,
                PortNo:    portNo,
            })
            nodeId++
            portNo++
        }
    }
    
    return configs
}

func (r *Runner) ExecuteTestCase(tc TestCase) {
	log.Printf("[Runner] Executing %d transactions for Set %d...", len(tc.Transactions), tc.SetNumber)

	batches := r.splitTransactionsIntoBatches(tc.Transactions)

   for _, batch := range batches {
        if batch.isControl {
            cmd := batch.transactions[0]
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

func (r *Runner) cleanup() {
    fmt.Println("\n[Runner] Cleaning up persistence...")
    
    // 1. Delete all node state/WAL files
    CleanupPersistence()  // This deletes logs/, state/, wal/ directories
    
    // 2. Reset runner's configuration
    r.muConfig.Lock()
    r.currentConfig = nil
    r.muConfig.Unlock()
    
    // 3. Close and cleanup client
    if r.client != nil {
        r.client.closeAllConnections()
        r.client = nil
    }
    
    fmt.Println("✓ Cleanup complete")
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
    for _, cfg := range r.currentConfig.NodeConfigs {
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
    r.muConfig.RLock()
    configs := r.currentConfig.NodeConfigs
    r.muConfig.RUnlock()

    for _, nodeCfg := range configs {
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


func (r *Runner) getClusterId(id int32) int32 {
	r.muConfig.RLock()
	totalDataPoints := r.currentConfig.TotalDataItems
    numClusters := r.currentConfig.NumClusters
    r.muConfig.RUnlock()

	if id < 1 || id > totalDataPoints {
        return -1
    }
    
    itemsPerShard := totalDataPoints / numClusters
    clusterId := ((id - 1) / itemsPerShard) + 1
    
    if clusterId > numClusters {
        clusterId = numClusters
    }
    
    return clusterId
}

func (r *Runner) PrintBalanceAll(datapoint string){
    datapointInt, err := strconv.ParseInt(datapoint, 10, 0)
	if err != nil {
		log.Printf("failed to convert string to int32: %v", err)
        return
	}

    targetClusterId := r.getClusterId(int32(datapointInt))

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
