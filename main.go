/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"flag"
	"fmt"
	"io"

	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	pb "transaction-processor/message"

	"google.golang.org/grpc"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	
	serverMode := flag.Bool("s", false, "Run in server mode (spawn nodes)")
	runnerMode := flag.Bool("r", false, "Run in runner mode (full simulation)")
	testCasesGenMode := flag.Bool("t", false, "Run in to generate random test cases")
	cleanupMode := flag.Bool("d", false, "Run in to generate random test cases")
	
	flag.Parse()

	switch {
	case *serverMode:
		if len(os.Args) > 2 && os.Args[2] == "node" {
			runNode(os.Args[3], os.Args[4])
		} else {
			spawnAllNodes()
		}
	case *runnerMode:
		log.Println("[Runner] Launching full simulation...")
		runner := NewRunner()
		runner.RunAllTestSets()
	case *testCasesGenMode:
		gen_test_cases()
	case *cleanupMode:
		CleanupPersistence()
	default:
		log.Println("Usage:")
		log.Println("  go run . -s node <id> <servicePort> <controlPort>  (Run a single node)")
		// log.Println("  go run . -c                                        (Run client simulator)")
		log.Println("  go run . -r                                        (Run full test runner)")
	}
}

func spawnAllNodes() {
	// Need to figure out how this will work in deployed env
	projectDir := "/Users/limarkdcunha/Desktop/dist-sys/project3/code"

	for _, cfg := range getNodeCluster() {
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

func runNode(nodeID, port string) {
	fmt.Printf("\n[Running] NodeID=%s Port =%s\n", nodeID, port)
	log.SetOutput(io.Discard)

	// file, err := os.OpenFile(fmt.Sprintf("logs/node%s.log",nodeID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer file.Close()

	// log.SetOutput(file)

	nodeIdInt,_ :=strconv.Atoi(nodeID)
	portNumber,_ := strconv.Atoi(port)
	
	node,_ := NewNode(int32(nodeIdInt), int32(portNumber))

	// // Using goroutine so gRPC doesnt block
	go node.spawnMessageServer()

    // // This had cause. kind of deadlock in the past be careful
	select {} 
}


func (node *Node) spawnMessageServer() {
    lis, err := net.Listen("tcp",fmt.Sprintf(":%d",node.portNo))

    if err != nil{
        log.Printf("failed to listen: %v", err)
		return
    }

    grpcServer := grpc.NewServer()
	pb.RegisterMessageServiceServer(grpcServer, node)

	log.Printf("Server listening on port %d",node.portNo)

	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("failed to serve: %v", err)
		return
	}
}