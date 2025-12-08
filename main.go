/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

import (
	"flag"
	"fmt"

	"log"
	"net"
	"os"
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
		log.Println("  go run . -r (Run full test runner)")
	}
}

func runNode(nodeID, port string) {
	fmt.Printf("\n[Running] NodeID=%s Port =%s\n", nodeID, port)
	// log.SetOutput(io.Discard)

	file, err := os.OpenFile(fmt.Sprintf("logs/node%s.log",nodeID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	log.SetOutput(file)

	nodeIdInt,_ :=strconv.Atoi(nodeID)
	portNumber,_ := strconv.Atoi(port)
	
	node,_ := NewNode(int32(nodeIdInt), int32(portNumber))

	go node.spawnMessageServer()

	<-node.shutdownChan
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