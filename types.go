/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main


type NodeConfig struct{
	NodeId int32
	PortNo int32
	ClusterId int32
}

type ClusterInfo struct {
    ClusterId int32
    NodeIds   []int32     
    LeaderId  int32
}

type Transaction struct{
	Sender string 
	Receiver string 
	Amount int32
}

type TestTransaction struct {
	IsFailNode      bool  
	IsRecoverNode   bool 
	TargetNodeId    int
	Sender          string 
	Receiver        string 
	Amount          int  
}
type TestCase struct {
    SetNumber      int
    Transactions   []TestTransaction
    LiveNodes      []int
}