/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main


type NodeConfig struct{
	NodeId int32
	PortNo int32
}


type Transaction struct{
	Sender string 
	Receiver string 
	Amount int32
}

type TestTransaction struct {
	IsLeaderFailure bool   // "LF command"
	Sender  string 
	Receiver string 
	Amount  int  
}

type TestCase struct {
    SetNumber      int
    Transactions   []TestTransaction
    LiveNodes      []int
}