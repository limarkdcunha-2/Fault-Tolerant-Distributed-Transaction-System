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
