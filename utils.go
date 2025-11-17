/*
 * Copyright (c) 2025 [Limark Dcunha]
 * All rights reserved.
 */

package main

func getNodeCluster() []NodeConfig {
	return []NodeConfig{
		{NodeId: 1, PortNo: 8001},
		{NodeId: 2, PortNo: 8002},
		{NodeId: 3, PortNo: 8003},
		{NodeId: 4, PortNo: 8004},
		{NodeId: 5, PortNo: 8005},
	}
}


func getClientCluster() []ClientConfig {
    return []ClientConfig{
        {ClientId: 1, ClientName: "A", Port: 9000},
        {ClientId: 2, ClientName: "B", Port: 9001},
        {ClientId: 3, ClientName: "C", Port: 9002},
        {ClientId: 4, ClientName: "D", Port: 9003},
        {ClientId: 5, ClientName: "E", Port: 9004},
        {ClientId: 6, ClientName: "F", Port: 9005},
        {ClientId: 7, ClientName: "G", Port: 9006},
        {ClientId: 8, ClientName: "H", Port: 9007},
        {ClientId: 9, ClientName: "I", Port: 9008},
        {ClientId: 10, ClientName: "J", Port: 9009},
    }
}


func getAllNodeIDs() []int32 {
	var ids []int32

	for _, cfg := range getNodeCluster() {
		ids = append(ids, cfg.NodeId)
	}
	return ids
}
