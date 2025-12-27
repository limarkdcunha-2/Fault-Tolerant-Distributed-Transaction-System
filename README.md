# Fault-Tolerant-Distributed-Transaction-System (Spanner-Inspired Sharded Transaction Engine)

Copyright (c) 2025 [Limark Dcunha]  
All rights reserved.

## 🌟 Project Executive Summary

In modern cloud-scale databases, the ultimate challenge is achieving global consistency at massive scale. I built a distributed transaction engine inspired by the architecture of Google Spanner, designed to handle sharded data across multiple autonomous clusters.

The system provides a robust framework for financial transactions where data is distributed to scale horizontally. By combining Paxos for local replication and Two-Phase Commit (2PC) for global atomicity, the engine ensures that complex transfers remain ACID-compliant even when they span across different hardware clusters. To push the performance boundaries, I integrated an Intelligent Resharding Engine as an optimization layer that dynamically re-balances data based on real-world transaction patterns.

### Key Value Delivered

- **Google Spanner Architecture:** Implemented a "Paxos-per-shard" design, where each data partition is maintained by a cluster of nodes to ensure high availability and linearizable consistency.
- **Atomic Distributed Transactions:** Engineered a Two-Phase Commit (2PC) protocol that coordinates multiple independent Paxos clusters, ensuring that cross-shard transfers either complete entirely or fail gracefully.
- **Industrial-Grade Durability:** Leveraged Pebble (RocksDB-based) for persistence and implemented a custom Write-Ahead Log (WAL) to guarantee system recovery and state integrity after node failures.
- **Dynamic Data Locality (Optimization Feature):** Developed a graph-based optimization engine that analyzes transaction history to move "hot" data pairs into the same cluster, reducing expensive cross-shard communication by up to 40%.

## 🧠 Technical Deep Dive: Scaling Consensus & Transactions

### 1. The Consensus Backbone (Multi-Paxos)

The core of the system is built on a Multi-Paxos implementation. Each shard is managed by a cluster that elects a leader to sequence transactions.

- **Leader Election:** Nodes use a ballot-based Paxos protocol to agree on a single coordinator, ensuring the cluster remains functional as long as a majority of nodes are active.
- **Replicated State Machine:** Every transaction is proposed, accepted, and committed through the Paxos pipeline before being applied to the local Pebble database.

### 2. Distributed Atomicity (2PC + WAL)

To handle "Cross-Shard" transactions (e.g., Shard A sending to Shard B), the system upgrades to a Two-Phase Commit protocol.

- **The Coordinator:** The leader of the sender's cluster acts as the 2PC coordinator.
- **Safety via Logging:** I implemented a Write-Ahead Log (WAL) that records the state of every 2PC phase. This allows the system to resolve "in-doubt" transactions and perform rollbacks (undoing credits/debits) if a cluster crashes during the process.

### 3. Optimization Feature: Graph-Based Resharding

While the system is strictly correct via 2PC, cross-shard operations are inherently slower. To optimize this:

- **Transaction Telemetry:** The system tracks interactions between accounts as a weighted graph.
- **Partitioning:** Using a graph-balancing algorithm, the "Runner" periodically calculates a more efficient Shard Map.
- **Live Re-sharding:** The engine can repopulate cluster data based on this map, effectively turning high-latency cross-shard transactions into high-speed intra-shard ones.

## 🚀 Running the Project

**Note:** This project was developed on macOS. It uses AppleScript to automatically spawn 9 independent terminal windows, providing a real-time "Command Center" view of the distributed nodes communicating.

### 1. Setup

Install the Pebble storage engine and Go dependencies.

```bash
go mod download
```

### 2. Launch the Clusters (The Servers)

Spawns three distinct Paxos clusters (\(c_1, c_2, c_3\)), each responsible for a specific range of account shards.

```bash
go run . -s
```

### 3. Execute the Simulation (The Runner)

Runs the full suite of test cases, including node failures, recoveries, and both intra-shard and cross-shard transaction batches.

```bash
go run . -r
```

## 🛠 Tech Stack

- **Language:** Go (Golang) — Chosen for its powerful concurrency primitives (goroutines/channels).
- **Storage:** Pebble (LSM-Tree) — High-performance persistent key-value storage.
- **Networking:** gRPC & Protocol Buffers — For low-latency, strongly-typed inter-node communication.
- **Algorithms:** Multi-Paxos (Consensus), Two-Phase Commit (Atomicity), and Weighted Graph Partitioning (Optimization).
- **Observability:** Custom performance tracking measuring Throughput (Req/Sec) and p99 Latency.
