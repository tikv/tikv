---
title: TiKV Roadmap
category: Roadmap
---

# TiKV Roadmap

This document defines the roadmap for TiKV development.

## Engine

- [ ] Titan - Use the separated key-value engine in production
- [ ] Pluggable Engine Interface - Clean up the engine wrapper code and provide more extendibility

## Raft

- [ ] Joint Consensus - Change multi members safely
- [ ] Quiescent Region - Reduce the heartbeat cost of inactive Regions
- [ ] Raft engine - Customize engine to replace RocksDB
- [ ] Remove KV RocksDB WAL - Use Raft log as the WAL of state machine engine
- [ ] Crossing datacenter optimization
    - [ ] Raft witness role - Can only vote but not sync logs from Raft leader
    - [ ] Follower snapshot - Get the snapshot from the follower when a new node is added 
    - [ ] Chain replication - Get Raft logs from the non-leader node

## Distributed Transaction 

- [ ] Flow control - Do flow control in scheduler to avoid write stall in advance
- [ ] History version - Use another place to save history version data
- [ ] Performance
    - [ ] Optimize storage format 
    - [ ] Reduce the latency of getting timestamp
    - [ ] Optimize transaction conflicts

## Coprocessor

- [ ] More pushdown expressions support
- [ ] Common expression pipeline optimization 
- [ ] Batch executor - Process multiple rows at a time
- [ ] Vectorized expression evaluation - Evaluate expressions by column
- [ ] Chunk - Compute over the data stored in the continuous memory
- [ ] Explain support - Evaluate the cost of execution

## Tools

- [ ] Use Raft learner to support Backup/Restore/Replication 

## Client

- [ ] Rust
- [ ] Go
- [ ] Java
- [ ] C++

## PD

- [ ] Visualization - Show the cluster status visually
- [ ] Learner scheduler - Schedule learner Regions to specified nodes
- [ ] Improve hot Region scheduler
- [ ] Improve simulator
- [ ] Range scheduler - Schedule Regions in the specified range

## Misc

- [ ] Tolerate corrupted Region - TiKV can be started even when the data in some Regions is corrupted
- [ ] Support huge Region whose size is larger than 1 GB