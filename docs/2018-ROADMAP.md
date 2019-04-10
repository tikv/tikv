---
title: TiKV Roadmap
category: Roadmap
---

# TiKV Roadmap

This document defines the roadmap for TiKV development.

## Raft

- [x] Region Merge - Merge small Regions together to reduce overhead
- [x] Local Read Thread - Process read requests in a local read thread
- [x] Split Region in Batch - Speed up Region split for large Regions
- [x] Raft Learner - Support Raft learner to smooth the configuration change process
- [x] Raft Pre-vote - Support Raft pre-vote to avoid unnecessary leader election on network isolation
- [ ] Joint Consensus - Change multi members safely
- [X] Multi-thread Raftstore - Process Region Raft logic in multiple threads
- [X] Multi-thread Apply Pool - Apply Region Raft committed entries in multiple threads

## Engine

- [ ] Titan - Separate large key-values from LSM-Tree
- [ ] Pluggable Engine Interface - Clean up the engine wrapper code and provide more extendibility

## Storage

- [ ] Flow Control - Do flow control in scheduler to avoid write stall in advance

## Transaction

- [X] Optimize transaction conflicts
- [X] Distributed GC - Distribute MVCC garbage collection control to TiKV

## Coprocessor

- [x] Streaming - Cut large data set into small chunks to optimize memory consumption
- [ ] Chunk Execution - Process data in chunk to improve performance
- [ ] Request Tracing - Provide per-request execution details

## Tools

- [x] TiKV Importer - Speed up data importing by SST file ingestion

## Client

- [ ] TiKV Client (Rust crate)
- [ ] Batch gRPC Message - Reduce message overhead

## PD

- [ ] Optimize Region Metadata - Save Region metadata in detached storage engine
