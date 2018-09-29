---
title: TiKV Roadmap
category: Roadmap
---

# TiKV Roadmap

This document defines the roadmap for TiKV development.

## Raft
- [x] Region Merge - Merge small regions together to reduce overhead
- [x] Local Read Thread - Process read requests in a local read thread
- [x] Split Region in Batch - Speed up region split for large regions
- [x] Raft learner - Support raft learner to smooth the configuration change process
- [x] Raft Pre-vote - Support raft pre-vote to avoid unnecessary leader election on network isolation
- [ ] Multi-thread Raftstore - Process region raft logic in multiple threads
- [ ] Multi-thread apply pool - Apply region raft committed entries in multiple threads

## Engine
- [ ] Titan - Separate large key-values from LSM-Tree
- [ ] Pluggable Engine Interface - Clean up the engine wrapper code and provide more extendibility

## Transaction
- [x] Optimize transaction conflicts
- [ ] Distributed GC - Distribute MVCC garbage collection control to TiKV

## Coprocessor
- [ ] Coprocessor Chunk Execution - Process data in chunk to improve performance

## Tool
- [x] TiKV Importer - Speed up data importing by SST file ingestion

## Flow control and degradation
- [ ] Client
    - [ ] TiKV client (Rust crate)
