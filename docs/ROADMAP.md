---
title: TiKV Roadmap
category: Roadmap
---

# TiKV Roadmap

This document defines the roadmap for TiKV development.

- [ ] Raft
    - [x] Region merge
    - [ ] Local read thread
    - [ ] Multi-thread raftstore
    - [x] None voter
    - [x] Pre-vote
    - [ ] Multi-thread apply pool
    - [ ] Split region in batch
    - [ ] Raft Engine
- [x] RocksDB 
    - [x] DeleteRange
    - [ ] BlobDB 
- [x] Transaction
    - [x] Optimize transaction conflicts
    - [ ] Distributed GC
- [x] Coprocessor
    - [x] Streaming
- [ ] Tool
    - [x] Import distributed data
    - [ ] Export distributed data
    - [ ] Disaster Recovery
    - [ ] TiKV client (Rust crate)
- [ ] Flow control and degradation
- [ ] Client
    - [ ] TiKV client (Rust crate)
   
