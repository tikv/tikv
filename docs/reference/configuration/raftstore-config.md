---
title: Raftstore Configuration 
summary: Learn about Raftstore configuration in TiKV.
category: reference
---

# Raftstore Configurations

Raftstore is TiKV's implementation of [Multi-raft](https://tikv.org/deep-dive/scalability/multi-raft/) to manage multiple Raft consensus groups on one node. Raftstore is comprised of two major components:

- **Raftstore** component writes Raft logs into RaftDB.
- **Apply** component resolves Raft logs and flush the data in the log into the underlying storage engine. 

This section introduces the following features of Raftstore and their configurations:

- [Multi-thread Raftstore](#multi-thread-raftstore)
- [Hibernate Region](#hibernate-region)

## Multi-thread Raftstore

 Multi-thread support for the Raftstore and the Apply components means higher throughput and lower latency for standalone machines. In the multi-thread mode, each thread obtain peers from the queue in batch, so that small writes of multiple peers can be consolidated into a big write for better throughput.

![Multi-thread Raftstore Model](../../images/multi-thread-raftstore.png)

> **Note:**
>
> In the multi-thread mode, peers are obtained in batch, so pressure from hot write regions cannot be scattered evenly to each CPU. For better load balancing, it is recommended you use smaller regions.

### Configuration items

You can specify the following items in the TiKV configuration file to configure multi-thread Raftstore:

**`raftstore.store_max_batch_size`**

Determines the maximum number of peers that a single thread can obtain in a batch. The value must be a positive integer. A smaller value provides better load balancing for CPU, but may cause more frequent writes.

**`raftstore.store_pool_size`**

Determines the number of threads to process peers in batch. The value must be an positive integer. For better performance, we recommend you set a value less than or equal to the number of your CPU cores.
 
**`apply_max_batch_size`**

Determines the maximum number of peers that a single thread can resolve in a batch. The value must be a positive integer. A smaller value provides better load balancing for CPU, but may cause more frequent writes.

**`apply_pool_size`**

Determines the number of threads. The value must be a positive integer. For better performance, we recommend you set a value less than or equal to the number of your CPU cores.

## Hibernate Region

Hibernate Region is a Raftstore feature to reduced the extra overhead caused by heartbeat messages between the Raft leader and the followers for idle regions. With this feature enabled, a region idle for a long time is automatically set as hibernated. The heartbeat interval for the leader to maintain its lease becomes much longer, and followers do not initiate a re-selection simply because they could not receive heartbeats from the leader.

> **Note:**
> 
>  - Hibernate Region is still an Experimental feature and is disabled by default.
> - Any requests from the client or disconnections will re-activate the region from the hibernated state.

### Configuration items

You can specify the following items in the TiKV configuration file to configure Hibernate Region:

**`raftstore.hibernate-regions`**

Enables or disables Hibernate Region. Possible values are true and false. The default value is false.

**`raftstore.peer_stale_state_check_interval`**

Modifies the state check interval for hibernated Regions. The default value is 5s. This value also determines the heartbeat interval between the leader and followers of the hibernated regions.


