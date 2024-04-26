# TiKV Roadmap (2024~2025)

- Last Updated: 2024-04-23

This is a roadmap of TiKV project from engineering and architecture aspects. It expresses the intension of how we will improve this project to make TiKV fit more users' use cases in next 1 to 2 years. The roadmap includes several domains like latency and performance improvement, stability, observability, etc. Notice: all content list in the roadmap is our intension, we will try our best efforts to make it happen, but it doesn't mean we have a commitment for it, please don't rely on features/capabilities currently TiKV doesn't have for your projects which have strict timeline requirement, even these features listed in the roadmap.

## QoS: Stable and Predictable Latency

This domain aims to enhance TiKV to provide predictable single-digit millisecond latency under different scenarios like too many MVCC versions, disk IO temporarily jitter, high read QPS in a small data range, etc. 

### In-Memory Engine

TiKV tends to pileup many MVCC versions for a data range if there are high frequency UPDATE operations in this specified data range. It will slowdown the scan/read speed dramatically in this data range, and easily cause high read latency which may cause applications failure/timeout. The In-Memory Engine was designed to accelerate the scan/read speed when there are too many MVCC versions. The In-Memory Engine only cache latest few versions for each row which can reduce the amount of retrieving KVs for most read/scan queries.

### Mitigate the Impact of Temporary Disk IO Jitter

IO latency is very critical to the QoS of TiKV, because TiKV highly depends on the persistence of raftlog and read data from disk(in case of data not in block-cache). If there is a disk IO jitter, the application may suffer from high latency of queries. As the total number of TiKV nodes increases for a TiKV cluster, the higher possibility the TiKV cluster will encouter disk IO jitter issues. We experienced more disk IO jitter issues in cloud disk like AWS EBS compare to local nvm-e disk. We will improve the capability of tolerating single Disk IO jitter issues in a cluster by introduce improvements like apply-before-persistent, mitigate-latency-amplification, etc.

### Low Read Latency of Hot Region

Read queries being evenly distributed in different data ranges is an ideal workload, which can make good use of all resources across different TiKV nodes. But in the real world, queries tend to skew to some small data ranges, like latest added/modified data has higher possibility to be retrieved, a specified category's data has higher read queries in some special time range, etc. We call these highly retrieved data ranges as hot regions. Hot regions tend to cause higher query latency since there might be some queries need to wait. A faster in-memory engine cache layer might have a great benefit in this hot region case since it will use less resource to serve a query.

### Data Warmup before Leader Transfer

When the leader of a data range transferred to another TiKV node, because the data of this range is not in memory yet in the new TiKV node, if there are some read queries route to this new leader, it tends to cause higher read latency for these queries. Scenarios like leader re-balancing, TiKV nodes rolling restart may cause leader transfer. We probably need a data warm-up mechanism for the new leader before transfer the leader role to it to reduce the read latency in such scenario.

### Stable Latency During Planned Operations

As a distributed system, there are some planned operations may happen, like rolling restart, rolling upgrade, scale-in some TiKV nodes, scale-out some TiKV nodes, etc. As the cornerstone of other mission critical databases like TiDB, TiKV should provide continuous service and stable latency even when there is a planned operation.

### Stable Latency Under High Data Density Scenario

Some customers' data volumne of single TiKV cluster reach as large as 500TB, and each TiKV node carries more than 4TB data. These large clusters experienced some latency jitter issues, those issues caused by reasons like too many regions in a single TiKV node, background jobs cause resrouces competition, large compaction jobs, impact of large RocksDB instance's global mutex, etc. We are going to elinimate or mitigate these latency jitter issues, and provide stable latency in such high data density scenario.

## Comprehensive Memory Management

### Component-level Memory Trace

Introduce better memory trace for different internal components like transaction module, read pool, raftstore, etc. In this way, we can monitor the memory usage of different components.

### Memory Quota Limit

TiKV has some soft limits for different components' memory usage like scheduler, raftstore, but there are still some components like coprocessor don't have memory quota limit. In the coprocessor, we use the amount of pending queries to throttle the resources usage of coprocessor, but this still can result in Out-Of-Memory(OOM) when there are too many "BIG" queries which retrieve a large amount of data.

## Powerful and Robust Storage Engine

### RocksDB Engine

Newer version RocksDB introduced some promising features like "WAL compression support" and "async-io", we are going to upgrade TiKV's RocksDB to 8.10 version and leverage these powerful features.

### Titan Engine

Titan engine can reduce write amplification dramatically for large value sceanrios, but there are also some drawbacks of Titan engine like space amplification caused by lazy GC, range scan speed has regression in some cases. We will introduce "Punch Hole" feature into Titan to reduce the space amplification. We probably will introduce "Level-Merge" feature to gain better range scan performance for Titan engine. We also will improve the read performance by prioritize the caching of blob file meta blocks.

## Observability & Diagnosability

Improve TiKV's observability and diagnosability, making it easy to locate issues that might impact the quality of TiKV service.

### Locate Hot Read/Write Data Range Easily

Heavy read and write queries distributed in some small data ranges is normal, how to easily locate which data range has extreme high workload and then resolve or mitigate the potential impact? Does TiKV built-in hot read/write balancing policy take works? We are going to provide better observability to answer these questions.

### Locate Latency Jitter Issues Easily

Latency jitter may happen when there is disk IO jitter, network jitter, or other environmental or non-environmental causes. We are going to improve the observability to easily locate the root cause of temporary latency jitter.

## Developer Experience & Efficiency

### TiKV API-level e2e Local Test Tool

It would be great that if we can test all TiKV API locally, it dramatically increase the developers' experience and efficiency. Comprehensive local testing mechanism also can guarantee every code change is well verified locally. 

### Enhance TiKV Clients

client-go, client-rust, client-java are 3 widely used TiKV clients, we are going to enhance the test coverage and testability of these TiKV clients.