---
title: TiKV Storage Configuration 
summary: Learn how to configure TiKV Storage.
category: reference
---

# TiKV Storage Configuration

In TiKV, Storage is the component responsible for handling read and write requests. Note that if you are using TiKV with TiDB, most read requests are handled by the Coprocessor component instead of Storage.

## Configuration

There are two sections related to Storage: `[readpool.storage]` and `[storage]`.

### `[readpool.storage]`

This configuration section mainly affects storage read operations. Most read requests from TiDB are not controlled by this configuration section. For configuring the read requests from TiDB, see [Coprocessor configurations](coprocessor-config.md).

There are 3 thread pools for handling read operations, namely read-high, read-normal and read-low, which process high-priority, normal-priority and low-priority read requests respectively. The priority can be specified by corresponding fields in the gRPC request.

#### `high-concurrency`

- Specifies the thread pool size for handling high priority requests 
- Default value: 4. It means at most 4 CPU cores are used
- Minimum value: 1
- It must be larger than zero but should not exceed the number of CPU cores of the host machine 
- If you are running multiple TiKV instances on the same machine, make sure that the sum of this configuration item does not exceed number of CPU cores. For example, assuming that you have a 48 core server running 3 TiKVs, then the `high-concurrency` value for each instance should be less than 16
- Do not set this configuration item to a too small value, otherwise your read request QPS is limited. On the other hand, larger value is not always the most optimal choice because there could be larger resource contention


#### `normal-concurrency`

- Specifies the thread pool size for handling normal priority requests
- Default value: 4
- Minimum value: 1

#### `low-concurrency`

- Specifies the thread pool size for handling low priority requests
- Default value: 4
- Minimum value: 1
- Generally, you don’t need to ensure that the sum of high + normal + low < number of CPU cores, because a single request is handled by only one of them

#### `max-tasks-per-worker-high`

- Specifies the max number of running operations for each thread in the read-high thread pool, which handles high priority read requests. Because a throttle of the thread-pool level instead of single thread level is performed, the max number of running operations for the read-high thread pool is limited to `max-tasks-per-worker-high * high-concurrency`
- Default value: 2000
- Minimum value: 2000
- If the number of running operations exceeds this configuration, new operations are simply rejected without being handled and it will contain an error header telling that TiKV is busy
- Generally, you don’t need to adjust this configuration unless you are following trustworthy advice

#### `max-tasks-per-worker-normal`

- Specifies the max running operations for each thread in the read-normal thread pool, which handles normal priority read requests.
- Default value: 2000
- Minimum value: 2000

#### `max-tasks-per-worker-low`

- Specifies the max running operations for each thread in the read-low thread pool, which handles low priority read requests
- Default value: 2000
- Minimum value: 2000

#### `stack-size`

- Sets the stack size for each thread in the three thread pools. For large requests, you need a large stack to handle
- Default value: 10MB
- Minimum value: 2MB

### `[storage]`

This configuration section mainly affects storage write operations, including where data is stored and the TiKV component Scheduler. Scheduler is the core component in Storage that coordinates and processes write requests. It contains a channel to coordinate requests and a thread pool to process requests.

#### `data-dir`

- Specifies the path to the data directory
- Default value: /tmp/tikv/store
- Make sure that the data directory is moved before changing this configuration

#### `scheduler-notify-capacity`

- Specifies the Scheduler channel size
- Default value: 10240
- Do not set it too small, otherwise TiKV might crash
- Do not set it too large, because it might consume more memory 
- Generally, you don’t need to adjust this configuration unless you are following trustworthy advice

#### `scheduler-concurrency`

- Specifies the number of slots of Scheduler’s latch, which controls concurrent write requests
- Default value: 2048000
- You can set it to a larger value to reduce latch contention if there are a lot of write requests. But it will consume more memory

#### `scheduler-worker-pool-size`

- Specifies the Scheduler’s thread pool size. Write requests are finally handled by each worker thread of this thread pool
- Default value: 8 (>= 16 cores) or 4 (< 16 cores)
- Minimum value: 1
- This configuration must be set larger than zero but should not exceed the number of CPU cores of the host machine 
- On machines with more than 16 CPU cores, the default value of this configuration is 8, otherwise 4 
- If you have heavy write requests, you can set this configuration to a larger value. If you are running multiple TiKV instances on the same machine, make sure that the sum of this configuration item does not exceed the number of CPU cores
- You should not set this configuration item to a too small value, otherwise your write request QPS is limited. On the other hand, a larger value is not always the most optimal choice because there could be larger resource contention

#### `scheduler-pending-write-threshold`

- Specifies the maximum allowed byte size of pending writes 
- Default value: 100MB
- If the size of pending write bytes exceeds this threshold, new requests are simply rejected with the “scheduler too busy” error and not be handled