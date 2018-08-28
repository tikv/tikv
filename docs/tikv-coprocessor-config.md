---
title: TiKV Coprocessor Configuration 
summary: Learn how to configure TiKV Coprocessor.
category: operations
---

# Configure TiKV Coprocessor

Coprocessor is the component that handles most of the read requests from TiDB. Unlike storage, it is more high-leveled that it not only fetches KV data but also does computing like filter or aggregation. TiKV is used as a distribution computing engine and it is also used to reduce data serialization and traffic. This document describes how to configure TiKV Coprocessor.

## Configuration

Most Coprocessor configurations are in the `[readpool.coprocessor]` section and some configurations are in the `[server]` section.

### `[readpool.coprocessor]`

There are three thread pools for handling high priority, normal priority and low priority requests respectively. TiDB point select is high priority, range scan is normal priority and background jobs like table analyzing is low priority.

#### `high-concurrency`

- Specifies the thread pool size for handling high priority Coprocessor requests 
- Default value: number of cores * 0.8 (> 8 cores) or 8 (<= 8 cores)
- Minimum value: 1
- It must be larger than zero but should not exceed the number of CPU cores of the host machine
- On a machine with more than 8 CPU cores, its default value is NUM_CPUS * 0.8. Otherwise it will be 8 
- If you are running multiple TiKV instances on the same machine, make sure that the sum of this configuration item does not exceed the number of CPU cores. For example, assuming that you have a 48 core server running 3 TiKVs, then the `high-concurrency` value for each instance should be less than 16
- Do not set it to a too small value, otherwise your read request QPS will be limited. On the other hand, larger value is not always the most optimal choice because there may be larger resource contention

#### `normal-concurrency`

- Specifies the thread pool size for handling normal priority Coprocessor requests
- Default value: number of cores * 0.8 (> 8 cores) or 8 (<= 8 cores)
- Minimum value: 1

#### `low-concurrency`

- Specifies the thread pool size for handling low priority Coprocessor requests
- Default value: number of cores * 0.8 (> 8 cores) or 8 (<= 8 cores)
- Minimum value: 1
- Generally, you don’t need to ensure that the sum of high + normal + low < the number of CPU cores, because a single Coprocessor request will be handled by only one of them

#### `max-tasks-per-worker-high`

- Specifies the max running operations for each thread in high priority thread pool
- Default value: number of cores * 0.8 (> 8 cores) or 8 (<= 8 cores)
- Minimum value: 1
- Because actually a thread-pool level throttle instead of single thread level is performed, the max running operations for the thread pool will be limited to `max-tasks-per-worker-high * high-concurrency`. If the number of running operations exceeds this configuration, new operations will be simply rejected without being handled and it will contain an error header telling that TiKV is busy 
- Generally, you don’t need to adjust this configuration unless you are following trustworthy advice

#### `max-tasks-per-worker-normal`

- Specifies the max running operations for each thread in the normal priority thread pool
- Default value: 2000
- Minimum value: 2000

#### `max-tasks-per-worker-low`

- Specifies the max running operations for each thread in the low priority thread pool
- Default value: 2000
- Minimum value: 2000

#### `stack-size`

- Sets the stack size for each thread in the three thread pools 
- Default value: 10MB
- Minimum value: 2MB
- For large requests, you need a large stack to handle. Some Coprocessor requests are extremely large, change with caution

### `[server]`

#### `end-point-recursion-limit`

- Sets the max allowed recursions when decoding Coprocessor DAG expressions 
- Default value: 1000
- Minimum value: 100
- Smaller value is likely to cause large Coprocessor DAG requests to fail

#### `end-point-request-max-handle-duration`

- Sets the max allowed waiting time for each request 
- Default value: 60s
- Minimum value: 60s
- When there are many backlog Coprocessor requests, new requests are likely to wait in queue. If the waiting time of a request exceeds this configuration, it will be rejected with the TiKV busy error and not be handled