---
title: Scheduling Rate Limit
summary: Learn how to configure scheduling speed limit in TiKV
category: how-to
---

# Scheduling Rate Limit

This section describes how to configure scheduling rate limit, specifically, at the store level.

In TiKV, PD generates different scheduling operators based on the information gathered from TiKV and scheduling strategies. The operators are then sent to TiKV to perform scheduling on Regions. You can use `*-schedule-limit` to set speed limits on different operators, but this may cause performance bottlenecks in certain scenarios because these parameters function globally on the entire cluster. Rate limit at the store level allows you to control scheduling more flexibly with more refined granularities.

## How to configure scheduling speed on stores

PD provides the following two methods to configure scheduling rate limits on stores:

- Configure the rate limit using **`store-balance-rate`**.

    `store-balance-rate` specifies the maximum number of scheduling tasks allowed for each store per minute. The scheduling steps include adding Peers or Learners. Set this parameter in the PD configuration file. The default value is 15. This configuration is persistent.

    Use the `pd-ctl` tool to modify `store-balance-rate` and make it persistent.

    Example:

    ```bash
    >> config set store-balance-rate 20
    ```

    > **Note:**
    >
    > The modification immediately only takes effect on newly added stores, and will be applied to the entire cluster after TiKV restarts. If you want to apply speed limits on all stores or an individual store, consider using the second method below.

- Use the `pd-ctl` tool to view or modify the upper limit of the scheduling rate. The commands are:

    - **`stores show limit`**
        
        Example:

        ```bash
        » stores show  // If store-balance-rate is set to 15, the corresponding rate for all stores should be 15 / 60 = 0.25.
        {
        "4": {
        "rate": 0.25
        },
        "5": {
        "rate": 0.25
        },
        ...
        }
        ```
    - **`stores set limit <rate>`**

        Example:

        ```bash
        >> stores set limit 2 // Set the upper limit of scheduling rate for all stores to be 2 scheduling tasks per second.
        ```
    
    - **`store limit <store_id> <rate>`**

        Example:

        ```bash
        >> store limit 2 1 // Set the upper limit of scheduling speed for store 2 to be 1 scheduling task per second.
        ```
    > **Note:**
    >
    > This method is not persistent, and the configuration will lose its efficacy after you restart TiKV.

    See [PD Control](../../reference/tools/pd-control.md) for more detailed description of these commands.
