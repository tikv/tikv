---
title: Store Limits
summary: Learn how to configure scheduling rate limit on stores
category: how-to
---

# Store Limit

This section describes how to configure scheduling rate limit, specifically, at the store level.

In TiKV, PD generates different scheduling operators based on the information gathered from TiKV and scheduling strategies. The operators are then sent to TiKV to perform scheduling on Regions. You can use `*-schedule-limit` to set speed limits on different operators, but this may cause performance bottlenecks in certain scenarios because these parameters function globally on the entire cluster. Rate limit at the store level allows you to control scheduling more flexibly with more refined granularities.

## How to configure scheduling rate limits on stores

PD provides the following two methods to configure scheduling rate limits on stores:

- Configure the rate limit using **`store-balance-rate`**.

    `store-balance-rate` specifies the maximum number of scheduling tasks allowed for each store per minute. The scheduling steps include adding peers or learners. Set this parameter in the PD configuration file. The default value is 15. This configuration is persistent.

    Use the `pd-ctl` tool to modify `store-balance-rate` and make it persistent.

    Example:

    ```bash
    >> config set store-balance-rate 20
    ```

    > **Note:**
    >
    > The modification only takes effect on stores added after this configuration change, and will be applied to all stores in the cluster after you restart TiKV. If you want this change to work immediately on all stores or some individual stores before the change without restarting, combine this configuration with the `pd-ctl` tool method below. See [Sample usages](#sample-usages) for more details.

- Use the `pd-ctl` tool to view or modify the upper limit of the scheduling rate. The commands are:

    - **`stores show limit`**
        
        Example:

        ```bash
        » stores show limit // If store-balance-rate is set to 15, the corresponding rate for all stores should be 15.
        {
        "4": {
        "rate": 15
        },
        "5": {
        "rate": 15
        },
        ...
        }
        ```
    
    - **`stores set limit <rate>`**

        Example:

        ```bash
        >> stores set limit 20 // Set the upper limit of scheduling rate for all stores to be 20 scheduling tasks per minute.
        ```
    
    - **`store limit <store_id> <rate>`**

        Example:

        ```bash
        >> store limit 2 10 // Set the upper limit of scheduling speed for store 2 to be 10 scheduling tasks per minute.
        ```
    > **Note:**
    >
    > This method is not persistent, and the configuration will lose its efficacy after you restart TiKV.

    See [PD Control](../../reference/tools/pd-control.md) for more detailed description of these commands.

## Sample usages

- The following example modifies the rate limit to 20 and applies immediately to all stores. The configuration is still valid after restart.

    ```bash
    >> config set store-balance-rate 20
    >> stores set limit 20
    ```

- The following example modifies the rate limit for all stores to 20 and applies immediately. After restart, the configuration becomes invalid, and the rate limit for all stores specified by `store-balance-rate` takes over.
    
    ```bash
     >> stores set limit 20
    ```
    
- The following example modifies the rate limit for store 2 to 20 and applies immediately. After restart, the configuration becomes invalid, and the rate limit for store 2 becomes the value specified by  `store-balance-rate`.

    ```bash
    >> store limit 2 20
    ```

