---
title: Titan
summary: Learn how to enable Titan in TiKV.
category: how-to
---

# Titan

Titan is a plugin of RocksDB developed by PingCAP to provide key-value separation. The goal of Titan is to reduce write amplification of RocksDB when using large values.

## How Titan works

![Titan Architecture](../../images/titan-architecture.png)

Titan separates values from the LSM-tree during flush and compaction. While the actual value is stored in a blob file, the value in the LSM tree functions as the position index of the actual value. When a GET operation is performed, Titan obtains the blob index for the corresponding key from the LSM tree. Using the index, Titan identifies the actual value from the blob file and returns it. For more details on design and implementation of Titan, please refer to [Titan: A RocksDB Plugin to Reduce Write Amplification](https://pingcap.com/blog/titan-storage-engine-design-and-implementation/).

> Caveat:
>
> Titan's improved write performance is at the cost of sacrificing storage space and range query performance. It's mostly recommended for scenarios of large values (>= 1KB).

## How to enable Titan

As Titan has not reached an appropriate maturity to be applied in production, it is disabled in TiKV by default. Before enabling it, make sure you understand the caveat as mentioned above and that you have evaluated your scenarios and needs.

To enable Titan in TiKV, in the TiKV configuration file, specify:

```toml
[rocksdb.titan]
# Enables or disables `Titan`. Note that Titan is still an experimental feature.
# default: false
enabled = true
```

## How to fall back to RocksDB

If you find Titan does not help or is causing read or other performance issues, you can take the following steps to fall back to RocksDB:

1. Enter the fallback mode by specifying:

   ```
   tikv-ctl --host 127.0.0.1:20160 modify-tikv-config -m kvdb -n default.blob_run_mode -v "kFallback"
   ```
  
    > **Note:**
    >
    > Make sure you have already enabled Titan.

2. Wait until the number of blob files reduced to 0. Alternatively, you can do this 
quicky via `tikv-ctl compact-cluster`.

3. In the TiKV configuration file, specify `rocksdb.titan.enabled=false`, and restart TiKV.
