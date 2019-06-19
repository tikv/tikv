---
title: Titan
summary: Learn how to enable Titan in TiKV.
category: how-to
---

# Titan

Titan is an alternative key-value storage engine developed by PingCAP. It is based on RocksDB, but can reduce write amplification in RocksDB when using large values.

## How Titan works

![Titan Architecture](../../images/titan-architeture)

Titan seperates values from the LSM-tree during flush and compaction. While the actual value is stored in a blob file, the value in the LSM tree functions as the position index of the actual value. When a GET operation is performed, Titan obtains the blob index for the corresponding key from the LSM tree. Using the index, Titan identifies the actual value from the blob file and returns it. For more details on design and implementation of Titan, please refer to [Titan: A RocksDB Plugin to Reduce Write Amplification](https://pingcap.com/blog/titan-storage-engine-design-and-implementation/).

> Caveat:
>
> Titan's improved write performance is at the cost of sacrificing storage space and range query performance. It's only recommended for scenarios of large values (>= 1K).

## How to enable Titan

As Titan has not reached an appropriate maturity to be applied in production, it is disabled in TiKV by default. Before enabling it, make sure you understand the caveat as mentioned above and that you have evaluated your scenarios and needs.

To enable Titan in TiKV:

In the TiKV configuration file, specify `rocksdb.titan.enabled=true`.

## How to fall back to RocksDB

If you find Titan does not help or is causing read or other performance issues, you can take the following steps to fall back to RocksDB:

1. Enter the `tikv-ctl` interface and specify `blob-run-mode = “fallback” / “read-only”`.

2. Wait until the number of blob files reduced to 0. Alternatively, you can do this 
quicky via `tikv-ctl compact-cluster`.

3. In the TiKV configuration file, specify `rocksdb.titan.enabled=false`.









