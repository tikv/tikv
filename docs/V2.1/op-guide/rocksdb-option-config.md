---
title: RocksDB Option Configuration 
summary: Learn how to configure RocksDB options.
category: operations
---

# RocksDB Option Configuration

TiKV uses RocksDB as its underlying storage engine for storing both Raft logs and KV (key-value) pairs. [RocksDB](https://github.com/facebook/rocksdb/wiki) is a highly customizable persistent key-value store that can be tuned to run on a variety of production environments, including pure memory, Flash, hard disks or HDFS. It supports various compression algorithms and good tools for production support and debugging.

## Configuration

TiKV creates two RocksDB instances called `rocksdb` and `raftdb` separately. 

- `rocksdb` has three column families:
    
    - `rocksdb.defaultcf` is used to store actual KV pairs of TiKV
    - `rocksdb.writecf` is used to store the commit information in the MVCC model
    - `rocksdb.lockcf` is used to store the lock information in the MVCC model

- `raftdb` has only one column family called `raftdb.defaultcf`, which is used to store the Raft logs.

Each RocksDB instance and column family is configurable. Below explains the details of DBOptions for tuning the RocksDB instance and CFOptions for tuning the column family.

### DBOptions

#### max-background-jobs

- The maximum number of concurrent background jobs (compactions and flushes)

#### max-sub-compactions

- The maximum number of threads that will concurrently perform a compaction job by breaking the job into multiple smaller ones that run simultaneously

#### max-open-files

- The number of open files that can be used by RocksDB. You may need to increase this if your database has a large working set 
- Value -1 means files opened are always kept open. You can estimate the number of files based on `target_file_size_base` and `target_file_size_multiplier` for level-based compaction 
- If max-open-files = -1, RocksDB will prefetch index blocks and filter blocks into block cache at startup, so if your database has a large working set, it will take several minutes to open RocksDB

#### max-manifest-file-size

- The maximum size of RocksDB's MANIFEST file. For details, see [MANIFEST](https://github.com/facebook/rocksdb/wiki/MANIFEST)

#### create-if-missing

- If it is true, the database will be created when it is missing

#### wal-recovery-mode

RocksDB WAL(write-ahead log) recovery mode:

- `0`: TolerateCorruptedTailRecords, tolerates incomplete record in trailing data on all logs
- `1`: AbsoluteConsistency, tolerates no We don't expect to find any  corruption (all the I/O errors are considered as corruptions) in the WAL
- `2`: PointInTimeRecovery, recovers to point-in-time consistency
- `3`: SkipAnyCorruptedRecords, recovery after a disaster

#### wal-dir

- RocksDB write-ahead logs directory path. This specifies the absolute directory path for write-ahead logs
- If it is empty, the log files will be in the same directory as data 
- When you set the path to the RocksDB directory in memory like in `/dev/shm`, you may want to set `wal-dir` to a directory on a persistent storage. For details, see [RocksDB documentation](https://github.com/facebook/rocksdb/wiki/How-to-persist-in-memory-RocksDB-database) 

#### wal-ttl-seconds

See [wal-size-limit](#wal-size-limit)

#### wal-size-limit

`wal-ttl-seconds` and `wal-size-limit` affect how archived write-ahead logs will be deleted

- If both are set to 0, logs will be deleted immediately and will not get into the archive
- If `wal-ttl-seconds` is 0 and `wal-size-limit` is not 0,
   WAL files will be checked every 10 minutes and if the total size is greater
   than `wal-size-limit`, WAL files will be deleted from the earliest position with the
   earliest until `size_limit` is met. All empty files will be deleted
- If `wal-ttl-seconds` is not 0 and `wal-size-limit` is 0, 
   WAL files will be checked every wal-ttl-seconds / 2 and those that
   are older than `wal-ttl-seconds` will be deleted
- If both are not 0, WAL files will be checked every 10 minutes and both `ttl` and `size`   checks will be performed with ttl being first
- When you set the path to the RocksDB directory in memory like in `/dev/shm`, you may want to set `wal-ttl-seconds` to a value greater than 0 (like 86400) and backup your RocksDB on a regular basis. For details, see [RocksDB documentation](https://github.com/facebook/rocksdb/wiki/How-to-persist-in-memory-RocksDB-database)  

#### wal-bytes-per-sync

- Allows OS to incrementally synchronize WAL to the disk while the log is being written

#### max-total-wal-size

- Once the total size of write-ahead logs exceeds this size, RocksDB will start forcing the flush of column families whose memtables are backed up by the oldest live WAL file
- If it is set to 0, we will dynamically set the WAL size limit to be [sum of all write_buffer_size * max_write_buffer_number] * 4

#### enable-statistics

- RocksDB statistics provide cumulative statistics over time. Turning statistics on will introduce about 5%-10% overhead for RocksDB, but it is worthwhile to know the internal status of RocksDB

#### stats-dump-period 

- Dumps statistics periodically in information logs

#### compaction-readahead-size

- According to [RocksDB FAQ](https://github.com/facebook/rocksdb/wiki/RocksDB-FAQ): if you want to use RocksDB on multi disks or spinning disks, you should set this value to at least 2MB

#### writable-file-max-buffer-size

- The maximum buffer size that is used by `WritableFileWrite`

#### use-direct-io-for-flush-and-compaction 

- Uses `O_DIRECT` for both reads and writes in background flush and compactions

#### rate-bytes-per-sec

- Limits the disk I/O of compaction and flush 
- Compaction and flush can cause terrible spikes if they exceed a certain threshold. It is recommended to set this to 50% ~ 80% of the disk throughput for a more stable result. But for heavy write workload, limiting compaction and flush speed can cause write stalls too

#### enable-pipelined-write

- Enables/Disables the pipelined write. For details, see [Pipelined Write](https://github.com/facebook/rocksdb/wiki/Pipelined-Write)

#### bytes-per-sync

- Allows OS to incrementally synchronize files to the disk while the files are being written asynchronously in the background

#### info-log-max-size

- Specifies the maximum size of the RocksDB log file 
- If the log file is larger than `max_log_file_size`, a new log file will be created
- If max_log_file_size == 0, all logs will be written to one log file

#### info-log-roll-time

- Time for the RocksDB log file to roll (in seconds) 
- If it is specified with non-zero value, the log file will be rolled when its active time is longer than `log_file_time_to_roll`

#### info-log-keep-log-file-num

- The maximum number of RocksDB log files to be kept

#### info-log-dir

- Specifies the RocksDB info log directory 
- If it is empty, the log files will be in the same directory as data 
- If it is non-empty, the log files will be in the specified directory, and the absolute path of RocksDB data directory will be used as the prefix of the log file name

### CFOptions

#### compression-per-level

- Per level compression. The compression method (if any) is used to compress a block
    
    - no: kNoCompression
    - snappy: kSnappyCompression
    - zlib: kZlibCompression
    - bzip2: kBZip2Compression
    - lz4: kLZ4Compression
    - lz4hc: kLZ4HCCompression
    - zstd: kZSTD

- For details, see [Compression of RocksDB](https://github.com/facebook/rocksdb/wiki/Compression)

#### block-size

- Approximate size of user data packed per block. The block size specified here corresponds to the uncompressed data

#### bloom-filter-bits-per-key

- If you're doing point lookups, you definitely want to turn bloom filters on. Bloom filter is used to avoid unnecessary disk read 
- Default: 10, which yields ~1% false positive rate 
- Larger values will reduce false positive rate, but will increase memory usage and space amplification

#### block-based-bloom-filter

- False: one `sst` file has a corresponding bloom filter 
- True: every block has a corresponding bloom filter

#### level0-file-num-compaction-trigger 

- The number of files to trigger level-0 compaction
- A value less than 0 means that level-0 compaction will not be triggered by the number of files

#### level0-slowdown-writes-trigger

- Soft limit on the number of level-0 files. The write performance is slowed down at this point

#### level0-stop-writes-trigger

- The maximum number of level-0 files. The write operation is stopped at this point

#### write-buffer-size

- The amount of data to build up in memory (backed up by an unsorted log on the disk) before it is converted to a sorted on-disk file

#### max-write-buffer-number

- The maximum number of write buffers that are built up in memory

#### min-write-buffer-number-to-merge

- The minimum number of write buffers that will be merged together before writing to the storage

#### max-bytes-for-level-base

- Controls the maximum total data size for the base level (level 1).

#### target-file-size-base

- Target file size for compaction

#### max-compaction-bytes

- The maximum bytes for `compaction.max_compaction_bytes`

#### compaction-pri

There are four different algorithms to pick files to compact:

- `0`: ByCompensatedSize
- `1`: OldestLargestSeqFirst
- `2`: OldestSmallestSeqFirst
- `3`: MinOverlappingRatio

#### block-cache-size

- Caches uncompressed blocks
- Big block-cache can speed up the read performance. Generally, this should be set to 30%-50% of the system's total memory

#### cache-index-and-filter-blocks

- Indicates if index/filter blocks will be put to the block cache 
- If it is not specified, each "table reader" object will pre-load the index/filter blocks during table initialization

#### pin-l0-filter-and-index-blocks

- Pins level0 filter and index blocks in the cache

#### read-amp-bytes-per-bit

Enables read amplification statistics
- value  =>  memory usage (percentage of loaded blocks memory)
- 0      =>  disable
- 1      =>  12.50 %
- 2      =>  06.25 %
- 4      =>  03.12 %
- 8      =>  01.56 %
- 16    =>  00.78 %

#### dynamic-level-bytes

- Picks the target size of each level dynamically 
- This feature can reduce space amplification. It is highly recommended to setit to true. For details, see [Dynamic Level Size for Level-Based Compaction]( https://rocksdb.org/blog/2015/07/23/dynamic-level.html)

## Template

This template shows the default RocksDB configuration for TiKV:

```
[rocksdb]
max-background-jobs = 8
max-sub-compactions = 1
max-open-files = 40960
max-manifest-file-size = "20MB"
create-if-missing = true
wal-recovery-mode = 2
wal-dir = "/tmp/tikv/store"
wal-ttl-seconds = 0
wal-size-limit = 0
max-total-wal-size = "4GB"
enable-statistics = true
stats-dump-period = "10m"
compaction-readahead-size = 0
writable-file-max-buffer-size = "1MB"
use-direct-io-for-flush-and-compaction = false
rate-bytes-per-sec = 0
enable-pipelined-write = true
bytes-per-sync = "0MB"
wal-bytes-per-sync = "0KB"
info-log-max-size = "1GB"
info-log-roll-time = "0"
info-log-keep-log-file-num = 10
info-log-dir = ""

# Column Family default used to store actual data of the database.
[rocksdb.defaultcf]
compression-per-level = ["no", "no", "lz4", "lz4", "lz4", "zstd", "zstd"]
block-size = "64KB"
bloom-filter-bits-per-key = 10
block-based-bloom-filter = false
level0-file-num-compaction-trigger = 4
level0-slowdown-writes-trigger = 20
level0-stop-writes-trigger = 36
write-buffer-size = "128MB"
max-write-buffer-number = 5
min-write-buffer-number-to-merge = 1
max-bytes-for-level-base = "512MB"
target-file-size-base = "8MB"
max-compaction-bytes = "2GB"
compaction-pri = 3
block-cache-size = "1GB"
cache-index-and-filter-blocks = true
pin-l0-filter-and-index-blocks = true
read-amp-bytes-per-bit = 0
dynamic-level-bytes = true

# Options for Column Family write
# Column Family write used to store commit information in MVCC model
[rocksdb.writecf]
compression-per-level = ["no", "no", "lz4", "lz4", "lz4", "zstd", "zstd"]
block-size = "64KB"
write-buffer-size = "128MB"
max-write-buffer-number = 5
min-write-buffer-number-to-merge = 1
max-bytes-for-level-base = "512MB"
target-file-size-base = "8MB"
# In normal cases it should be tuned to 10%-30% of the system's total memory.
block-cache-size = "256MB"
level0-file-num-compaction-trigger = 4
level0-slowdown-writes-trigger = 20
level0-stop-writes-trigger = 36
cache-index-and-filter-blocks = true
pin-l0-filter-and-index-blocks = true
compaction-pri = 3
read-amp-bytes-per-bit = 0
dynamic-level-bytes = true

[rocksdb.lockcf]
compression-per-level = ["no", "no", "no", "no", "no", "no", "no"]
block-size = "16KB"
write-buffer-size = "128MB"
max-write-buffer-number = 5
min-write-buffer-number-to-merge = 1
max-bytes-for-level-base = "128MB"
target-file-size-base = "8MB"
block-cache-size = "256MB"
level0-file-num-compaction-trigger = 1
level0-slowdown-writes-trigger = 20
level0-stop-writes-trigger = 36
cache-index-and-filter-blocks = true
pin-l0-filter-and-index-blocks = true
compaction-pri = 0
read-amp-bytes-per-bit = 0
dynamic-level-bytes = true

[raftdb]
max-sub-compactions = 1
max-open-files = 40960
max-manifest-file-size = "20MB"
create-if-missing = true
enable-statistics = true
stats-dump-period = "10m"
compaction-readahead-size = 0
writable-file-max-buffer-size = "1MB"
use-direct-io-for-flush-and-compaction = false
enable-pipelined-write = true
allow-concurrent-memtable-write = false
bytes-per-sync = "0MB"
wal-bytes-per-sync = "0KB"
info-log-max-size = "1GB"
info-log-roll-time = "0"
info-log-keep-log-file-num = 10
info-log-dir = ""

[raftdb.defaultcf]
compression-per-level = ["no", "no", "lz4", "lz4", "lz4", "zstd", "zstd"]
block-size = "64KB"
write-buffer-size = "128MB"
max-write-buffer-number = 5
min-write-buffer-number-to-merge = 1
max-bytes-for-level-base = "512MB"
target-file-size-base = "8MB"
# should tune to 256MB~2GB.
block-cache-size = "256MB" 
level0-file-num-compaction-trigger = 4
level0-slowdown-writes-trigger = 20
level0-stop-writes-trigger = 36
cache-index-and-filter-blocks = true
pin-l0-filter-and-index-blocks = true
compaction-pri = 0
read-amp-bytes-per-bit = 0
dynamic-level-bytes = true
```