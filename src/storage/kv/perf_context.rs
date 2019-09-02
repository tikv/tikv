// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::ops::{Deref, DerefMut};

use engine::rocks::{PerfContext, PerfLevel,set_perf_level};
use prometheus::local::LocalHistogramVec;

#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign, KV)]
pub struct PerfStatisticsFields {
    // pub internal_key_skipped_count: usize,
    // pub internal_delete_skipped_count: usize,
    // pub block_cache_hit_count: usize,
    // pub block_read_count: usize,
    // pub block_read_byte: usize,
    pub user_key_comparison_count: usize, // total number of user key comparisons
    pub block_cache_hit_count: usize,     // total number of block cache hits
    pub block_read_count: usize,          // total number of block reads (with IO)
    pub block_read_byte: usize,           // total number of bytes from block reads
    pub block_read_time: usize,           // total nanos spent on block reads
    pub block_checksum_time: usize,       // total nanos spent on block checksum
    pub block_decompress_time: usize,     // total nanos spent on block decompression

    pub get_read_bytes: usize,      // bytes for vals returned by Get
    pub multiget_read_bytes: usize, // bytes for vals returned by MultiGet
    pub iter_read_bytes: usize,     // bytes for keys/vals decoded by iterator

    // total number of internal keys skipped over during iteration.
    // There are several reasons for it:
    // 1. when calling Next(), the iterator is in the position of the previous
    //    key, so that we'll need to skip it. It means this counter will always
    //    be incremented in Next().
    // 2. when calling Next(), we need to skip internal entries for the previous
    //    keys that are overwritten.
    // 3. when calling Next(), Seek() or SeekToFirst(), after previous key
    //    before calling Next(), the seek key in Seek() or the beginning for
    //    SeekToFirst(), there may be one or more deleted keys before the next
    //    valid key that the operation should place the iterator to. We need
    //    to skip both of the tombstone and updates hidden by the tombstones. The
    //    tombstones are not included in this counter, while previous updates
    //    hidden by the tombstones will be included here.
    // 4. symmetric cases for Prev() and SeekToLast()
    // internal_recent_skipped_count is not included in this counter.
    //
    pub internal_key_skipped_count: usize,
    // Total number of deletes and single deletes skipped over during iteration
    // When calling Next(), Seek() or SeekToFirst(), after previous position
    // before calling Next(), the seek key in Seek() or the beginning for
    // SeekToFirst(), there may be one or more deleted keys before the next valid
    // key. Every deleted key is counted once. We don't recount here if there are
    // still older updates invalidated by the tombstones.
    //
    pub internal_delete_skipped_count: usize,
    // How many times iterators skipped over internal keys that are more recent
    // than the snapshot that iterator is using.
    //
    pub internal_recent_skipped_count: usize,
    // How many values were fed into merge operator by iterators.
    //
    pub internal_merge_count: usize,

    pub get_snapshot_time: usize, // total nanos spent on getting snapshot
    pub get_from_memtable_time: usize, // total nanos spent on querying memtables
    pub get_from_memtable_count: usize, // number of mem tables queried
    // total nanos spent after Get() finds a key
    pub get_post_process_time: usize,
    pub get_from_output_files_time: usize, // total nanos reading from output files
    // total nanos spent on seeking memtable
    pub seek_on_memtable_time: usize,
    // number of seeks issued on memtable
    // (including SeekForPrev but not SeekToFirst and SeekToLast)
    pub seek_on_memtable_count: usize,
    // number of Next()s issued on memtable
    pub next_on_memtable_count: usize,
    // number of Prev()s issued on memtable
    pub prev_on_memtable_count: usize,
    // total nanos spent on seeking child iters
    pub seek_child_seek_time: usize,
    // number of seek issued in child iterators
    pub seek_child_seek_count: usize,
    pub seek_min_heap_time: usize, // total nanos spent on the merge min heap
    pub seek_max_heap_time: usize, // total nanos spent on the merge max heap
    // total nanos spent on seeking the internal entries
    pub seek_internal_seek_time: usize,
    // total nanos spent on iterating internal entries to find the next user entry
    pub find_next_user_entry_time: usize,

    // This group of stats provide a breakdown of time spent by Write().
    // May be inaccurate when 2PC, two_write_queues or enable_pipelined_write
    // are enabled.
    //
    // total nanos spent on writing to WAL
    pub write_wal_time: usize,
    // total nanos spent on writing to mem tables
    pub write_memtable_time: usize,
    // total nanos spent on delaying or throttling write
    pub write_delay_time: usize,
    // // total nanos spent on switching memtable/wal and scheduling
    // // flushes/compactions.
    // pub write_scheduling_flushes_compactions_time: usize,
    // total nanos spent on writing a record, excluding the above four things
    pub write_pre_and_post_process_time: usize,

    // // time spent waiting for other threads of the batch group
    // pub write_thread_wait_nanos: usize,

    // time spent on acquiring DB mutex.
    pub db_mutex_lock_nanos: usize,
    // Time spent on waiting with a condition variable created with DB mutex.
    pub db_condition_wait_nanos: usize,
    // Time spent on merge operator.
    pub merge_operator_time_nanos: usize,

    // Time spent on reading index block from block cache or SST file
    pub read_index_block_nanos: usize,
    // Time spent on reading filter block from block cache or SST file
    pub read_filter_block_nanos: usize,
    // Time spent on creating data block iterator
    pub new_table_block_iter_nanos: usize,
    // Time spent on creating a iterator of an SST file.
    pub new_table_iterator_nanos: usize,
    // Time spent on seeking a key in data/index blocks
    pub block_seek_nanos: usize,
    // Time spent on finding or creating a table reader
    pub find_table_nanos: usize,
    // total number of mem table bloom hits
    pub bloom_memtable_hit_count: usize,
    // total number of mem table bloom misses
    pub bloom_memtable_miss_count: usize,
    // total number of SST table bloom hits
    pub bloom_sst_hit_count: usize,
    // total number of SST table bloom misses
    pub bloom_sst_miss_count: usize,
    // // Time spent waiting on key locks in transaction lock manager.
    // pub key_lock_wait_time: usize,
    // // number of times acquiring a lock was blocked by another transaction.
    // pub key_lock_wait_count: usize,
}

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This statistics store instant values.
#[derive(Debug, Clone, Copy)]
pub struct PerfStatisticsInstant(pub PerfStatisticsFields);

impl slog::KV for PerfStatisticsInstant {
    fn serialize(
        &self,
        record: &::slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::KV::serialize(&self.0, record, serializer)
    }
}

impl PerfStatisticsInstant {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new() -> Self {
        let perf_context = PerfContext::get();
        PerfStatisticsInstant(PerfStatisticsFields {
            // internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            // internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            // block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            // block_read_count: perf_context.block_read_count() as usize,
            // block_read_byte: perf_context.block_read_byte() as usize,
            user_key_comparison_count: perf_context.user_key_comparison_count() as usize,
            block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            block_read_count: perf_context.block_read_count() as usize,
            block_read_byte: perf_context.block_read_byte() as usize,
            block_read_time: perf_context.block_read_time() as usize,
            block_checksum_time: perf_context.block_checksum_time() as usize,
            block_decompress_time: perf_context.block_decompress_time() as usize,
            get_read_bytes: perf_context.get_read_bytes() as usize,
            multiget_read_bytes: perf_context.multiget_read_bytes() as usize,
            iter_read_bytes: perf_context.iter_read_bytes() as usize,
            internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            internal_recent_skipped_count: perf_context.internal_recent_skipped_count() as usize,
            internal_merge_count: perf_context.internal_merge_count() as usize,
            get_snapshot_time: perf_context.get_snapshot_time() as usize,
            get_from_memtable_time: perf_context.get_from_memtable_time() as usize,
            get_from_memtable_count: perf_context.get_from_memtable_count() as usize,
            get_post_process_time: perf_context.get_post_process_time() as usize,
            get_from_output_files_time: perf_context.get_from_output_files_time() as usize,
            seek_on_memtable_time: perf_context.seek_on_memtable_time() as usize,
            seek_on_memtable_count: perf_context.seek_on_memtable_count() as usize,
            next_on_memtable_count: perf_context.next_on_memtable_count() as usize,
            prev_on_memtable_count: perf_context.prev_on_memtable_count() as usize,
            seek_child_seek_time: perf_context.seek_child_seek_time() as usize,
            seek_child_seek_count: perf_context.seek_child_seek_count() as usize,
            seek_min_heap_time: perf_context.seek_min_heap_time() as usize,
            seek_max_heap_time: perf_context.seek_max_heap_time() as usize,
            seek_internal_seek_time: perf_context.seek_internal_seek_time() as usize,
            find_next_user_entry_time: perf_context.find_next_user_entry_time() as usize,
            write_wal_time: perf_context.write_wal_time() as usize,
            write_memtable_time: perf_context.write_memtable_time() as usize,
            write_delay_time: perf_context.write_delay_time() as usize,
            // write_scheduling_flushes_compactions_time: perf_context.write_scheduling_flushes_compactions_time() as usize,
            write_pre_and_post_process_time: perf_context.write_pre_and_post_process_time()
                as usize,
            // write_thread_wait_nanos: perf_context.write_thread_wait_nanos() as usize,
            db_mutex_lock_nanos: perf_context.db_mutex_lock_nanos() as usize,
            db_condition_wait_nanos: perf_context.db_condition_wait_nanos() as usize,
            merge_operator_time_nanos: perf_context.merge_operator_time_nanos() as usize,
            read_index_block_nanos: perf_context.read_index_block_nanos() as usize,
            read_filter_block_nanos: perf_context.read_filter_block_nanos() as usize,
            new_table_block_iter_nanos: perf_context.new_table_block_iter_nanos() as usize,
            new_table_iterator_nanos: perf_context.new_table_iterator_nanos() as usize,
            block_seek_nanos: perf_context.block_seek_nanos() as usize,
            find_table_nanos: perf_context.find_table_nanos() as usize,
            bloom_memtable_hit_count: perf_context.bloom_memtable_hit_count() as usize,
            bloom_memtable_miss_count: perf_context.bloom_memtable_miss_count() as usize,
            bloom_sst_hit_count: perf_context.bloom_sst_hit_count() as usize,
            bloom_sst_miss_count: perf_context.bloom_sst_miss_count() as usize,
            // key_lock_wait_time: perf_context.key_lock_wait_time() as usize,
            // key_lock_wait_count: perf_context.key_lock_wait_count() as usize,
        })
    }

    /// Calculate delta values until now.
    pub fn delta(&self) -> PerfStatisticsDelta {
        let now = Self::new();
        PerfStatisticsDelta(now.0 - self.0)
    }
}

impl Deref for PerfStatisticsInstant {
    type Target = PerfStatisticsFields;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PerfStatisticsInstant {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}


pub struct PerfTask{
    local_histogram: LocalHistogramVec,
    perf_level: PerfLevel,
    init_stats: Option<PerfStatisticsInstant>,
    label: &'static str,
}

impl PerfTask{
    pub fn new(local_histogram: LocalHistogramVec, perf_level: PerfLevel, label: &'static str) -> Self{
        PerfTask{
            local_histogram,
            perf_level,
            init_stats: None,
            label,
        }
    }

    pub fn start_perf(&mut self){
        set_perf_level(self.perf_level);
        self.init_stats = Some(PerfStatisticsInstant::new());
    }

    fn persist_perf_data(&mut self) {
        if let Some(stats) = self.init_stats{
            let delta = stats.delta();
            self.local_histogram
                .with_label_values(&[self.label, "user_key_comparison_count"])
                .observe(delta.user_key_comparison_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_cache_hit_count"])
                .observe(delta.block_cache_hit_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_read_count"])
                .observe(delta.block_read_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_read_byte"])
                .observe(delta.block_read_byte as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_read_time"])
                .observe(delta.block_read_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_checksum_time"])
                .observe(delta.block_checksum_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_decompress_time"])
                .observe(delta.block_decompress_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_read_bytes"])
                .observe(delta.get_read_bytes as f64);
            self.local_histogram
                .with_label_values(&[self.label, "multiget_read_bytes"])
                .observe(delta.multiget_read_bytes as f64);
            self.local_histogram
                .with_label_values(&[self.label, "iter_read_bytes"])
                .observe(delta.iter_read_bytes as f64);
            self.local_histogram
                .with_label_values(&[self.label, "internal_key_skipped_count"])
                .observe(delta.internal_key_skipped_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "internal_delete_skipped_count"])
                .observe(delta.internal_delete_skipped_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "internal_recent_skipped_count"])
                .observe(delta.internal_recent_skipped_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "internal_merge_count"])
                .observe(delta.internal_merge_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_snapshot_time"])
                .observe(delta.get_snapshot_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_from_memtable_time"])
                .observe(delta.get_from_memtable_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_from_memtable_count"])
                .observe(delta.get_from_memtable_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_post_process_time"])
                .observe(delta.get_post_process_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "get_from_output_files_time"])
                .observe(delta.get_from_output_files_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_on_memtable_time"])
                .observe(delta.seek_on_memtable_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_on_memtable_count"])
                .observe(delta.seek_on_memtable_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "next_on_memtable_count"])
                .observe(delta.next_on_memtable_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "prev_on_memtable_count"])
                .observe(delta.prev_on_memtable_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_child_seek_time"])
                .observe(delta.seek_child_seek_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_child_seek_count"])
                .observe(delta.seek_child_seek_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_min_heap_time"])
                .observe(delta.seek_min_heap_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_max_heap_time"])
                .observe(delta.seek_max_heap_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "seek_internal_seek_time"])
                .observe(delta.seek_internal_seek_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "find_next_user_entry_time"])
                .observe(delta.find_next_user_entry_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "write_wal_time"])
                .observe(delta.write_wal_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "write_memtable_time"])
                .observe(delta.write_memtable_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "write_delay_time"])
                .observe(delta.write_delay_time as f64);
            // write_perf.with_label_values(&[self.label, "write_scheduling_flushes_compactions_time"]).observe(delta.write_scheduling_flushes_compactions_time as f64);
            self.local_histogram
                .with_label_values(&[self.label, "write_pre_and_post_process_time"])
                .observe(delta.write_pre_and_post_process_time as f64);
            // write_perf.with_label_values(&[self.label, "write_thread_wait_nanos"]).observe(delta.write_thread_wait_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "db_mutex_lock_nanos"])
                .observe(delta.db_mutex_lock_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "db_condition_wait_nanos"])
                .observe(delta.db_condition_wait_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "merge_operator_time_nanos"])
                .observe(delta.merge_operator_time_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "read_index_block_nanos"])
                .observe(delta.read_index_block_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "read_filter_block_nanos"])
                .observe(delta.read_filter_block_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "new_table_block_iter_nanos"])
                .observe(delta.new_table_block_iter_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "new_table_iterator_nanos"])
                .observe(delta.new_table_iterator_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "block_seek_nanos"])
                .observe(delta.block_seek_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "find_table_nanos"])
                .observe(delta.find_table_nanos as f64);
            self.local_histogram
                .with_label_values(&[self.label, "bloom_memtable_hit_count"])
                .observe(delta.bloom_memtable_hit_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "bloom_memtable_miss_count"])
                .observe(delta.bloom_memtable_miss_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "bloom_sst_hit_count"])
                .observe(delta.bloom_sst_hit_count as f64);
            self.local_histogram
                .with_label_values(&[self.label, "bloom_sst_miss_count"])
                .observe(delta.bloom_sst_miss_count as f64);
            // write_perf.with_label_values(&[self.label, "key_lock_wait_time"]).observe(delta.key_lock_wait_time as f64);
            // write_perf.with_label_values(&[self.label, "key_lock_wait_count"]).observe(delta.key_lock_wait_count as f64);
        }
    }
}

impl Drop for PerfTask{
    fn drop(&mut self){
        self.persist_perf_data();
        if self.perf_level != PerfLevel::EnableCount {
            set_perf_level(PerfLevel::EnableCount);
        }
    }
}


/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This this statistics store delta values between two instant statistics.
#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign)]
pub struct PerfStatisticsDelta(pub PerfStatisticsFields);

impl slog::KV for PerfStatisticsDelta {
    fn serialize(
        &self,
        record: &::slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::KV::serialize(&self.0, record, serializer)
    }
}

impl Deref for PerfStatisticsDelta {
    type Target = PerfStatisticsFields;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PerfStatisticsDelta {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_operations() {
        let f1 = PerfStatisticsFields {
            user_key_comparison_count: 0,
            block_cache_hit_count: 1,
            block_read_count: 2,
            block_read_byte: 3,
            block_read_time: 4,
            block_checksum_time: 5,
            block_decompress_time: 6,
            get_read_bytes: 7,
            multiget_read_bytes: 8,
            iter_read_bytes: 9,
            internal_key_skipped_count: 11,
            internal_delete_skipped_count: 12,
            internal_recent_skipped_count: 13,
            internal_merge_count: 14,
            get_snapshot_time: 15,
            get_from_memtable_time: 16,
            get_from_memtable_count: 17,
            get_post_process_time: 18,
            get_from_output_files_time: 19,
            seek_on_memtable_time: 20,
            seek_on_memtable_count: 21,
            next_on_memtable_count: 22,
            prev_on_memtable_count: 23,
            seek_child_seek_time: 24,
            seek_child_seek_count: 25,
            seek_min_heap_time: 26,
            seek_max_heap_time: 27,
            seek_internal_seek_time: 28,
            find_next_user_entry_time: 29,
            write_wal_time: 30,
            write_memtable_time: 31,
            write_delay_time: 32,
            write_pre_and_post_process_time: 33,
            db_mutex_lock_nanos: 34,
            db_condition_wait_nanos: 35,
            merge_operator_time_nanos: 36,
            read_index_block_nanos: 37,
            read_filter_block_nanos: 38,
            new_table_block_iter_nanos: 39,
            new_table_iterator_nanos: 40,
            block_seek_nanos: 41,
            find_table_nanos: 42,
            bloom_memtable_hit_count: 43,
            bloom_memtable_miss_count: 44,
            bloom_sst_hit_count: 45,
            bloom_sst_miss_count: 46,
        };
        let f2 = PerfStatisticsFields {
            user_key_comparison_count: 0,
            block_cache_hit_count: 1,
            block_read_count: 2,
            block_read_byte: 3,
            block_read_time: 4,
            block_checksum_time: 5,
            block_decompress_time: 6,
            get_read_bytes: 7,
            multiget_read_bytes: 8,
            iter_read_bytes: 9,
            internal_key_skipped_count: 11,
            internal_delete_skipped_count: 12,
            internal_recent_skipped_count: 13,
            internal_merge_count: 14,
            get_snapshot_time: 15,
            get_from_memtable_time: 16,
            get_from_memtable_count: 17,
            get_post_process_time: 18,
            get_from_output_files_time: 19,
            seek_on_memtable_time: 20,
            seek_on_memtable_count: 21,
            next_on_memtable_count: 22,
            prev_on_memtable_count: 23,
            seek_child_seek_time: 24,
            seek_child_seek_count: 25,
            seek_min_heap_time: 26,
            seek_max_heap_time: 27,
            seek_internal_seek_time: 28,
            find_next_user_entry_time: 29,
            write_wal_time: 30,
            write_memtable_time: 31,
            write_delay_time: 32,
            write_pre_and_post_process_time: 33,
            db_mutex_lock_nanos: 34,
            db_condition_wait_nanos: 35,
            merge_operator_time_nanos: 36,
            read_index_block_nanos: 37,
            read_filter_block_nanos: 38,
            new_table_block_iter_nanos: 39,
            new_table_iterator_nanos: 40,
            block_seek_nanos: 41,
            find_table_nanos: 42,
            bloom_memtable_hit_count: 43,
            bloom_memtable_miss_count: 44,
            bloom_sst_hit_count: 45,
            bloom_sst_miss_count: 46,
        };
        let f3 = f1 + f2;
        assert_eq!(f3.internal_key_skipped_count, 22);
        assert_eq!(f3.block_cache_hit_count, 2);
        assert_eq!(f3.block_read_byte, 6);

        let mut f3 = f1;
        f3 += f2;
        assert_eq!(f3.internal_key_skipped_count, 33);
        assert_eq!(f3.block_cache_hit_count, 3);
        assert_eq!(f3.block_read_byte, 9);

        let f3 = f2 - f1;
        assert_eq!(f3.internal_key_skipped_count, 0);
        assert_eq!(f3.block_cache_hit_count, 0);
        assert_eq!(f3.block_read_byte, 0);

        let mut f3 = f2;
        f3 -= f1;
        assert_eq!(f3.internal_key_skipped_count, 0);
        assert_eq!(f3.block_cache_hit_count, 0);
        assert_eq!(f3.block_read_byte, 0);
    }

    #[test]
    fn test_deref() {
        let mut stats = PerfStatisticsDelta(PerfStatisticsFields {
            user_key_comparison_count: 0,
            block_cache_hit_count: 1,
            block_read_count: 2,
            block_read_byte: 3,
            block_read_time: 4,
            block_checksum_time: 5,
            block_decompress_time: 6,
            get_read_bytes: 7,
            multiget_read_bytes: 8,
            iter_read_bytes: 9,
            internal_key_skipped_count: 11,
            internal_delete_skipped_count: 12,
            internal_recent_skipped_count: 13,
            internal_merge_count: 14,
            get_snapshot_time: 15,
            get_from_memtable_time: 16,
            get_from_memtable_count: 17,
            get_post_process_time: 18,
            get_from_output_files_time: 19,
            seek_on_memtable_time: 20,
            seek_on_memtable_count: 21,
            next_on_memtable_count: 22,
            prev_on_memtable_count: 23,
            seek_child_seek_time: 24,
            seek_child_seek_count: 25,
            seek_min_heap_time: 26,
            seek_max_heap_time: 27,
            seek_internal_seek_time: 28,
            find_next_user_entry_time: 29,
            write_wal_time: 30,
            write_memtable_time: 31,
            write_delay_time: 32,
            write_pre_and_post_process_time: 33,
            db_mutex_lock_nanos: 34,
            db_condition_wait_nanos: 35,
            merge_operator_time_nanos: 36,
            read_index_block_nanos: 37,
            read_filter_block_nanos: 38,
            new_table_block_iter_nanos: 39,
            new_table_iterator_nanos: 40,
            block_seek_nanos: 41,
            find_table_nanos: 42,
            bloom_memtable_hit_count: 43,
            bloom_memtable_miss_count: 44,
            bloom_sst_hit_count: 45,
            bloom_sst_miss_count: 46,
        });
        assert_eq!(stats.block_cache_hit_count, 1);
        stats.block_cache_hit_count = 6;
        assert_eq!(stats.block_cache_hit_count, 6);
    }
}
