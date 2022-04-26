// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::{raw::PerfContext, ReadPerfContextFields};

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This statistics store instant values.
#[derive(Debug, Clone)]
pub struct PerfStatisticsInstant(ReadPerfContextFields);

impl slog::KV for PerfStatisticsInstant {
    fn serialize(
        &self,
        record: &::slog::Record<'_>,
        serializer: &mut dyn slog::Serializer,
    ) -> slog::Result {
        slog::KV::serialize(&self.0, record, serializer)
    }
}

impl !Send for PerfStatisticsInstant {}
impl !Sync for PerfStatisticsInstant {}

impl PerfStatisticsInstant {
    /// Create an instance which stores instant statistics values, retrieved at creation.
    pub fn new() -> Self {
        let perf_context = PerfContext::get();
        PerfStatisticsInstant(ReadPerfContextFields {
            user_key_comparison_count: perf_context.user_key_comparison_count(),
            block_cache_hit_count: perf_context.block_cache_hit_count(),
            block_read_count: perf_context.block_read_count(),
            block_read_byte: perf_context.block_read_byte(),
            block_read_time: perf_context.block_read_time(),
            block_cache_index_hit_count: perf_context.block_cache_index_hit_count(),
            index_block_read_count: perf_context.index_block_read_count(),
            block_cache_filter_hit_count: perf_context.block_cache_filter_hit_count(),
            filter_block_read_count: perf_context.filter_block_read_count(),
            block_checksum_time: perf_context.block_checksum_time(),
            block_decompress_time: perf_context.block_decompress_time(),
            get_read_bytes: perf_context.get_read_bytes(),
            iter_read_bytes: perf_context.iter_read_bytes(),
            internal_key_skipped_count: perf_context.internal_key_skipped_count(),
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count(),
            internal_recent_skipped_count: perf_context.internal_recent_skipped_count(),
            get_snapshot_time: perf_context.get_snapshot_time(),
            get_from_memtable_time: perf_context.get_from_memtable_time(),
            get_from_memtable_count: perf_context.get_from_memtable_count(),
            get_post_process_time: perf_context.get_post_process_time(),
            get_from_output_files_time: perf_context.get_from_output_files_time(),
            seek_on_memtable_time: perf_context.seek_on_memtable_time(),
            seek_on_memtable_count: perf_context.seek_on_memtable_count(),
            next_on_memtable_count: perf_context.next_on_memtable_count(),
            prev_on_memtable_count: perf_context.prev_on_memtable_count(),
            seek_child_seek_time: perf_context.seek_child_seek_time(),
            seek_child_seek_count: perf_context.seek_child_seek_count(),
            seek_min_heap_time: perf_context.seek_min_heap_time(),
            seek_max_heap_time: perf_context.seek_max_heap_time(),
            seek_internal_seek_time: perf_context.seek_internal_seek_time(),
            db_mutex_lock_nanos: perf_context.db_mutex_lock_nanos(),
            db_condition_wait_nanos: perf_context.db_condition_wait_nanos(),
            read_index_block_nanos: perf_context.read_index_block_nanos(),
            read_filter_block_nanos: perf_context.read_filter_block_nanos(),
            new_table_block_iter_nanos: perf_context.new_table_block_iter_nanos(),
            new_table_iterator_nanos: perf_context.new_table_iterator_nanos(),
            block_seek_nanos: perf_context.block_seek_nanos(),
            find_table_nanos: perf_context.find_table_nanos(),
            bloom_memtable_hit_count: perf_context.bloom_memtable_hit_count(),
            bloom_memtable_miss_count: perf_context.bloom_memtable_miss_count(),
            bloom_sst_hit_count: perf_context.bloom_sst_hit_count(),
            bloom_sst_miss_count: perf_context.bloom_sst_miss_count(),
            get_cpu_nanos: perf_context.get_cpu_nanos(),
            iter_next_cpu_nanos: perf_context.iter_next_cpu_nanos(),
            iter_prev_cpu_nanos: perf_context.iter_prev_cpu_nanos(),
            iter_seek_cpu_nanos: perf_context.iter_seek_cpu_nanos(),
            encrypt_data_nanos: perf_context.encrypt_data_nanos(),
            decrypt_data_nanos: perf_context.decrypt_data_nanos(),
        })
    }

    /// Calculate delta values until now.
    pub fn delta(&self) -> ReadPerfContextFields {
        let now = Self::new();
        now.0 - self.0
    }
}

impl Default for PerfStatisticsInstant {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_operations() {
        let f1 = ReadPerfContextFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        };
        let f2 = ReadPerfContextFields {
            internal_key_skipped_count: 2,
            internal_delete_skipped_count: 3,
            block_cache_hit_count: 5,
            block_read_count: 7,
            block_read_byte: 11,
            ..Default::default()
        };
        let f3 = f1.clone() + f2;
        assert_eq!(f3.internal_key_skipped_count, 3);
        assert_eq!(f3.block_cache_hit_count, 8);
        assert_eq!(f3.block_read_byte, 16);

        let mut f3 = f1;
        f3 += f2;
        assert_eq!(f3.internal_key_skipped_count, 3);
        assert_eq!(f3.block_cache_hit_count, 8);
        assert_eq!(f3.block_read_byte, 16);

        let f3 = f2 - f1;
        assert_eq!(f3.internal_key_skipped_count, 1);
        assert_eq!(f3.block_cache_hit_count, 2);
        assert_eq!(f3.block_read_byte, 6);

        let mut f3 = f2;
        f3 -= f1;
        assert_eq!(f3.internal_key_skipped_count, 1);
        assert_eq!(f3.block_cache_hit_count, 2);
        assert_eq!(f3.block_read_byte, 6);
    }

    #[test]
    fn test_deref() {
        let mut stats = ReadPerfContextFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        };
        assert_eq!(stats.block_cache_hit_count, 3);
        stats.block_cache_hit_count = 6;
        assert_eq!(stats.block_cache_hit_count, 6);
    }
}
