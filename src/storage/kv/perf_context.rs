// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use engine_rocks::raw::PerfContext;

#[derive(Default, Debug, Clone, Copy, Add, AddAssign, Sub, SubAssign, KV)]
pub struct PerfStatisticsFields {
    pub internal_key_skipped_count: usize,
    pub internal_delete_skipped_count: usize,
    pub block_cache_hit_count: usize,
    pub block_read_count: usize,
    pub block_read_time: usize,
    pub block_read_byte: usize,
    pub encrypt_data_nanos: usize,
    pub decrypt_data_nanos: usize,
    pub block_checksum_time: usize,
    pub block_decompress_time: usize,
    pub get_snapshot_time: usize,
    pub get_from_memtable_time: usize,
    pub get_post_process_time: usize,
    pub get_from_output_files_time: usize,
    pub read_index_block_nanos: usize,
    pub read_filter_block_nanos: usize,
    pub new_table_block_iter_nanos: usize,
    pub block_seek_nanos: usize,
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
            internal_key_skipped_count: perf_context.internal_key_skipped_count() as usize,
            internal_delete_skipped_count: perf_context.internal_delete_skipped_count() as usize,
            block_cache_hit_count: perf_context.block_cache_hit_count() as usize,
            block_read_count: perf_context.block_read_count() as usize,
            block_read_byte: perf_context.block_read_byte() as usize,
            encrypt_data_nanos: perf_context.encrypt_data_nanos() as usize,
            decrypt_data_nanos: perf_context.decrypt_data_nanos() as usize,
            block_read_time: perf_context.block_read_time() as usize,
            block_checksum_time: perf_context.block_checksum_time() as usize,
            block_decompress_time: perf_context.block_decompress_time() as usize,
            get_snapshot_time: perf_context.get_snapshot_time() as usize,
            get_from_memtable_time: perf_context.get_from_memtable_time() as usize,
            get_post_process_time: perf_context.get_post_process_time() as usize,
            get_from_output_files_time: perf_context.get_from_output_files_time() as usize,
            read_index_block_nanos: perf_context.read_index_block_nanos() as usize,
            read_filter_block_nanos: perf_context.read_filter_block_nanos() as usize,
            new_table_block_iter_nanos: perf_context.new_table_block_iter_nanos() as usize,
            block_seek_nanos: perf_context.block_seek_nanos() as usize,
        })
    }

    /// Calculate delta values until now.
    pub fn delta(&self) -> PerfStatisticsDelta {
        let now = Self::new();
        PerfStatisticsDelta(now.0 - self.0)
    }

    pub fn delta_and_update(&mut self) -> PerfStatisticsDelta {
        let now = Self::new();
        let delta = PerfStatisticsDelta(now.0 - self.0);
        self.0 = now.0;
        delta
    }
}

/// Store statistics we need. Data comes from RocksDB's `PerfContext`.
/// This statistics store delta values between two instant statistics.
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_field_operations() {
        let f1 = PerfStatisticsFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        };
        let f2 = PerfStatisticsFields {
            internal_key_skipped_count: 2,
            internal_delete_skipped_count: 3,
            block_cache_hit_count: 5,
            block_read_count: 7,
            block_read_byte: 11,
            ..Default::default()
        };
        let f3 = f1 + f2;
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
        let mut stats = PerfStatisticsDelta(PerfStatisticsFields {
            internal_key_skipped_count: 1,
            internal_delete_skipped_count: 2,
            block_cache_hit_count: 3,
            block_read_count: 4,
            block_read_byte: 5,
            ..Default::default()
        });
        assert_eq!(stats.0.block_cache_hit_count, 3);
        stats.0.block_cache_hit_count = 6;
        assert_eq!(stats.0.block_cache_hit_count, 6);
    }
}
