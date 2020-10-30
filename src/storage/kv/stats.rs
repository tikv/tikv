// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::{GcKeysCF, GcKeysDetail};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use kvproto::kvrpcpb::{ScanDetail, ScanInfo};
pub use raftstore::store::{FlowStatistics, FlowStatsReporter};

const STAT_PROCESSED_KEYS: &str = "processed_keys";
const STAT_GET: &str = "get";
const STAT_NEXT: &str = "next";
const STAT_PREV: &str = "prev";
const STAT_SEEK: &str = "seek";
const STAT_SEEK_FOR_PREV: &str = "seek_for_prev";
const STAT_OVER_SEEK_BOUND: &str = "over_seek_bound";

const STAT_NEXT_TOMBSTONE: &str = "next_tombstone";
const STAT_NEXT_BLOCK_READ_TIME: &str = "next_block_read_time";
const STAT_NEXT_BLOCK_READ_COUNT: &str = "next_block_read_count";
const STAT_NEXT_BLOCK_CHECKSUM_TIME: &str = "next_block_checksum_time";
const STAT_NEXT_BLOCK_DECOMPRESS_TIME: &str = "next_block_decompress_time";
const STAT_NEXT_GET_SNAPSHOT_TIME: &str = "next_get_snapshot_time";
const STAT_NEXT_GET_FROM_MEMTABLE_TIME: &str = "next_get_from_memtable_time";
const STAT_NEXT_GET_POST_PROCESS_TIME: &str = "next_get_post_process_time";
const STAT_NEXT_GET_FROM_OUTPUT_FILES_TIME: &str = "next_get_from_output_files_time";
const STAT_NEXT_READ_INDEX_BLOCK_NANOS: &str = "next_read_index_block_nanos";
const STAT_NEXT_READ_FILTER_BLOCK_NANOS: &str = "next_read_filter_block_nanos";
const STAT_NEXT_NEW_TABLE_BLOCK_ITER_NANOS: &str = "next_new_table_block_iter_nanos";
const STAT_NEXT_BLOCK_SEEK_NANOS: &str = "next_block_seek_nanos";

const STAT_PREV_TOMBSTONE: &str = "prev_tombstone";
const STAT_PREV_BLOCK_READ_TIME: &str = "prev_block_read_time";
const STAT_PREV_BLOCK_READ_COUNT: &str = "prev_block_read_count";
const STAT_PREV_BLOCK_CHECKSUM_TIME: &str = "prev_block_checksum_time";
const STAT_PREV_BLOCK_DECOMPRESS_TIME: &str = "prev_block_decompress_time";
const STAT_PREV_GET_SNAPSHOT_TIME: &str = "prev_get_snapshot_time";
const STAT_PREV_GET_FROM_MEMTABLE_TIME: &str = "prev_get_from_memtable_time";
const STAT_PREV_GET_POST_PROCESS_TIME: &str = "prev_get_post_process_time";
const STAT_PREV_GET_FROM_OUTPUT_FILES_TIME: &str = "prev_get_from_output_files_time";
const STAT_PREV_READ_INDEX_BLOCK_NANOS: &str = "prev_read_index_block_nanos";
const STAT_PREV_READ_FILTER_BLOCK_NANOS: &str = "prev_read_filter_block_nanos";
const STAT_PREV_NEW_TABLE_BLOCK_ITER_NANOS: &str = "prev_new_table_block_iter_nanos";
const STAT_PREV_BLOCK_SEEK_NANOS: &str = "prev_block_seek_nanos";

const STAT_SEEK_TOMBSTONE: &str = "seek_tombstone";
const STAT_SEEK_BLOCK_READ_TIME: &str = "seek_block_read_time";
const STAT_SEEK_BLOCK_READ_COUNT: &str = "seek_block_read_count";
const STAT_SEEK_BLOCK_CHECKSUM_TIME: &str = "seek_block_checksum_time";
const STAT_SEEK_BLOCK_DECOMPRESS_TIME: &str = "seek_block_decompress_time";
const STAT_SEEK_GET_SNAPSHOT_TIME: &str = "seek_get_snapshot_time";
const STAT_SEEK_GET_FROM_MEMTABLE_TIME: &str = "seek_get_from_memtable_time";
const STAT_SEEK_GET_POST_PROCESS_TIME: &str = "seek_get_post_process_time";
const STAT_SEEK_GET_FROM_OUTPUT_FILES_TIME: &str = "seek_get_from_output_files_time";
const STAT_SEEK_READ_INDEX_BLOCK_NANOS: &str = "seek_read_index_block_nanos";
const STAT_SEEK_READ_FILTER_BLOCK_NANOS: &str = "seek_read_filter_block_nanos";
const STAT_SEEK_NEW_TABLE_BLOCK_ITER_NANOS: &str = "seek_new_table_block_iter_nanos";
const STAT_SEEK_BLOCK_SEEK_NANOS: &str = "seek_block_seek_nanos";

const STAT_SEEK_FOR_PREV_TOMBSTONE: &str = "seek_for_prev_tombstone";
const STAT_SEEK_FOR_PREV_BLOCK_READ_TIME: &str = "seek_for_prev_block_read_time";
const STAT_SEEK_FOR_PREV_BLOCK_READ_COUNT: &str = "seek_for_prev_block_read_count";
const STAT_SEEK_FOR_PREV_BLOCK_CHECKSUM_TIME: &str = "seek_for_prev_block_checksum_time";
const STAT_SEEK_FOR_PREV_BLOCK_DECOMPRESS_TIME: &str = "seek_for_prev_block_decompress_time";
const STAT_SEEK_FOR_PREV_GET_SNAPSHOT_TIME: &str = "seek_for_prev_get_snapshot_time";
const STAT_SEEK_FOR_PREV_GET_FROM_MEMTABLE_TIME: &str = "seek_for_prev_get_from_memtable_time";
const STAT_SEEK_FOR_PREV_GET_POST_PROCESS_TIME: &str = "seek_for_prev_get_post_process_time";
const STAT_SEEK_FOR_PREV_GET_FROM_OUTPUT_FILES_TIME: &str =
    "seek_for_prev_get_from_output_files_time";
const STAT_SEEK_FOR_PREV_READ_INDEX_BLOCK_NANOS: &str = "seek_for_prev_read_index_block_nanos";
const STAT_SEEK_FOR_PREV_READ_FILTER_BLOCK_NANOS: &str = "seek_for_prev_read_filter_block_nanos";
const STAT_SEEK_FOR_PREV_NEW_TABLE_BLOCK_ITER_NANOS: &str =
    "seek_for_prev_new_table_block_iter_nanos";
const STAT_SEEK_FOR_PREV_BLOCK_SEEK_NANOS: &str = "seek_for_prev_block_seek_nanos";

use crate::storage::kv::{CursorOperation, PerfStatisticsDelta};

/// Statistics collects the ops taken when fetching data.
#[derive(Default, Clone, Debug)]
pub struct CfStatistics {
    // How many keys that's visible to user
    pub processed_keys: usize,

    pub get: usize,
    pub next: usize,
    pub prev: usize,
    pub seek: usize,
    pub seek_for_prev: usize,
    pub over_seek_bound: usize,

    pub flow_stats: FlowStatistics,

    pub next_tombstone: usize,
    pub next_block_read_time: usize,
    pub next_block_read_count: usize,
    pub next_block_checksum_time: usize,
    pub next_block_decompress_time: usize,
    pub next_get_snapshot_time: usize,
    pub next_get_from_memtable_time: usize,
    pub next_get_post_process_time: usize,
    pub next_get_from_output_files_time: usize,
    pub next_read_index_block_nanos: usize,
    pub next_read_filter_block_nanos: usize,
    pub next_new_table_block_iter_nanos: usize,
    pub next_block_seek_nanos: usize,

    pub prev_tombstone: usize,
    pub prev_block_read_time: usize,
    pub prev_block_read_count: usize,
    pub prev_block_checksum_time: usize,
    pub prev_block_decompress_time: usize,
    pub prev_get_snapshot_time: usize,
    pub prev_get_from_memtable_time: usize,
    pub prev_get_post_process_time: usize,
    pub prev_get_from_output_files_time: usize,
    pub prev_read_index_block_nanos: usize,
    pub prev_read_filter_block_nanos: usize,
    pub prev_new_table_block_iter_nanos: usize,
    pub prev_block_seek_nanos: usize,

    pub seek_tombstone: usize,
    pub seek_block_read_time: usize,
    pub seek_block_read_count: usize,
    pub seek_block_checksum_time: usize,
    pub seek_block_decompress_time: usize,
    pub seek_get_snapshot_time: usize,
    pub seek_get_from_memtable_time: usize,
    pub seek_get_post_process_time: usize,
    pub seek_get_from_output_files_time: usize,
    pub seek_read_index_block_nanos: usize,
    pub seek_read_filter_block_nanos: usize,
    pub seek_new_table_block_iter_nanos: usize,
    pub seek_block_seek_nanos: usize,

    pub seek_for_prev_tombstone: usize,
    pub seek_for_prev_block_read_time: usize,
    pub seek_for_prev_block_read_count: usize,
    pub seek_for_prev_block_checksum_time: usize,
    pub seek_for_prev_block_decompress_time: usize,
    pub seek_for_prev_get_snapshot_time: usize,
    pub seek_for_prev_get_from_memtable_time: usize,
    pub seek_for_prev_get_post_process_time: usize,
    pub seek_for_prev_get_from_output_files_time: usize,
    pub seek_for_prev_read_index_block_nanos: usize,
    pub seek_for_prev_read_filter_block_nanos: usize,
    pub seek_for_prev_new_table_block_iter_nanos: usize,
    pub seek_for_prev_block_seek_nanos: usize,
}

impl CfStatistics {
    #[inline]
    pub fn total_op_count(&self) -> usize {
        self.get + self.next + self.prev + self.seek + self.seek_for_prev
    }

    pub fn details(&self) -> [(&'static str, usize); 59] {
        [
            (STAT_PROCESSED_KEYS, self.processed_keys),
            (STAT_GET, self.get),
            (STAT_NEXT, self.next),
            (STAT_PREV, self.prev),
            (STAT_SEEK, self.seek),
            (STAT_SEEK_FOR_PREV, self.seek_for_prev),
            (STAT_OVER_SEEK_BOUND, self.over_seek_bound),
            (STAT_NEXT_TOMBSTONE, self.next_tombstone),
            (STAT_NEXT_BLOCK_READ_TIME, self.next_block_read_time),
            (STAT_NEXT_BLOCK_READ_COUNT, self.next_block_read_count),
            (STAT_NEXT_BLOCK_CHECKSUM_TIME, self.next_block_checksum_time),
            (
                STAT_NEXT_BLOCK_DECOMPRESS_TIME,
                self.next_block_decompress_time,
            ),
            (STAT_NEXT_GET_SNAPSHOT_TIME, self.next_get_snapshot_time),
            (
                STAT_NEXT_GET_FROM_MEMTABLE_TIME,
                self.next_get_from_memtable_time,
            ),
            (
                STAT_NEXT_GET_POST_PROCESS_TIME,
                self.next_get_post_process_time,
            ),
            (
                STAT_NEXT_GET_FROM_OUTPUT_FILES_TIME,
                self.next_get_from_output_files_time,
            ),
            (
                STAT_NEXT_READ_INDEX_BLOCK_NANOS,
                self.next_read_index_block_nanos,
            ),
            (
                STAT_NEXT_READ_FILTER_BLOCK_NANOS,
                self.next_read_filter_block_nanos,
            ),
            (
                STAT_NEXT_NEW_TABLE_BLOCK_ITER_NANOS,
                self.next_new_table_block_iter_nanos,
            ),
            (STAT_NEXT_BLOCK_SEEK_NANOS, self.next_block_seek_nanos),
            (STAT_PREV_TOMBSTONE, self.prev_tombstone),
            (STAT_PREV_BLOCK_READ_TIME, self.prev_block_read_time),
            (STAT_PREV_BLOCK_READ_COUNT, self.prev_block_read_count),
            (STAT_PREV_BLOCK_CHECKSUM_TIME, self.prev_block_checksum_time),
            (
                STAT_PREV_BLOCK_DECOMPRESS_TIME,
                self.prev_block_decompress_time,
            ),
            (STAT_PREV_GET_SNAPSHOT_TIME, self.prev_get_snapshot_time),
            (
                STAT_PREV_GET_FROM_MEMTABLE_TIME,
                self.prev_get_from_memtable_time,
            ),
            (
                STAT_PREV_GET_POST_PROCESS_TIME,
                self.prev_get_post_process_time,
            ),
            (
                STAT_PREV_GET_FROM_OUTPUT_FILES_TIME,
                self.prev_get_from_output_files_time,
            ),
            (
                STAT_PREV_READ_INDEX_BLOCK_NANOS,
                self.prev_read_index_block_nanos,
            ),
            (
                STAT_PREV_READ_FILTER_BLOCK_NANOS,
                self.prev_read_filter_block_nanos,
            ),
            (
                STAT_PREV_NEW_TABLE_BLOCK_ITER_NANOS,
                self.prev_new_table_block_iter_nanos,
            ),
            (STAT_PREV_BLOCK_SEEK_NANOS, self.prev_block_seek_nanos),
            (STAT_SEEK_TOMBSTONE, self.seek_tombstone),
            (STAT_SEEK_BLOCK_READ_TIME, self.seek_block_read_time),
            (STAT_SEEK_BLOCK_READ_COUNT, self.seek_block_read_count),
            (STAT_SEEK_BLOCK_CHECKSUM_TIME, self.seek_block_checksum_time),
            (
                STAT_SEEK_BLOCK_DECOMPRESS_TIME,
                self.seek_block_decompress_time,
            ),
            (STAT_SEEK_GET_SNAPSHOT_TIME, self.seek_get_snapshot_time),
            (
                STAT_SEEK_GET_FROM_MEMTABLE_TIME,
                self.seek_get_from_memtable_time,
            ),
            (
                STAT_SEEK_GET_POST_PROCESS_TIME,
                self.seek_get_post_process_time,
            ),
            (
                STAT_SEEK_GET_FROM_OUTPUT_FILES_TIME,
                self.seek_get_from_output_files_time,
            ),
            (
                STAT_SEEK_READ_INDEX_BLOCK_NANOS,
                self.seek_read_index_block_nanos,
            ),
            (
                STAT_SEEK_READ_FILTER_BLOCK_NANOS,
                self.seek_read_filter_block_nanos,
            ),
            (
                STAT_SEEK_NEW_TABLE_BLOCK_ITER_NANOS,
                self.seek_new_table_block_iter_nanos,
            ),
            (STAT_SEEK_BLOCK_SEEK_NANOS, self.seek_block_seek_nanos),
            (STAT_SEEK_FOR_PREV_TOMBSTONE, self.seek_for_prev_tombstone),
            (
                STAT_SEEK_FOR_PREV_BLOCK_READ_TIME,
                self.seek_for_prev_block_read_time,
            ),
            (
                STAT_SEEK_FOR_PREV_BLOCK_READ_COUNT,
                self.seek_for_prev_block_read_count,
            ),
            (
                STAT_SEEK_FOR_PREV_BLOCK_CHECKSUM_TIME,
                self.seek_for_prev_block_checksum_time,
            ),
            (
                STAT_SEEK_FOR_PREV_BLOCK_DECOMPRESS_TIME,
                self.seek_for_prev_block_decompress_time,
            ),
            (
                STAT_SEEK_FOR_PREV_GET_SNAPSHOT_TIME,
                self.seek_for_prev_get_snapshot_time,
            ),
            (
                STAT_SEEK_FOR_PREV_GET_FROM_MEMTABLE_TIME,
                self.seek_for_prev_get_from_memtable_time,
            ),
            (
                STAT_SEEK_FOR_PREV_GET_POST_PROCESS_TIME,
                self.seek_for_prev_get_post_process_time,
            ),
            (
                STAT_SEEK_FOR_PREV_GET_FROM_OUTPUT_FILES_TIME,
                self.seek_for_prev_get_from_output_files_time,
            ),
            (
                STAT_SEEK_FOR_PREV_READ_INDEX_BLOCK_NANOS,
                self.seek_for_prev_read_index_block_nanos,
            ),
            (
                STAT_SEEK_FOR_PREV_READ_FILTER_BLOCK_NANOS,
                self.seek_for_prev_read_filter_block_nanos,
            ),
            (
                STAT_SEEK_FOR_PREV_NEW_TABLE_BLOCK_ITER_NANOS,
                self.seek_for_prev_new_table_block_iter_nanos,
            ),
            (
                STAT_SEEK_FOR_PREV_BLOCK_SEEK_NANOS,
                self.seek_for_prev_block_seek_nanos,
            ),
        ]
    }

    pub fn details_enum(&self) -> [(GcKeysDetail, usize); 11] {
        [
            (GcKeysDetail::processed_keys, self.processed_keys),
            (GcKeysDetail::get, self.get),
            (GcKeysDetail::next, self.next),
            (GcKeysDetail::prev, self.prev),
            (GcKeysDetail::seek, self.seek),
            (GcKeysDetail::seek_for_prev, self.seek_for_prev),
            (GcKeysDetail::over_seek_bound, self.over_seek_bound),
            (GcKeysDetail::next_tombstone, self.next_tombstone),
            (GcKeysDetail::prev_tombstone, self.prev_tombstone),
            (GcKeysDetail::seek_tombstone, self.seek_tombstone),
            (
                GcKeysDetail::seek_for_prev_tombstone,
                self.seek_for_prev_tombstone,
            ),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.processed_keys = self.processed_keys.saturating_add(other.processed_keys);
        self.get = self.get.saturating_add(other.get);
        self.next = self.next.saturating_add(other.next);
        self.prev = self.prev.saturating_add(other.prev);
        self.seek = self.seek.saturating_add(other.seek);
        self.seek_for_prev = self.seek_for_prev.saturating_add(other.seek_for_prev);
        self.over_seek_bound = self.over_seek_bound.saturating_add(other.over_seek_bound);
        self.flow_stats.add(&other.flow_stats);

        self.next_tombstone = self.next_tombstone.saturating_add(other.next_tombstone);
        self.next_block_read_time = self
            .next_block_read_time
            .saturating_add(other.next_block_read_time);
        self.next_block_read_count = self
            .next_block_read_count
            .saturating_add(other.next_block_read_count);
        self.next_block_checksum_time = self
            .next_block_checksum_time
            .saturating_add(other.next_block_checksum_time);
        self.next_block_decompress_time = self
            .next_block_decompress_time
            .saturating_add(other.next_block_decompress_time);
        self.next_get_snapshot_time = self
            .next_get_snapshot_time
            .saturating_add(other.next_get_snapshot_time);
        self.next_get_from_memtable_time = self
            .next_get_from_memtable_time
            .saturating_add(other.next_get_from_memtable_time);
        self.next_get_post_process_time = self
            .next_get_post_process_time
            .saturating_add(other.next_get_post_process_time);
        self.next_get_from_output_files_time = self
            .next_get_from_output_files_time
            .saturating_add(other.next_get_from_output_files_time);
        self.next_read_index_block_nanos = self
            .next_read_index_block_nanos
            .saturating_add(other.next_read_index_block_nanos);
        self.next_read_filter_block_nanos = self
            .next_read_filter_block_nanos
            .saturating_add(other.next_read_filter_block_nanos);
        self.next_new_table_block_iter_nanos = self
            .next_new_table_block_iter_nanos
            .saturating_add(other.next_new_table_block_iter_nanos);
        self.next_block_seek_nanos = self
            .next_block_seek_nanos
            .saturating_add(other.next_block_seek_nanos);

        self.prev_tombstone = self.prev_tombstone.saturating_add(other.prev_tombstone);
        self.prev_block_read_time = self
            .prev_block_read_time
            .saturating_add(other.prev_block_read_time);
        self.prev_block_read_count = self
            .prev_block_read_count
            .saturating_add(other.prev_block_read_count);
        self.prev_block_checksum_time = self
            .prev_block_checksum_time
            .saturating_add(other.prev_block_checksum_time);
        self.prev_block_decompress_time = self
            .prev_block_decompress_time
            .saturating_add(other.prev_block_decompress_time);
        self.prev_get_snapshot_time = self
            .prev_get_snapshot_time
            .saturating_add(other.prev_get_snapshot_time);
        self.prev_get_from_memtable_time = self
            .prev_get_from_memtable_time
            .saturating_add(other.prev_get_from_memtable_time);
        self.prev_get_post_process_time = self
            .prev_get_post_process_time
            .saturating_add(other.prev_get_post_process_time);
        self.prev_get_from_output_files_time = self
            .prev_get_from_output_files_time
            .saturating_add(other.prev_get_from_output_files_time);
        self.prev_read_index_block_nanos = self
            .prev_read_index_block_nanos
            .saturating_add(other.prev_read_index_block_nanos);
        self.prev_read_filter_block_nanos = self
            .prev_read_filter_block_nanos
            .saturating_add(other.prev_read_filter_block_nanos);
        self.prev_new_table_block_iter_nanos = self
            .prev_new_table_block_iter_nanos
            .saturating_add(other.prev_new_table_block_iter_nanos);
        self.prev_block_seek_nanos = self
            .prev_block_seek_nanos
            .saturating_add(other.prev_block_seek_nanos);

        self.seek_tombstone = self.seek_tombstone.saturating_add(other.seek_tombstone);
        self.seek_block_read_time = self
            .seek_block_read_time
            .saturating_add(other.seek_block_read_time);
        self.seek_block_read_count = self
            .seek_block_read_count
            .saturating_add(other.seek_block_read_count);
        self.seek_block_checksum_time = self
            .seek_block_checksum_time
            .saturating_add(other.seek_block_checksum_time);
        self.seek_block_decompress_time = self
            .seek_block_decompress_time
            .saturating_add(other.seek_block_decompress_time);
        self.seek_get_snapshot_time = self
            .seek_get_snapshot_time
            .saturating_add(other.seek_get_snapshot_time);
        self.seek_get_from_memtable_time = self
            .seek_get_from_memtable_time
            .saturating_add(other.seek_get_from_memtable_time);
        self.seek_get_post_process_time = self
            .seek_get_post_process_time
            .saturating_add(other.seek_get_post_process_time);
        self.seek_get_from_output_files_time = self
            .seek_get_from_output_files_time
            .saturating_add(other.seek_get_from_output_files_time);
        self.seek_read_index_block_nanos = self
            .seek_read_index_block_nanos
            .saturating_add(other.seek_read_index_block_nanos);
        self.seek_read_filter_block_nanos = self
            .seek_read_filter_block_nanos
            .saturating_add(other.seek_read_filter_block_nanos);
        self.seek_new_table_block_iter_nanos = self
            .seek_new_table_block_iter_nanos
            .saturating_add(other.seek_new_table_block_iter_nanos);
        self.seek_block_seek_nanos = self
            .seek_block_seek_nanos
            .saturating_add(other.seek_block_seek_nanos);

        self.seek_for_prev_tombstone = self
            .seek_for_prev_tombstone
            .saturating_add(other.seek_for_prev_tombstone);
        self.seek_for_prev_block_read_time = self
            .seek_for_prev_block_read_time
            .saturating_add(other.seek_for_prev_block_read_time);
        self.seek_for_prev_block_read_count = self
            .seek_for_prev_block_read_count
            .saturating_add(other.seek_for_prev_block_read_count);
        self.seek_for_prev_block_checksum_time = self
            .seek_for_prev_block_checksum_time
            .saturating_add(other.seek_for_prev_block_checksum_time);
        self.seek_for_prev_block_decompress_time = self
            .seek_for_prev_block_decompress_time
            .saturating_add(other.seek_for_prev_block_decompress_time);
        self.seek_for_prev_get_snapshot_time = self
            .seek_for_prev_get_snapshot_time
            .saturating_add(other.seek_for_prev_get_snapshot_time);
        self.seek_for_prev_get_from_memtable_time = self
            .seek_for_prev_get_from_memtable_time
            .saturating_add(other.seek_for_prev_get_from_memtable_time);
        self.seek_for_prev_get_post_process_time = self
            .seek_for_prev_get_post_process_time
            .saturating_add(other.seek_for_prev_get_post_process_time);
        self.seek_for_prev_get_from_output_files_time = self
            .seek_for_prev_get_from_output_files_time
            .saturating_add(other.seek_for_prev_get_from_output_files_time);
        self.seek_for_prev_read_index_block_nanos = self
            .seek_for_prev_read_index_block_nanos
            .saturating_add(other.seek_for_prev_read_index_block_nanos);
        self.seek_for_prev_read_filter_block_nanos = self
            .seek_for_prev_read_filter_block_nanos
            .saturating_add(other.seek_for_prev_read_filter_block_nanos);
        self.seek_for_prev_new_table_block_iter_nanos = self
            .seek_for_prev_new_table_block_iter_nanos
            .saturating_add(other.seek_for_prev_new_table_block_iter_nanos);
        self.seek_for_prev_block_seek_nanos = self
            .seek_for_prev_block_seek_nanos
            .saturating_add(other.seek_for_prev_block_seek_nanos);
    }

    pub fn apply_perf_context_delta(&mut self, op: CursorOperation, delta: &PerfStatisticsDelta) {
        match op {
            CursorOperation::Next => {
                self.next_tombstone += delta.0.internal_delete_skipped_count;
                self.next_block_read_time += delta.0.block_read_time;
                self.next_block_read_count += delta.0.block_read_count;
                self.next_block_checksum_time += delta.0.block_checksum_time;
                self.next_block_decompress_time += delta.0.block_decompress_time;
                self.next_get_snapshot_time += delta.0.get_snapshot_time;
                self.next_get_from_memtable_time += delta.0.get_from_memtable_time;
                self.next_get_post_process_time += delta.0.get_post_process_time;
                self.next_get_from_output_files_time += delta.0.get_from_output_files_time;
                self.next_read_index_block_nanos += delta.0.read_index_block_nanos;
                self.next_read_filter_block_nanos += delta.0.read_filter_block_nanos;
                self.next_new_table_block_iter_nanos += delta.0.new_table_block_iter_nanos;
                self.next_block_seek_nanos += delta.0.block_seek_nanos;
            }
            CursorOperation::Prev => {
                self.prev_tombstone += delta.0.internal_delete_skipped_count;
                self.prev_block_read_time += delta.0.block_read_time;
                self.prev_block_read_count += delta.0.block_read_count;
                self.prev_block_checksum_time += delta.0.block_checksum_time;
                self.prev_block_decompress_time += delta.0.block_decompress_time;
                self.prev_get_snapshot_time += delta.0.get_snapshot_time;
                self.prev_get_from_memtable_time += delta.0.get_from_memtable_time;
                self.prev_get_post_process_time += delta.0.get_post_process_time;
                self.prev_get_from_output_files_time += delta.0.get_from_output_files_time;
                self.prev_read_index_block_nanos += delta.0.read_index_block_nanos;
                self.prev_read_filter_block_nanos += delta.0.read_filter_block_nanos;
                self.prev_new_table_block_iter_nanos += delta.0.new_table_block_iter_nanos;
                self.prev_block_seek_nanos += delta.0.block_seek_nanos;
            }
            CursorOperation::Seek => {
                self.seek_tombstone += delta.0.internal_delete_skipped_count;
                self.seek_block_read_time += delta.0.block_read_time;
                self.seek_block_read_count += delta.0.block_read_count;
                self.seek_block_checksum_time += delta.0.block_checksum_time;
                self.seek_block_decompress_time += delta.0.block_decompress_time;
                self.seek_get_snapshot_time += delta.0.get_snapshot_time;
                self.seek_get_from_memtable_time += delta.0.get_from_memtable_time;
                self.seek_get_post_process_time += delta.0.get_post_process_time;
                self.seek_get_from_output_files_time += delta.0.get_from_output_files_time;
                self.seek_read_index_block_nanos += delta.0.read_index_block_nanos;
                self.seek_read_filter_block_nanos += delta.0.read_filter_block_nanos;
                self.seek_new_table_block_iter_nanos += delta.0.new_table_block_iter_nanos;
                self.seek_block_seek_nanos += delta.0.block_seek_nanos;
            }
            CursorOperation::SeekForPrev => {
                self.seek_for_prev_tombstone += delta.0.internal_delete_skipped_count;
                self.seek_for_prev_block_read_time += delta.0.block_read_time;
                self.seek_for_prev_block_read_count += delta.0.block_read_count;
                self.seek_for_prev_block_checksum_time += delta.0.block_checksum_time;
                self.seek_for_prev_block_decompress_time += delta.0.block_decompress_time;
                self.seek_for_prev_get_snapshot_time += delta.0.get_snapshot_time;
                self.seek_for_prev_get_from_memtable_time += delta.0.get_from_memtable_time;
                self.seek_for_prev_get_post_process_time += delta.0.get_post_process_time;
                self.seek_for_prev_get_from_output_files_time += delta.0.get_from_output_files_time;
                self.seek_for_prev_read_index_block_nanos += delta.0.read_index_block_nanos;
                self.seek_for_prev_read_filter_block_nanos += delta.0.read_filter_block_nanos;
                self.seek_for_prev_new_table_block_iter_nanos += delta.0.new_table_block_iter_nanos;
                self.seek_for_prev_block_seek_nanos += delta.0.block_seek_nanos;
            }
        }
    }

    /// Deprecated
    pub fn scan_info(&self) -> ScanInfo {
        let mut info = ScanInfo::default();
        info.set_processed(self.processed_keys as i64);
        info.set_total(self.total_op_count() as i64);
        info
    }
}

#[derive(Default, Clone, Debug)]
pub struct Statistics {
    pub lock: CfStatistics,
    pub write: CfStatistics,
    pub data: CfStatistics,
}

impl Statistics {
    pub fn details(&self) -> [(&'static str, [(&'static str, usize); 59]); 3] {
        [
            (CF_DEFAULT, self.data.details()),
            (CF_LOCK, self.lock.details()),
            (CF_WRITE, self.write.details()),
        ]
    }

    pub fn details_enum(&self) -> [(GcKeysCF, [(GcKeysDetail, usize); 11]); 3] {
        [
            (GcKeysCF::default, self.data.details_enum()),
            (GcKeysCF::lock, self.lock.details_enum()),
            (GcKeysCF::write, self.write.details_enum()),
        ]
    }

    pub fn add(&mut self, other: &Self) {
        self.lock.add(&other.lock);
        self.write.add(&other.write);
        self.data.add(&other.data);
    }

    /// Deprecated
    pub fn scan_detail(&self) -> ScanDetail {
        let mut detail = ScanDetail::default();
        detail.set_data(self.data.scan_info());
        detail.set_lock(self.lock.scan_info());
        detail.set_write(self.write.scan_info());
        detail
    }

    pub fn mut_cf_statistics(&mut self, cf: &str) -> &mut CfStatistics {
        if cf.is_empty() {
            return &mut self.data;
        }
        match cf {
            CF_DEFAULT => &mut self.data,
            CF_LOCK => &mut self.lock,
            CF_WRITE => &mut self.write,
            _ => unreachable!(),
        }
    }
}

#[derive(Default, Debug)]
pub struct StatisticsSummary {
    pub stat: Statistics,
    pub count: u64,
}

impl StatisticsSummary {
    pub fn add_statistics(&mut self, v: &Statistics) {
        self.stat.add(v);
        self.count += 1;
    }
}
