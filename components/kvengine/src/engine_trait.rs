// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::BTreeMap, ops::Deref};

use engine_traits::{
    CFNamesExt, CFOptionsExt, CompactExt, CompactedEvent, DBOptions, DBOptionsExt, DBVector,
    DeleteStrategy, FlowControlFactorsExt, ImportExt, IngestExternalFileOptions, IterOptions,
    Iterable, KvEngine, MiscExt, Mutable, MvccProperties, MvccPropertiesExt, Peekable, PerfContext,
    PerfContextExt, PerfContextKind, PerfLevel, Range, RangePropertiesExt, ReadOptions, SeekKey,
    Snapshot, SstCompressionType, SstExt, SstWriterBuilder, SyncMutable, TablePropertiesExt,
    TitanDBOptions, TtlProperties, TtlPropertiesExt, WriteBatchExt, WriteOptions,
};

use crate::*;

type TraitsResult<T> = std::result::Result<T, engine_traits::Error>;

impl CFNamesExt for Engine {
    fn cf_names(&self) -> Vec<&str> {
        vec!["write", "lock", "extra"]
    }
}

impl CFOptionsExt for Engine {
    type ColumnFamilyOptions = engine_rocks::RocksColumnFamilyOptions;

    fn get_options_cf(&self, _cf: &str) -> TraitsResult<Self::ColumnFamilyOptions> {
        Ok(engine_rocks::RocksColumnFamilyOptions::from_raw(
            rocksdb::ColumnFamilyOptions::default(),
        ))
    }
    fn set_options_cf(&self, _cf: &str, _options: &[(&str, &str)]) -> TraitsResult<()> {
        Ok(())
    }
}

impl CompactExt for Engine {
    type CompactedEvent = EngineCompactedEvent;

    fn auto_compactions_is_disabled(&self) -> TraitsResult<bool> {
        panic!()
    }

    fn compact_range(
        &self,
        _cf: &str,
        _start_key: Option<&[u8]>,
        _end_key: Option<&[u8]>,
        _exclusive_manual: bool,
        _max_subcompactions: u32,
    ) -> TraitsResult<()> {
        panic!()
    }

    fn compact_files_in_range(
        &self,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
        _output_level: Option<i32>,
    ) -> TraitsResult<()> {
        panic!()
    }

    fn compact_files_in_range_cf(
        &self,
        _cf: &str,
        _start: Option<&[u8]>,
        _end: Option<&[u8]>,
        _output_level: Option<i32>,
    ) -> TraitsResult<()> {
        panic!()
    }

    fn compact_files_cf(
        &self,
        _cf: &str,
        _files: Vec<String>,
        _output_level: Option<i32>,
        _max_subcompactions: u32,
        _exclude_l0: bool,
    ) -> TraitsResult<()> {
        panic!()
    }
}

pub struct EngineCompactedEvent;

impl CompactedEvent for EngineCompactedEvent {
    fn total_bytes_declined(&self) -> u64 {
        panic!()
    }

    fn is_size_declining_trivial(&self, _split_check_diff: u64) -> bool {
        panic!()
    }

    fn output_level_label(&self) -> String {
        panic!()
    }

    fn calc_ranges_declined_bytes(
        self,
        _ranges: &BTreeMap<Vec<u8>, u64>,
        _bytes_threshold: u64,
    ) -> Vec<(u64, u64)> {
        panic!()
    }

    fn cf(&self) -> &str {
        panic!()
    }
}

impl DBOptionsExt for Engine {
    type DBOptions = EngineDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        EngineDBOptions::new()
    }
    fn set_db_options(&self, _options: &[(&str, &str)]) -> TraitsResult<()> {
        Ok(())
    }
}

pub struct EngineDBOptions;

impl DBOptions for EngineDBOptions {
    type TitanDBOptions = EngineTitanDBOptions;

    fn new() -> Self {
        EngineDBOptions {}
    }

    fn get_max_background_jobs(&self) -> i32 {
        2
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        None
    }

    fn set_rate_bytes_per_sec(&mut self, _rate_bytes_per_sec: i64) -> TraitsResult<()> {
        Ok(())
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        None
    }

    fn set_rate_limiter_auto_tuned(&mut self, _rate_limiter_auto_tuned: bool) -> TraitsResult<()> {
        Ok(())
    }

    fn set_titandb_options(&mut self, _opts: &Self::TitanDBOptions) {
        panic!()
    }
}

pub struct EngineTitanDBOptions;

impl TitanDBOptions for EngineTitanDBOptions {
    fn new() -> Self {
        panic!()
    }
    fn set_min_blob_size(&mut self, _size: u64) {
        panic!()
    }
}

#[derive(Debug)]
pub struct EngineDBVector;

impl DBVector for EngineDBVector {}

impl Deref for EngineDBVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        panic!()
    }
}

impl<'a> PartialEq<&'a [u8]> for EngineDBVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl KvEngine for Engine {
    type Snapshot = EngineSnapshot;

    fn snapshot(&self) -> Self::Snapshot {
        panic!()
    }
    fn sync(&self) -> TraitsResult<()> {
        panic!()
    }
    fn bad_downcast<T: 'static>(&self) -> &T {
        panic!()
    }
    fn flush_metrics(&self, instance: &str) {
        metrics::flush_engine_properties(&self, instance);
    }
}

impl Peekable for Engine {
    type DBVector = EngineDBVector;

    fn get_value_opt(
        &self,
        _opts: &ReadOptions,
        _key: &[u8],
    ) -> TraitsResult<Option<Self::DBVector>> {
        panic!()
    }

    fn get_value_cf_opt(
        &self,
        _opts: &ReadOptions,
        _cf: &str,
        _key: &[u8],
    ) -> TraitsResult<Option<Self::DBVector>> {
        panic!()
    }
}

impl SyncMutable for Engine {
    fn put(&self, _key: &[u8], _value: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn put_cf(&self, _cf: &str, _key: &[u8], _value: &[u8]) -> TraitsResult<()> {
        panic!()
    }

    fn delete(&self, _key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_cf(&self, _cf: &str, _key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_range(&self, _begin_key: &[u8], _end_key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_range_cf(&self, _cf: &str, _begin_key: &[u8], _end_key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
}

impl Iterable for Engine {
    type Iterator = EngineIterator;

    fn iterator_opt(&self, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, _cf: &str, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
}

pub struct EngineIterator;

impl engine_traits::Iterator for EngineIterator {
    fn seek(&mut self, _key: SeekKey<'_>) -> TraitsResult<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, _key: SeekKey<'_>) -> TraitsResult<bool> {
        panic!()
    }

    fn prev(&mut self) -> TraitsResult<bool> {
        panic!()
    }
    fn next(&mut self) -> TraitsResult<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> TraitsResult<bool> {
        panic!()
    }
}

impl ImportExt for Engine {
    type IngestExternalFileOptions = EngineIngestExternalFileOptions;

    fn ingest_external_file_cf(&self, _cf: &str, _files: &[&str]) -> TraitsResult<()> {
        panic!()
    }
}

pub struct EngineIngestExternalFileOptions;

impl IngestExternalFileOptions for EngineIngestExternalFileOptions {
    fn new() -> Self {
        panic!()
    }
    fn move_files(&mut self, _f: bool) {
        panic!()
    }

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, _f: bool) {
        panic!()
    }
}

impl FlowControlFactorsExt for Engine {
    fn get_cf_num_files_at_level(
        &self,
        _cf: &str,
        _level: usize,
    ) -> engine_traits::Result<Option<u64>> {
        // TODO(x)
        Ok(Some(1))
    }

    fn get_cf_num_immutable_mem_table(&self, _cf: &str) -> engine_traits::Result<Option<u64>> {
        // TODO(x)
        Ok(Some(1))
    }

    fn get_cf_pending_compaction_bytes(&self, _cf: &str) -> engine_traits::Result<Option<u64>> {
        // TODO(x)
        Ok(Some(0))
    }
}

impl MiscExt for Engine {
    fn flush(&self, _sync: bool) -> TraitsResult<()> {
        panic!()
    }

    fn flush_cf(&self, _cf: &str, _sync: bool) -> TraitsResult<()> {
        panic!()
    }

    fn delete_ranges_cf(
        &self,
        _cf: &str,
        _strategy: DeleteStrategy,
        _ranges: &[Range<'_>],
    ) -> TraitsResult<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(
        &self,
        _cf: &str,
        _range: &Range<'_>,
    ) -> TraitsResult<(u64, u64)> {
        panic!()
    }

    fn ingest_maybe_slowdown_writes(&self, _cf: &str) -> TraitsResult<bool> {
        panic!()
    }

    fn get_engine_used_size(&self) -> TraitsResult<u64> {
        Ok(self.size())
    }

    fn roughly_cleanup_ranges(&self, _ranges: &[(Vec<u8>, Vec<u8>)]) -> TraitsResult<()> {
        panic!()
    }

    fn path(&self) -> &str {
        self.opts.local_dir.to_str().unwrap()
    }

    fn sync_wal(&self) -> TraitsResult<()> {
        panic!()
    }

    fn exists(_path: &str) -> bool {
        panic!()
    }

    fn dump_stats(&self) -> TraitsResult<String> {
        panic!()
    }

    fn get_latest_sequence_number(&self) -> u64 {
        panic!()
    }

    fn get_oldest_snapshot_sequence_number(&self) -> Option<u64> {
        panic!()
    }

    fn get_total_sst_files_size_cf(&self, _cf: &str) -> TraitsResult<Option<u64>> {
        panic!()
    }

    fn get_range_entries_and_versions(
        &self,
        _cf: &str,
        _start: &[u8],
        _end: &[u8],
    ) -> TraitsResult<Option<(u64, u64)>> {
        panic!()
    }

    fn is_stalled_or_stopped(&self) -> bool {
        panic!()
    }
}

impl MvccPropertiesExt for Engine {
    fn get_mvcc_properties_cf(
        &self,
        _cf: &str,
        _safe_point: txn_types::TimeStamp,
        _start_key: &[u8],
        _end_key: &[u8],
    ) -> Option<MvccProperties> {
        panic!()
    }
}

impl PerfContextExt for Engine {
    type PerfContext = EnginePerfContext;

    fn get_perf_context(&self, _level: PerfLevel, _kind: PerfContextKind) -> Self::PerfContext {
        panic!()
    }
}

pub struct EnginePerfContext;

impl PerfContext for EnginePerfContext {
    fn start_observe(&mut self) {
        panic!()
    }

    fn report_metrics(&mut self) {
        panic!()
    }
}

impl RangePropertiesExt for Engine {
    fn get_range_approximate_keys(
        &self,
        _range: Range<'_>,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_keys_cf(
        &self,
        _cfname: &str,
        _range: Range<'_>,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_size(
        &self,
        _range: Range<'_>,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_size_cf(
        &self,
        _cfname: &str,
        _range: Range<'_>,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_split_keys(
        &self,
        _range: Range<'_>,
        _key_count: usize,
    ) -> TraitsResult<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        _cfname: &str,
        _range: Range<'_>,
        _key_count: usize,
    ) -> TraitsResult<Vec<Vec<u8>>> {
        panic!()
    }
}

#[derive(Clone, Debug)]
pub struct EngineSnapshot {}

impl Snapshot for EngineSnapshot {
    fn cf_names(&self) -> Vec<&str> {
        panic!()
    }
}

impl Peekable for EngineSnapshot {
    type DBVector = EngineDBVector;

    fn get_value_opt(
        &self,
        _opts: &ReadOptions,
        _key: &[u8],
    ) -> TraitsResult<Option<Self::DBVector>> {
        panic!()
    }
    fn get_value_cf_opt(
        &self,
        _opts: &ReadOptions,
        _cf: &str,
        _key: &[u8],
    ) -> TraitsResult<Option<Self::DBVector>> {
        panic!()
    }
}

impl Iterable for EngineSnapshot {
    type Iterator = PanicSnapshotIterator;

    fn iterator_opt(&self, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, _cf: &str, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
}

pub struct PanicSnapshotIterator;

impl engine_traits::Iterator for PanicSnapshotIterator {
    fn seek(&mut self, _key: SeekKey<'_>) -> TraitsResult<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, _key: SeekKey<'_>) -> TraitsResult<bool> {
        panic!()
    }

    fn prev(&mut self) -> TraitsResult<bool> {
        panic!()
    }
    fn next(&mut self) -> TraitsResult<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> TraitsResult<bool> {
        panic!()
    }
}

impl SstExt for Engine {
    type SstReader = engine_rocks::RocksSstReader;
    type SstWriter = engine_rocks::RocksSstWriter;
    type SstWriterBuilder = EngineSstWriterBuilder;
}

pub struct EngineSstWriterBuilder {
    builder: engine_rocks::RocksSstWriterBuilder,
}

impl SstWriterBuilder<Engine> for EngineSstWriterBuilder {
    fn new() -> Self {
        Self {
            builder: engine_rocks::RocksSstWriterBuilder::new(),
        }
    }
    fn set_db(self, _db: &Engine) -> Self {
        // TODO(x): need to find a way to pass RocksDB Env and CFOptions to the builder.
        self
    }
    fn set_cf(mut self, cf: &str) -> Self {
        self.builder = self.builder.set_cf(cf);
        self
    }
    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.builder = self.builder.set_in_memory(in_memory);
        self
    }
    fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.builder = self.builder.set_compression_type(compression);
        self
    }
    fn set_compression_level(mut self, level: i32) -> Self {
        self.builder = self.builder.set_compression_level(level);
        self
    }

    fn build(self, path: &str) -> TraitsResult<engine_rocks::RocksSstWriter> {
        self.builder.build(path)
    }
}

pub struct UserCollectedProperties;

impl engine_traits::UserCollectedProperties for UserCollectedProperties {
    fn get(&self, _: &[u8]) -> Option<&[u8]> {
        None
    }
    fn approximate_size_and_keys(&self, _: &[u8], _: &[u8]) -> Option<(usize, usize)> {
        None
    }
}
pub struct TablePropertiesCollection;
impl engine_traits::TablePropertiesCollection for TablePropertiesCollection {
    type UserCollectedProperties = UserCollectedProperties;
    fn iter_user_collected_properties<F>(&self, _: F)
    where
        F: FnMut(&Self::UserCollectedProperties) -> bool,
    {
    }
}

impl TablePropertiesExt for Engine {
    type TablePropertiesCollection = TablePropertiesCollection;

    fn table_properties_collection(
        &self,
        _cf: &str,
        _ranges: &[Range<'_>],
    ) -> TraitsResult<Self::TablePropertiesCollection> {
        panic!()
    }
}

impl TtlPropertiesExt for Engine {
    fn get_range_ttl_properties_cf(
        &self,
        _cf: &str,
        _start_key: &[u8],
        _end_key: &[u8],
    ) -> TraitsResult<Vec<(String, TtlProperties)>> {
        panic!()
    }
}

impl WriteBatchExt for Engine {
    type WriteBatch = EngineWriteBatch;

    const WRITE_BATCH_MAX_KEYS: usize = 1;

    fn write_batch(&self) -> Self::WriteBatch {
        panic!()
    }
    fn write_batch_with_cap(&self, _cap: usize) -> Self::WriteBatch {
        panic!()
    }
}

pub struct EngineWriteBatch;

impl engine_traits::WriteBatch for EngineWriteBatch {
    fn write_opt(&self, _: &WriteOptions) -> TraitsResult<()> {
        panic!()
    }

    fn data_size(&self) -> usize {
        panic!()
    }
    fn count(&self) -> usize {
        panic!()
    }
    fn is_empty(&self) -> bool {
        panic!()
    }
    fn should_write_to_engine(&self) -> bool {
        panic!()
    }

    fn clear(&mut self) {
        panic!()
    }
    fn set_save_point(&mut self) {
        panic!()
    }
    fn pop_save_point(&mut self) -> TraitsResult<()> {
        panic!()
    }
    fn rollback_to_save_point(&mut self) -> TraitsResult<()> {
        panic!()
    }
    fn merge(&mut self, _src: Self) -> TraitsResult<()> {
        panic!()
    }
}

impl Mutable for EngineWriteBatch {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn put_cf(&mut self, _cf: &str, _key: &[u8], _value: &[u8]) -> TraitsResult<()> {
        panic!()
    }

    fn delete(&mut self, _key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_cf(&mut self, _cf: &str, _key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_range(&mut self, _begin_key: &[u8], _end_key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete_range_cf(
        &mut self,
        _cf: &str,
        _begin_key: &[u8],
        _end_key: &[u8],
    ) -> TraitsResult<()> {
        panic!()
    }
}
