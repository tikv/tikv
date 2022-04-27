// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::*;
use engine_traits::{
    CFNamesExt, CFOptionsExt, ColumnFamilyOptions, CompactExt, CompactedEvent, DBOptions,
    DBOptionsExt, DBVector, DeleteStrategy, ExternalSstFileInfo, FlowControlFactorsExt, ImportExt,
    IngestExternalFileOptions, IterOptions, Iterable, KvEngine, MiscExt, Mutable, MvccProperties,
    MvccPropertiesExt, Peekable, PerfContext, PerfContextExt, PerfContextKind, PerfLevel, Range,
    RangePropertiesExt, ReadOptions, SeekKey, Snapshot, SstCompressionType, SstExt,
    SstPartitionerFactory, SstReader, SstWriter, SstWriterBuilder, SyncMutable, TablePropertiesExt,
    TitanDBOptions, TtlProperties, TtlPropertiesExt, WriteBatchExt, WriteOptions,
};
use std::collections::BTreeMap;
use std::ops::Deref;
use std::path::PathBuf;

type TraitsResult<T> = std::result::Result<T, engine_traits::Error>;

impl CFNamesExt for Engine {
    fn cf_names(&self) -> Vec<&str> {
        vec!["write", "lock", "extra"]
    }
}

impl CFOptionsExt for Engine {
    type ColumnFamilyOptions = EngineColumnFamilyOptions;

    fn get_options_cf(&self, _cf: &str) -> TraitsResult<Self::ColumnFamilyOptions> {
        panic!()
    }
    fn set_options_cf(&self, _cf: &str, _options: &[(&str, &str)]) -> TraitsResult<()> {
        panic!()
    }
}

pub struct EngineColumnFamilyOptions;

impl ColumnFamilyOptions for EngineColumnFamilyOptions {
    type TitanDBOptions = EngineTitanDBOptions;

    fn new() -> Self {
        panic!()
    }
    fn get_max_write_buffer_number(&self) -> u32 {
        panic!()
    }
    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn set_level_zero_file_num_compaction_trigger(&mut self, _v: i32) {
        panic!()
    }
    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, _capacity: u64) -> std::result::Result<(), String> {
        panic!()
    }
    fn set_titandb_options(&mut self, _opts: &Self::TitanDBOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn set_disable_auto_compactions(&mut self, _v: bool) {
        panic!()
    }
    fn get_disable_auto_compactions(&self) -> bool {
        panic!()
    }
    fn get_disable_write_stall(&self) -> bool {
        panic!()
    }
    fn set_sst_partitioner_factory<F: SstPartitionerFactory>(&mut self, _factory: F) {
        panic!()
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
        panic!()
    }
    fn set_db_options(&self, _options: &[(&str, &str)]) -> TraitsResult<()> {
        panic!()
    }
}

pub struct EngineDBOptions;

impl DBOptions for EngineDBOptions {
    type TitanDBOptions = EngineTitanDBOptions;

    fn new() -> Self {
        panic!()
    }

    fn get_max_background_jobs(&self) -> i32 {
        panic!()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        panic!()
    }

    fn set_rate_bytes_per_sec(&mut self, _rate_bytes_per_sec: i64) -> TraitsResult<()> {
        panic!()
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        panic!()
    }

    fn set_rate_limiter_auto_tuned(&mut self, _rate_limiter_auto_tuned: bool) -> TraitsResult<()> {
        panic!()
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
    fn seek(&mut self, _key: SeekKey) -> TraitsResult<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, _key: SeekKey) -> TraitsResult<bool> {
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
        return Ok(Some(1));
    }

    fn get_cf_num_immutable_mem_table(&self, _cf: &str) -> engine_traits::Result<Option<u64>> {
        // TODO(x)
        return Ok(Some(1));
    }

    fn get_cf_pending_compaction_bytes(&self, _cf: &str) -> engine_traits::Result<Option<u64>> {
        // TODO(x)
        return Ok(Some(0));
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
        _ranges: &[Range],
    ) -> TraitsResult<()> {
        panic!()
    }

    fn get_approximate_memtable_stats_cf(
        &self,
        _cf: &str,
        _range: &Range,
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
        self.fs.local_dir().to_str().unwrap()
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
        _range: Range,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_keys_cf(
        &self,
        _cfname: &str,
        _range: Range,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_size(
        &self,
        _range: Range,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_size_cf(
        &self,
        _cfname: &str,
        _range: Range,
        _large_threshold: u64,
    ) -> TraitsResult<u64> {
        panic!()
    }

    fn get_range_approximate_split_keys(
        &self,
        _range: Range,
        _key_count: usize,
    ) -> TraitsResult<Vec<Vec<u8>>> {
        panic!()
    }

    fn get_range_approximate_split_keys_cf(
        &self,
        _cfname: &str,
        _range: Range,
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
    fn seek(&mut self, _key: SeekKey) -> TraitsResult<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, _key: SeekKey) -> TraitsResult<bool> {
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
    type SstReader = EngineSstReader;
    type SstWriter = EngineSstWriter;
    type SstWriterBuilder = EngineSstWriterBuilder;
}

pub struct EngineSstReader;

impl SstReader for EngineSstReader {
    fn open(_path: &str) -> TraitsResult<Self> {
        panic!()
    }
    fn verify_checksum(&self) -> TraitsResult<()> {
        panic!()
    }
    fn iter(&self) -> Self::Iterator {
        panic!()
    }
}

impl Iterable for EngineSstReader {
    type Iterator = EngineSstReaderIterator;

    fn iterator_opt(&self, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
    fn iterator_cf_opt(&self, _cf: &str, _opts: IterOptions) -> TraitsResult<Self::Iterator> {
        panic!()
    }
}

pub struct EngineSstReaderIterator;

impl engine_traits::Iterator for EngineSstReaderIterator {
    fn seek(&mut self, _key: SeekKey) -> TraitsResult<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, _key: SeekKey) -> TraitsResult<bool> {
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

pub struct EngineSstWriter;

impl SstWriter for EngineSstWriter {
    type ExternalSstFileInfo = EngineExternalSstFileInfo;
    type ExternalSstFileReader = EngineExternalSstFileReader;

    fn put(&mut self, _key: &[u8], _val: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn delete(&mut self, _key: &[u8]) -> TraitsResult<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> TraitsResult<Self::ExternalSstFileInfo> {
        panic!()
    }
    fn finish_read(self) -> TraitsResult<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        panic!()
    }
}

pub struct EngineSstWriterBuilder;

impl SstWriterBuilder<Engine> for EngineSstWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, _db: &Engine) -> Self {
        panic!()
    }
    fn set_cf(self, _cf: &str) -> Self {
        panic!()
    }
    fn set_in_memory(self, _in_memory: bool) -> Self {
        panic!()
    }
    fn set_compression_type(self, _compression: Option<SstCompressionType>) -> Self {
        panic!()
    }
    fn set_compression_level(self, _level: i32) -> Self {
        panic!()
    }

    fn build(self, _path: &str) -> TraitsResult<EngineSstWriter> {
        panic!()
    }
}

pub struct EngineExternalSstFileInfo;

impl ExternalSstFileInfo for EngineExternalSstFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        panic!()
    }
    fn smallest_key(&self) -> &[u8] {
        panic!()
    }
    fn largest_key(&self) -> &[u8] {
        panic!()
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        panic!()
    }
    fn num_entries(&self) -> u64 {
        panic!()
    }
}

pub struct EngineExternalSstFileReader;

impl std::io::Read for EngineExternalSstFileReader {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
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
        _ranges: &[Range],
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
    fn merge(&mut self, _src: Self) {
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
