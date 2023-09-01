// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ffi::CString, fs, path::Path, str::FromStr, sync::Arc};

use engine_traits::{Engines, Range, Result, CF_DEFAULT};
use fail::fail_point;
use rocksdb::{
    load_latest_options, CColumnFamilyDescriptor, CFHandle, ColumnFamilyOptions, CompactionFilter,
    CompactionFilterContext, CompactionFilterDecision, CompactionFilterFactory,
    CompactionFilterValueType, DBTableFileCreationReason, Env, Range as RocksRange, SliceTransform,
    DB,
};
use slog_global::warn;

use crate::{
    cf_options::RocksCfOptions, db_options::RocksDbOptions, engine::RocksEngine, r2e,
    rocks_metrics_defs::*, RocksStatistics,
};

pub fn new_temp_engine(path: &tempfile::TempDir) -> Engines<RocksEngine, RocksEngine> {
    let raft_path = path.path().join(std::path::Path::new("raft"));
    Engines::new(
        new_engine(path.path().to_str().unwrap(), engine_traits::ALL_CFS).unwrap(),
        new_engine(raft_path.to_str().unwrap(), &[engine_traits::CF_DEFAULT]).unwrap(),
    )
}

pub fn new_default_engine(path: &str) -> Result<RocksEngine> {
    new_engine(path, &[CF_DEFAULT])
}

pub fn new_engine(path: &str, cfs: &[&str]) -> Result<RocksEngine> {
    let mut db_opts = RocksDbOptions::default();
    db_opts.set_statistics(&RocksStatistics::new_titan());
    let cf_opts = cfs.iter().map(|name| (*name, Default::default())).collect();
    new_engine_opt(path, db_opts, cf_opts)
}

pub fn new_engine_opt(
    path: &str,
    db_opt: RocksDbOptions,
    cf_opts: Vec<(&str, RocksCfOptions)>,
) -> Result<RocksEngine> {
    let mut db_opt = db_opt.into_raw();
    if cf_opts.iter().all(|(name, _)| *name != CF_DEFAULT) {
        return Err(engine_traits::Error::Engine(
            engine_traits::Status::with_error(
                engine_traits::Code::InvalidArgument,
                "default cf must be specified",
            ),
        ));
    }
    let mut cf_opts: Vec<_> = cf_opts
        .into_iter()
        .map(|(name, opt)| (name, opt.into_raw()))
        .collect();

    // Creates a new db if it doesn't exist.
    if !db_exist(path) {
        db_opt.create_if_missing(true);
        db_opt.create_missing_column_families(true);

        let db = DB::open_cf(db_opt, path, cf_opts.into_iter().collect()).map_err(r2e)?;

        return Ok(RocksEngine::new(db));
    }

    db_opt.create_if_missing(false);

    // Lists all column families in current db.
    let cfs_list = DB::list_column_families(&db_opt, path).map_err(r2e)?;
    let existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
    let needed: Vec<&str> = cf_opts.iter().map(|(name, _)| *name).collect();

    let cf_descs = if !existed.is_empty() {
        let env = match db_opt.env() {
            Some(env) => env,
            None => Arc::new(Env::default()),
        };
        // panic if OPTIONS not found for existing instance?
        let (_, tmp) = load_latest_options(path, &env, true)
            .unwrap_or_else(|e| panic!("failed to load_latest_options {:?}", e))
            .unwrap_or_else(|| panic!("couldn't find the OPTIONS file"));
        tmp
    } else {
        vec![]
    };

    for cf in &existed {
        if cf_opts.iter().all(|(name, _)| name != cf) {
            cf_opts.push((cf, ColumnFamilyOptions::default()));
        }
    }
    for (name, opt) in &mut cf_opts {
        adjust_dynamic_level_bytes(&cf_descs, name, opt);
    }

    let cfds: Vec<_> = cf_opts.into_iter().collect();
    // We have added all missing options by iterating `existed`. If two vecs still
    // have same length, then they must have same column families dispite their
    // orders. So just open db.
    if needed.len() == existed.len() && needed.len() == cfds.len() {
        let db = DB::open_cf(db_opt, path, cfds).map_err(r2e)?;
        return Ok(RocksEngine::new(db));
    }

    // Opens db.
    db_opt.create_missing_column_families(true);
    let mut db = DB::open_cf(db_opt, path, cfds).map_err(r2e)?;

    // Drops discarded column families.
    for cf in cfs_diff(&existed, &needed) {
        // We have checked it at the very beginning, so it must be needed.
        assert_ne!(cf, CF_DEFAULT);
        db.drop_cf(cf).map_err(r2e)?;
    }

    Ok(RocksEngine::new(db))
}

/// Turns "dynamic level size" off for the existing column family which was off
/// before. Column families are small, HashMap isn't necessary.
fn adjust_dynamic_level_bytes(
    cf_descs: &[CColumnFamilyDescriptor],
    name: &str,
    opt: &mut ColumnFamilyOptions,
) {
    if let Some(cf_desc) = cf_descs.iter().find(|cf_desc| cf_desc.name() == name) {
        let existed_dynamic_level_bytes =
            cf_desc.options().get_level_compaction_dynamic_level_bytes();
        if existed_dynamic_level_bytes != opt.get_level_compaction_dynamic_level_bytes() {
            warn!(
                "change dynamic_level_bytes for existing column family is danger";
                "old_value" => existed_dynamic_level_bytes,
                "new_value" => opt.get_level_compaction_dynamic_level_bytes(),
            );
        }
        opt.set_level_compaction_dynamic_level_bytes(existed_dynamic_level_bytes);
    }
}

pub fn db_exist(path: &str) -> bool {
    let path = Path::new(path);
    if !path.exists() || !path.is_dir() {
        return false;
    }
    let current_file_path = path.join("CURRENT");
    if !current_file_path.exists() || !current_file_path.is_file() {
        return false;
    }

    // If path is not an empty directory, and current file exists, we say db exists.
    // If path is not an empty directory but db has not been created,
    // `DB::list_column_families` fails and we can clean up the directory by
    // this indication.
    fs::read_dir(path).unwrap().next().is_some()
}

/// Returns a Vec of cf which is in `a' but not in `b'.
fn cfs_diff<'a>(a: &[&'a str], b: &[&str]) -> Vec<&'a str> {
    a.iter()
        .filter(|x| !b.iter().any(|y| *x == y))
        .cloned()
        .collect()
}

pub fn get_cf_handle<'a>(db: &'a DB, cf: &str) -> Result<&'a CFHandle> {
    db.cf_handle(cf)
        .ok_or_else(|| format!("cf {} not found", cf))
        .map_err(r2e)
}

pub fn range_to_rocks_range<'a>(range: &Range<'a>) -> RocksRange<'a> {
    RocksRange::new(range.start_key, range.end_key)
}

pub fn get_engine_cf_used_size(engine: &DB, handle: &CFHandle) -> u64 {
    let mut cf_used_size = engine
        .get_property_int_cf(handle, ROCKSDB_TOTAL_SST_FILES_SIZE)
        .expect("rocksdb is too old, missing total-sst-files-size property");
    // For memtable
    if let Some(mem_table) = engine.get_property_int_cf(handle, ROCKSDB_CUR_SIZE_ALL_MEM_TABLES) {
        cf_used_size += mem_table;
    }
    // For blob files
    if let Some(live_blob) = engine.get_property_int_cf(handle, ROCKSDB_TITANDB_LIVE_BLOB_FILE_SIZE)
    {
        cf_used_size += live_blob;
    }
    if let Some(obsolete_blob) =
        engine.get_property_int_cf(handle, ROCKSDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
    {
        cf_used_size += obsolete_blob;
    }

    cf_used_size
}

/// Gets engine's compression ratio at given level.
pub fn get_engine_compression_ratio_at_level(
    engine: &DB,
    handle: &CFHandle,
    level: usize,
) -> Option<f64> {
    let prop = format!("{}{}", ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, level);
    if let Some(v) = engine.get_property_value_cf(handle, &prop) {
        if let Ok(f) = f64::from_str(&v) {
            // RocksDB returns -1.0 if the level is empty.
            if f >= 0.0 {
                return Some(f);
            }
        }
    }
    None
}

/// Gets the number of files at given level of given column family.
pub fn get_cf_num_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_NUM_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of blob files at given level of given column family.
pub fn get_cf_num_blob_files_at_level(engine: &DB, handle: &CFHandle, level: usize) -> Option<u64> {
    let prop = format!("{}{}", ROCKSDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL, level);
    engine.get_property_int_cf(handle, &prop)
}

/// Gets the number of immutable mem-table of given column family.
pub fn get_cf_num_immutable_mem_table(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_NUM_IMMUTABLE_MEM_TABLE)
}

/// Gets the amount of pending compaction bytes of given column family.
pub fn get_cf_pending_compaction_bytes(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_PENDING_COMPACTION_BYTES)
}

pub struct FixedSuffixSliceTransform {
    pub suffix_len: usize,
}

impl FixedSuffixSliceTransform {
    pub fn new(suffix_len: usize) -> FixedSuffixSliceTransform {
        FixedSuffixSliceTransform { suffix_len }
    }
}

impl SliceTransform for FixedSuffixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        let mid = key.len() - self.suffix_len;
        let (left, _) = key.split_at(mid);
        left
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.suffix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct FixedPrefixSliceTransform {
    pub prefix_len: usize,
}

impl FixedPrefixSliceTransform {
    pub fn new(prefix_len: usize) -> FixedPrefixSliceTransform {
        FixedPrefixSliceTransform { prefix_len }
    }
}

impl SliceTransform for FixedPrefixSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        &key[..self.prefix_len]
    }

    fn in_domain(&mut self, key: &[u8]) -> bool {
        key.len() >= self.prefix_len
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub struct NoopSliceTransform;

impl SliceTransform for NoopSliceTransform {
    fn transform<'a>(&mut self, key: &'a [u8]) -> &'a [u8] {
        key
    }

    fn in_domain(&mut self, _: &[u8]) -> bool {
        true
    }

    fn in_range(&mut self, _: &[u8]) -> bool {
        true
    }
}

pub fn to_raw_perf_level(level: engine_traits::PerfLevel) -> rocksdb::PerfLevel {
    match level {
        engine_traits::PerfLevel::Uninitialized => rocksdb::PerfLevel::Uninitialized,
        engine_traits::PerfLevel::Disable => rocksdb::PerfLevel::Disable,
        engine_traits::PerfLevel::EnableCount => rocksdb::PerfLevel::EnableCount,
        engine_traits::PerfLevel::EnableTimeExceptForMutex => {
            rocksdb::PerfLevel::EnableTimeExceptForMutex
        }
        engine_traits::PerfLevel::EnableTimeAndCpuTimeExceptForMutex => {
            rocksdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex
        }
        engine_traits::PerfLevel::EnableTime => rocksdb::PerfLevel::EnableTime,
        engine_traits::PerfLevel::OutOfBounds => rocksdb::PerfLevel::OutOfBounds,
    }
}

pub fn from_raw_perf_level(level: rocksdb::PerfLevel) -> engine_traits::PerfLevel {
    match level {
        rocksdb::PerfLevel::Uninitialized => engine_traits::PerfLevel::Uninitialized,
        rocksdb::PerfLevel::Disable => engine_traits::PerfLevel::Disable,
        rocksdb::PerfLevel::EnableCount => engine_traits::PerfLevel::EnableCount,
        rocksdb::PerfLevel::EnableTimeExceptForMutex => {
            engine_traits::PerfLevel::EnableTimeExceptForMutex
        }
        rocksdb::PerfLevel::EnableTimeAndCPUTimeExceptForMutex => {
            engine_traits::PerfLevel::EnableTimeAndCpuTimeExceptForMutex
        }
        rocksdb::PerfLevel::EnableTime => engine_traits::PerfLevel::EnableTime,
        rocksdb::PerfLevel::OutOfBounds => engine_traits::PerfLevel::OutOfBounds,
    }
}

struct OwnedRange {
    start_key: Box<[u8]>,
    end_key: Box<[u8]>,
}

type FilterByReason = [bool; 4];

fn reason_to_index(reason: DBTableFileCreationReason) -> usize {
    match reason {
        DBTableFileCreationReason::Flush => 0,
        DBTableFileCreationReason::Compaction => 1,
        DBTableFileCreationReason::Recovery => 2,
        DBTableFileCreationReason::Misc => 3,
    }
}

fn filter_by_reason(factory: &impl CompactionFilterFactory) -> FilterByReason {
    let mut r = FilterByReason::default();
    r[reason_to_index(DBTableFileCreationReason::Flush)] =
        factory.should_filter_table_file_creation(DBTableFileCreationReason::Flush);
    r[reason_to_index(DBTableFileCreationReason::Compaction)] =
        factory.should_filter_table_file_creation(DBTableFileCreationReason::Compaction);
    r[reason_to_index(DBTableFileCreationReason::Recovery)] =
        factory.should_filter_table_file_creation(DBTableFileCreationReason::Recovery);
    r[reason_to_index(DBTableFileCreationReason::Misc)] =
        factory.should_filter_table_file_creation(DBTableFileCreationReason::Misc);
    r
}

pub struct StackingCompactionFilterFactory<A: CompactionFilterFactory, B: CompactionFilterFactory> {
    outer_should_filter: FilterByReason,
    outer: A,
    inner_should_filter: FilterByReason,
    inner: B,
}

impl<A: CompactionFilterFactory, B: CompactionFilterFactory> StackingCompactionFilterFactory<A, B> {
    /// Creates a factory of stacked filter with `outer` on top of `inner`.
    /// Table keys will be filtered through `outer` first before reaching
    /// `inner`.
    pub fn new(outer: A, inner: B) -> Self {
        let outer_should_filter = filter_by_reason(&outer);
        let inner_should_filter = filter_by_reason(&inner);
        Self {
            outer_should_filter,
            outer,
            inner_should_filter,
            inner,
        }
    }
}

impl<A: CompactionFilterFactory, B: CompactionFilterFactory> CompactionFilterFactory
    for StackingCompactionFilterFactory<A, B>
{
    type Filter = StackingCompactionFilter<A::Filter, B::Filter>;

    fn create_compaction_filter(
        &self,
        context: &CompactionFilterContext,
    ) -> Option<(CString, Self::Filter)> {
        let i = reason_to_index(context.reason());
        let mut outer_filter = None;
        let mut inner_filter = None;
        let mut full_name = String::new();
        if self.outer_should_filter[i]
            && let Some((name, filter)) = self.outer.create_compaction_filter(context)
        {
            outer_filter = Some(filter);
            full_name = name.into_string().unwrap();
        }
        if self.inner_should_filter[i]
            && let Some((name, filter)) = self.inner.create_compaction_filter(context)
        {
            inner_filter = Some(filter);
            if !full_name.is_empty() {
                full_name += ".";
            }
            full_name += name.to_str().unwrap();
        }
        if outer_filter.is_none() && inner_filter.is_none() {
            None
        } else {
            let filter = StackingCompactionFilter {
                outer: outer_filter,
                inner: inner_filter,
            };
            Some((CString::new(full_name).unwrap(), filter))
        }
    }

    fn should_filter_table_file_creation(&self, reason: DBTableFileCreationReason) -> bool {
        let i = reason_to_index(reason);
        self.outer_should_filter[i] || self.inner_should_filter[i]
    }
}

pub struct StackingCompactionFilter<A: CompactionFilter, B: CompactionFilter> {
    outer: Option<A>,
    inner: Option<B>,
}

impl<A: CompactionFilter, B: CompactionFilter> CompactionFilter for StackingCompactionFilter<A, B> {
    fn unsafe_filter(
        &mut self,
        level: usize,
        key: &[u8],
        seqno: u64,
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if let Some(outer) = self.outer.as_mut()
            && let r = outer.unsafe_filter(level, key, seqno, value, value_type)
            && !matches!(r, CompactionFilterDecision::Keep)
        {
            r
        } else if let Some(inner) = self.inner.as_mut() {
            inner.unsafe_filter(level, key, seqno, value, value_type)
        } else {
            CompactionFilterDecision::Keep
        }
    }
}

#[derive(Clone)]
pub struct RangeCompactionFilterFactory(Arc<OwnedRange>);

impl RangeCompactionFilterFactory {
    pub fn new(start_key: Box<[u8]>, end_key: Box<[u8]>) -> Self {
        fail_point!("unlimited_range_compaction_filter", |_| {
            let range = OwnedRange {
                start_key: keys::data_key(b"").into_boxed_slice(),
                end_key: keys::data_end_key(b"").into_boxed_slice(),
            };
            Self(Arc::new(range))
        });
        let range = OwnedRange { start_key, end_key };
        Self(Arc::new(range))
    }
}

impl CompactionFilterFactory for RangeCompactionFilterFactory {
    type Filter = RangeCompactionFilter;

    fn create_compaction_filter(
        &self,
        _context: &CompactionFilterContext,
    ) -> Option<(CString, Self::Filter)> {
        Some((
            CString::new("range_filter").unwrap(),
            RangeCompactionFilter(self.0.clone()),
        ))
    }

    fn should_filter_table_file_creation(&self, _reason: DBTableFileCreationReason) -> bool {
        true
    }
}

/// Filters out all keys outside the key range.
pub struct RangeCompactionFilter(Arc<OwnedRange>);

impl CompactionFilter for RangeCompactionFilter {
    fn unsafe_filter(
        &mut self,
        _level: usize,
        key: &[u8],
        _seqno: u64,
        _value: &[u8],
        _value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if key < self.0.start_key.as_ref() {
            CompactionFilterDecision::RemoveAndSkipUntil(self.0.start_key.to_vec())
        } else if key >= self.0.end_key.as_ref() {
            assert!(key < keys::DATA_MAX_KEY);
            CompactionFilterDecision::RemoveAndSkipUntil(keys::DATA_MAX_KEY.to_vec())
        } else {
            CompactionFilterDecision::Keep
        }
    }
}

#[cfg(test)]
mod tests {
    use engine_traits::{
        CfOptionsExt, FlowControlFactorsExt, Iterable, MiscExt, Peekable, SyncMutable, CF_DEFAULT,
    };
    use rocksdb::DB;
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_cfs_diff() {
        let a = vec!["1", "2", "3"];
        let a_diff_a = cfs_diff(&a, &a);
        assert!(a_diff_a.is_empty());
        let b = vec!["4"];
        assert_eq!(a, cfs_diff(&a, &b));
        let c = vec!["4", "5", "3", "6"];
        assert_eq!(vec!["1", "2"], cfs_diff(&a, &c));
        assert_eq!(vec!["4", "5", "6"], cfs_diff(&c, &a));
        let d = vec!["1", "2", "3", "4"];
        let a_diff_d = cfs_diff(&a, &d);
        assert!(a_diff_d.is_empty());
        assert_eq!(vec!["4"], cfs_diff(&d, &a));
    }

    #[test]
    fn test_new_engine_opt() {
        let path = Builder::new()
            .prefix("_util_rocksdb_test_check_column_families")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        // create db when db not exist
        let mut cfs_opts = vec![(CF_DEFAULT, RocksCfOptions::default())];
        let mut opts = RocksCfOptions::default();
        opts.set_level_compaction_dynamic_level_bytes(true);
        cfs_opts.push(("cf_dynamic_level_bytes", opts.clone()));
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes"]);
        check_dynamic_level_bytes(&db);
        drop(db);

        // add cf1.
        let cfs_opts = vec![
            (CF_DEFAULT, opts.clone()),
            ("cf_dynamic_level_bytes", opts.clone()),
            ("cf1", opts.clone()),
        ];
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
        check_dynamic_level_bytes(&db);
        for cf in &[CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"] {
            db.put_cf(cf, b"k", b"v").unwrap();
        }
        drop(db);

        // change order should not cause data corruption.
        let cfs_opts = vec![
            ("cf_dynamic_level_bytes", opts.clone()),
            ("cf1", opts.clone()),
            (CF_DEFAULT, opts),
        ];
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"]);
        check_dynamic_level_bytes(&db);
        for cf in &[CF_DEFAULT, "cf_dynamic_level_bytes", "cf1"] {
            assert_eq!(db.get_value_cf(cf, b"k").unwrap().unwrap(), b"v");
        }
        drop(db);

        // drop cf1.
        let cfs = vec![CF_DEFAULT, "cf_dynamic_level_bytes"];
        let db = new_engine(path_str, &cfs).unwrap();
        column_families_must_eq(path_str, cfs);
        check_dynamic_level_bytes(&db);
        drop(db);

        // drop all cfs.
        new_engine(path_str, &[CF_DEFAULT]).unwrap();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);

        // not specifying default cf should error.
        new_engine(path_str, &[]).unwrap_err();
        column_families_must_eq(path_str, vec![CF_DEFAULT]);
    }

    fn column_families_must_eq(path: &str, excepted: Vec<&str>) {
        let opts = RocksDbOptions::default();
        let cfs_list = DB::list_column_families(&opts, path).unwrap();

        let mut cfs_existed: Vec<&str> = cfs_list.iter().map(|v| v.as_str()).collect();
        let mut cfs_excepted: Vec<&str> = excepted.clone();
        cfs_existed.sort_unstable();
        cfs_excepted.sort_unstable();
        assert_eq!(cfs_existed, cfs_excepted);
    }

    fn check_dynamic_level_bytes(db: &RocksEngine) {
        let tmp_cf_opts = db.get_options_cf(CF_DEFAULT).unwrap();
        assert!(!tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
        let tmp_cf_opts = db.get_options_cf("cf_dynamic_level_bytes").unwrap();
        assert!(tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
    }

    #[test]
    fn test_range_filter() {
        let path = Builder::new()
            .prefix("test_range_filter")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();

        let mut cf_opts = RocksCfOptions::default();
        cf_opts
            .set_compaction_filter_factory(
                "range",
                RangeCompactionFilterFactory::new(
                    b"b".to_vec().into_boxed_slice(),
                    b"c".to_vec().into_boxed_slice(),
                ),
            )
            .unwrap();
        let cfs_opts = vec![(CF_DEFAULT, cf_opts)];
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();

        // in-range keys.
        db.put(b"b1", b"").unwrap();
        db.put(b"c2", b"").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            db.get_cf_num_files_at_level(CF_DEFAULT, 0).unwrap(),
            Some(1)
        );

        // put then delete.
        db.put(b"a1", b"").unwrap();
        // avoid merging put and delete.
        let _iter = db.iterator(CF_DEFAULT).unwrap();
        db.delete(b"a1").unwrap();
        db.delete(b"a1").unwrap();
        db.put(b"c1", b"").unwrap();
        let _iter = db.iterator(CF_DEFAULT).unwrap();
        db.delete(b"c1").unwrap();
        db.delete(b"c1").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            db.get_cf_num_files_at_level(CF_DEFAULT, 0).unwrap(),
            Some(1)
        );

        // multiple puts.
        db.put(b"a2", b"").unwrap();
        db.put(b"a2", b"").unwrap();
        db.put(b"c2", b"").unwrap();
        db.put(b"c2", b"").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            db.get_cf_num_files_at_level(CF_DEFAULT, 0).unwrap(),
            Some(1)
        );

        // multiple deletes.
        db.delete(b"a3").unwrap();
        db.delete(b"a3").unwrap();
        db.delete(b"c3").unwrap();
        db.delete(b"c3").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        assert_eq!(
            db.get_cf_num_files_at_level(CF_DEFAULT, 0).unwrap(),
            Some(1)
        );
    }
}
