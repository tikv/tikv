// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ffi::CString,
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
    str::FromStr,
    sync::Arc,
};

use engine_traits::{CF_DEFAULT, Engines, Range, Result};
use fail::fail_point;
use rocksdb::{
    CColumnFamilyDescriptor, CFHandle, ColumnFamilyOptions, CompactionFilter,
    CompactionFilterContext, CompactionFilterDecision, CompactionFilterFactory,
    CompactionFilterValueType, DB, DBTableFileCreationReason, Env, Range as RocksRange,
    SliceTransform, load_latest_options,
};
use slog_global::warn;

use crate::{
    RocksStatistics, cf_options::RocksCfOptions, db_options::RocksDbOptions, engine::RocksEngine,
    r2e, rocks_metrics_defs::*,
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

    // A crash while RocksDB was switching CURRENT (or a filesystem losing the
    // write on power failure) can leave it empty, truncated or pointing at a
    // deleted MANIFEST: the open below then fails forever even though the
    // MANIFEST is intact. Worse, a *missing* CURRENT makes `db_exist` report
    // false, and creating the "new" DB either fails on the leftover WAL or
    // silently starts an empty DB over the old data when no WAL survived.
    // Repair the pointer from the newest MANIFEST on disk before opening.
    recover_current_if_needed(path)?;

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

/// Temp file used to atomically rewrite RocksDB's `CURRENT`. The `dbtmp.plain`
/// suffix matches the temp files RocksDB itself uses when updating `CURRENT`:
/// `KeyManagedEncryptedEnv` always keeps such files (and `CURRENT` itself)
/// plaintext, so accessing them through the raw filesystem is consistent with
/// encryption-at-rest.
const CURRENT_RECOVER_TMP: &str = "CURRENT.recover.dbtmp.plain";

/// State of RocksDB's `CURRENT` pointer file, classified for crash recovery.
enum CurrentState {
    /// `CURRENT` names a `MANIFEST-*` file that exists.
    Healthy,
    /// `CURRENT` does not exist. Either the DB was never created, or a crash
    /// during DB creation lost it while other files remain.
    Missing,
    /// `CURRENT` exists but its content is not `MANIFEST-<digits>\n`: empty,
    /// zero-filled or otherwise truncated by an incomplete write. The payload
    /// is kept for logging.
    Truncated(Vec<u8>),
    /// `CURRENT` parses but the MANIFEST it names is gone; the referenced
    /// manifest number is kept to forbid rolling back to an older one.
    Dangling(u64),
}

fn classify_current(dir: &Path) -> std::io::Result<CurrentState> {
    let bytes = match fs::read(dir.join("CURRENT")) {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(CurrentState::Missing),
        Err(e) => return Err(e),
    };
    match parse_current_manifest(&bytes) {
        Some((name, num)) => {
            if dir.join(name).is_file() {
                Ok(CurrentState::Healthy)
            } else {
                Ok(CurrentState::Dangling(num))
            }
        }
        None => Ok(CurrentState::Truncated(bytes)),
    }
}

/// Parses a RocksDB `CURRENT` payload, which must be exactly
/// `MANIFEST-<digits>\n` (single trailing newline, no other whitespace).
/// Returns the manifest file name and its number.
fn parse_current_manifest(bytes: &[u8]) -> Option<(&str, u64)> {
    let name = std::str::from_utf8(bytes.strip_suffix(b"\n")?).ok()?;
    let digits = name.strip_prefix("MANIFEST-")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((name, digits.parse().ok()?))
}

/// Returns the highest-numbered `MANIFEST-*` regular file in `dir`, if any.
fn latest_manifest(dir: &Path) -> std::io::Result<Option<(String, u64)>> {
    let mut best: Option<(String, u64)> = None;
    for ent in fs::read_dir(dir)? {
        let Ok(ent) = ent else { continue };
        if !ent.file_type().is_ok_and(|t| t.is_file()) {
            continue;
        }
        let name = ent.file_name();
        let Some(name) = name.to_str() else { continue };
        let Some(digits) = name.strip_prefix("MANIFEST-") else {
            continue;
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let Ok(num) = digits.parse::<u64>() else {
            continue;
        };
        if best.as_ref().is_none_or(|(_, b)| num > *b) {
            best = Some((name.to_owned(), num));
        }
    }
    Ok(best)
}

fn recover_io_err(op: &str, e: std::io::Error) -> engine_traits::Error {
    engine_traits::Error::Engine(engine_traits::Status::with_error(
        engine_traits::Code::IoError,
        format!("recover CURRENT: {}: {}", op, e),
    ))
}

/// Repairs RocksDB's `CURRENT` file when a crash left it unusable, by
/// atomically rewriting it to name the latest `MANIFEST-*` on disk
/// (write + fsync temp file, rename over `CURRENT`, fsync directory — the
/// same durability sequence RocksDB uses when switching `CURRENT`).
///
/// A crash or power loss while RocksDB updates its meta files can leave
/// `CURRENT` empty, zero-filled or without its trailing newline; RocksDB then
/// refuses to open the DB forever ("CURRENT file does not end with newline")
/// even though the MANIFEST it needs is intact on disk. A *missing* `CURRENT`
/// is worse: `db_exist` reports false, so the open path tries to create a new
/// DB — it fails on the leftover WAL ("While creating a new Db, wal_dir
/// contains existing log file") or, when no WAL file survived, silently
/// creates a brand-new empty DB over the old data. Repairing the pointer
/// restores access to the newest MANIFEST that survived, which is what
/// RocksDB would have pointed to.
///
/// Repair is conservative:
/// - When `CURRENT` still parses but names a missing MANIFEST, it is only
///   rewritten to an equal-or-higher-numbered manifest, never to an older one,
///   so a store cannot silently roll back to earlier metadata.
/// - Truncated MANIFEST contents are out of scope; only the pointer file is
///   repaired, and RocksDB's own manifest validation still runs at open.
/// - `CURRENT` is always plaintext even with encryption-at-rest enabled
///   (`KeyManagedEncryptedEnv` intentionally skips it), so reading and
///   rewriting it through the raw filesystem is safe.
///
/// Returns `Ok(true)` when `CURRENT` was rewritten.
fn recover_current_if_needed(path: &str) -> Result<bool> {
    let dir = Path::new(path);
    if !dir.is_dir() {
        return Ok(false);
    }
    let state = classify_current(dir).map_err(|e| recover_io_err("read CURRENT", e))?;
    if matches!(state, CurrentState::Healthy) {
        return Ok(false);
    }
    let manifest = latest_manifest(dir).map_err(|e| recover_io_err("scan MANIFEST files", e))?;
    let Some((manifest, manifest_num)) = manifest else {
        // Nothing to repair from. For a fresh (or empty) DB dir this is the
        // normal creation path; otherwise RocksDB will report the corruption.
        return Ok(false);
    };
    let reason = match state {
        CurrentState::Healthy => unreachable!(),
        CurrentState::Missing => "CURRENT is missing".to_owned(),
        CurrentState::Truncated(payload) => format!(
            "CURRENT is truncated (payload {:?}, {} bytes)",
            String::from_utf8_lossy(&payload[..payload.len().min(32)]),
            payload.len()
        ),
        CurrentState::Dangling(named) => {
            if manifest_num < named {
                warn!(
                    "RocksDB CURRENT names a missing MANIFEST and only older ones remain; \
                     refusing to roll back";
                    "path" => %dir.display(),
                    "named_manifest_num" => named,
                    "latest_manifest_num" => manifest_num,
                );
                return Ok(false);
            }
            format!("CURRENT names missing MANIFEST-{:06}", named)
        }
    };

    let tmp = dir.join(CURRENT_RECOVER_TMP);
    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .map_err(|e| recover_io_err("open temp file", e))?;
        f.write_all(format!("{}\n", manifest).as_bytes())
            .map_err(|e| recover_io_err("write temp file", e))?;
        f.sync_all()
            .map_err(|e| recover_io_err("sync temp file", e))?;
    }
    fs::rename(&tmp, dir.join("CURRENT")).map_err(|e| recover_io_err("rename over CURRENT", e))?;
    file_system::sync_dir(dir).map_err(|e| recover_io_err("sync db dir", e))?;

    warn!(
        "recovered RocksDB CURRENT to the latest MANIFEST";
        "path" => %dir.display(),
        "manifest" => %manifest,
        "reason" => %reason,
    );
    Ok(true)
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

pub fn get_engine_cfs_used_size(engine: &DB) -> Result<u64> {
    let mut cfs_used_size = 0;
    for cf in engine.cf_names() {
        let handle = engine
            .cf_handle(cf)
            .ok_or_else(|| format!("cf {} not found", cf))
            .map_err(r2e)?;
        cfs_used_size += get_engine_cf_used_size(engine, handle);
    }
    Ok(cfs_used_size)
}

/// Gets engine's compression ratio at given level.
pub fn get_engine_compression_ratio_at_level(
    engine: &DB,
    handle: &CFHandle,
    level: usize,
) -> Option<f64> {
    let prop = format!("{}{}", ROCKSDB_COMPRESSION_RATIO_AT_LEVEL, level);
    if let Some(v) = engine.get_property_value_cf(handle, &prop)
        && let Ok(f) = f64::from_str(&v)
    {
        // RocksDB returns -1.0 if the level is empty.
        if f >= 0.0 {
            return Some(f);
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

/// Gets the base level of given column family.
pub fn get_cf_base_level(engine: &DB, handle: &CFHandle) -> Option<u64> {
    engine.get_property_int_cf(handle, ROCKSDB_BASE_LEVEL)
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
        value: &[u8],
        value_type: CompactionFilterValueType,
    ) -> CompactionFilterDecision {
        if let Some(outer) = self.outer.as_mut()
            && let r = outer.unsafe_filter(level, key, value, value_type)
            && !matches!(r, CompactionFilterDecision::Keep)
        {
            r
        } else if let Some(inner) = self.inner.as_mut() {
            inner.unsafe_filter(level, key, value, value_type)
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
        CF_DEFAULT, CfOptionsExt, FlowControlFactorsExt, Iterable, MiscExt, Peekable, SyncMutable,
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
        opts.set_level_compaction_dynamic_level_bytes(false);
        cfs_opts.push(("cf_dynamic_level_bytes_disabled", opts.clone()));
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(
            path_str,
            vec![CF_DEFAULT, "cf_dynamic_level_bytes_disabled"],
        );
        check_dynamic_level_bytes(&db);
        drop(db);

        // add cf1.
        let cfs_opts = vec![
            (CF_DEFAULT, opts.clone()),
            ("cf_dynamic_level_bytes_disabled", opts.clone()),
            ("cf1", opts.clone()),
        ];
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(
            path_str,
            vec![CF_DEFAULT, "cf_dynamic_level_bytes_disabled", "cf1"],
        );
        check_dynamic_level_bytes(&db);
        for cf in &[CF_DEFAULT, "cf_dynamic_level_bytes_disabled", "cf1"] {
            db.put_cf(cf, b"k", b"v").unwrap();
        }
        drop(db);

        // change order should not cause data corruption.
        let cfs_opts = vec![
            ("cf_dynamic_level_bytes_disabled", opts.clone()),
            ("cf1", opts.clone()),
            (CF_DEFAULT, opts),
        ];
        let db = new_engine_opt(path_str, RocksDbOptions::default(), cfs_opts).unwrap();
        column_families_must_eq(
            path_str,
            vec![CF_DEFAULT, "cf_dynamic_level_bytes_disabled", "cf1"],
        );
        check_dynamic_level_bytes(&db);
        for cf in &[CF_DEFAULT, "cf_dynamic_level_bytes_disabled", "cf1"] {
            assert_eq!(db.get_value_cf(cf, b"k").unwrap().unwrap(), b"v");
        }
        drop(db);

        // drop cf1.
        let cfs = vec![CF_DEFAULT, "cf_dynamic_level_bytes_disabled"];
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
        assert!(tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
        let tmp_cf_opts = db
            .get_options_cf("cf_dynamic_level_bytes_disabled")
            .unwrap();
        assert!(!tmp_cf_opts.get_level_compaction_dynamic_level_bytes());
    }

    fn current_is_healthy(path_str: &str) -> bool {
        matches!(
            classify_current(Path::new(path_str)),
            Ok(CurrentState::Healthy)
        )
    }

    /// Creates a DB with committed data in two CFs and closes it.
    fn build_db_with_data(path_str: &str) {
        let db = new_engine(path_str, &[CF_DEFAULT, "write"]).unwrap();
        db.put(b"k1", b"v-committed").unwrap();
        db.put_cf("write", b"wk", b"wv").unwrap();
        db.flush_cf(CF_DEFAULT, true).unwrap();
        db.flush_cf("write", true).unwrap();
        drop(db);
    }

    fn open_db(path_str: &str) -> Result<RocksEngine> {
        new_engine_opt(
            path_str,
            RocksDbOptions::default(),
            vec![
                (CF_DEFAULT, Default::default()),
                ("write", Default::default()),
            ],
        )
    }

    fn assert_data_intact(db: &RocksEngine) {
        assert_eq!(
            db.get_value(b"k1").unwrap().unwrap().as_ref(),
            b"v-committed"
        );
        assert_eq!(
            db.get_value_cf("write", b"wk").unwrap().unwrap().as_ref(),
            b"wv"
        );
    }

    #[test]
    fn test_parse_current_manifest_strict() {
        assert_eq!(
            parse_current_manifest(b"MANIFEST-000001\n"),
            Some(("MANIFEST-000001", 1))
        );
        assert_eq!(
            parse_current_manifest(b"MANIFEST-018446744073709551615\n"),
            Some(("MANIFEST-018446744073709551615", u64::MAX))
        );
        // Empty / missing newline.
        assert!(parse_current_manifest(b"").is_none());
        assert!(parse_current_manifest(b"MANIFEST-000001").is_none());
        // Whitespace, extra newlines, junk, non-digits, overflow.
        assert!(parse_current_manifest(b" MANIFEST-000001\n").is_none());
        assert!(parse_current_manifest(b"MANIFEST-000001\n\n").is_none());
        assert!(parse_current_manifest(b"MANIFEST-000001 \n").is_none());
        assert!(parse_current_manifest(b"NOT-A-MANIFEST\n").is_none());
        assert!(parse_current_manifest(b"MANIFEST-\n").is_none());
        assert!(parse_current_manifest(b"MANIFEST-abc\n").is_none());
        assert!(parse_current_manifest(b"MANIFEST-99999999999999999999\n").is_none());
        assert!(parse_current_manifest(b"\x00\x00\x00\n").is_none());
    }

    #[test]
    fn test_latest_manifest_selection() {
        let path = Builder::new()
            .prefix("rocksdb_latest_manifest")
            .tempdir()
            .unwrap();
        let dir = path.path();
        assert_eq!(latest_manifest(dir).unwrap(), None);

        // Numeric comparison must win over lexicographic (9 vs 10), invalid
        // names are ignored, and so are directories named like a MANIFEST.
        std::fs::write(dir.join("MANIFEST-000009"), b"x").unwrap();
        std::fs::write(dir.join("MANIFEST-000010"), b"x").unwrap();
        std::fs::write(dir.join("MANIFEST-1x"), b"x").unwrap();
        std::fs::write(dir.join("MANIFEST-"), b"x").unwrap();
        std::fs::create_dir(dir.join("MANIFEST-999999")).unwrap();
        assert_eq!(
            latest_manifest(dir).unwrap(),
            Some(("MANIFEST-000010".to_owned(), 10))
        );
    }

    /// Empty CURRENT (crash mid-meta-write) makes raw RocksDB open fail with
    /// "CURRENT file does not end with newline" while MANIFEST remains.
    #[test]
    fn test_truncated_current_raw_open_fails() {
        let path = Builder::new()
            .prefix("rocksdb_truncated_current_raw_fail")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        build_db_with_data(path_str);

        // Simulate incomplete CURRENT write leaving an empty file.
        let current = path.path().join("CURRENT");
        let before = std::fs::read(&current).unwrap();
        assert!(before.ends_with(b"\n"), "precondition: CURRENT valid");
        assert!(
            latest_manifest(path.path()).unwrap().is_some(),
            "precondition: MANIFEST present"
        );
        std::fs::write(&current, b"").unwrap();
        assert!(!current_is_healthy(path_str));

        // Direct open without the recovery path must fail.
        let opts = RocksDbOptions::default().into_raw();
        let err = DB::open_cf(opts, path_str, vec![(CF_DEFAULT, Default::default())]).unwrap_err();
        let msg = format!("{:?}", err);
        assert!(
            msg.contains("CURRENT") || msg.contains("Corruption") || msg.contains("newline"),
            "expected CURRENT corruption error, got: {}",
            msg
        );
    }

    /// Every truncation shape a crash can leave in CURRENT (empty, missing
    /// newline, zero-filled, garbage, extra newline) is repaired and committed
    /// data stays readable.
    #[test]
    fn test_recover_truncated_current_variants() {
        let path = Builder::new()
            .prefix("rocksdb_recover_truncated_current")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        build_db_with_data(path_str);
        let current = path.path().join("CURRENT");

        type Corruptor = fn(&[u8]) -> Vec<u8>;
        let corruptions: [(&str, Corruptor); 5] = [
            ("empty", |_| Vec::new()),
            ("missing trailing newline", |healthy| {
                healthy[..healthy.len() - 1].to_vec()
            }),
            ("zero-filled", |healthy| vec![0; healthy.len()]),
            ("garbage", |_| b"\xff\xfegarbage".to_vec()),
            ("extra newline", |healthy| [healthy, b"\n"].concat()),
        ];
        for (name, corrupt) in corruptions {
            let healthy = std::fs::read(&current).unwrap();
            std::fs::write(&current, corrupt(&healthy)).unwrap();
            assert!(!current_is_healthy(path_str), "case {}", name);

            // Recover explicitly, then verify the pointer is healthy again
            // and the engine opens with all committed data.
            assert!(
                recover_current_if_needed(path_str).unwrap(),
                "case {}",
                name
            );
            assert!(current_is_healthy(path_str), "case {}", name);
            let db = open_db(path_str).unwrap_or_else(|e| panic!("case {}: {:?}", name, e));
            assert_data_intact(&db);
            drop(db);
        }

        // End to end: new_engine_opt itself must auto-recover.
        std::fs::write(&current, b"").unwrap();
        let db = open_db(path_str).expect("new_engine_opt should recover empty CURRENT");
        assert_data_intact(&db);
    }

    /// A missing CURRENT makes `db_exist` report false, so without recovery
    /// the open path tries to create a brand-new DB: it fails on a leftover
    /// WAL file, or silently succeeds with an empty DB when no WAL remains.
    /// Recovery must reconstruct the pointer and preserve the data instead.
    #[test]
    fn test_recover_missing_current_preserves_data() {
        let path = Builder::new()
            .prefix("rocksdb_recover_missing_current")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        build_db_with_data(path_str);

        std::fs::remove_file(path.path().join("CURRENT")).unwrap();
        assert!(!db_exist(path_str), "precondition: db_exist is fooled");

        let db = open_db(path_str).unwrap();
        assert_data_intact(&db);
    }

    /// CURRENT pointing at a deleted MANIFEST is repaired only forward (to an
    /// equal-or-higher-numbered manifest), never back to an older one.
    #[test]
    fn test_dangling_current_forward_only() {
        let path = Builder::new()
            .prefix("rocksdb_dangling_current")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        build_db_with_data(path_str);
        let current = path.path().join("CURRENT");
        let healthy = std::fs::read(&current).unwrap();

        // Forward repair: named manifest is gone, a higher-numbered one exists.
        assert!(!path.path().join("MANIFEST-000000").exists());
        std::fs::write(&current, b"MANIFEST-000000\n").unwrap();
        assert!(recover_current_if_needed(path_str).unwrap());
        assert_eq!(std::fs::read(&current).unwrap(), healthy);
        let db = open_db(path_str).unwrap();
        assert_data_intact(&db);
        drop(db);

        // Rollback refused: named manifest is newer than anything on disk.
        // CURRENT must stay untouched and open must fail (status quo), not
        // silently regress to older metadata.
        let healthy = std::fs::read(&current).unwrap();
        std::fs::write(&current, b"MANIFEST-999999\n").unwrap();
        assert!(!recover_current_if_needed(path_str).unwrap());
        assert_eq!(std::fs::read(&current).unwrap(), b"MANIFEST-999999\n");
        open_db(path_str).unwrap_err();

        // Restoring the real pointer opens fine again.
        std::fs::write(&current, &healthy).unwrap();
        let db = open_db(path_str).unwrap();
        assert_data_intact(&db);
    }

    /// Healthy stores and fresh directories are never touched, and recovery
    /// is idempotent.
    #[test]
    fn test_recover_current_noop_when_healthy() {
        // Nonexistent path: nothing to do.
        assert!(!recover_current_if_needed("/nonexistent/db/path").unwrap());

        // Fresh empty dir: untouched, normal creation still works after.
        let path = Builder::new()
            .prefix("rocksdb_recover_current_noop")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        assert!(!recover_current_if_needed(path_str).unwrap());
        build_db_with_data(path_str);

        // Healthy store: byte-identical no-op, idempotently.
        let current = path.path().join("CURRENT");
        let healthy = std::fs::read(&current).unwrap();
        assert!(!recover_current_if_needed(path_str).unwrap());
        assert!(!recover_current_if_needed(path_str).unwrap());
        assert_eq!(std::fs::read(&current).unwrap(), healthy);
    }

    /// A temp file orphaned by a crash during a previous recovery attempt
    /// does not prevent recovering again.
    #[test]
    fn test_recover_current_with_orphaned_tmp() {
        let path = Builder::new()
            .prefix("rocksdb_recover_orphaned_tmp")
            .tempdir()
            .unwrap();
        let path_str = path.path().to_str().unwrap();
        build_db_with_data(path_str);

        let tmp = path.path().join(CURRENT_RECOVER_TMP);
        std::fs::write(&tmp, b"stale junk from interrupted recovery").unwrap();
        std::fs::write(path.path().join("CURRENT"), b"").unwrap();

        let db = open_db(path_str).unwrap();
        assert_data_intact(&db);
        assert!(!tmp.exists(), "tmp must be consumed by the rename");
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
