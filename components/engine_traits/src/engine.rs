// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    fmt::Debug,
    io::Write,
    path::{Path, PathBuf},
    str,
    vec::Vec,
};

use tikv_util::error;

use crate::*;

// FIXME: Revisit the remaining types and methods on KvEngine. Some of these are
// here for lack of somewhere better to put them at the time of writing.
// Consider moving everything into other traits and making KvEngine essentially
// a trait typedef.

/// A TiKV key-value store
pub trait KvEngine:
    Peekable
    + SyncMutable
    + Iterable
    + WriteBatchExt
    + DbOptionsExt
    + CfNamesExt
    + CfOptionsExt
    + ImportExt
    + SstExt
    + CompactExt
    + RangePropertiesExt
    + MvccPropertiesExt
    + TtlPropertiesExt
    + TablePropertiesExt
    + PerfContextExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + Unpin
    + Checkpointable
    + 'static
{
    /// A consistent read-only snapshot of the database
    type Snapshot: Snapshot;

    /// Create a snapshot
    fn snapshot(&self) -> Self::Snapshot;

    /// Syncs any writes to disk
    fn sync(&self) -> Result<()>;

    /// Flush metrics to prometheus
    ///
    /// `instance` is the label of the metric to flush.
    fn flush_metrics(&self, _instance: &str) {}

    /// Reset internal statistics
    fn reset_statistics(&self) {}

    /// Cast to a concrete engine type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;

    /// Returns false if KvEngine can't apply snapshot for this region now.
    /// Some KvEngines need to do some transforms before apply data from
    /// snapshot. These procedures can be batched in background if there are
    /// more than one incoming snapshots, thus not blocking applying thread.
    fn can_apply_snapshot(&self, _is_timeout: bool, _new_batch: bool, _region_id: u64) -> bool {
        true
    }
}

/// TabletAccessor is the trait to access all the tablets with provided accessor
///
/// For single rocksdb instance, it essentially accesses the global kvdb with
/// the accessor For multi rocksdb instances, it accesses all the tablets with
/// the accessor
pub trait TabletAccessor<EK> {
    /// Loop visit all opened tablets by the specified function.
    fn for_each_opened_tablet(&self, _f: &mut (dyn FnMut(u64, u64, &EK)));

    /// return true if it's single engine;
    /// return false if it's a multi-tablet factory;
    fn is_single_engine(&self) -> bool;
}

/// max error count to log
const MAX_ERROR_COUNT: u32 = 5;

/// TabletErrorCollector is the facility struct to handle errors when using
/// TabletAccessor::for_each_opened_tablet
///
/// It will choose the last failed result as the final result, meanwhile logging
/// errors up to MAX_ERROR_COUNT.
pub struct TabletErrorCollector {
    errors: Vec<u8>,
    max_error_count: u32,
    error_count: u32,
    result: std::result::Result<(), Box<dyn std::error::Error>>,
}

impl TabletErrorCollector {
    pub fn new() -> Self {
        Self {
            errors: vec![],
            max_error_count: MAX_ERROR_COUNT,
            error_count: 0,
            result: Ok(()),
        }
    }

    pub fn add_result(&mut self, region_id: u64, suffix: u64, result: Result<()>) {
        if result.is_ok() {
            return;
        }
        self.result = Err(Box::from(result.err().unwrap()));
        self.error_count += 1;
        if self.error_count > self.max_error_count {
            return;
        }
        writeln!(
            &mut self.errors,
            "Tablet {}_{} encountered error: {:?}.",
            region_id, suffix, self.result
        )
        .unwrap();
    }

    fn flush_error(&self) {
        if self.error_count > 0 {
            error!(
                "Total count {}. Sample errors: {}",
                self.error_count,
                str::from_utf8(&self.errors).unwrap()
            );
        }
    }

    pub fn take_result(&mut self) -> std::result::Result<(), Box<dyn std::error::Error>> {
        std::mem::replace(&mut self.result, Ok(()))
    }

    pub fn get_error_count(&self) -> u32 {
        self.error_count
    }
}

impl Default for TabletErrorCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TabletErrorCollector {
    fn drop(&mut self) {
        self.flush_error()
    }
}

/// OpenOptionsn is used for specifiying the way of opening a tablet.
#[derive(Default, Clone)]
pub struct OpenOptions {
    // create tablet if non-exist
    create: bool,
    create_new: bool,
    read_only: bool,
    cache_only: bool,
    skip_cache: bool,
}

impl OpenOptions {
    /// Sets the option to create a tablet, or open it if it already exists.
    pub fn set_create(mut self, create: bool) -> Self {
        self.create = create;
        self
    }

    /// Sets the option to create a new tablet, failing if it already exists.
    pub fn set_create_new(mut self, create_new: bool) -> Self {
        self.create_new = create_new;
        self
    }

    /// Sets the option for read only
    pub fn set_read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    /// Sets the option for only reading from cache.
    pub fn set_cache_only(mut self, cache_only: bool) -> Self {
        self.cache_only = cache_only;
        self
    }

    /// Sets the option to open a tablet without updating the cache.
    pub fn set_skip_cache(mut self, skip_cache: bool) -> Self {
        self.skip_cache = skip_cache;
        self
    }

    pub fn create(&self) -> bool {
        self.create
    }

    pub fn create_new(&self) -> bool {
        self.create_new
    }

    pub fn read_only(&self) -> bool {
        self.read_only
    }

    pub fn cache_only(&self) -> bool {
        self.cache_only
    }

    pub fn skip_cache(&self) -> bool {
        self.skip_cache
    }
}

pub const SPLIT_PREFIX: &str = "split_";
pub const MERGE_PREFIX: &str = "merge_";

/// A factory trait to create new engine.
// It should be named as `EngineFactory` for consistency, but we are about to
// rename engine to tablet, so always use tablet for new traits/types.
pub trait TabletFactory<EK>: TabletAccessor<EK> + Send + Sync {
    /// Open the tablet with id and suffix according to the OpenOptions.
    ///
    /// The id is likely the region Id, the suffix could be the current raft log
    /// index. They together could specify a unique path for a region's
    /// tablet. The reason to have suffix is that we can keep more than one
    /// tablet for a region.
    fn open_tablet(&self, id: u64, suffix: Option<u64>, options: OpenOptions) -> Result<EK>;

    /// Open tablet by raw path without updating cache.
    fn open_tablet_raw(
        &self,
        path: &Path,
        id: u64,
        suffix: u64,
        options: OpenOptions,
    ) -> Result<EK>;

    /// Create the shared db for v1
    fn create_shared_db(&self) -> Result<EK>;

    /// Destroy the tablet and its data
    fn destroy_tablet(&self, id: u64, suffix: u64) -> Result<()>;

    /// Check if the tablet with specified id/suffix exists
    #[inline]
    fn exists(&self, id: u64, suffix: u64) -> bool {
        self.exists_raw(&self.tablet_path(id, suffix))
    }

    /// Check if the tablet with specified path exists
    fn exists_raw(&self, path: &Path) -> bool;

    /// Get the tablet path by id and suffix
    fn tablet_path(&self, id: u64, suffix: u64) -> PathBuf {
        self.tablet_path_with_prefix("", id, suffix)
    }

    /// Get the tablet path by id and suffix
    ///
    /// Used in special situations
    /// Ex: split/merge.
    fn tablet_path_with_prefix(&self, prefix: &str, id: u64, suffix: u64) -> PathBuf;

    /// Tablets root path
    fn tablets_path(&self) -> PathBuf;

    /// Load the tablet from path for id and suffix--for scenarios such as
    /// applying snapshot
    fn load_tablet(&self, _path: &Path, _id: u64, _suffix: u64) -> Result<EK> {
        unimplemented!();
    }

    /// Mark the tablet with specified id and suffix tombostone
    fn mark_tombstone(&self, _id: u64, _suffix: u64) {
        unimplemented!();
    }

    /// Check if the tablet with specified id and suffix tombostone
    fn is_tombstoned(&self, _region_id: u64, _suffix: u64) -> bool {
        unimplemented!();
    }

    fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()>;
}

pub struct DummyFactory<EK>
where
    EK: CfOptionsExt + Clone + Send + 'static,
{
    pub engine: Option<EK>,
    pub root_path: String,
}

impl<EK> TabletFactory<EK> for DummyFactory<EK>
where
    EK: CfOptionsExt + Clone + Send + Sync + 'static,
{
    fn create_shared_db(&self) -> Result<EK> {
        Ok(self.engine.as_ref().unwrap().clone())
    }

    fn open_tablet(&self, _id: u64, _suffix: Option<u64>, _options: OpenOptions) -> Result<EK> {
        Ok(self.engine.as_ref().unwrap().clone())
    }

    fn open_tablet_raw(
        &self,
        _path: &Path,
        _id: u64,
        _suffix: u64,
        _options: OpenOptions,
    ) -> Result<EK> {
        Ok(self.engine.as_ref().unwrap().clone())
    }

    fn destroy_tablet(&self, _id: u64, _suffix: u64) -> Result<()> {
        Ok(())
    }

    fn exists_raw(&self, _path: &Path) -> bool {
        true
    }

    fn tablet_path_with_prefix(&self, _prefix: &str, _id: u64, _suffix: u64) -> PathBuf {
        PathBuf::from(&self.root_path)
    }

    fn tablets_path(&self) -> PathBuf {
        PathBuf::from(&self.root_path)
    }

    fn set_shared_block_cache_capacity(&self, capacity: u64) -> Result<()> {
        let opt = self
            .engine
            .as_ref()
            .unwrap()
            .get_options_cf(CF_DEFAULT)
            .unwrap(); // FIXME unwrap
        opt.set_block_cache_capacity(capacity)
    }
}

impl<EK> TabletAccessor<EK> for DummyFactory<EK>
where
    EK: CfOptionsExt + Clone + Send + 'static,
{
    fn for_each_opened_tablet(&self, f: &mut dyn FnMut(u64, u64, &EK)) {
        if let Some(engine) = &self.engine {
            f(0, 0, engine);
        }
    }

    fn is_single_engine(&self) -> bool {
        true
    }
}

impl<EK> DummyFactory<EK>
where
    EK: CfOptionsExt + Clone + Send + 'static,
{
    pub fn new(engine: Option<EK>, root_path: String) -> DummyFactory<EK> {
        DummyFactory { engine, root_path }
    }
}

impl<EK: CfOptionsExt + Clone + Send + 'static> Default for DummyFactory<EK> {
    fn default() -> Self {
        Self::new(None, "/tmp".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tablet_error_collector_ok() {
        let mut err = TabletErrorCollector::new();
        err.add_result(1, 1, Ok(()));
        err.take_result().unwrap();
        assert_eq!(err.get_error_count(), 0);
    }

    #[test]
    fn test_tablet_error_collector_err() {
        let mut err = TabletErrorCollector::new();
        err.add_result(1, 1, Ok(()));
        err.add_result(1, 1, Err(Status::with_code(Code::Aborted).into()));
        err.add_result(1, 1, Err(Status::with_code(Code::NotFound).into()));
        err.add_result(1, 1, Ok(()));
        err.take_result().unwrap_err();
        assert_eq!(err.get_error_count(), 2);
    }
}
