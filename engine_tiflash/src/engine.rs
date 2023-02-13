// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

// Disable warnings for unused engine_rocks's feature.
#![allow(dead_code)]
#![allow(unused_variables)]
use std::{
    fmt::{self, Debug, Formatter},
    fs,
    ops::Deref,
    path::Path,
    sync::{
        atomic::{AtomicIsize, Ordering},
        Arc,
    },
};

use engine_rocks::{RocksDbVector, RocksEngineIterator, RocksSnapshot};
use engine_traits::{
    Checkpointable, Checkpointer, DbVector, Error, IterOptions, Iterable, KvEngine, Peekable,
    ReadOptions, Result, SyncMutable,
};
use rocksdb::{Writable, DB};

use crate::{
    proxy_utils::{engine_ext::*, EngineStoreHub},
    r2e,
    util::get_cf_handle,
};

// This struct should be safe to copy.
#[derive(Clone)]
pub struct ProxyEngineExt {
    pub engine_store_server_helper: isize,
    pub pool_capacity: usize,
    pub pending_applies_count: Arc<AtomicIsize>,
    pub ffi_hub: Option<Arc<dyn EngineStoreHub + Send + Sync>>,
    pub config_set: Option<Arc<crate::ProxyConfigSet>>,
    pub cached_region_info_manager: Option<Arc<crate::CachedRegionInfoManager>>,
}

impl Default for ProxyEngineExt {
    fn default() -> Self {
        ProxyEngineExt {
            engine_store_server_helper: 0,
            pool_capacity: 0,
            pending_applies_count: Arc::new(AtomicIsize::new(0)),
            ffi_hub: None,
            config_set: None,
            cached_region_info_manager: None,
        }
    }
}

#[derive(Clone)]
pub struct RocksEngine {
    // Must ensure rocks is the first field, for RocksEngine::from_ref.
    // We must own a engine_rocks::RocksEngine, since TiKV has not decouple from engine_rocks yet.
    pub rocks: engine_rocks::RocksEngine,
    pub proxy_ext: ProxyEngineExt,
    pub ps_ext: Option<PageStorageExt>,
}

impl std::fmt::Debug for RocksEngine {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TiFlashEngine")
            .field("rocks", &self.rocks)
            .field(
                "engine_store_server_helper",
                &self.proxy_ext.engine_store_server_helper,
            )
            .finish()
    }
}

impl RocksEngine {
    pub(crate) fn new(db: DB) -> RocksEngine {
        RocksEngine::from_db(Arc::new(db))
    }

    pub fn init(
        &mut self,
        engine_store_server_helper: isize,
        snap_handle_pool_size: usize,
        ffi_hub: Option<Arc<dyn EngineStoreHub + Send + Sync>>,
        config_set: Option<Arc<crate::ProxyConfigSet>>,
    ) {
        #[cfg(feature = "enable-pagestorage")]
        tikv_util::info!("enabled pagestorage");
        #[cfg(not(feature = "enable-pagestorage"))]
        tikv_util::info!("disabled pagestorage");
        self.proxy_ext = ProxyEngineExt {
            engine_store_server_helper,
            pool_capacity: snap_handle_pool_size,
            pending_applies_count: Arc::new(AtomicIsize::new(0)),
            ffi_hub,
            config_set,
            cached_region_info_manager: Some(Arc::new(crate::CachedRegionInfoManager::new())),
        };
        self.ps_ext = Some(PageStorageExt {
            engine_store_server_helper,
        });
    }

    pub fn from_rocks(rocks: engine_rocks::RocksEngine) -> Self {
        RocksEngine {
            rocks,
            proxy_ext: ProxyEngineExt::default(),
            ps_ext: None,
        }
    }

    pub fn from_db(db: Arc<DB>) -> Self {
        RocksEngine {
            rocks: engine_rocks::RocksEngine::from_db(db),
            proxy_ext: ProxyEngineExt::default(),
            ps_ext: None,
        }
    }

    pub fn from_ref(db: &Arc<DB>) -> &Self {
        unsafe { &*(db as *const Arc<DB> as *const RocksEngine) }
    }

    pub fn as_inner(&self) -> &Arc<DB> {
        self.rocks.as_inner()
    }

    pub fn get_sync_db(&self) -> Arc<DB> {
        self.rocks.get_sync_db()
    }

    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }

        // If path is not an empty directory, we say db exists. If path is not an empty
        // directory but db has not been created, `DB::list_column_families`
        // fails and we can clean up the directory by this indication.
        fs::read_dir(&path).unwrap().next().is_some()
    }

    pub fn support_multi_batch_write(&self) -> bool {
        self.rocks.support_multi_batch_write()
    }
}

impl KvEngine for RocksEngine {
    type Snapshot = RocksSnapshot;

    fn snapshot(&self) -> RocksSnapshot {
        self.rocks.snapshot()
    }

    fn sync(&self) -> Result<()> {
        self.rocks.sync()
    }

    fn flush_metrics(&self, instance: &str) {
        self.rocks.flush_metrics(instance);
    }

    fn bad_downcast<T: 'static>(&self) -> &T {
        self.rocks.bad_downcast()
    }

    // The whole point is:
    // 1. When `handle_pending_applies` is called by `on_timeout`, we can handle at
    // least one. 2. When `handle_pending_applies` is called when we receive a
    // new task,    or when `handle_pending_applies` need to handle multiple
    // snapshots.    We need to compare to what's in queue.

    fn can_apply_snapshot(&self, is_timeout: bool, new_batch: bool, region_id: u64) -> bool {
        fail::fail_point!("on_can_apply_snapshot", |e| e
            .unwrap()
            .parse::<bool>()
            .unwrap());
        if let Some(s) = self.proxy_ext.config_set.as_ref() {
            if s.engine_store.enable_fast_add_peer {
                // TODO Return true if this is an empty snapshot.
                // We need to test if the region is still in fast add peer mode.
                let result = self
                    .proxy_ext
                    .cached_region_info_manager
                    .as_ref()
                    .expect("expect cached_region_info_manager")
                    .get_inited_or_fallback(region_id);
                match result {
                    Some(true) => {
                        // Do nothing.
                        tikv_util::debug!("can_apply_snapshot no fast path. do normal checking";
                            "region_id" => region_id,
                        );
                    }
                    None | Some(false) => {
                        // Otherwise, try fast path.
                        return true;
                    }
                };
            }
        }
        // is called after calling observer's pre_handle_snapshot
        let in_queue = self.proxy_ext.pending_applies_count.load(Ordering::SeqCst);
        if is_timeout && new_batch {
            // If queue is full, we should begin to handle
            true
        } else {
            // Otherwise, we wait until the queue is full.
            // In order to batch more tasks.
            in_queue > (self.proxy_ext.pool_capacity as isize)
        }
    }
}

impl Iterable for RocksEngine {
    type Iterator = RocksEngineIterator;

    #[cfg(feature = "enable-pagestorage")]
    fn scan<F>(
        &self,
        cf: &str,
        start_key: &[u8],
        end_key: &[u8],
        fill_cache: bool,
        f: F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], &[u8]) -> Result<bool>,
    {
        let mut f = f;
        self.ps_ext
            .as_ref()
            .unwrap()
            .scan_page(start_key.into(), end_key.into(), &mut f);
        Ok(())
    }

    fn iterator_opt(&self, cf: &str, opts: IterOptions) -> Result<Self::Iterator> {
        self.rocks.iterator_opt(cf, opts)
    }
}

pub struct PsDbVector(Vec<u8>);

impl PsDbVector {
    pub fn from_raw(raw: Vec<u8>) -> PsDbVector {
        PsDbVector(raw)
    }
}

impl DbVector for PsDbVector {}

impl Deref for PsDbVector {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.0
    }
}

impl Debug for PsDbVector {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:?}", &**self)
    }
}

impl<'a> PartialEq<&'a [u8]> for PsDbVector {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl Peekable for RocksEngine {
    #[cfg(not(feature = "enable-pagestorage"))]
    type DbVector = RocksDbVector;

    #[cfg(not(feature = "enable-pagestorage"))]
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<RocksDbVector>> {
        self.rocks.get_value_opt(opts, key)
    }

    #[cfg(not(feature = "enable-pagestorage"))]
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<RocksDbVector>> {
        self.rocks.get_value_cf_opt(opts, cf, key)
    }

    #[cfg(feature = "enable-pagestorage")]
    type DbVector = PsDbVector;

    #[cfg(feature = "enable-pagestorage")]
    fn get_value_opt(&self, opts: &ReadOptions, key: &[u8]) -> Result<Option<PsDbVector>> {
        let result = self.ps_ext.as_ref().unwrap().read_page(key);
        return match result {
            None => Ok(None),
            Some(v) => Ok(Some(PsDbVector::from_raw(v))),
        };
    }

    #[cfg(feature = "enable-pagestorage")]
    fn get_value_cf_opt(
        &self,
        opts: &ReadOptions,
        cf: &str,
        key: &[u8],
    ) -> Result<Option<PsDbVector>> {
        self.get_value_opt(opts, key)
    }
}

impl RocksEngine {
    fn do_write(&self, cf: &str, key: &[u8]) -> bool {
        crate::do_write(cf, key)
    }
}

impl SyncMutable for RocksEngine {
    #[cfg(not(feature = "enable-pagestorage"))]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.get_sync_db().put(key, value).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(not(feature = "enable-pagestorage"))]
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let db = self.rocks.get_sync_db();
            let handle = get_cf_handle(&db, cf)?;
            return self
                .rocks
                .get_sync_db()
                .put_cf(handle, key, value)
                .map_err(r2e);
        }
        Ok(())
    }

    #[cfg(not(feature = "enable-pagestorage"))]
    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            return self.rocks.get_sync_db().delete(key).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(not(feature = "enable-pagestorage"))]
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let db = self.rocks.get_sync_db();
            let handle = get_cf_handle(&db, cf)?;
            return self.rocks.get_sync_db().delete_cf(handle, key).map_err(r2e);
        }
        Ok(())
    }

    #[cfg(feature = "enable-pagestorage")]
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(feature = "enable-pagestorage")]
    fn put_cf(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_put_page(ps_wb.ptr, key, value);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(feature = "enable-pagestorage")]
    fn delete(&self, key: &[u8]) -> Result<()> {
        if self.do_write(engine_traits::CF_DEFAULT, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    #[cfg(feature = "enable-pagestorage")]
    fn delete_cf(&self, cf: &str, key: &[u8]) -> Result<()> {
        if self.do_write(cf, key) {
            let ps_wb = self.ps_ext.as_ref().unwrap().create_write_batch();
            self.ps_ext
                .as_ref()
                .unwrap()
                .write_batch_del_page(ps_wb.ptr, key);
            self.ps_ext.as_ref().unwrap().consume_write_batch(ps_wb.ptr);
        }
        Ok(())
    }

    fn delete_range(&self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }

    fn delete_range_cf(&self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        // do nothing
        Ok(())
    }
}

pub struct TiFlashCheckpointer {}

impl Checkpointable for RocksEngine {
    type Checkpointer = TiFlashCheckpointer;

    fn new_checkpointer(&self) -> Result<Self::Checkpointer> {
        Err(Error::Other("TiFlash don't support Checkpointable".into()))
    }
}

impl Checkpointer for TiFlashCheckpointer {
    fn create_at(
        &mut self,
        db_out_dir: &Path,
        titan_out_dir: Option<&Path>,
        log_size_for_flush: u64,
    ) -> Result<()> {
        Err(Error::Other(
            "TiFlash don't support Checkpointer::create_at".into(),
        ))
    }
}
