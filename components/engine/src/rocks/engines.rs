use std::sync::Arc;
use std::path::Path;
use std::time::Duration;

use crate::{Engines, errors::{Result, Error}};
use super::{DB, util::{self, CFOptions, metrics_flusher::{MetricsFlusher, DEFAULT_FLUSHER_INTERVAL}}, WriteBatch, WriteOptions, DBOptions};

#[derive(Clone, Debug)]
pub struct RocksEngines {
    pub kv: Arc<DB>,
    pub raft: Arc<DB>,
    pub shared_block_cache: bool,
}

impl RocksEngines {
    pub fn new(kv_opts: DBOptions, kv_cfs_opts: Vec<CFOptions>, raft_opts: DBOptions, 
        raft_cfs_opts: Vec<CFOptions>, shared_block_cache: bool, data_dir: &str, raftdb_path: &str) -> Self {

        let store_path = Path::new(data_dir);

        let raft_engine = util::new_engine_opt(
            Path::new(raftdb_path),
            raft_opts,
            raft_cfs_opts,
        )
        .unwrap_or_else(|s| panic!("failed to create raft engine: {}", s));

        // Create kv engine, storage.
        let kv_engine = util::new_engine_opt(store_path, kv_opts, kv_cfs_opts)
            .unwrap_or_else(|s| panic!("failed to create kv engine: {}", s));

        Self {
            kv: Arc::new(kv_engine),
            raft: Arc::new(raft_engine),
            shared_block_cache,
        }
    }

    pub fn new_from_dbs(kv: Arc<DB>, raft: Arc<DB>, shared_block_cache: bool) -> Self {
        Self {
            kv,
            raft,
            shared_block_cache,
        }
    }

    pub fn new_metrics_flusher(&self) -> MetricsFlusher {
        MetricsFlusher::new(self.kv.clone(), self.raft.clone(), self.shared_block_cache, 
            Duration::from_millis(DEFAULT_FLUSHER_INTERVAL))
    }
}

impl Engines for RocksEngines {
    fn shared_block_cache(&self) -> bool {
        self.shared_block_cache
    }

    fn write_kv(&self, wb: &WriteBatch) -> Result<()> {
        self.kv.write(wb).map_err(Error::RocksDb)
    }

    fn write_kv_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.kv.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    fn sync_kv(&self) -> Result<()> {
        self.kv.sync_wal().map_err(Error::RocksDb)
    }

    fn write_raft(&self, wb: &WriteBatch) -> Result<()> {
        self.raft.write(wb).map_err(Error::RocksDb)
    }

    fn write_raft_opt(&self, wb: &WriteBatch, opts: &WriteOptions) -> Result<()> {
        self.raft.write_opt(wb, opts).map_err(Error::RocksDb)
    }

    fn sync_raft(&self) -> Result<()> {
        self.raft.sync_wal().map_err(Error::RocksDb)
    }

    fn kv(&self) -> &Arc<DB> {
        &self.kv
    }

    fn raft(&self) -> &Arc<DB> {
        &self.raft
    }
}
