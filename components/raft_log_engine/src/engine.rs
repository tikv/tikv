// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use futures::future::BoxFuture;
use std::fs;
use std::path::Path;

use engine_traits::metrics::*;
use engine_traits::{CacheStats, RaftEngine, RaftLogBatch as RaftLogBatchTrait, Result};
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;
use raft_engine::{EntryExt, Error as RaftEngineError, LogBatch, RaftLogEngine as RawRaftEngine};

pub use raft_engine::{Config as RaftEngineConfig, RecoveryMode};

#[derive(Clone)]
pub struct EntryExtTyped;

impl EntryExt<Entry> for EntryExtTyped {
    fn index(e: &Entry) -> u64 {
        e.index
    }
}

#[derive(Clone)]
pub struct RaftLogEngine(RawRaftEngine<Entry, EntryExtTyped>);

impl RaftLogEngine {
    pub fn new(config: RaftEngineConfig) -> Self {
        RaftLogEngine(RawRaftEngine::new(config))
    }

    /// If path is not an empty directory, we say db exists.
    pub fn exists(path: &str) -> bool {
        let path = Path::new(path);
        if !path.exists() || !path.is_dir() {
            return false;
        }
        fs::read_dir(&path).unwrap().next().is_some()
    }
}

#[derive(Default)]
pub struct RaftLogBatch(LogBatch<Entry, EntryExtTyped>);

const RAFT_LOG_STATE_KEY: &[u8] = b"R";

impl RaftLogBatchTrait for RaftLogBatch {
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        self.0.add_entries(raft_group_id, entries);
        Ok(())
    }

    fn cut_logs(&mut self, _: u64, _: u64, _: u64) {
        // It's unnecessary because overlapped entries can be handled in `append`.
    }

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        box_try!(self
            .0
            .put_msg(raft_group_id, RAFT_LOG_STATE_KEY.to_vec(), state));
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.0.items.is_empty()
    }
}

impl RaftEngine for RaftLogEngine {
    type LogBatch = RaftLogBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        RaftLogBatch::default()
    }

    fn sync(&self) -> Result<()> {
        box_try!(self.0.sync());
        Ok(())
    }

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        let state = box_try!(self.0.get_msg(raft_group_id, RAFT_LOG_STATE_KEY));
        Ok(state)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.0
            .get_entry(raft_group_id, index)
            .map_err(|e| transfer_error(e))
    }

    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize> {
        self.0
            .fetch_entries_to(raft_group_id, begin, end, max_size, to)
            .map_err(|e| transfer_error(e))
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        let ret = box_try!(self.0.write(&mut batch.0, sync));
        Ok(ret)
    }

    fn async_write(&self, batch: Self::LogBatch, sync: bool) -> BoxFuture<'static, Result<usize>> {
        let f = self.0.async_write(batch.0, sync);
        Box::pin(async move {
            let ret = box_try!(f.await);
            Ok(ret)
        })
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        let ret = box_try!(self.0.write(&mut batch.0, sync));
        Ok(ret)
    }

    fn clean(
        &self,
        raft_group_id: u64,
        _: &RaftLocalState,
        batch: &mut RaftLogBatch,
    ) -> Result<()> {
        batch.0.clean_region(raft_group_id);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let mut batch = Self::LogBatch::default();
        batch.0.add_entries(raft_group_id, entries);
        let ret = box_try!(self.0.write(&mut batch.0, false));
        Ok(ret)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        box_try!(self.0.put_msg(raft_group_id, RAFT_LOG_STATE_KEY, state));
        Ok(())
    }

    fn gc(&self, raft_group_id: u64, _from: u64, to: u64) -> Result<usize> {
        Ok(self.0.compact_to(raft_group_id, to) as usize)
    }

    fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let ret = box_try!(self.0.purge_expired_files());
        Ok(ret)
    }

    fn has_builtin_entry_cache(&self) -> bool {
        true
    }

    fn gc_entry_cache(&self, raft_group_id: u64, to: u64) {
        self.0.compact_cache_to(raft_group_id, to)
    }
    /// Flush current cache stats.
    fn flush_stats(&self) -> Option<CacheStats> {
        let stat = self.0.flush_cache_stats();
        Some(engine_traits::CacheStats {
            hit: stat.hit,
            miss: stat.miss,
            cache_size: stat.cache_size,
        })
    }

    fn stop(&self) {
        self.0.stop();
    }

    fn flush_metrics(&self, instance: &str) {
        let name_enum = match instance {
            "kv" => TickerName::kv,
            "raft" => TickerName::raft,
            unexpected => panic!(format!("unexpected name {}", unexpected)),
        };

        if let Ok(statistic) = block_on(self.0.async_get_metric()) {
            STORE_ENGINE_WAL_FILE_SYNCED
                .get(name_enum)
                .inc_by(statistic.freq as i64);

            STORE_ENGINE_WRITE_VEC
                .with_label_values(&[instance, "write_average"])
                .set(statistic.avg_write_cost as f64);
            STORE_ENGINE_WRITE_VEC
                .with_label_values(&[instance, "write_max"])
                .set(statistic.max_write_cost as f64);

            STORE_ENGINE_WRITE_WAL_TIME_VEC
                .with_label_values(&[instance, "write_wal_micros_average"])
                .set((statistic.wal_cost / statistic.freq) as f64);
            STORE_ENGINE_WRITE_WAL_TIME_VEC
                .with_label_values(&[instance, "write_wal_micros_max"])
                .set(statistic.max_wal_cost as f64);

            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[instance, "wal_file_sync_average"])
                .set((statistic.sync_cost / statistic.freq) as f64);
            STORE_ENGINE_WAL_FILE_SYNC_MICROS_VEC
                .with_label_values(&[instance, "wal_file_sync_max"])
                .set(statistic.max_sync_cost as f64);
        }
    }

    fn dump_stats(&self) -> Result<String> {
        // Raft engine won't dump anything.
        Ok("".to_owned())
    }
}

fn transfer_error(e: RaftEngineError) -> engine_traits::Error {
    match e {
        RaftEngineError::StorageCompacted => engine_traits::Error::EntriesCompacted,
        RaftEngineError::StorageUnavailable => engine_traits::Error::EntriesUnavailable,
        e => {
            let e = box_err!(e);
            engine_traits::Error::Other(e)
        }
    }
}
