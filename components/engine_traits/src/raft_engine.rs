use crate::*;
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;

pub trait RaftEngine: Clone + Sync + Send + 'static {
    type LogBatch: RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Synchronize the Raft engine.
    fn sync(&self) -> Result<()>;

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>>;

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    /// Return count of fetched entries.
    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Consume the write batch by moving the content into the engine itself
    /// and return written bytes.
    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize>;

    /// Like `consume` but shrink `batch` if need.
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<usize>;

    fn clean(
        &self,
        raft_group_id: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some log entries and retrun written bytes.
    ///
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

    /// Append some log entries and retrun written bytes.
    ///
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append_slice(&self, raft_group_id: u64, entries: &[Entry]) -> Result<usize> {
        self.append(raft_group_id, entries.to_vec())
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    /// Like `cut_logs` but the range could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    fn purge_expired_files(&self) -> Result<Vec<u64>>;

    /// The `RaftEngine` has a builtin entry cache or not.
    fn has_builtin_entry_cache(&self) -> bool {
        false
    }

    /// GC the builtin entry cache.
    fn gc_entry_cache(&self, _raft_group_id: u64, _to: u64) {}

    fn flush_metrics(&self, _instance: &str) {}
    fn flush_stats(&self) -> Option<CacheStats> {
        None
    }
    fn reset_statistics(&self) {}

    fn stop(&self) {}

    fn dump_stats(&self) -> Result<String>;
}

pub trait RaftLogBatch: Send {
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

    /// Note: `RaftLocalState` won't be updated in this call.
    fn append_slice(&mut self, raft_group_id: u64, entries: &[Entry]) -> Result<()> {
        self.append(raft_group_id, entries.to_vec())
    }

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64);

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;

    fn is_empty(&self) -> bool;
}

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}

/// Reexports used by `def_raft_engine`
pub mod raft_engine_impl_reexports {
    pub use keys;
    pub use kvproto;
    pub use protobuf;
    pub use raft;
    pub use tikv_util;
}

/// Macro for implementing the `RaftEngine` and `RaftLogBatch` traits.
///
/// These needs to be implemented for all engines, and can be implemented in
/// terms of just the engine traits, without depending on engine-specific
/// internals, but can't currently be implemented as blanket impls.
///
/// This macro makes implementing the traits a one-liner.
#[macro_export]
macro_rules! def_raft_engine {
    ($engine_name:ident, $write_batch_name:ident) => {

        use $crate::{Error, RaftEngine, RaftLogBatch, Result};
        use $crate::{
            Iterable, KvEngine, MiscExt, Mutable, Peekable, SyncMutable, WriteBatchExt, WriteOptions,
            CF_DEFAULT,
        };
        use $crate::raft_engine_impl_reexports::{
            keys,
            kvproto::raft_serverpb::RaftLocalState,
            protobuf::Message,
            raft::eraftpb::Entry,
            tikv_util::{box_err, box_try},
        };

        const RAFT_LOG_MULTI_GET_CNT: u64 = 8;

        impl RaftEngine for $engine_name {
            type LogBatch = $write_batch_name;

            fn log_batch(&self, capacity: usize) -> Self::LogBatch {
                self.write_batch_with_cap(capacity)
            }

            fn sync(&self) -> Result<()> {
                self.sync_wal()
            }

            fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
                let key = keys::raft_state_key(raft_group_id);
                self.get_msg_cf(CF_DEFAULT, &key)
            }

            fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
                let key = keys::raft_log_key(raft_group_id, index);
                self.get_msg_cf(CF_DEFAULT, &key)
            }

            fn fetch_entries_to(
                &self,
                region_id: u64,
                low: u64,
                high: u64,
                max_size: Option<usize>,
                buf: &mut Vec<Entry>,
            ) -> Result<usize> {
                let (max_size, mut total_size, mut count) = (max_size.unwrap_or(usize::MAX), 0, 0);

                if high - low <= RAFT_LOG_MULTI_GET_CNT {
                    // If election happens in inactive regions, they will just try to fetch one empty log.
                    for i in low..high {
                        if total_size > 0 && total_size >= max_size {
                            break;
                        }
                        let key = keys::raft_log_key(region_id, i);
                        match self.get_value(&key) {
                            Ok(None) => return Err(Error::EntriesCompacted),
                            Ok(Some(v)) => {
                                let mut entry = Entry::default();
                                entry.merge_from_bytes(&v)?;
                                assert_eq!(entry.get_index(), i);
                                buf.push(entry);
                                total_size += v.len();
                                count += 1;
                            }
                            Err(e) => return Err(box_err!(e)),
                        }
                    }
                    return Ok(count);
                }

                let (mut check_compacted, mut next_index) = (true, low);
                let start_key = keys::raft_log_key(region_id, low);
                let end_key = keys::raft_log_key(region_id, high);
                self.scan(
                    &start_key,
                    &end_key,
                    true, // fill_cache
                    |_, value| {
                        let mut entry = Entry::default();
                        entry.merge_from_bytes(value)?;

                        if check_compacted {
                            if entry.get_index() != low {
                                // May meet gap or has been compacted.
                                return Ok(false);
                            }
                            check_compacted = false;
                        } else {
                            assert_eq!(entry.get_index(), next_index);
                        }
                        next_index += 1;

                        buf.push(entry);
                        total_size += value.len();
                        count += 1;
                        Ok(total_size < max_size)
                    },
                )?;

                // If we get the correct number of entries, returns.
                // Or the total size almost exceeds max_size, returns.
                if count == (high - low) as usize || total_size >= max_size {
                    return Ok(count);
                }

                // Here means we don't fetch enough entries.
                Err(Error::EntriesUnavailable)
            }

            fn consume(&self, batch: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
                let bytes = batch.data_size();
                let mut opts = WriteOptions::default();
                opts.set_sync(sync_log);
                self.write_opt(batch, &opts)?;
                batch.clear();
                Ok(bytes)
            }

            fn consume_and_shrink(
                &self,
                batch: &mut Self::LogBatch,
                sync_log: bool,
                max_capacity: usize,
                shrink_to: usize,
            ) -> Result<usize> {
                let data_size = self.consume(batch, sync_log)?;
                if data_size > max_capacity {
                    *batch = self.write_batch_with_cap(shrink_to);
                }
                Ok(data_size)
            }

            fn clean(
                &self,
                raft_group_id: u64,
                state: &RaftLocalState,
                batch: &mut Self::LogBatch,
            ) -> Result<()> {
                batch.delete(&keys::raft_state_key(raft_group_id))?;
                let seek_key = keys::raft_log_key(raft_group_id, 0);
                let prefix = keys::raft_log_prefix(raft_group_id);
                if let Some((key, _)) = self.seek(&seek_key)? {
                    if !key.starts_with(&prefix) {
                        // No raft logs for the raft group.
                        return Ok(());
                    }
                    let first_index = match keys::raft_log_index(&key) {
                        Ok(index) => index,
                        Err(_) => return Ok(()),
                    };
                    for index in first_index..=state.last_index {
                        let key = keys::raft_log_key(raft_group_id, index);
                        batch.delete(&key)?;
                    }
                }
                Ok(())
            }

            fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
                self.append_slice(raft_group_id, &entries)
            }

            fn append_slice(&self, raft_group_id: u64, entries: &[Entry]) -> Result<usize> {
                let mut wb = self.write_batch();
                let buf = Vec::with_capacity(1024);
                wb.append_impl(raft_group_id, entries, buf)?;
                self.consume(&mut wb, false)
            }

            fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
                self.put_msg(&keys::raft_state_key(raft_group_id), state)
            }

            fn gc(&self, raft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
                if from >= to {
                    return Ok(0);
                }
                if from == 0 {
                    let start_key = keys::raft_log_key(raft_group_id, 0);
                    let prefix = keys::raft_log_prefix(raft_group_id);
                    match self.seek(&start_key)? {
                        Some((k, _)) if k.starts_with(&prefix) => from = box_try!(keys::raft_log_index(&k)),
                        // No need to gc.
                        _ => return Ok(0),
                    }
                }

                let mut raft_wb = self.write_batch_with_cap(4 * 1024);
                for idx in from..to {
                    let key = keys::raft_log_key(raft_group_id, idx);
                    raft_wb.delete(&key)?;
                    if raft_wb.count() >= Self::WRITE_BATCH_MAX_KEYS {
                        self.write(&raft_wb)?;
                        raft_wb.clear();
                    }
                }

                // TODO: disable WAL here.
                if !Mutable::is_empty(&raft_wb) {
                    self.write(&raft_wb)?;
                }
                Ok((to - from) as usize)
            }

            fn purge_expired_files(&self) -> Result<Vec<u64>> {
                Ok(vec![])
            }

            fn has_builtin_entry_cache(&self) -> bool {
                false
            }

            fn flush_metrics(&self, instance: &str) {
                KvEngine::flush_metrics(self, instance)
            }

            fn reset_statistics(&self) {
                KvEngine::reset_statistics(self)
            }

            fn dump_stats(&self) -> Result<String> {
                MiscExt::dump_stats(self)
            }
        }

        impl RaftLogBatch for $write_batch_name {
            fn append(&mut self, raft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
                self.append_slice(raft_group_id, &entries)
            }

            fn append_slice(&mut self, raft_group_id: u64, entries: &[Entry]) -> Result<()> {
                if let Some(max_size) = entries.iter().map(|e| e.compute_size()).max() {
                    let ser_buf = Vec::with_capacity(max_size as usize);
                    return self.append_impl(raft_group_id, entries, ser_buf);
                }
                Ok(())
            }

            fn cut_logs(&mut self, raft_group_id: u64, from: u64, to: u64) {
                for index in from..to {
                    let key = keys::raft_log_key(raft_group_id, index);
                    self.delete(&key).unwrap();
                }
            }

            fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
                self.put_msg(&keys::raft_state_key(raft_group_id), state)
            }

            fn is_empty(&self) -> bool {
                Mutable::is_empty(self)
            }
        }

        impl $write_batch_name {
            fn append_impl(
                &mut self,
                raft_group_id: u64,
                entries: &[Entry],
                mut ser_buf: Vec<u8>,
            ) -> Result<()> {
                for entry in entries {
                    let key = keys::raft_log_key(raft_group_id, entry.get_index());
                    ser_buf.clear();
                    entry.write_to_vec(&mut ser_buf).unwrap();
                    self.put(&key, &ser_buf)?;
                }
                Ok(())
            }
        }
    }
}
