// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! A helper class to detect flush event and trace apply index.
//!
//! The whole idea is when all CFs have flushed to disk, then the apply index
//! should be able to be advanced to the latest. The implementations depends on
//! the assumption that memtable/write buffer is frozen one by one and flushed
//! one by one.
//!
//! Because apply index can be arbitrary value after restart, so apply related
//! states like `RaftApplyState` and `RegionLocalState` are mapped to index.
//! Once apply index is confirmed, the latest states before apply index should
//! be used as the start state.

use std::{
    collections::LinkedList,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

use kvproto::import_sstpb::SstMeta;
use slog_global::{info, warn};
use tikv_util::{set_panic_mark, time::Instant};

use crate::{data_cf_offset, RaftEngine, RaftLogBatch, DATA_CFS_LEN};

const HEAVY_WORKER_THRESHOLD: Duration = Duration::from_millis(25);

#[derive(Debug)]
pub struct ApplyProgress {
    cf: String,
    apply_index: u64,
    smallest_seqno: u64,
}

impl ApplyProgress {
    fn merge(&mut self, pr: ApplyProgress) {
        debug_assert_eq!(self.cf, pr.cf);
        debug_assert!(self.apply_index <= pr.apply_index);
        self.apply_index = pr.apply_index;
    }

    pub fn applied_index(&self) -> u64 {
        self.apply_index
    }

    pub fn cf(&self) -> &str {
        &self.cf
    }
}

#[derive(Default, Debug)]
struct FlushProgress {
    prs: LinkedList<ApplyProgress>,
    last_flushed: [u64; DATA_CFS_LEN],
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding sst apply index, when
/// apply ingest sst request, it should ensure the sst can be deleted
/// if the flushed index greater than it .
#[derive(Debug, Clone)]
pub struct SstApplyState {
    // vec from cf to Vec<SstApplyEntry>.
    ssts: Arc<RwLock<[Vec<SstApplyEntry>; DATA_CFS_LEN]>>,
}

impl Default for SstApplyState {
    fn default() -> Self {
        Self {
            ssts: Arc::new(RwLock::new(Default::default())),
        }
    }
}

#[derive(Debug)]
pub struct SstApplyEntry {
    pub applied_index: u64,
    pub sst: SstMeta,
}

impl SstApplyEntry {
    pub fn new(applied_index: u64, sst: SstMeta) -> Self {
        Self { applied_index, sst }
    }
}

impl SstApplyState {
    #[inline]
    pub fn register_ssts(&self, applied_index: u64, ssts: Vec<SstMeta>) {
        let mut sst_list = self.ssts.write().unwrap();
        for sst in ssts {
            let cf_index = data_cf_offset(sst.get_cf_name());
            let entry = SstApplyEntry::new(applied_index, sst);
            sst_list.get_mut(cf_index).unwrap().push(entry);
        }
    }

    #[inline]
    pub fn stale_ssts(&self, cf: &str, flushed_index: u64) -> Vec<SstMeta> {
        let sst_list = self.ssts.read().unwrap();
        let cf_index = data_cf_offset(cf);
        if let Some(ssts) = sst_list.get(cf_index) {
            return ssts
                .iter()
                .filter(|entry| entry.applied_index <= flushed_index)
                .map(|entry| entry.sst.clone())
                .collect();
        }
        vec![]
    }

    pub fn delete_ssts(&self, ssts: &Vec<SstMeta>) {
        let mut sst_list = self.ssts.write().unwrap();
        for sst in ssts {
            let cf_index = data_cf_offset(sst.get_cf_name());
            if let Some(metas) = sst_list.get_mut(cf_index) {
                metas.retain(|entry| entry.sst.get_uuid() != sst.get_uuid());
            }
        }
    }
}

/// A share state between raftstore and underlying engine.
///
/// raftstore will update state changes and corresponding apply index, when
/// flush, `PersistenceListener` will query states related to the memtable
/// and persist the relation to raft engine.
#[derive(Debug)]
pub struct FlushState {
    applied_index: AtomicU64,

    // This is only used for flush before server stop.
    // It provides a direct path for flush progress by letting raftstore directly know the current
    // flush progress.
    flushed_index: [AtomicU64; DATA_CFS_LEN],
}

impl FlushState {
    pub fn new(applied_index: u64) -> Self {
        Self {
            applied_index: AtomicU64::new(applied_index),
            flushed_index: Default::default(),
        }
    }

    /// Set the latest applied index.
    #[inline]
    pub fn set_applied_index(&self, index: u64) {
        self.applied_index.store(index, Ordering::Release);
    }

    /// Query the applied index.
    #[inline]
    pub fn applied_index(&self) -> u64 {
        self.applied_index.load(Ordering::Acquire)
    }

    #[inline]
    pub fn flushed_index(&self) -> &[AtomicU64; DATA_CFS_LEN] {
        &self.flushed_index
    }
}

/// A helper trait to avoid exposing `RaftEngine` to `TabletFactory`.
pub trait StateStorage: Sync + Send {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress);
}

/// A flush listener that maps memtable to apply index and persist the relation
/// to raft engine.
pub struct PersistenceListener {
    region_id: u64,
    tablet_index: u64,
    state: Arc<FlushState>,
    progress: Mutex<FlushProgress>,
    storage: Arc<dyn StateStorage>,
}

impl PersistenceListener {
    pub fn new(
        region_id: u64,
        tablet_index: u64,
        state: Arc<FlushState>,
        storage: Arc<dyn StateStorage>,
    ) -> Self {
        Self {
            region_id,
            tablet_index,
            state,
            progress: Mutex::new(FlushProgress::default()),
            storage,
        }
    }
}

impl PersistenceListener {
    pub fn flush_state(&self) -> &Arc<FlushState> {
        &self.state
    }

    /// Called when memtable is frozen.
    ///
    /// `smallest_seqno` should be the smallest seqno of the memtable.
    ///
    /// Note: After https://github.com/tikv/rocksdb/pull/347, rocksdb global lock will
    /// be held during this method, so we should avoid do heavy things in it.
    pub fn on_memtable_sealed(&self, cf: String, smallest_seqno: u64, largest_seqno: u64) {
        let t = Instant::now_coarse();
        (|| {
            fail_point!("on_memtable_sealed", |t| {
                assert_eq!(t.unwrap().as_str(), cf);
            })
        })();
        // The correctness relies on the assumption that there will be only one
        // thread writting to the DB and increasing apply index.
        // Apply index will be set within DB lock, so it's correct even with manual
        // flush.
        let offset = data_cf_offset(&cf);
        let apply_index = self.state.applied_index.load(Ordering::SeqCst);
        let mut prs = self.progress.lock().unwrap();
        let flushed = prs.last_flushed[offset];
        if flushed > smallest_seqno {
            panic!(
                "sealed seqno conflict with latest flushed index, cf {},
                sealed smallest_seqno {}, sealed largest_seqno {}, last_flushed {}, apply_index {}",
                cf, smallest_seqno, largest_seqno, flushed, apply_index,
            );
        }
        prs.prs.push_back(ApplyProgress {
            cf,
            apply_index,
            smallest_seqno,
        });
        if t.saturating_elapsed() > HEAVY_WORKER_THRESHOLD {
            warn!(
                "heavy work in on_memtable_sealed, the code should be reviewed";
            );
        }
    }

    /// Called a memtable finished flushing.
    ///
    /// `largest_seqno` should be the largest seqno of the generated file.
    pub fn on_flush_completed(&self, cf: &str, largest_seqno: u64, file_no: u64) {
        fail_point!("on_flush_completed", |_| {});
        // Maybe we should hook the compaction to avoid the file is compacted before
        // being recorded.
        let offset = data_cf_offset(cf);
        let pr = {
            let mut prs = self.progress.lock().unwrap();
            let flushed = prs.last_flushed[offset];
            if flushed >= largest_seqno {
                // According to facebook/rocksdb#11183, it's possible OnFlushCompleted can be
                // called out of order. But it's guaranteed files are installed in order.
                info!(
                    "flush complete reorder found";
                    "flushed" => flushed,
                    "largest_seqno" => largest_seqno,
                    "file_no" => file_no,
                    "cf" => cf
                );
                return;
            }
            prs.last_flushed[offset] = largest_seqno;
            let mut cursor = prs.prs.cursor_front_mut();
            let mut flushed_pr = None;
            while let Some(pr) = cursor.current() {
                if pr.cf != cf {
                    cursor.move_next();
                    continue;
                }
                if pr.smallest_seqno <= largest_seqno {
                    match &mut flushed_pr {
                        None => flushed_pr = cursor.remove_current(),
                        Some(flushed_pr) => {
                            flushed_pr.merge(cursor.remove_current().unwrap());
                        }
                    }
                    continue;
                }
                break;
            }
            match flushed_pr {
                Some(pr) => pr,
                None => {
                    set_panic_mark();
                    panic!(
                        "[region_id={}] [tablet_index={}] {} {} {} not found in {:?}",
                        self.region_id, self.tablet_index, cf, largest_seqno, file_no, prs
                    )
                }
            }
        };
        let apply_index = pr.apply_index;
        self.storage
            .persist_progress(self.region_id, self.tablet_index, pr);
        self.state.flushed_index[offset].store(apply_index, Ordering::SeqCst);
    }
}

impl<R: RaftEngine> StateStorage for R {
    fn persist_progress(&self, region_id: u64, tablet_index: u64, pr: ApplyProgress) {
        if pr.apply_index == 0 {
            return;
        }
        let mut batch = self.log_batch(1);
        // TODO: It's possible that flush succeeds but fails to call
        // `on_flush_completed` before exit. In this case the flushed data will
        // be replayed again after restarted. To solve the problem, we need to
        // (1) persist flushed file numbers in `on_flush_begin` and (2) check
        // the file number in `on_compaction_begin`. After restart, (3) check if the
        // file exists. If (1) && ((2) || (3)), then we don't need to replay the data.
        batch
            .put_flushed_index(region_id, &pr.cf, tablet_index, pr.apply_index)
            .unwrap();
        self.consume(&mut batch, true).unwrap();
    }
}

#[cfg(test)]
mod test {
    use std::vec;

    use kvproto::import_sstpb::SstMeta;

    use super::SstApplyState;

    #[test]
    pub fn test_sst_apply_state() {
        let stat = SstApplyState::default();
        let mut sst = SstMeta::default();
        sst.set_cf_name("write".to_owned());
        sst.set_uuid(vec![1, 2, 3, 4]);
        stat.register_ssts(10, vec![sst.clone()]);
        assert!(stat.stale_ssts("default", 10).is_empty());
        let sst = stat.stale_ssts("write", 10);
        assert_eq!(sst[0].get_uuid(), vec![1, 2, 3, 4]);
        stat.delete_ssts(&sst);
    }
}
