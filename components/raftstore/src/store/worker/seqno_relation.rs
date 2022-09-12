// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, fmt,
    sync::{atomic::Ordering, Arc, Mutex},
};

use collections::{HashMap, HashMapEntry};
use engine_traits::{
    util::FlushedSeqno, Engines, KvEngine, RaftEngine, RaftLogBatch, Snapshot, DATA_CFS,
};
use kvproto::raft_serverpb::{
    MergeState, PeerState, RaftApplyState, RegionLocalState, RegionSequenceNumberRelation,
};
use tikv_util::{
    debug, error,
    sequence_number::{SequenceNumber, SequenceNumberWindow, SYNCED_MAX_SEQUENCE_NUMBER},
    worker::{Runnable, Scheduler},
};

use super::metrics::*;
use crate::store::{
    async_io::write::{RAFT_WB_DEFAULT_SIZE, RAFT_WB_SHRINK_SIZE},
    fsm::{store::ApplyResNotifier, ApplyNotifier, ApplyRes, ExecResult, StoreMeta},
    peer_storage::write_initial_apply_state_raft,
    util::gc_seqno_relations,
    worker::RaftlogGcTask,
};

const RAFT_WB_MAX_KEYS: usize = 256;

pub enum Task<S: Snapshot> {
    ApplyRes(Vec<ApplyRes<S>>),
    MemtableSealed(u64),
    MemtableFlushed { cf: Option<String>, seqno: u64 },
    InitRaftlogGcScheduler(Scheduler<RaftlogGcTask>),
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("SeqnoRelationTask");
        match self {
            Task::ApplyRes(ref apply_res) => de
                .field("name", &"apply_res")
                .field("apply_res", &apply_res.len())
                .finish(),
            Task::MemtableSealed(ref seqno) => de
                .field("name", &"memtable_sealed")
                .field("seqno", &seqno)
                .finish(),
            Task::MemtableFlushed { ref cf, ref seqno } => de
                .field("name", &"memtable_flushed")
                .field("cf", &cf)
                .field("seqno", &seqno)
                .finish(),
            Task::InitRaftlogGcScheduler(_) => {
                de.field("name", &"init_raftlog_gc_scheduler").finish()
            }
        }
    }
}

struct SeqnoRelation {
    region_id: u64,
    seqno: SequenceNumber,
    apply_state: RaftApplyState,
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    store_id: Option<u64>,
    store_meta: Arc<Mutex<StoreMeta>>,
    apply_res_notifier: ApplyResNotifier<EK, ER>,
    engines: Engines<EK, ER>,
    wb: ER::LogBatch,
    seqno_window: SequenceNumberWindow,
    inflight_seqno_relations: HashMap<u64, SeqnoRelation>,
    last_persisted_seqno: u64,
    flushed_seqno: FlushedSeqno,
    raftlog_gc_scheduler: Option<Scheduler<RaftlogGcTask>>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        store_meta: Arc<Mutex<StoreMeta>>,
        apply_res_notifier: ApplyResNotifier<EK, ER>,
        engines: Engines<EK, ER>,
    ) -> Self {
        Runner {
            store_id: None,
            store_meta,
            wb: engines.raft.log_batch(RAFT_WB_MAX_KEYS),
            flushed_seqno: FlushedSeqno::new(DATA_CFS, engines.kv.get_latest_sequence_number()),
            engines,
            apply_res_notifier,
            seqno_window: SequenceNumberWindow::default(),
            inflight_seqno_relations: HashMap::default(),
            last_persisted_seqno: 0,
            raftlog_gc_scheduler: None,
        }
    }

    fn on_apply_res(&mut self, apply_res: Vec<ApplyRes<EK::Snapshot>>) {
        let mut sync_relations = HashMap::default();
        for res in &apply_res {
            for seqno in &res.write_seqno {
                let relation = SeqnoRelation {
                    region_id: res.region_id,
                    seqno: *seqno,
                    apply_state: res.apply_state.clone(),
                };
                let relations = match seqno.number.cmp(&self.last_persisted_seqno) {
                    cmp::Ordering::Less | cmp::Ordering::Equal => &mut sync_relations,
                    cmp::Ordering::Greater => &mut self.inflight_seqno_relations,
                };
                match relations.entry(res.region_id) {
                    HashMapEntry::Occupied(mut e) => {
                        if e.get().seqno < relation.seqno {
                            *e.get_mut() = relation;
                        }
                    }
                    HashMapEntry::Vacant(e) => {
                        e.insert(relation);
                    }
                }
                self.seqno_window.push(*seqno);
            }
            self.handle_exec_res(res.region_id, &res.apply_state, res.exec_res.iter());
        }
        if !sync_relations.is_empty() {
            self.handle_sync_relations(sync_relations);
        }

        self.engines.raft.sync().unwrap();
        SYNCED_MAX_SEQUENCE_NUMBER.store(self.seqno_window.committed_seqno(), Ordering::SeqCst);

        SEQNO_UNCOMMITTED_COUNT.set(self.seqno_window.pending_count() as i64);
        debug!("pending seqno count"; "count" => self.seqno_window.pending_count());
        self.apply_res_notifier.notify(apply_res);
    }

    fn on_memtable_sealed(&mut self, seqno: u64) {
        self.last_persisted_seqno = seqno;
        let sync_relations = std::mem::take(&mut self.inflight_seqno_relations);
        self.handle_sync_relations(sync_relations);
    }

    // GC relations
    fn on_memtable_flushed(&mut self, cf: Option<String>, seqno: u64) {
        let min_flushed = match cf.as_ref() {
            Some(cf) => self.flushed_seqno.update(cf, seqno),
            None => self.flushed_seqno.update_all(seqno),
        };
        self.engines
            .raft
            .put_flushed_seqno(&self.flushed_seqno)
            .unwrap();
        if let Some(min) = min_flushed {
            gc_seqno_relations(min, &self.engines.raft, &mut self.wb).unwrap();
            if !self.wb.is_empty() {
                self.engines
                    .raft
                    .consume_and_shrink(
                        &mut self.wb,
                        true,
                        RAFT_WB_SHRINK_SIZE,
                        RAFT_WB_DEFAULT_SIZE,
                    )
                    .unwrap();
            }
            let scheduler = self.raftlog_gc_scheduler.as_ref().unwrap();
            if let Err(e) = scheduler.schedule(RaftlogGcTask::MemtableFlushed { cf, seqno }) {
                error!("failed to notify memtable flushed to raftlog gc worker"; "err" => ?e);
            }
        }
    }

    fn handle_sync_relations(&mut self, relations: HashMap<u64, SeqnoRelation>) {
        let mut relation = RegionSequenceNumberRelation::default();
        let mut count = 0;
        let size = relations.len();
        for (region_id, r) in relations {
            assert!(r.seqno.number <= self.last_persisted_seqno);
            self.seqno_window.push(r.seqno);
            relation.set_region_id(r.region_id);
            relation.set_apply_state(r.apply_state);
            relation.set_sequence_number(r.seqno.number);
            self.wb.put_seqno_relation(region_id, &relation).unwrap();
            count += 1;
            if count % RAFT_WB_MAX_KEYS == 0 || count == size - 1 {
                SEQNO_RELATIONS_WRITE_SIZE_HISTOGRAM.observe(self.wb.persist_size() as f64);
                let _timer = SEQNO_RELATIONS_WRITE_DURATION_HISTOGRAM.start_timer();
                self.engines
                    .raft
                    .consume_and_shrink(
                        &mut self.wb,
                        false,
                        RAFT_WB_SHRINK_SIZE,
                        RAFT_WB_DEFAULT_SIZE,
                    )
                    .unwrap();
            }
        }
        if !self.wb.is_empty() {
            SEQNO_RELATIONS_WRITE_SIZE_HISTOGRAM.observe(self.wb.persist_size() as f64);
            let _timer = SEQNO_RELATIONS_WRITE_DURATION_HISTOGRAM.start_timer();
            self.engines
                .raft
                .consume_and_shrink(
                    &mut self.wb,
                    false,
                    RAFT_WB_SHRINK_SIZE,
                    RAFT_WB_DEFAULT_SIZE,
                )
                .unwrap();
        }
        SEQNO_RELATIONS_KEYS_FLOW.inc_by(count as u64);
    }

    fn handle_exec_res<'a>(
        &mut self,
        region_id: u64,
        apply_state: &RaftApplyState,
        exec_res: impl Iterator<Item = &'a ExecResult<EK::Snapshot>>,
    ) {
        let mut region_local_state = RegionLocalState::default();
        for res in exec_res {
            match res {
                ExecResult::ChangePeer(cp) => {
                    if cp.index == raft::INVALID_INDEX {
                        // Apply failed, skip.
                        continue;
                    }
                    let mut remove_self = true;
                    for peer in cp.region.get_peers() {
                        if peer.store_id == self.store_id().unwrap() {
                            remove_self = false;
                        }
                    }
                    let state = if remove_self {
                        PeerState::Tombstone
                    } else {
                        PeerState::Normal
                    };
                    region_local_state.set_region(cp.region.clone());
                    region_local_state.set_state(state);
                    region_local_state.clear_merge_state();
                    self.wb
                        .put_pending_region_state(
                            region_id,
                            apply_state.applied_index,
                            &region_local_state,
                        )
                        .unwrap();
                }
                ExecResult::SplitRegion {
                    regions,
                    new_split_regions,
                    ..
                } => {
                    for region in regions {
                        let applied_index = if region.get_id() == region_id {
                            apply_state.applied_index
                        } else {
                            0
                        };
                        region_local_state.set_region(region.clone());
                        region_local_state.set_state(PeerState::Normal);
                        region_local_state.clear_merge_state();
                        if let Some(new_split_peer) = new_split_regions.get(&region.get_id()) {
                            if new_split_peer.result.is_some() {
                                continue;
                            }
                            self.wb
                                .put_region_state(region.get_id(), &region_local_state)
                                .unwrap();
                            write_initial_apply_state_raft(&mut self.wb, region.get_id()).unwrap();
                        } else {
                            self.wb
                                .put_pending_region_state(
                                    region.get_id(),
                                    applied_index,
                                    &region_local_state,
                                )
                                .unwrap();
                        }
                    }
                }
                ExecResult::PrepareMerge { region, state } => {
                    region_local_state.set_region(region.clone());
                    region_local_state.set_state(PeerState::Merging);
                    region_local_state.set_merge_state(state.clone());
                    self.wb
                        .put_pending_region_state(
                            region.get_id(),
                            apply_state.applied_index,
                            &region_local_state,
                        )
                        .unwrap();
                }
                ExecResult::CommitMerge {
                    index,
                    region,
                    source,
                } => {
                    region_local_state.set_region(region.clone());
                    region_local_state.set_state(PeerState::Normal);
                    region_local_state.clear_merge_state();
                    self.wb
                        .put_pending_region_state(
                            region.get_id(),
                            apply_state.applied_index,
                            &region_local_state,
                        )
                        .unwrap();
                    // Write source state
                    let mut merging_state = MergeState::default();
                    merging_state.set_target(region.clone());
                    region_local_state.set_region(source.clone());
                    region_local_state.set_state(PeerState::Tombstone);
                    region_local_state.set_merge_state(merging_state);
                    self.wb
                        .put_pending_region_state(source.get_id(), *index, &region_local_state)
                        .unwrap();
                }
                ExecResult::RollbackMerge { region, .. } => {
                    region_local_state.set_region(region.clone());
                    region_local_state.set_state(PeerState::Normal);
                    region_local_state.clear_merge_state();
                    self.wb
                        .put_pending_region_state(
                            region.get_id(),
                            apply_state.applied_index,
                            &region_local_state,
                        )
                        .unwrap();
                }
                ExecResult::DeleteRange { .. }
                | ExecResult::IngestSst { .. }
                | ExecResult::TransferLeader { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::ComputeHash { .. } => (),
            }
        }
        let _timer = SEQNO_RELATIONS_WRITE_DURATION_HISTOGRAM.start_timer();
        if !self.wb.is_empty() {
            self.engines
                .raft
                .consume_and_shrink(
                    &mut self.wb,
                    false,
                    RAFT_WB_SHRINK_SIZE,
                    RAFT_WB_DEFAULT_SIZE,
                )
                .unwrap();
        }
    }

    fn store_id(&mut self) -> Option<u64> {
        self.store_id.or_else(|| {
            let meta = self.store_meta.lock().unwrap();
            self.store_id = meta.store_id;
            meta.store_id
        })
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task<EK::Snapshot>;
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ApplyRes(apply_res) => self.on_apply_res(apply_res),
            Task::MemtableSealed(seqno) => self.on_memtable_sealed(seqno),
            Task::MemtableFlushed { cf, seqno } => self.on_memtable_flushed(cf, seqno),
            Task::InitRaftlogGcScheduler(scheduler) => self.raftlog_gc_scheduler = Some(scheduler),
        }
    }
}
