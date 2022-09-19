// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, fmt, sync::atomic::Ordering};

use collections::{HashMap, HashMapEntry};
use engine_traits::{
    util::FlushedSeqno, Engines, KvEngine, RaftEngine, RaftLogBatch, Snapshot, DATA_CFS,
};
use kvproto::{
    metapb::Region,
    raft_serverpb::{
        MergeState, PeerState, RaftApplyState, RegionLocalState, RegionSequenceNumberRelation,
    },
};
use tikv_util::{
    debug, error, info,
    sequence_number::{SequenceNumberWindow, SYNCED_MAX_SEQUENCE_NUMBER},
    worker::{Runnable, Scheduler},
};

use super::metrics::*;
use crate::store::{
    async_io::write::{RAFT_WB_DEFAULT_SIZE, RAFT_WB_SHRINK_SIZE},
    fsm::{ApplyRes, ApplyTaskRes, ExecResult},
    peer_storage::write_initial_apply_state_raft,
    util::gc_seqno_relations,
    worker::RaftlogGcTask,
    PeerMsg, RaftRouter,
};

const RAFT_WB_MAX_KEYS: usize = 256;

pub enum Task<S: Snapshot> {
    ApplyRes(Vec<ApplyRes<S>>),
    DestroyRegion {
        region: Region,
        peer_id: u64,
        merge_from_snapshot: bool,
    },
    MemtableSealed(u64),
    MemtableFlushed {
        cf: Option<String>,
        seqno: u64,
    },
    Start,
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
            Task::Start => de.field("name", &"start").finish(),
            Task::DestroyRegion {
                region,
                peer_id,
                merge_from_snapshot,
            } => de
                .field("name", &"destroy-region")
                .field("region", &region)
                .field("peer_id", &peer_id)
                .field("merge_from_snapshot", &merge_from_snapshot)
                .finish(),
        }
    }
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    router: RaftRouter<EK, ER>,
    engines: Engines<EK, ER>,
    raft_wb: ER::LogBatch,
    seqno_window: SequenceNumberWindow,
    inflight_seqno_relations: HashMap<u64, RegionSequenceNumberRelation>,
    last_persisted_seqno: u64,
    flushed_seqno: FlushedSeqno,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    started: bool,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        router: RaftRouter<EK, ER>,
        engines: Engines<EK, ER>,
        raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    ) -> Self {
        Runner {
            router,
            raft_wb: engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
            flushed_seqno: FlushedSeqno::new(DATA_CFS, engines.kv.get_latest_sequence_number()),
            engines,
            seqno_window: SequenceNumberWindow::default(),
            inflight_seqno_relations: HashMap::default(),
            last_persisted_seqno: 0,
            raftlog_gc_scheduler,
            started: false,
        }
    }

    fn consume_raft_wb(&mut self, sync: bool) {
        if self.raft_wb.is_empty() {
            return;
        }
        SEQNO_RELATIONS_WRITE_SIZE_HISTOGRAM.observe(self.raft_wb.persist_size() as f64);
        let _timer = SEQNO_RELATIONS_WRITE_DURATION_HISTOGRAM.start_timer();
        self.engines
            .raft
            .consume_and_shrink(
                &mut self.raft_wb,
                sync,
                RAFT_WB_SHRINK_SIZE,
                RAFT_WB_DEFAULT_SIZE,
            )
            .unwrap();
    }

    fn on_apply_res(&mut self, apply_res: &[ApplyRes<EK::Snapshot>]) {
        let mut sync_relations = HashMap::default();
        for res in apply_res {
            for seqno in &res.write_seqno {
                self.seqno_window.push(*seqno);
            }
            let seqno = res.write_seqno.iter().max().unwrap();
            let mut relation = RegionSequenceNumberRelation::default();
            relation.set_region_id(res.region_id);
            relation.set_sequence_number(seqno.number);
            relation.set_apply_state(res.apply_state.clone());

            if let Some(region_state) = self.handle_exec_res(
                res.region_id,
                seqno.number,
                res.exec_res.iter(),
                &mut sync_relations,
            ) {
                info!("region state changed"; "region_id" => res.region_id, "region_state" => ?region_state, "apply_state" => ?res.apply_state);
                relation.set_region_state(region_state);
            }
            self.handle_relation(relation, &mut sync_relations);
        }
        if !sync_relations.is_empty() {
            self.handle_sync_relations(sync_relations);
        }

        self.consume_raft_wb(true);
        SYNCED_MAX_SEQUENCE_NUMBER.store(self.seqno_window.committed_seqno(), Ordering::SeqCst);

        SEQNO_UNCOMMITTED_COUNT.set(self.seqno_window.pending_count() as i64);
        debug!("pending seqno count"; "count" => self.seqno_window.pending_count());
    }

    fn handle_relation(
        &mut self,
        relation: RegionSequenceNumberRelation,
        sync_relations: &mut HashMap<u64, RegionSequenceNumberRelation>,
    ) {
        let relations = match relation.sequence_number.cmp(&self.last_persisted_seqno) {
            cmp::Ordering::Less | cmp::Ordering::Equal => sync_relations,
            cmp::Ordering::Greater => &mut self.inflight_seqno_relations,
        };
        match relations.entry(relation.region_id) {
            HashMapEntry::Occupied(mut e) => {
                if e.get().sequence_number < relation.sequence_number {
                    *e.get_mut() = relation;
                }
            }
            HashMapEntry::Vacant(e) => {
                e.insert(relation);
            }
        }
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
            gc_seqno_relations(min, &self.engines.raft, &mut self.raft_wb).unwrap();
            if !self.raft_wb.is_empty() {
                self.consume_raft_wb(true);
            }
            if let Err(e) = self
                .raftlog_gc_scheduler
                .schedule(RaftlogGcTask::MemtableFlushed { cf, seqno })
            {
                error!("failed to notify memtable flushed to raftlog gc worker"; "err" => ?e);
            }
        }
    }

    fn handle_sync_relations(&mut self, relations: HashMap<u64, RegionSequenceNumberRelation>) {
        let mut count = 0;
        let size = relations.len();
        for (region_id, relation) in relations {
            assert!(relation.sequence_number <= self.last_persisted_seqno);
            info!("save seqno relation to raftdb"; "region_id" => region_id, "relation" => ?relation);
            self.raft_wb
                .put_seqno_relation(region_id, &relation)
                .unwrap();
            count += 1;
            if count % RAFT_WB_MAX_KEYS == 0 || count == size - 1 {
                self.consume_raft_wb(false);
            }
        }
        if !self.raft_wb.is_empty() {
            self.consume_raft_wb(false);
        }
        SEQNO_RELATIONS_KEYS_FLOW.inc_by(count as u64);
    }

    fn handle_exec_res<'a>(
        &mut self,
        region_id: u64,
        seqno: u64,
        exec_res: impl Iterator<Item = &'a ExecResult<EK::Snapshot>>,
        sync_relations: &mut HashMap<u64, RegionSequenceNumberRelation>,
    ) -> Option<RegionLocalState> {
        let mut region_state = None;
        for res in exec_res {
            match res {
                ExecResult::ChangePeer(cp) => {
                    if cp.index == raft::INVALID_INDEX {
                        // Apply failed, skip.
                        continue;
                    }
                    if cp.remove_self {
                        info!("handle region self destroy"; "region_id" => region_id);
                        self.handle_destroy_region(cp.region.clone());
                    } else {
                        let mut state = RegionLocalState::default();
                        state.set_region(cp.region.clone());
                        state.set_state(PeerState::Normal);
                        region_state = Some(state);
                    };
                }
                ExecResult::SplitRegion {
                    regions,
                    new_split_regions,
                    ..
                } => {
                    for region in regions {
                        let mut state = RegionLocalState::default();
                        state.set_region(region.clone());
                        state.set_state(PeerState::Normal);
                        if let Some(new_split_peer) = new_split_regions.get(&region.get_id()) {
                            if new_split_peer.result.is_some() {
                                continue;
                            }
                            self.raft_wb
                                .put_region_state(region.get_id(), &state)
                                .unwrap();
                            write_initial_apply_state_raft(&mut self.raft_wb, region.get_id())
                                .unwrap();
                        }
                        if region.get_id() == region_id {
                            region_state = Some(state);
                        }
                    }
                }
                ExecResult::PrepareMerge {
                    region,
                    state: merge_state,
                } => {
                    let mut state = RegionLocalState::default();
                    state.set_region(region.clone());
                    state.set_state(PeerState::Merging);
                    state.set_merge_state(merge_state.clone());
                    region_state = Some(state);
                }
                ExecResult::CommitMerge {
                    index,
                    region,
                    source,
                } => {
                    let mut target_state = RegionLocalState::default();
                    target_state.set_region(region.clone());
                    target_state.set_state(PeerState::Normal);
                    region_state = Some(target_state);
                    // Write source state
                    let mut relation = RegionSequenceNumberRelation::default();
                    relation.set_region_id(source.get_id());
                    relation.set_sequence_number(seqno);
                    let mut apply_state = RaftApplyState::default();
                    apply_state.set_applied_index(*index);
                    relation.set_apply_state(apply_state);
                    let mut source_state = RegionLocalState::default();
                    let mut merging_state = MergeState::default();
                    merging_state.set_target(region.clone());
                    source_state.set_region(source.clone());
                    source_state.set_state(PeerState::Tombstone);
                    source_state.set_merge_state(merging_state);
                    relation.set_region_state(source_state);
                    self.handle_relation(relation, sync_relations);
                }
                ExecResult::RollbackMerge { region, .. } => {
                    let mut state = RegionLocalState::default();
                    state.set_region(region.clone());
                    state.set_state(PeerState::Normal);
                    region_state = Some(state);
                }
                ExecResult::DeleteRange { .. }
                | ExecResult::IngestSst { .. }
                | ExecResult::TransferLeader { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::ComputeHash { .. } => (),
            }
        }
        region_state
    }

    fn handle_destroy_region(&mut self, region: Region) {
        // Cleanup all stale relations.
        let raft_engine = self.engines.raft.clone();
        let region_id = region.get_id();
        self.inflight_seqno_relations.remove(&region_id);
        raft_engine
            .scan_seqno_relations(region_id, None, None, |seqno, _| {
                self.raft_wb
                    .delete_seqno_relation(region_id, seqno)
                    .unwrap();
                true
            })
            .unwrap();
        let mut region_state = RegionLocalState::default();
        region_state.set_region(region);
        region_state.set_state(PeerState::Tombstone);
        self.raft_wb
            .put_region_state(region_id, &region_state)
            .unwrap();
        info!("handle destroy region"; "region_id" => region_id, "region_state" => ?region_state);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task<EK::Snapshot>;
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ApplyRes(apply_res) => {
                if self.started {
                    self.on_apply_res(&apply_res);
                }
                for r in apply_res {
                    let _ = self.router.force_send(
                        r.region_id,
                        PeerMsg::ApplyRes {
                            res: ApplyTaskRes::Apply(r),
                        },
                    );
                }
            }
            Task::MemtableSealed(seqno) => {
                if self.started {
                    self.on_memtable_sealed(seqno)
                }
            }
            Task::MemtableFlushed { cf, seqno } => {
                if self.started {
                    self.on_memtable_flushed(cf, seqno)
                }
            }
            Task::Start => {
                self.started = true;
                info!("seqno relation worker started");
            }
            Task::DestroyRegion {
                region,
                peer_id,
                merge_from_snapshot,
            } => {
                let region_id = region.get_id();
                self.handle_destroy_region(region);
                self.consume_raft_wb(true);
                let _ = self.router.force_send(
                    region_id,
                    PeerMsg::ApplyRes {
                        res: ApplyTaskRes::Destroy {
                            region_id,
                            peer_id,
                            merge_from_snapshot,
                        },
                    },
                );
            }
        }
    }
}
