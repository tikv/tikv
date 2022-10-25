// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp, fmt, mem,
    sync::{atomic::AtomicUsize, Arc},
};

use collections::{HashMap, HashMapEntry};
use engine_traits::{
    util::{FlushedSeqno, SequenceNumberWindow},
    Engines, KvEngine, RaftEngine, RaftLogBatch, Snapshot, DATA_CFS,
};
use fail::fail_point;
use kvproto::{
    metapb::Region,
    raft_serverpb::{
        MergeState, PeerState, RaftApplyState, RegionLocalState, RegionSequenceNumberRelation,
    },
};
use tikv_util::{
    debug, error, info, warn,
    worker::{Runnable, Scheduler},
};

use super::metrics::*;
use crate::store::{
    async_io::write::{RAFT_WB_DEFAULT_SIZE, RAFT_WB_SHRINK_SIZE},
    fsm::{
        apply::{RecoverCallback, RecoverStatus},
        ApplyRes, ApplyTaskRes, ExecResult,
    },
    peer_storage::write_initial_apply_state_raft,
    util::{clear_region_seqno_relation, gc_seqno_relations},
    worker::RaftlogGcTask,
    PeerMsg, RaftRouter, RegionTask, SignificantMsg,
};

const RAFT_WB_MAX_KEYS: usize = 256;

pub enum Task<S: Snapshot> {
    ApplyRes(Vec<ApplyRes<S>>),
    DestroyRegion {
        region: Region,
        peer_id: u64,
        merge_from_snapshot: bool,
    },
    ApplySnapshot {
        region_id: u64,
        status: Arc<AtomicUsize>,
        peer_id: u64,
    },
    MemtableFlushed {
        cf: Option<String>,
        seqno: u64,
    },
    RecoverStatus {
        region_id: u64,
        status: RecoverStatus,
        cb: RecoverCallback,
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
            Task::ApplySnapshot {
                region_id, peer_id, ..
            } => de
                .field("name", &"applying-snapshot")
                .field("region_id", &region_id)
                .field("peer_id", &peer_id)
                .finish(),
            Task::RecoverStatus {
                region_id, status, ..
            } => de
                .field("name", &"recover-status")
                .field("region_id", &region_id)
                .field("status", &status)
                .finish(),
        }
    }
}

enum HandleExecResResult {
    ConfChange(RegionLocalState),
    PrepareMerge(RegionLocalState),
    RollbackMerge(RegionLocalState),
    SplitRegion {
        new_created_regions: Vec<RegionLocalState>,
        region_state: RegionLocalState,
    },
    CommitMerge {
        target_state: RegionLocalState,
        source_relation: RegionSequenceNumberRelation,
    },
    Destroy(Region),
    IngestSst,
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    router: RaftRouter<EK, ER>,
    engines: Engines<EK, ER>,
    raft_wb: ER::LogBatch,
    seqno_window: SequenceNumberWindow,
    inflight_seqno_relations: HashMap<u64, RegionSequenceNumberRelation>,
    flushed_seqno: FlushedSeqno,
    raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    started: bool,
    pending_clean_regions: Vec<u64>,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(
        router: RaftRouter<EK, ER>,
        engines: Engines<EK, ER>,
        raftlog_gc_scheduler: Scheduler<RaftlogGcTask>,
        region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    ) -> Self {
        Runner {
            router,
            raft_wb: engines.raft.log_batch(RAFT_WB_DEFAULT_SIZE),
            flushed_seqno: FlushedSeqno::new(DATA_CFS, engines.kv.get_latest_sequence_number()),
            engines,
            seqno_window: SequenceNumberWindow::default(),
            inflight_seqno_relations: HashMap::default(),
            raftlog_gc_scheduler,
            region_scheduler,
            started: false,
            pending_clean_regions: vec![],
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
        let mut sync_relation_with_current_batch = vec![];
        for res in apply_res {
            for seqno in &res.write_seqno {
                self.seqno_window.push(*seqno);
            }
            if !self.started {
                continue;
            }
            let seqno = res.write_seqno.iter().max().unwrap();
            let mut relation = RegionSequenceNumberRelation::default();
            relation.set_region_id(res.region_id);
            relation.set_sequence_number(seqno.get_number());
            relation.set_apply_state(res.apply_state.clone());
            let mut skip_handle_relation = false;

            for result in self.handle_exec_res(res.region_id, seqno.get_number(), &res.exec_res) {
                match result {
                    HandleExecResResult::ConfChange(region_state) => {
                        info!("region conf changed"; "region_id" => res.region_id, "region_state" => ?region_state, "apply_state" => ?res.apply_state);
                        relation.set_region_state(region_state);
                    }
                    HandleExecResResult::SplitRegion {
                        new_created_regions,
                        region_state,
                    } => {
                        info!(
                            "split region";
                            "region_id" => res.region_id,
                            "region_state" => ?region_state,
                            "new_created_regions" => ?new_created_regions,
                            "apply_state" => ?res.apply_state,
                        );
                        for state in new_created_regions {
                            let region = state.get_region();
                            self.raft_wb
                                .put_region_state(region.get_id(), &state)
                                .unwrap();
                            write_initial_apply_state_raft(&mut self.raft_wb, region.get_id())
                                .unwrap();
                        }
                        relation.set_region_state(region_state);
                        // The relation here should be written with new created
                        // regions's meta atomically, otherwise region ranges
                        // would be overlapped even after recovery after
                        // restart.
                        sync_relation_with_current_batch.push(res.region_id);
                    }
                    HandleExecResResult::PrepareMerge(region_state) => {
                        info!("prepare merge"; "region_id" => res.region_id, "region_state" => ?region_state, "apply_state" => ?res.apply_state);
                        relation.set_region_state(region_state);
                        sync_relation_with_current_batch.push(res.region_id);
                    }
                    HandleExecResResult::RollbackMerge(region_state) => {
                        info!("rollback merge"; "region_id" => res.region_id, "region_state" => ?region_state, "apply_state" => ?res.apply_state);
                        relation.set_region_state(region_state);
                        sync_relation_with_current_batch.push(res.region_id);
                    }
                    HandleExecResResult::CommitMerge {
                        target_state,
                        source_relation,
                    } => {
                        info!(
                            "commit merge";
                            "region_id" => res.region_id,
                            "target_state" => ?target_state,
                            "source_relation" => ?source_relation,
                            "apply_state" => ?res.apply_state,
                        );
                        let source_region_id = source_relation.get_region_id();
                        relation.set_region_state(target_state);
                        self.handle_relation(source_relation);
                        // Need to gurantee these two relations be synced atomically.
                        sync_relation_with_current_batch.push(res.region_id);
                        sync_relation_with_current_batch.push(source_region_id);
                    }
                    HandleExecResResult::Destroy(region) => {
                        info!(
                            "region destroyed";
                            "region_id" => res.region_id,
                            "apply_state" => ?res.apply_state,
                        );
                        self.handle_destroy_region(region);
                        // Skip handling relation after region was destroyed.
                        skip_handle_relation = true;
                    }
                    HandleExecResResult::IngestSst => {
                        // Sync this relation to make sure all IngestSst cmds are skipped during
                        // recovery.
                        sync_relation_with_current_batch.push(res.region_id);
                    }
                }
            }
            if !skip_handle_relation {
                self.handle_relation(relation);
            }
        }
        self.sync_region_relation(sync_relation_with_current_batch);

        SEQNO_UNCOMMITTED_COUNT.set(self.seqno_window.pending_count() as i64);
        debug!("pending seqno count"; "count" => self.seqno_window.pending_count());
    }

    fn handle_relation(&mut self, mut relation: RegionSequenceNumberRelation) {
        match self.inflight_seqno_relations.entry(relation.region_id) {
            HashMapEntry::Occupied(mut e) => {
                let prev = e.get_mut();
                // For a region, seqno must never fall back because we handle ApplyRes in order.
                assert!(prev.sequence_number <= relation.sequence_number);
                if !relation.has_region_state() && prev.has_region_state() {
                    info!("merge inflight relations"; "region_id" => relation.region_id, "prev" => ?prev, "new" => ?relation);
                    relation.set_region_state(prev.take_region_state());
                }
                *prev = relation;
            }
            HashMapEntry::Vacant(e) => {
                e.insert(relation);
            }
        }
    }

    fn sync_region_relation(&mut self, regions: Vec<u64>) {
        for region_id in regions {
            if let Some(relation) = self.inflight_seqno_relations.remove(&region_id) {
                info!("save seqno relation to raftdb"; "region_id" => region_id, "relation" => ?relation);
                self.raft_wb
                    .put_seqno_relation(region_id, &relation)
                    .unwrap();
            }
        }
        self.consume_raft_wb(true);
    }

    fn on_memtable_flushed(&mut self, cf: Option<String>, seqno: u64) {
        if !self.started {
            assert!(self.inflight_seqno_relations.is_empty());
        }
        let sync_relations = std::mem::take(&mut self.inflight_seqno_relations);
        if !sync_relations.is_empty() {
            self.handle_sync_relations(sync_relations);
        }
        let seqno = cmp::min(seqno, self.seqno_window.committed_seqno());
        let min_flushed = match cf.as_ref() {
            Some(cf) => self.flushed_seqno.update(cf, seqno),
            None => self.flushed_seqno.update_all(seqno),
        };
        let raft_engine = self.engines.raft.clone();
        raft_engine.put_flushed_seqno(&self.flushed_seqno).unwrap();
        if let Some(min) = min_flushed {
            for region_id in mem::take(&mut self.pending_clean_regions) {
                if let Some(raft_state) = self.engines.raft.get_raft_state(region_id).unwrap() {
                    if self.router.mailbox(region_id).is_none() {
                        let region_state =
                            raft_engine.get_region_state(region_id).unwrap().unwrap();
                        raft_engine
                            .clean(region_id, 0, &raft_state, &mut self.raft_wb)
                            .unwrap();
                        self.raft_wb
                            .put_region_state(region_id, &region_state)
                            .unwrap();
                    } else {
                        // Region destroy may be delayed, clean up meta later.
                        self.pending_clean_regions.push(region_id);
                    }
                }
            }
            let mut pending_clean_regions =
                gc_seqno_relations(min, &self.engines.raft, &self.router, &mut self.raft_wb)
                    .unwrap();
            self.pending_clean_regions
                .append(&mut pending_clean_regions);
            if !self.raft_wb.is_empty() {
                self.consume_raft_wb(true);
            }
            if let Err(e) = self
                .raftlog_gc_scheduler
                .schedule(RaftlogGcTask::TryExecPendingTask)
            {
                warn!("failed to notify memtable flushed to raftlog gc worker"; "err" => ?e);
            }
        }
    }

    fn handle_sync_relations(&mut self, relations: HashMap<u64, RegionSequenceNumberRelation>) {
        let mut count = 0;
        let size = relations.len();
        for (region_id, relation) in relations {
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
        self.engines.raft.sync().unwrap();
        SEQNO_RELATIONS_KEYS_FLOW.inc_by(count as u64);
    }

    fn handle_exec_res(
        &mut self,
        region_id: u64,
        seqno: u64,
        exec_res: &[ExecResult<EK::Snapshot>],
    ) -> Vec<HandleExecResResult> {
        if exec_res.is_empty() {
            return vec![];
        }
        info!("handle exec res"; "exec_res" => ?exec_res, "region_id" => region_id);
        let mut results = vec![];
        for res in exec_res {
            match res {
                ExecResult::ChangePeer(cp) => {
                    if cp.index == raft::INVALID_INDEX {
                        // Apply failed, skip.
                        continue;
                    }
                    if cp.remove_self {
                        info!("handle region self destroy"; "region_id" => region_id);
                        results.push(HandleExecResResult::Destroy(cp.region.clone()));
                    } else {
                        let mut state = RegionLocalState::default();
                        state.set_region(cp.region.clone());
                        state.set_state(PeerState::Normal);
                        results.push(HandleExecResResult::ConfChange(state));
                    };
                }
                ExecResult::SplitRegion {
                    regions,
                    new_split_regions,
                    ..
                } => {
                    let mut new_created_regions = vec![];
                    let mut region_state = None;
                    for region in regions {
                        let mut state = RegionLocalState::default();
                        state.set_region(region.clone());
                        state.set_state(PeerState::Normal);
                        if region.get_id() == region_id {
                            region_state = Some(state);
                            continue;
                        }
                        if let Some(new_split_peer) = new_split_regions.get(&region.get_id()) {
                            if new_split_peer.result.is_some() {
                                continue;
                            }
                            new_created_regions.push(state);
                        }
                    }
                    results.push(HandleExecResResult::SplitRegion {
                        new_created_regions,
                        region_state: region_state.unwrap(),
                    });
                }
                ExecResult::PrepareMerge {
                    region,
                    state: merge_state,
                } => {
                    let mut state = RegionLocalState::default();
                    state.set_region(region.clone());
                    state.set_state(PeerState::Merging);
                    state.set_merge_state(merge_state.clone());
                    results.push(HandleExecResResult::PrepareMerge(state));
                }
                ExecResult::CommitMerge {
                    region,
                    source,
                    commit,
                    ..
                } => {
                    let mut target_state = RegionLocalState::default();
                    target_state.set_region(region.clone());
                    target_state.set_state(PeerState::Normal);
                    // Write source state
                    let mut source_relation = RegionSequenceNumberRelation::default();
                    source_relation.set_region_id(source.get_id());
                    source_relation.set_sequence_number(seqno);
                    let mut apply_state = RaftApplyState::default();
                    apply_state.set_applied_index(*commit);
                    apply_state.set_commit_index(*commit);
                    let entry = self
                        .engines
                        .raft
                        .get_entry(source.get_id(), *commit)
                        .unwrap()
                        .unwrap_or_else(|| panic!("failed to get entry {}", commit));
                    apply_state.set_commit_term(entry.term);
                    source_relation.set_apply_state(apply_state);
                    let mut source_state = RegionLocalState::default();
                    let mut merging_state = MergeState::default();
                    merging_state.set_target(region.clone());
                    source_state.set_region(source.clone());
                    source_state.set_state(PeerState::Tombstone);
                    source_state.set_merge_state(merging_state);
                    source_relation.set_region_state(source_state);
                    results.push(HandleExecResResult::CommitMerge {
                        target_state,
                        source_relation,
                    });
                }
                ExecResult::RollbackMerge { region, .. } => {
                    let mut state = RegionLocalState::default();
                    state.set_region(region.clone());
                    state.set_state(PeerState::Normal);
                    results.push(HandleExecResResult::RollbackMerge(state));
                }
                ExecResult::IngestSst { .. } => {
                    results.push(HandleExecResResult::IngestSst);
                }
                ExecResult::DeleteRange { .. }
                | ExecResult::TransferLeader { .. }
                | ExecResult::VerifyHash { .. }
                | ExecResult::CompactLog { .. }
                | ExecResult::ComputeHash { .. } => (),
                ExecResult::SetFlashbackState { .. } => {
                    // TODO: need to handle this res?
                    todo!()
                }
            }
        }
        results
    }

    fn handle_destroy_region(&mut self, region: Region) {
        // Cleanup all stale relations.
        let raft_engine = self.engines.raft.clone();
        let region_id = region.get_id();
        self.inflight_seqno_relations.remove(&region_id);
        clear_region_seqno_relation(region_id, &raft_engine, &mut self.raft_wb);
        let mut region_state = RegionLocalState::default();
        region_state.set_region(region);
        region_state.set_state(PeerState::Tombstone);
        self.raft_wb
            .put_region_state(region_id, &region_state)
            .unwrap();
        info!("handle destroy region"; "region_id" => region_id, "region_state" => ?region_state);
    }

    fn handle_apply_snapshot(&mut self, region_id: u64, status: Arc<AtomicUsize>, peer_id: u64) {
        fail_point!("seqno_relation_apply_snapshot");
        let raft_engine = self.engines.raft.clone();
        self.inflight_seqno_relations.remove(&region_id);
        clear_region_seqno_relation(region_id, &raft_engine, &mut self.raft_wb);
        info!("handle apply snapshot"; "region_id" => region_id);
        if let Err(e) = self.region_scheduler.schedule(RegionTask::Apply {
            region_id,
            status,
            peer_id,
        }) {
            warn!("failed to schedule region task"; "err" => ?e, "region_id" => region_id);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task<EK::Snapshot>;
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ApplyRes(apply_res) => {
                self.on_apply_res(&apply_res);
                for r in apply_res {
                    let region_id = r.region_id;
                    if let Err(e) = self.router.force_send(
                        region_id,
                        PeerMsg::ApplyRes {
                            res: ApplyTaskRes::Apply(r),
                        },
                    ) {
                        error!(
                            "failed to force send apply res";
                            "region_id" => region_id,
                            "err" => ?e
                        );
                    }
                }
            }
            Task::MemtableFlushed { cf, seqno } => self.on_memtable_flushed(cf, seqno),
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
            Task::ApplySnapshot {
                region_id,
                status,
                peer_id,
            } => self.handle_apply_snapshot(region_id, status, peer_id),
            Task::RecoverStatus {
                region_id,
                status,
                cb,
            } => {
                let _ = self.router.force_send(
                    region_id,
                    PeerMsg::SignificantMsg(SignificantMsg::RecoverStatus { status, cb }),
                );
            }
        }
    }
}
