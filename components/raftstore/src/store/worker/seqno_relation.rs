// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, fmt};

use collections::{HashMap, HashMapEntry};
use engine_traits::{
    util::{set_max_synced_sequence_number, FlushedSeqno, SequenceNumberWindow},
    Engines, KvEngine, RaftEngine, RaftLogBatch, Snapshot, DATA_CFS,
};
use kvproto::raft_serverpb::RegionSequenceNumberRelation;
use tikv_util::{
    self, debug, error, info,
    worker::{Runnable, Scheduler},
};

use super::metrics::*;
use crate::store::{
    async_io::write::{RAFT_WB_DEFAULT_SIZE, RAFT_WB_SHRINK_SIZE},
    fsm::{ApplyRes, ApplyTaskRes},
    util::gc_seqno_relations,
    worker::RaftlogGcTask,
    PeerMsg, RaftRouter, RegionTask,
};

const RAFT_WB_MAX_KEYS: usize = 256;

pub enum Task<S: Snapshot> {
    ApplyRes(Vec<ApplyRes<S>>),
    MemtableSealed(u64),
    MemtableFlushed { cf: Option<String>, seqno: u64 },
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
    region_scheduler: Scheduler<RegionTask<EK::Snapshot>>,
    started: bool,
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
            last_persisted_seqno: 0,
            raftlog_gc_scheduler,
            region_scheduler,
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
            relation.set_sequence_number(seqno.get_number());
            relation.set_apply_state(res.apply_state.clone());
            // TODO: check exec results to get updated region state
            self.handle_relation(relation, &mut sync_relations);
        }
        if !sync_relations.is_empty() {
            self.handle_sync_relations(sync_relations);
        }

        self.consume_raft_wb(true);
        set_max_synced_sequence_number(self.seqno_window.committed_seqno());

        SEQNO_UNCOMMITTED_COUNT.set(self.seqno_window.pending_count() as i64);
        debug!("pending seqno count"; "count" => self.seqno_window.pending_count());
    }

    fn handle_relation(
        &mut self,
        mut relation: RegionSequenceNumberRelation,
        sync_relations: &mut HashMap<u64, RegionSequenceNumberRelation>,
    ) {
        let relations = match relation.sequence_number.cmp(&self.last_persisted_seqno) {
            cmp::Ordering::Less | cmp::Ordering::Equal => sync_relations,
            cmp::Ordering::Greater => &mut self.inflight_seqno_relations,
        };
        match relations.entry(relation.region_id) {
            HashMapEntry::Occupied(mut e) => {
                let prev = e.get_mut();
                if prev.sequence_number < relation.sequence_number {
                    if !relation.has_region_state() && prev.has_region_state() {
                        relation.set_region_state(prev.take_region_state());
                        debug!("merge inflight relations"; "region_id" => relation.region_id, "prev" => ?prev, "new" => ?relation);
                    }
                    *prev = relation;
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
        // TODO: save seqno to raftdb.
        let min_flushed = match cf.as_ref() {
            Some(cf) => self.flushed_seqno.update(cf, seqno),
            None => self.flushed_seqno.update_all(seqno),
        };
        // Prevent raft log gc before recovery done.
        if self.started {
            if let Some(min) = min_flushed {
                gc_seqno_relations(min, &self.engines.raft, &mut self.raft_wb).unwrap();
                if !self.raft_wb.is_empty() {
                    self.consume_raft_wb(true);
                }
                // TODO: notify gc worker to gc raft logs
                // if let Err(e) = self
                //     .raftlog_gc_scheduler
                //     .schedule(RaftlogGcTask::MemtableFlushed { cf, seqno })
                // {
                //     warn!("failed to notify memtable flushed to raftlog gc
                // worker"; "err" => ?e); }
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
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task<EK::Snapshot>;
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ApplyRes(apply_res) => {
                self.on_apply_res(&apply_res);
                if self.started {
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
            }
            Task::MemtableSealed(seqno) => self.on_memtable_sealed(seqno),
            Task::MemtableFlushed { cf, seqno } => self.on_memtable_flushed(cf, seqno),
        }
    }
}
