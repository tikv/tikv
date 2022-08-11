// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fmt, sync::atomic::Ordering};

use collections::{HashMap, HashMapEntry};
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch, Snapshot};
use kvproto::raft_serverpb::{RaftApplyState, RegionLocalState, RegionSequenceNumberRelation};
use tikv_util::{
    sequence_number::{SequenceNumber, SequenceNumberWindow, SYNCED_MAX_SEQUENCE_NUMBER},
    time::Instant,
    worker::Runnable,
};

use super::metrics::*;
use crate::store::{
    async_io::write::{RAFT_WB_DEFAULT_SIZE, RAFT_WB_SHRINK_SIZE},
    fsm::{store::ApplyResNotifier, ApplyNotifier, ApplyRes},
};

pub enum Task<S: Snapshot> {
    ApplyRes(Vec<Box<ApplyRes<S>>>),
    MemtableSealed(u64),
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
        }
    }
}

struct SeqnoRelation {
    region_id: u64,
    seqno: SequenceNumber,
    apply_state: RaftApplyState,
    region_local_state: Option<RegionLocalState>,
}

pub struct Runner<EK: KvEngine, ER: RaftEngine> {
    apply_res_notifier: ApplyResNotifier<EK, ER>,
    raftdb: ER,
    wb: ER::LogBatch,
    seqno_window: SequenceNumberWindow,
    inflight_seqno_relations: HashMap<u64, SeqnoRelation>,
    last_flushed_seqno: u64,
}

impl<EK: KvEngine, ER: RaftEngine> Runner<EK, ER> {
    pub fn new(apply_res_notifier: ApplyResNotifier<EK, ER>, raftdb: ER) -> Self {
        Runner {
            wb: raftdb.log_batch(0),
            raftdb,
            apply_res_notifier,
            seqno_window: SequenceNumberWindow::default(),
            inflight_seqno_relations: HashMap::default(),
            last_flushed_seqno: 0,
        }
    }

    fn on_apply_res(&mut self, apply_res: Vec<Box<ApplyRes<EK::Snapshot>>>) {
        use std::cmp::Ordering;

        let mut sync_relations = HashMap::default();
        for res in &apply_res {
            if let Some(seq) = res.last_seqno {
                let relation = SeqnoRelation {
                    region_id: res.region_id,
                    seqno: seq,
                    apply_state: res.apply_state.clone(),
                    region_local_state: res.region_local_state.clone(),
                };
                let relations = match seq.number.cmp(&self.last_flushed_seqno) {
                    Ordering::Less | Ordering::Equal => &mut sync_relations,
                    Ordering::Greater => &mut self.inflight_seqno_relations,
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
                self.seqno_window.push(seq);
            }
        }

        if !sync_relations.is_empty() {
            self.handle_sync_relations(sync_relations);
        }
        self.apply_res_notifier.notify(apply_res);
    }

    fn on_memtable_sealed(&mut self, seqno: u64) {
        self.last_flushed_seqno = seqno;
        let sync_relations = std::mem::take(&mut self.inflight_seqno_relations);
        self.handle_sync_relations(sync_relations);
    }

    fn handle_sync_relations(&mut self, relations: HashMap<u64, SeqnoRelation>) {
        let mut relation = RegionSequenceNumberRelation::default();
        let count = relations.len();
        for (region_id, r) in relations {
            assert!(r.seqno.number <= self.last_flushed_seqno);
            self.seqno_window.push(r.seqno);
            relation.set_region_id(r.region_id);
            relation.set_apply_state(r.apply_state);
            relation.set_sequence_number(r.seqno.number);
            self.wb.put_seqno_relation(region_id, &relation).unwrap();
        }
        SEQNO_RELATIONS_KEYS_FLOW.inc_by(count as u64);
        SEQNO_RELATIONS_WRITE_FLOW.inc_by(self.wb.persist_size() as u64);
        let start = Instant::now();
        self.raftdb
            .consume_and_shrink(
                &mut self.wb,
                true,
                RAFT_WB_SHRINK_SIZE,
                RAFT_WB_DEFAULT_SIZE,
            )
            .unwrap();
        SEQNO_RELATIONS_WRITE_DURATION_HISTOGRAM.observe(start.saturating_elapsed_secs());
        SYNCED_MAX_SEQUENCE_NUMBER.store(self.seqno_window.committed_seqno(), Ordering::SeqCst);
    }
}

impl<EK: KvEngine, ER: RaftEngine> Runnable for Runner<EK, ER> {
    type Task = Task<EK::Snapshot>;
    fn run(&mut self, task: Task<EK::Snapshot>) {
        match task {
            Task::ApplyRes(apply_res) => self.on_apply_res(apply_res),
            Task::MemtableSealed(seqno) => self.on_memtable_sealed(seqno),
        }
    }
}
