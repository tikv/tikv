// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    borrow::BorrowMut,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
};

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_serverpb::RaftSnapshotData;
use protobuf::Message;
use raft::eraftpb::Snapshot;
use raftstore::store::{metrics::STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER, SnapState};
use slog::{error, info};

use crate::{
    fsm::ApplyResReporter,
    raft::{Apply, Peer, Storage},
    router::{ApplyTask, GenSnapTask, PeerTick},
};

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_snapshot_generated(&mut self, snapshot: Box<Snapshot>) {
        self.storage_mut().on_snapshot_generated(snapshot);
        self.raft_group_mut().ping();
        self.set_has_ready();
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    /// Handle snapshot.
    ///
    /// Will schedule a task to read worker and then generate a snapshot
    /// asynchronously.
    pub fn handle_snapshot(&mut self, snap_task: GenSnapTask) {
        // Flush before do snapshot.
        self.flush();

        // Send generate snapshot task to region worker.
        let (last_applied_index, last_applied_term) = self.apply_progress();
        if let Err(e) = snap_task.generate_and_schedule_snapshot(
            self.tablet().clone(),
            self.region_state().clone(),
            last_applied_index,
            last_applied_term,
            self.read_scheduler(),
        ) {
            error!(
                self.logger,
                "schedule snapshot failed";
                "error" => ?e,
            );
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Storage<EK, ER> {
    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no
    /// unavailable snapshot.
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state_mut();
        match *snap_state {
            SnapState::Generating { .. } => {
                return Err(raft::Error::Store(
                    raft::StorageError::SnapshotTemporarilyUnavailable,
                ));
            }
            SnapState::Generated(ref s) => {
                let snap = *s.clone();
                *snap_state = SnapState::Relax;
                if self.validate_snap(&snap, request_index) {
                    return Ok(snap);
                }
            }
            _ => {}
        }

        if SnapState::Relax != *snap_state {
            panic!(
                "[region {}] unexpected state: {:?}",
                self.region().get_id(),
                *snap_state
            );
        }

        info!(
            self.logger(),
            "requesting snapshot";
            "tablet_index" => self.get_tablet_index(),
            "request_index" => request_index,
            "request_peer" => to,
        );
        let (_, receiver) = mpsc::sync_channel(1);
        let canceled = Arc::new(AtomicBool::new(false));
        let index = Arc::new(AtomicU64::new(0));
        *snap_state = SnapState::Generating {
            canceled: canceled.clone(),
            index: index.clone(),
            receiver,
        };

        let task = GenSnapTask::new(self.region().get_id(), index, canceled);
        let mut gen_snap_task = self.gen_snap_task_mut();
        assert!(gen_snap_task.is_none());
        *gen_snap_task = Box::new(Some(task));
        Err(raft::Error::Store(
            raft::StorageError::SnapshotTemporarilyUnavailable,
        ))
    }

    /// Validate the snapshot. Returns true if it's valid.
    fn validate_snap(&self, snap: &Snapshot, request_index: u64) -> bool {
        let idx = snap.get_metadata().get_index();
        // TODO(nolouch): check tuncated index
        if idx < request_index {
            // stale snapshot, should generate again.
            info!(
                self.logger(),
                "snapshot is stale, generate again";
                "snap_index" => idx,
                "request_index" => request_index,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.stale.inc();
            return false;
        }

        let mut snap_data = RaftSnapshotData::default();
        if let Err(e) = snap_data.merge_from_bytes(snap.get_data()) {
            error!(
                self.logger(),
                "failed to decode snapshot, it may be corrupted";
                "err" => ?e,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.decode.inc();
            return false;
        }
        let snap_epoch = snap_data.get_region().get_region_epoch();
        let latest_epoch = self.region().get_region_epoch();
        if snap_epoch.get_conf_ver() < latest_epoch.get_conf_ver() {
            info!(
                self.logger(),
                "snapshot epoch is stale";
                "snap_epoch" => ?snap_epoch,
                "latest_epoch" => ?latest_epoch,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.epoch.inc();
            return false;
        }

        true
    }

    /// Cancel generating snapshot.
    pub fn cancel_generating_snap(&mut self, compact_to: Option<u64>) {
        let mut snap_state = self.snap_state_mut();
        if let SnapState::Generating {
            ref canceled,
            ref index,
            ..
        } = *snap_state
        {
            if let Some(idx) = compact_to {
                let snap_index = index.load(Ordering::SeqCst);
                if snap_index == 0 || idx <= snap_index + 1 {
                    return;
                }
            }
            canceled.store(true, Ordering::SeqCst);
            *snap_state = SnapState::Relax;
            info!(
                self.logger(),
                "snapshot is canceled";
                "compact_to" => compact_to,
            );
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.cancel.inc();
        }
    }

    pub fn on_snapshot_generated(&mut self, snap: Box<Snapshot>) {
        let mut snap_state = self.snap_state_mut();
        if let SnapState::Generating {
            ref canceled,
            ref index,
            ..
        } = *snap_state
        {
            *snap_state = SnapState::Generated(snap);
            info!(
                self.logger(),
                "snapshot is generated";
            );
        }
    }
}
