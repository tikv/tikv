// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains snapshot relative processing logic.
//!
//! # Snapshot State
//!
//! generator and apply snapshot works asynchronously. the snap_sate indicates
//! the curren snapshot state.
//!
//! # Process Overview
//!
//! generate snapshot:
//! - Raft call `snapshot` interface to acquire a snapshot, then storage setup
//!   the gen_snap_task.
//! - handle ready will send the gen_snap_task to the apply work
//! - apply worker schedule a gen tablet snapshot task to async read worker with
//!   region state and apply state.
//! - async read worker generates the tablet snapshot and sends the result to
//!   peer fsm, then Raft will get the snapshot.

use std::{
    borrow::BorrowMut,
    fmt::{self, Debug},
    mem,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc, Arc,
    },
};

use engine_traits::{KvEngine, RaftEngine};
use kvproto::raft_serverpb::{RaftSnapshotData, RegionLocalState};
use protobuf::Message;
use raft::eraftpb::Snapshot;
use raftstore::store::{metrics::STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER, GenSnapRes, ReadTask};
use slog::{error, info, warn};
use tikv_util::{box_try, worker::Scheduler};

use crate::{
    fsm::ApplyResReporter,
    raft::{Apply, Peer, Storage},
    router::{ApplyTask, PeerTick},
    Result,
};

#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating {
        canceled: Arc<AtomicBool>,
        index: Arc<AtomicU64>,
    },
    Generated(Box<Snapshot>),
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (&SnapState::Relax, &SnapState::Relax)
            | (&SnapState::Generating { .. }, &SnapState::Generating { .. }) => true,
            (&SnapState::Generated(ref snap1), &SnapState::Generated(ref snap2)) => {
                *snap1 == *snap2
            }
            _ => false,
        }
    }
}

pub struct GenSnapTask {
    region_id: u64,
    // The snapshot will be sent to the peer.
    to_peer: u64,
    // Fill it when you are going to generate the snapshot.
    // index used to check if the gen task should be canceled.
    index: Arc<AtomicU64>,
    // Set it to true to cancel the task if necessary.
    canceled: Arc<AtomicBool>,
    // indicates whether the snapshot is triggered due to load balance
    for_balance: bool,
}

impl GenSnapTask {
    pub fn new(
        region_id: u64,
        to_peer: u64,
        index: Arc<AtomicU64>,
        canceled: Arc<AtomicBool>,
    ) -> GenSnapTask {
        GenSnapTask {
            region_id,
            to_peer,
            index,
            canceled,
            for_balance: false,
        }
    }

    pub fn set_for_balance(&mut self) {
        self.for_balance = true;
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("region_id", &self.region_id)
            .finish()
    }
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_snapshot_generated(&mut self, snapshot: GenSnapRes) {
        if self.storage_mut().on_snapshot_generated(snapshot) {
            self.raft_group_mut().ping();
            self.set_has_ready();
        }
    }
}

impl<EK: KvEngine, R: ApplyResReporter> Apply<EK, R> {
    /// Handle snapshot.
    ///
    /// Will schedule a task to read worker and then generate a snapshot
    /// asynchronously.
    pub fn schedule_gen_snapshot(&mut self, snap_task: GenSnapTask) {
        // Do not generate, the peer is removed.
        if self.tombstone() {
            snap_task.canceled.store(true, Ordering::SeqCst);
            error!(
                self.logger,
                "cancel generating snapshot because it's already destroyed";
            );
            return;
        }
        // Flush before do snapshot.
        if snap_task.canceled.load(Ordering::SeqCst) {
            return;
        }
        self.flush();

        // Send generate snapshot task to region worker.
        let (last_applied_index, last_applied_term) = self.apply_progress();
        snap_task.index.store(last_applied_index, Ordering::SeqCst);
        let gen_tablet_sanp_task = ReadTask::GenTabletSnapshot {
            region_id: snap_task.region_id,
            to_peer: snap_task.to_peer,
            tablet: self.tablet().clone(),
            region_state: self.region_state().clone(),
            last_applied_term,
            last_applied_index,
            for_balance: snap_task.for_balance,
            canceled: snap_task.canceled.clone(),
        };
        if let Err(e) = self.read_scheduler().schedule(gen_tablet_sanp_task) {
            error!(
                self.logger,
                "schedule snapshot failed";
                "error" => ?e,
            );
            snap_task.canceled.store(true, Ordering::SeqCst);
        }
    }
}

impl<EK: KvEngine, ER: RaftEngine> Storage<EK, ER> {
    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no
    /// unavailable snapshot.
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        let mut snap_state = self.snap_state_mut();
        match *snap_state {
            SnapState::Generating { ref canceled, .. } => {
                if canceled.load(Ordering::SeqCst) {
                    self.cancel_generating_snap(None);
                } else {
                    return Err(raft::Error::Store(
                        raft::StorageError::SnapshotTemporarilyUnavailable,
                    ));
                }
            }
            SnapState::Generated(ref s) => {
                let SnapState::Generated(snap) = mem::replace(&mut *snap_state, SnapState::Relax) else { unreachable!() };
                if self.validate_snap(&snap, request_index) {
                    return Ok(*snap);
                }
            }
            _ => {}
        }

        if SnapState::Relax != *snap_state {
            panic!(
                "{:?} unexpected state: {:?}",
                self.logger().list(),
                *snap_state
            );
        }

        info!(
            self.logger(),
            "requesting snapshot";
            "request_index" => request_index,
            "request_peer" => to,
        );
        let canceled = Arc::new(AtomicBool::new(false));
        let index = Arc::new(AtomicU64::new(0));
        *snap_state = SnapState::Generating {
            canceled: canceled.clone(),
            index: index.clone(),
        };

        let task = GenSnapTask::new(self.region().get_id(), to, index, canceled);
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
    pub fn cancel_generating_snap(&self, compact_to: Option<u64>) {
        let mut snap_state = self.snap_state_mut();
        let SnapState::Generating {
           ref canceled,
           ref index,
        } = *snap_state else { return };

        if let Some(idx) = compact_to {
            let snap_index = index.load(Ordering::SeqCst);
            if snap_index == 0 || idx <= snap_index + 1 {
                return;
            }
        }
        canceled.store(true, Ordering::SeqCst);
        *snap_state = SnapState::Relax;
        self.gen_snap_task_mut().take();
        info!(
            self.logger(),
            "snapshot is canceled";
            "compact_to" => compact_to,
        );
        STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.cancel.inc();
    }

    /// Try to switch snap state to generated. only `Generating` can switch to
    /// `Generated`.
    ///  TODO: make the snap state more clearer, the snapshot must be consumed.
    pub fn on_snapshot_generated(&self, res: GenSnapRes) -> bool {
        if res.is_none() {
            self.cancel_generating_snap(None);
            return false;
        }
        let snap = res.unwrap();
        let mut snap_state = self.snap_state_mut();
        let SnapState::Generating {
            ref canceled,
            ref index,
         } = *snap_state else { return false };

        if snap.get_metadata().get_index() < index.load(Ordering::SeqCst) {
            warn!(
                self.logger(),
                "snapshot is staled, skip";
                "snap index" => snap.get_metadata().get_index(),
                "required index" => index.load(Ordering::SeqCst),
            );
            return false;
        }
        // Should changed `SnapState::Generated` to `SnapState::Relax` when the
        // snap is consumed or canceled. Such as leader changed, the state of generated
        // should be reset.
        *snap_state = SnapState::Generated(snap);
        true
    }
}
