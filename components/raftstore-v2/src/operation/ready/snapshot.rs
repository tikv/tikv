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
    assert_matches::assert_matches,
    fmt::{self, Debug},
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};

use engine_traits::{KvEngine, RaftEngine, RaftLogBatch, TabletContext, TabletRegistry, ALL_CFS};
use kvproto::raft_serverpb::{PeerState, RaftSnapshotData};
use protobuf::Message;
use raft::{eraftpb::Snapshot, StateRole};
use raftstore::{
    coprocessor::RegionChangeEvent,
    store::{
        metrics::STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER, GenSnapRes, ReadTask, TabletSnapKey,
        TabletSnapManager, Transport, WriteTask, RAFT_INIT_LOG_INDEX,
    },
};
use slog::{error, info, warn};
use tikv_util::box_err;

use crate::{
    fsm::ApplyResReporter,
    operation::command::temp_split_path,
    raft::{Apply, Peer, Storage},
    Result, StoreContext,
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
            (SnapState::Relax, SnapState::Relax)
            | (SnapState::Generating { .. }, SnapState::Generating { .. }) => true,
            (SnapState::Generated(snap1), SnapState::Generated(snap2)) => *snap1 == *snap2,
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

    pub fn to_peer(&self) -> u64 {
        self.to_peer
    }
}

impl Debug for GenSnapTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenSnapTask")
            .field("region_id", &self.region_id)
            .finish()
    }
}

pub fn recv_snap_path(
    snap_mgr: &TabletSnapManager,
    region_id: u64,
    peer_id: u64,
    term: u64,
    index: u64,
) -> PathBuf {
    let key = TabletSnapKey::new(region_id, peer_id, term, index);
    snap_mgr.final_recv_path(&key)
}

/// Move the tablet from `source` to managed path.
///
/// Returns false if `source` doesn't exist.
pub fn install_tablet<EK: KvEngine>(
    registry: &TabletRegistry<EK>,
    source: &Path,
    region_id: u64,
    tablet_index: u64,
) -> bool {
    if !source.exists() {
        return false;
    }
    let target_path = registry.tablet_path(region_id, tablet_index);
    assert_matches!(
        EK::locked(source.to_str().unwrap()),
        Ok(false),
        "source is locked: {} => {}",
        source.display(),
        target_path.display()
    );
    if let Err(e) = fs::rename(source, &target_path) {
        panic!(
            "failed to rename tablet {} => {}: {:?}",
            source.display(),
            target_path.display(),
            e
        );
    }
    true
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    pub fn on_snapshot_generated(&mut self, snapshot: GenSnapRes) {
        if self.storage_mut().on_snapshot_generated(snapshot) {
            self.raft_group_mut().ping();
            self.set_has_ready();
        }
    }

    pub fn on_snapshot_sent(&mut self, to_peer_id: u64, status: raft::SnapshotStatus) {
        let to_peer = match self.peer_from_cache(to_peer_id) {
            Some(peer) => peer,
            None => {
                // If to_peer is gone, ignore this snapshot status
                warn!(
                    self.logger,
                    "peer not found, ignore snapshot status";
                    "to_peer_id" => to_peer_id,
                    "status" => ?status,
                );
                return;
            }
        };
        info!(
            self.logger,
            "report snapshot status";
            "to" => ?to_peer,
            "status" => ?status,
        );
        self.raft_group_mut().report_snapshot(to_peer_id, status);
    }

    pub fn on_applied_snapshot<T: Transport>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::Create,
            StateRole::Follower,
        );
        let persisted_index = self.persisted_index();
        let first_index = self.storage().entry_storage().first_index();
        if first_index == persisted_index + 1 {
            let region_id = self.region_id();
            self.reset_flush_state();
            let flush_state = self.flush_state().clone();
            let mut tablet_ctx = TabletContext::new(self.region(), Some(persisted_index));
            // Use a new FlushState to avoid conflicts with the old one.
            tablet_ctx.flush_state = Some(flush_state);
            ctx.tablet_registry.load(tablet_ctx, false).unwrap();
            self.record_tablet_as_tombstone_and_refresh(persisted_index, ctx);
            self.schedule_apply_fsm(ctx);
            self.storage_mut().on_applied_snapshot();
            self.raft_group_mut().advance_apply_to(persisted_index);
            {
                let mut meta = ctx.store_meta.lock().unwrap();
                meta.set_region(self.region(), true, &self.logger);
                meta.readers
                    .insert(region_id, self.generate_read_delegate());
                meta.region_read_progress
                    .insert(region_id, self.read_progress().clone());
            }
            self.read_progress_mut()
                .update_applied_core(persisted_index);
            let split = self.storage_mut().split_init_mut().take();
            if split.as_ref().map_or(true, |s| {
                !s.scheduled || persisted_index != RAFT_INIT_LOG_INDEX
            }) {
                info!(self.logger, "apply tablet snapshot completely");
            }
            if let Some(init) = split {
                info!(self.logger, "init with snapshot finished");
                self.post_split_init(ctx, init);
            }
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
    pub fn is_generating_snapshot(&self) -> bool {
        let snap_states = self.snap_states.borrow_mut();
        for (_, state) in snap_states.iter() {
            if matches!(*state, SnapState::Generating { .. }) {
                return true;
            }
        }
        false
    }

    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no
    /// unavailable snapshot.
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        if let Some(state) = self.snap_states.borrow_mut().get_mut(&to) {
            match state {
                SnapState::Generating { ref canceled, .. } => {
                    if canceled.load(Ordering::SeqCst) {
                        self.cancel_generating_snap(Some(to));
                    } else {
                        return Err(raft::Error::Store(
                            raft::StorageError::SnapshotTemporarilyUnavailable,
                        ));
                    }
                }
                SnapState::Generated(ref s) => {
                    let snap = *s.clone();
                    *state = SnapState::Relax;
                    if self.validate_snap(&snap, request_index) {
                        return Ok(snap);
                    }
                }
                _ => {}
            };
        }

        info!(
            self.logger(),
            "requesting snapshot";
            "request_index" => request_index,
            "request_peer" => to,
        );
        let canceled = Arc::new(AtomicBool::new(false));
        let index = Arc::new(AtomicU64::new(0));
        let mut gen_snap_task = self.gen_snap_task_mut();
        if gen_snap_task.is_none() {
            self.snap_states.borrow_mut().insert(
                to,
                SnapState::Generating {
                    canceled: canceled.clone(),
                    index: index.clone(),
                },
            );
            let task = GenSnapTask::new(self.region().get_id(), to, index, canceled);
            *gen_snap_task = Box::new(Some(task));
        }
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

    pub fn cancel_generating_snap(&self, to_peer: Option<u64>) {
        if let Some(id) = to_peer {
            let mut states = self.snap_states.borrow_mut();
            if let Some(state) = states.get(&id)
                && matches!(*state, SnapState::Generating { .. })
            {
                info!(
                    self.logger(),
                    "snapshot is canceled";
                    "to_peer" => to_peer,
                );
                self.cancel_snap_task(to_peer);
                states.remove(&id);
            }
        } else {
            self.cancel_snap_task(to_peer);
            self.snap_states.borrow_mut().clear();
        }
        STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.cancel.inc();
    }

    pub fn cancel_generating_snap_due_to_compacted(&self, compact_to: u64) {
        let mut states = self.snap_states.borrow_mut();
        states.retain(|id, state| {
            let SnapState::Generating {
                ref index,
                ..
            } = *state else { return true; };
            let snap_index = index.load(Ordering::SeqCst);
            if snap_index == 0 || compact_to <= snap_index + 1 {
                return true;
            }
            info!(
                self.logger(),
                "snapshot is canceled";
                "compact_to" => compact_to,
                "to_peer" => id,
            );
            self.cancel_snap_task(Some(*id));
            STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER.cancel.inc();
            false
        });
    }

    /// Try to switch snap state to generated. only `Generating` can switch to
    /// `Generated`.
    ///  TODO: make the snap state more clearer, the snapshot must be consumed.
    pub fn on_snapshot_generated(&self, res: GenSnapRes) -> bool {
        if res.is_none() {
            self.cancel_generating_snap(None);
            return false;
        }
        let (snapshot, to_peer_id) = *res.unwrap();
        if let Some(state) = self.snap_states.borrow_mut().get_mut(&to_peer_id) {
            let SnapState::Generating {
                ref index,
                ..
            } = *state else { return false };
            if snapshot.get_metadata().get_index() < index.load(Ordering::SeqCst) {
                warn!(
                    self.logger(),
                    "snapshot is staled, skip";
                    "snap index" => snapshot.get_metadata().get_index(),
                    "required index" => index.load(Ordering::SeqCst),
                    "to_peer_id" => to_peer_id,
                );
                return false;
            }
            *state = SnapState::Generated(Box::new(snapshot));
        }
        true
    }

    pub fn on_applied_snapshot(&mut self) {
        let entry = self.entry_storage_mut();
        let term = entry.truncated_term();
        let index = entry.truncated_index();
        entry.set_applied_term(term);
        entry.apply_state_mut().set_applied_index(index);
        self.apply_trace_mut().reset_snapshot(index);
    }

    pub fn apply_snapshot(
        &mut self,
        snap: &Snapshot,
        task: &mut WriteTask<EK, ER>,
        snap_mgr: TabletSnapManager,
        reg: TabletRegistry<EK>,
    ) -> Result<()> {
        let region_id = self.region().get_id();
        let peer_id = self.peer().get_id();
        info!(
            self.logger(),
            "begin to apply snapshot";
        );

        let mut snap_data = RaftSnapshotData::default();
        snap_data.merge_from_bytes(snap.get_data())?;
        let region = snap_data.take_region();
        if region.get_id() != region_id {
            return Err(box_err!(
                "mismatch region id {}!={}",
                region_id,
                region.get_id()
            ));
        }

        let last_index = snap.get_metadata().get_index();
        let last_term = snap.get_metadata().get_term();
        let region_state = self.region_state_mut();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region);
        region_state.set_tablet_index(last_index);
        let entry_storage = self.entry_storage_mut();
        entry_storage.raft_state_mut().set_last_index(last_index);
        entry_storage.set_truncated_index(last_index);
        entry_storage.set_truncated_term(last_term);
        entry_storage.set_last_term(last_term);

        self.apply_trace_mut().reset_should_persist();
        self.set_ever_persisted();
        let lb = task
            .extra_write
            .ensure_v2(|| self.entry_storage().raft_engine().log_batch(3));
        lb.put_apply_state(region_id, last_index, self.apply_state())
            .unwrap();
        lb.put_region_state(region_id, last_index, self.region_state())
            .unwrap();
        // We assume there should be flush records in all CFs. Skip any CF here may
        // break the constraint.
        for cf in ALL_CFS {
            lb.put_flushed_index(region_id, cf, last_index, last_index)
                .unwrap();
        }

        let (path, clean_split) = match self.split_init_mut() {
            // If index not match, the peer may accept a newer snapshot after split.
            Some(init) if init.scheduled && last_index == RAFT_INIT_LOG_INDEX => {
                (temp_split_path(&reg, region_id), false)
            }
            si => (
                recv_snap_path(&snap_mgr, region_id, peer_id, last_term, last_index),
                si.is_some(),
            ),
        };

        let logger = self.logger().clone();
        // The snapshot require no additional processing such as ingest them to DB, but
        // it should load it into the factory after it persisted.
        let hook = move || {
            if !install_tablet(&reg, &path, region_id, last_index) {
                panic!(
                    "{:?} failed to install tablet, path: {}, region_id: {}, tablet_index: {}",
                    logger.list(),
                    path.display(),
                    region_id,
                    last_index
                );
            }
            if clean_split {
                let path = temp_split_path(&reg, region_id);
                let _ = fs::remove_dir_all(path);
            }
        };
        task.persisted_cbs.push(Box::new(hook));
        task.has_snapshot = true;
        Ok(())
    }
}
