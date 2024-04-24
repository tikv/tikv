// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

//! This module contains snapshot relative processing logic.
//!
//! # Snapshot State
//!
//! generator and apply snapshot works asynchronously. the snap_sate indicates
//! the current snapshot state.
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

use encryption_export::DataKeyManager;
use engine_traits::{
    EncryptionKeyManager, KvEngine, RaftEngine, RaftLogBatch, TabletContext, TabletRegistry,
    ALL_CFS,
};
use fail::fail_point;
use kvproto::{
    metapb::PeerRole,
    raft_serverpb::{PeerState, RaftSnapshotData},
};
use protobuf::Message;
use raft::{eraftpb::Snapshot, StateRole};
use raftstore::{
    coprocessor::RegionChangeEvent,
    store::{
        metrics::STORE_SNAPSHOT_VALIDATION_FAILURE_COUNTER, worker_metrics::SNAP_COUNTER,
        GenSnapRes, ReadTask, TabletSnapKey, TabletSnapManager, Transport, WriteTask,
        RAFT_INIT_LOG_INDEX, RAFT_INIT_LOG_TERM,
    },
};
use slog::{debug, error, info, warn};
use tikv_util::{box_err, log::SlogFormat, slog_panic, store::find_peer_by_id};

use crate::{
    fsm::ApplyResReporter,
    operation::{command::temp_split_path, SharedReadTablet},
    raft::{Apply, Peer, Storage},
    router::ApplyTask,
    worker::tablet,
    Result, StoreContext,
};

/// Snapshot generating task state.
/// snapshot send success: Relax --> Generating --> Generated --> Sending -->
/// Relax snapshot send failed: Relax --> Generating --> Generated --> Sending
/// snapshot send again: Sending --> Relax
#[derive(Debug)]
pub enum SnapState {
    Relax,
    Generating {
        canceled: Arc<AtomicBool>,
        index: Arc<AtomicU64>,
    },
    Generated(Box<Snapshot>),
    Sending(Box<Snapshot>),
}

impl PartialEq for SnapState {
    fn eq(&self, other: &SnapState) -> bool {
        match (self, other) {
            (SnapState::Relax, SnapState::Relax) => true,
            (SnapState::Generating { .. }, SnapState::Generating { .. }) => true,
            (SnapState::Generated(snap1), SnapState::Generated(snap2)) => *snap1 == *snap2,
            (SnapState::Sending(snap1), SnapState::Sending(snap2)) => *snap1 == *snap2,
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
    key_manager: Option<&DataKeyManager>,
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
    if let Some(m) = &key_manager {
        m.link_file(source.to_str().unwrap(), target_path.to_str().unwrap())
            .unwrap();
    }
    if let Err(e) = fs::rename(source, &target_path) {
        if let Some(m) = &key_manager {
            m.remove_dir(&target_path, Some(source)).unwrap();
        }
        panic!(
            "failed to rename tablet {} => {}: {:?}",
            source.display(),
            target_path.display(),
            e
        );
    }
    if let Some(m) = &key_manager {
        m.remove_dir(source, Some(&target_path)).unwrap();
    }
    true
}

impl<EK: KvEngine, ER: RaftEngine> Peer<EK, ER> {
    /// Check whether there is a pending generate snapshot task, the task
    /// needs to be sent to the apply system.
    /// Always sending snapshot task after apply task, so it gets latest
    /// snapshot.
    #[inline]
    pub fn maybe_schedule_gen_snapshot(&mut self) {
        if let Some(gen_task) = self.storage_mut().take_gen_snap_task() {
            self.apply_scheduler()
                .unwrap()
                .send(ApplyTask::Snapshot(gen_task));
        }
    }

    pub fn on_snap_gc<T>(&self, ctx: &mut StoreContext<EK, ER, T>, keys: Box<[TabletSnapKey]>) {
        let mut stale_keys = Vec::from(keys);
        if self.is_leader() {
            stale_keys.retain(
                |key| match self.storage().snap_states.borrow().get(&key.to_peer) {
                    Some(SnapState::Relax) => true,
                    _ => !self.has_peer(key.to_peer),
                },
            )
        }
        if stale_keys.is_empty() {
            return;
        }
        let _ = ctx
            .schedulers
            .tablet
            .schedule(tablet::Task::SnapGc(stale_keys.into()));
    }

    pub fn on_snapshot_generated(&mut self, snapshot: GenSnapRes) {
        let commit_index = self.raft_group().raft.raft_log.committed;
        if self
            .storage_mut()
            .on_snapshot_generated(snapshot, commit_index)
        {
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
        self.storage().report_snapshot(to_peer_id, status);
        self.raft_group_mut().report_snapshot(to_peer_id, status);
    }

    pub fn on_applied_snapshot<T: Transport>(&mut self, ctx: &mut StoreContext<EK, ER, T>) {
        ctx.coprocessor_host.on_region_changed(
            self.region(),
            RegionChangeEvent::Create,
            StateRole::Follower,
        );
        let persisted_index = self.persisted_index();
        self.compact_log_context_mut()
            .set_last_applying_index(persisted_index);
        let snapshot_index = self.entry_storage().truncated_index();
        assert!(snapshot_index >= RAFT_INIT_LOG_INDEX, "{:?}", self.logger);
        self.compact_log_context_mut()
            .set_last_compacted_idx(snapshot_index + 1 /* first index */);
        // If leader sends a message append to the follower while it's applying
        // snapshot (via split init for example), the persisted_index may be larger
        // than the first index. But as long as first index is not larger, the
        // latest snapshot should be applied.
        if snapshot_index <= persisted_index {
            let region_id = self.region_id();
            self.reset_flush_state(snapshot_index);
            let flush_state = self.flush_state().clone();
            let mut tablet_ctx = TabletContext::new(self.region(), Some(snapshot_index));
            // Use a new FlushState to avoid conflicts with the old one.
            tablet_ctx.flush_state = Some(flush_state);
            let path = ctx.tablet_registry.tablet_path(region_id, snapshot_index);
            assert!(
                path.exists(),
                "{} {} not exists",
                SlogFormat(&self.logger),
                path.display()
            );
            let tablet = ctx
                .tablet_registry
                .tablet_factory()
                .open_tablet(tablet_ctx, &path)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger,
                        "failed to load tablet";
                        "path" => path.display(),
                        "error" => ?e
                    );
                });

            self.storage_mut().on_applied_snapshot();
            self.raft_group_mut().advance_apply_to(snapshot_index);
            if self.proposal_control().has_applied_prepare_merge() {
                // After applying a snapshot, merge is rollbacked implicitly.
                info!(
                    self.logger,
                    "rollback merge after applying snapshot";
                    "index" => snapshot_index,
                    "region" => ?self.region(),
                );
                self.rollback_merge(ctx);
            }
            let read_tablet = SharedReadTablet::new(tablet.clone());
            {
                let mut meta = ctx.store_meta.lock().unwrap();
                meta.set_region(self.region(), true, &self.logger);
                meta.readers
                    .insert(region_id, (self.generate_read_delegate(), read_tablet));
                meta.region_read_progress
                    .insert(region_id, self.read_progress().clone());
            }
            if let Some(tablet) = self.set_tablet(tablet) {
                self.record_tombstone_tablet(ctx, tablet, snapshot_index);
            }
            self.read_progress_mut().update_applied_core(snapshot_index);
            let split = self.storage_mut().split_init_mut().take();
            if split.as_ref().map_or(true, |s| {
                !s.scheduled || snapshot_index != RAFT_INIT_LOG_INDEX
            }) {
                info!(self.logger, "apply tablet snapshot completely");
                // Tablet sent from region leader should have already be trimmed.
                self.storage_mut().set_has_dirty_data(false);
                SNAP_COUNTER.apply.success.inc();

                fail_point!("apply_snapshot_complete");
            }
            if let Some(init) = split {
                info!(self.logger, "init split with snapshot finished");
                self.post_split_init(ctx, init);

                fail_point!("post_split_init_complete");
            }
            self.schedule_apply_fsm(ctx);
            if self.remove_tombstone_tablets(snapshot_index) {
                let counter = self.remember_persisted_tablet_index();
                let _ = ctx
                    .schedulers
                    .tablet
                    .schedule(tablet::Task::destroy(region_id, snapshot_index));
                counter.store(snapshot_index, Ordering::Relaxed);
            }
            if let Some(msg) = self.split_pending_append_mut().take_append_message() {
                let _ = ctx.router.send_raft_message(msg);
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
        debug!(self.logger, "scheduling snapshot"; "task" => ?snap_task);
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
        let gen_tablet_snap_task = ReadTask::GenTabletSnapshot {
            region_id: snap_task.region_id,
            to_peer: snap_task.to_peer,
            tablet: self.tablet().clone(),
            region_state: self.region_state().clone(),
            last_applied_term,
            last_applied_index,
            for_balance: snap_task.for_balance,
            canceled: snap_task.canceled.clone(),
        };
        if let Err(e) = self.read_scheduler().schedule(gen_tablet_snap_task) {
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

    fn report_snapshot(&self, peer_id: u64, status: raft::SnapshotStatus) {
        if status == raft::SnapshotStatus::Finish {
            self.snap_states.borrow_mut().remove(&peer_id);
        }
    }

    /// Gets a snapshot. Returns `SnapshotTemporarilyUnavailable` if there is no
    /// unavailable snapshot.
    pub fn snapshot(&self, request_index: u64, to: u64) -> raft::Result<Snapshot> {
        if let Some(state) = self.snap_states.borrow_mut().get_mut(&to) {
            info!(
                self.logger(),
                "requesting snapshot";
                "request_index" => request_index,
                "request_peer" => to,
                "state" => ?state,
            );
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
                    if self.validate_snap(&snap, request_index) {
                        *state = SnapState::Sending(s.clone());
                        return Ok(snap);
                    }
                    *state = SnapState::Relax;
                }
                SnapState::Sending(ref s) => {
                    if self.validate_snap(s, request_index) {
                        return Ok(*s.clone());
                    }
                    *state = SnapState::Relax;
                }
                _ => {}
            };
        }

        if self.has_dirty_data() {
            info!(self.logger(), "delay generating snapshot as there are still dirty data"; "request_index" => request_index, "request_peer" => to);
            // It's OK to delay. If there are still dirty data, it means the tablet is just
            // split. In normal cases, all peers will apply split, so reject generates
            // snapshot may actually good for all peers as they are more likely
            // to be initialized by split.
            return Err(raft::Error::Store(
                raft::StorageError::SnapshotTemporarilyUnavailable,
            ));
        } else {
            info!(
                self.logger(),
                "requesting new snapshot";
                "request_index" => request_index,
                "request_peer" => to,
            );
        }
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
        if idx < RAFT_INIT_LOG_INDEX || snap.get_metadata().get_term() < RAFT_INIT_LOG_TERM {
            info!(
                self.logger(),
                "corrupted snapshot detected, generate again";
                "snap" => ?snap,
                "request_index" => request_index,
            );
            return false;
        }
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
            let SnapState::Generating { ref index, .. } = *state else {
                return true;
            };
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
    pub fn on_snapshot_generated(&self, res: GenSnapRes, commit_index: u64) -> bool {
        if res.is_none() {
            self.cancel_generating_snap(None);
            return false;
        }
        let (mut snapshot, to_peer_id) = *res.unwrap();
        if let Some(state) = self.snap_states.borrow_mut().get_mut(&to_peer_id) {
            let SnapState::Generating { ref index, .. } = *state else {
                return false;
            };
            if snapshot.get_metadata().get_index() < index.load(Ordering::SeqCst) {
                warn!(
                    self.logger(),
                    "snapshot is staled, skip";
                    "snap_index" => snapshot.get_metadata().get_index(),
                    "required_index" => index.load(Ordering::SeqCst),
                    "to_peer_id" => to_peer_id,
                );
                return false;
            }
            // Set commit index for learner snapshots. It's needed to address
            // compatibility issues between v1 and v2 snapshots.
            // See https://github.com/pingcap/tiflash/issues/7568#issuecomment-1576382311
            if let Some(p) = find_peer_by_id(self.region(), to_peer_id)
                && p.get_role() == PeerRole::Learner
            {
                let mut snapshot_data = RaftSnapshotData::default();
                if snapshot_data.merge_from_bytes(snapshot.get_data()).is_ok() {
                    snapshot_data.mut_meta().set_commit_index_hint(commit_index);
                    snapshot.set_data(snapshot_data.write_to_bytes().unwrap().into());
                }
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
        self.apply_trace_mut().on_applied_snapshot(index);
    }

    pub fn apply_snapshot(
        &mut self,
        snap: &Snapshot,
        task: &mut WriteTask<EK, ER>,
        snap_mgr: &TabletSnapManager,
        reg: &TabletRegistry<EK>,
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
        let removed_records = snap_data.take_removed_records();
        let merged_records = snap_data.take_merged_records();
        if region.get_id() != region_id {
            return Err(box_err!(
                "mismatch region id {}!={}",
                region_id,
                region.get_id()
            ));
        }

        let old_last_index = self.entry_storage().last_index();
        if self.entry_storage().first_index() <= old_last_index {
            // All states are rewritten in the following blocks. Stale states will be
            // cleaned up by compact worker. Have to use raft write batch here because
            // raft log engine expects deletes before writes.
            let raft_engine = self.entry_storage().raft_engine();
            if task.raft_wb.is_none() {
                task.raft_wb = Some(raft_engine.log_batch(64));
            }
            let wb = task.raft_wb.as_mut().unwrap();
            raft_engine
                .clean(region.get_id(), 0, self.entry_storage().raft_state(), wb)
                .unwrap_or_else(|e| {
                    slog_panic!(
                        self.logger(),
                        "failed to clean up region";
                        "error" => ?e
                    )
                });
            self.entry_storage_mut().clear();
        }

        let last_index = snap.get_metadata().get_index();
        let last_term = snap.get_metadata().get_term();
        assert!(
            last_index >= RAFT_INIT_LOG_INDEX && last_term >= RAFT_INIT_LOG_TERM,
            "{}",
            SlogFormat(self.logger())
        );
        let mut region_state = self.region_state().clone();
        region_state.set_state(PeerState::Normal);
        region_state.set_region(region);
        region_state.set_removed_records(removed_records);
        region_state.set_merged_records(merged_records);
        region_state.set_tablet_index(last_index);
        // We need set_region_state here to update the peer.
        self.set_region_state(region_state);

        let entry_storage = self.entry_storage_mut();
        entry_storage.raft_state_mut().set_last_index(last_index);
        entry_storage.set_truncated_index(last_index);
        entry_storage.set_truncated_term(last_term);
        entry_storage.set_last_term(last_term);

        self.apply_trace_mut().restore_snapshot(last_index);
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
        task.flushed_epoch = Some(self.region_state().get_region().get_region_epoch().clone());

        let (path, clean_split) = match self.split_init_mut() {
            // If index not match, the peer may accept a newer snapshot after split.
            Some(init) if init.scheduled && last_index == RAFT_INIT_LOG_INDEX => {
                lb.put_dirty_mark(region_id, last_index, true).unwrap();
                self.set_has_dirty_data(true);
                (temp_split_path(reg, region_id), false)
            }
            si => (
                recv_snap_path(snap_mgr, region_id, peer_id, last_term, last_index),
                si.is_some(),
            ),
        };

        let logger = self.logger().clone();
        // The snapshot require no additional processing such as ingest them to DB, but
        // it should load it into the factory after it persisted.
        let reg = reg.clone();
        let key_manager = snap_mgr.key_manager().clone();
        let hook = move || {
            fail::fail_point!("region_apply_snap");
            if !install_tablet(&reg, key_manager.as_deref(), &path, region_id, last_index) {
                slog_panic!(
                    logger,
                    "failed to install tablet";
                    "path" => %path.display(),
                    "tablet_index" => last_index
                );
            }
            if clean_split {
                let path = temp_split_path(&reg, region_id);
                if let Some(m) = key_manager {
                    let _ = m.remove_dir(&path, None);
                }
                let _ = fs::remove_dir_all(path);
            }
        };
        task.persisted_cbs.push(Box::new(hook));
        task.has_snapshot = true;
        Ok(())
    }
}
