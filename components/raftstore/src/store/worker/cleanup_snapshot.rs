// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt;

use crossbeam::channel::TrySendError;
use engine_traits::{KvEngine, RaftEngine};
use fail::fail_point;
use tikv_util::{debug, error, info, warn, worker::Runnable};

use crate::{
    store::{
        CasualMessage, PeerMsg, RaftRouter, SnapKey, SnapManager, Snapshot, StoreMsg, StoreRouter,
    },
    Result,
};

pub enum Task {
    GcSnapshot,
    DeleteSnapshotFiles {
        key: SnapKey,
        snapshot: Box<Snapshot>,
        check_entry: bool,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &*self {
            Task::GcSnapshot => write!(f, "Gc Snapshot"),
            Task::DeleteSnapshotFiles { key, .. } => write!(f, "Delete Snapshot Files for {}", key),
        }
    }
}

pub struct Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    store_id: u64,
    router: RaftRouter<EK, ER>,
    snap_mgr: SnapManager,
}

impl<EK, ER> Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    pub fn new(store_id: u64, router: RaftRouter<EK, ER>, snap_mgr: SnapManager) -> Runner<EK, ER> {
        Runner {
            store_id,
            router,
            snap_mgr,
        }
    }

    /// handle snap mgr gc
    fn handle_snap_mgr_gc(&self) -> Result<()> {
        fail_point!("peer_2_handle_snap_mgr_gc", self.store_id == 2, |_| Ok(()));
        let snap_keys = self.snap_mgr.list_idle_snap()?;
        if snap_keys.is_empty() {
            return Ok(());
        }
        let (mut last_region_id, mut keys) = (0, vec![]);
        let schedule_gc_snap = |region_id: u64, snaps| -> Result<()> {
            debug!(
                "schedule snap gc";
                "store_id" => self.store_id,
                "region_id" => region_id,
            );

            let gc_snap = PeerMsg::CasualMessage(CasualMessage::GcSnap { snaps });
            match (*self.router).send(region_id, gc_snap) {
                Ok(()) => Ok(()),
                Err(TrySendError::Disconnected(_)) if self.router.is_shutdown() => Ok(()),
                Err(TrySendError::Disconnected(PeerMsg::CasualMessage(
                    CasualMessage::GcSnap { snaps },
                ))) => {
                    // The snapshot exists because MsgAppend has been rejected. So the
                    // peer must have been exist. But now it's disconnected, so the peer
                    // has to be destroyed instead of being created.
                    info!(
                        "region is disconnected, remove snaps";
                        "region_id" => region_id,
                        "snaps" => ?snaps,
                    );
                    for (key, is_sending) in snaps {
                        let snap = match self.snap_mgr.get_snapshot_for_gc(&key, is_sending) {
                            Ok(snap) => snap,
                            Err(e) => {
                                error!(%e;
                                    "failed to load snapshot";
                                    "snapshot" => ?key,
                                );
                                continue;
                            }
                        };
                        self.snap_mgr.delete_snapshot(&key, snap.as_ref(), false);
                    }
                    Ok(())
                }
                Err(TrySendError::Full(_)) => Ok(()),
                Err(TrySendError::Disconnected(_)) => unreachable!(),
            }
        };
        for (key, is_sending) in snap_keys {
            if last_region_id == key.region_id {
                keys.push((key, is_sending));
                continue;
            }

            if !keys.is_empty() {
                schedule_gc_snap(last_region_id, keys)?;
                keys = vec![];
            }

            last_region_id = key.region_id;
            keys.push((key, is_sending));
        }
        if !keys.is_empty() {
            schedule_gc_snap(last_region_id, keys)?;
        }
        Ok(())
    }

    fn delete_snapshot(&self, key: &SnapKey, snap: &Snapshot, check_entry: bool) -> bool {
        self.snap_mgr.delete_snapshot(key, snap, check_entry)
    }
}

impl<EK, ER> Runnable for Runner<EK, ER>
where
    EK: KvEngine,
    ER: RaftEngine,
{
    type Task = Task;

    fn run(&mut self, task: Task) {
        match task {
            Task::GcSnapshot => {
                if let Err(e) = self.handle_snap_mgr_gc() {
                    error!(?e;
                        "handle gc snap failed";
                        "store_id" => self.store_id,
                    );
                }

                let msg = StoreMsg::GcSnapshotFinish;
                if let Err(e) = StoreRouter::send(&self.router, msg) {
                    error!(%e; "send StoreMsg::GcSnapshotFinish failed");
                }
            }
            Task::DeleteSnapshotFiles {
                key,
                mut snapshot,
                check_entry,
            } => {
                let snapshot = snapshot.as_mut();
                if let Err(e) = snapshot.load_snapshot_meta_if_necessary() {
                    warn!(
                        "failed to load existent snapshot meta when try to build snapshot";
                        "snapshot" => %snapshot.path(),
                        "err" => ?e,
                    );
                }
                let ok = self.delete_snapshot(&key, snapshot, check_entry);
                if !ok {
                    error!("failed to delete snapshot for key {}", key);
                }
            }
        }
    }
}
