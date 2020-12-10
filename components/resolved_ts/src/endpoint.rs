// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use engine_traits::{KvEngine, Snapshot};
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ObserveID, StoreMeta};
use raftstore::store::util::RemoteLease;
use raftstore::Error as RaftStoreError;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use txn_types::{Key, Lock, TimeStamp};

use crate::cmd::{ChangeLog, ChangeRow};
use crate::errors::Result;
use crate::observer::ChangeDataSnapshot;
use crate::resolver::Resolver;
use crate::scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool};

enum ResolverStatus {
    Pending {
        locks: Vec<PendingLock>,
        cancelled: Arc<AtomicBool>,
    },
    Ready,
}

enum PendingLock {
    Track {
        key: Key,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Key,
        start_ts: TimeStamp,
        commit_ts: Option<TimeStamp>,
    },
}

struct ObserveRegion {
    meta: Region,
    observe_id: ObserveID,
    lease: Option<RemoteLease>,
    resolver: Option<Resolver>,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(meta: Region) -> Self {
        ObserveRegion {
            resolver: None,
            meta,
            observe_id: ObserveID::new(),
            lease: None,
            resolver_status: ResolverStatus::Pending {
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        }
    }

    fn mut_resolver(&mut self) -> &mut Resolver {
        self.resolver.as_mut().unwrap()
    }

    fn track_change_log(&mut self, change_logs: &[ChangeLog]) {
        match self.resolver_status {
            ResolverStatus::Pending { ref mut locks, .. } => {
                for log in change_logs {
                    log.rows.iter().for_each(|row| match row {
                        ChangeRow::Prewrite { key, lock, .. } => locks.push(PendingLock::Track {
                            key: key.clone(),
                            start_ts: lock.ts,
                        }),
                        ChangeRow::Commit {
                            key,
                            commit_ts,
                            write,
                            ..
                        } => locks.push(PendingLock::Untrack {
                            key: key.clone(),
                            start_ts: write.start_ts,
                            commit_ts: *commit_ts,
                        }),
                    })
                }
            }
            ResolverStatus::Ready => {
                for log in change_logs {
                    log.rows.iter().for_each(|row| match row {
                        ChangeRow::Prewrite { key, lock, .. } => {
                            self.mut_resolver().track_lock(lock.ts, key)
                        }
                        ChangeRow::Commit {
                            key,
                            commit_ts,
                            write,
                            ..
                        } => self
                            .mut_resolver()
                            .untrack_lock(write.start_ts, *commit_ts, key),
                    })
                }
            }
        }
    }

    fn track_scan_locks(&mut self, entries: Vec<ScanEntry>) {
        for es in entries {
            match es {
                ScanEntry::Lock(locks) => {
                    if let ResolverStatus::Ready = self.resolver_status {
                        panic!("region {:?} resolver has ready", self.meta.id)
                    }
                    for (key, lock) in locks {
                        self.mut_resolver().track_lock(lock.ts, &key);
                    }
                }
                ScanEntry::None => {
                    let status =
                        std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready);
                    match status {
                        ResolverStatus::Pending { locks, .. } => {
                            locks.into_iter().for_each(|lock| match lock {
                                PendingLock::Track { key, start_ts } => {
                                    self.mut_resolver().track_lock(start_ts, &key)
                                }
                                PendingLock::Untrack {
                                    key,
                                    start_ts,
                                    commit_ts,
                                } => self.mut_resolver().untrack_lock(start_ts, commit_ts, &key),
                            })
                        }
                        ResolverStatus::Ready => {
                            panic!("region {:?} resolver has ready", self.meta.id)
                        }
                    }
                }
                ScanEntry::TxnEntry(_) => panic!("unexpected entry type"),
            }
        }
    }
}

pub trait CmdSinker: Send {
    fn sink_cmd<S: Snapshot>(
        &mut self,
        observe_id: ObserveID,
        change_logs: Vec<ChangeLog>,
        snapshot: ChangeDataSnapshot<S>,
    );
}

pub struct Endpoint<T, E: KvEngine, S> {
    store_meta: StoreMeta,
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    sinker: S,
    _phantom: PhantomData<T>,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine, S: CmdSinker> Endpoint<T, E, S> {
    fn register_region(&mut self, region: Region) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let observe_region = ObserveRegion::new(region.clone());
        let observe_id = observe_region.observe_id;
        let cancelled = match observe_region.resolver_status {
            ResolverStatus::Pending { ref cancelled, .. } => cancelled.clone(),
            ResolverStatus::Ready => panic!("illeagal created observe region"),
        };
        let scheduler = self.scheduler.clone();
        let scan_task = ScanTask {
            id: observe_region.observe_id,
            tag: String::new(),
            mode: ScanMode::LockOnly,
            region,
            checkpoint_ts: TimeStamp::zero(),
            cancelled: Box::new(move || cancelled.load(Ordering::Acquire)),
            send_entries: Box::new(move |entries| {
                scheduler
                    .schedule(Task::ScanLocks {
                        region_id,
                        observe_id,
                        entries,
                    })
                    .unwrap_or_else(|e| debug!("schedule resolved ts task failed"; "err" => ?e));
            }),
            before_start: None,
        };
        self.scanner_pool.spawn_task(scan_task);
    }

    fn deregister_region(&mut self, region: &Region) {
        let observe_region = self.regions.remove(&region.id).unwrap();
        if let ResolverStatus::Pending { cancelled, .. } = observe_region.resolver_status {
            cancelled.store(true, Ordering::Release);
        }
    }

    fn region_destroyed(&mut self, region: &Region) {
        self.deregister_region(&region);
    }

    fn region_updated(&mut self, region: Region) {
        self.deregister_region(&region);
        self.register_region(region);
    }

    fn region_role_changed(&mut self, region: Region, role: StateRole) {
        match role {
            StateRole::Leader => self.register_region(region),
            other => self.deregister_region(&region),
        }
    }

    fn region_error(&mut self, region: Region, observe_id: ObserveID, error: RaftStoreError) {
        if let Some(observe_region) = self.regions.get(&region.id) {
            if observe_region.observe_id != observe_id {
                return;
            }
            info!("region met error, recreate it"; "region_id" => region.id, "error" => ?error);
            return self.region_updated(region);
        }
    }

    fn advance_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp) {
        if regions.is_empty() {
            return;
        }
        for region_id in regions {
            if let Some(observe_region) = self.regions.get_mut(&region_id) {
                if let ResolverStatus::Ready = observe_region.resolver_status {
                    observe_region.mut_resolver().resolve(ts);
                }
            }
        }
    }

    fn handle_change_log(&mut self, cmd_batch: Vec<CmdBatch>) {
        for mut batch in cmd_batch {
            batch.filter_admin();
            if !batch.is_empty() {
                let observe_region = self
                    .regions
                    .get_mut(&batch.region_id)
                    .expect("cannot find region to handle change log");
                if observe_region.observe_id == batch.observe_id {
                    let change_logs: Vec<_> = batch
                        .into_iter(observe_region.meta.id)
                        .map(|cmd| ChangeLog::encode_change_log(cmd))
                        .collect();
                    observe_region.track_change_log(&change_logs);
                    self.sinker.sink_cmd(observe_region.observe_id, change_logs);
                } else {
                    debug!("resolved ts CmdBatch discarded";
                        "region_id" => batch.region_id,
                        "observe_id" => ?batch.observe_id,
                        "current" => ?observe_region.observe_id,
                    );
                }
            }
        }
    }

    fn handle_scan_locks(
        &mut self,
        region_id: u64,
        observe_id: ObserveID,
        entries: Vec<ScanEntry>,
    ) {
        match self.regions.get_mut(&region_id) {
            Some(observe_region) => {
                if observe_region.observe_id == observe_id {
                    observe_region.track_scan_locks(entries);
                }
            }
            None => {
                debug!("scan locks region not exist"; "region_id" => region_id, "observe_id" => ?observe_id);
            }
        }
    }
}

pub enum Task<S: Snapshot> {
    RegionDestroyed(Region),
    RegionUpdated(Region),
    RegionRoleChanged {
        region: Region,
        role: StateRole,
    },
    RegionError {
        region: Region,
        observe_id: ObserveID,
        error: RaftStoreError,
    },
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
        snapshot: ChangeDataSnapshot<S>,
    },
    ScanLocks {
        region_id: u64,
        observe_id: ObserveID,
        entries: Vec<ScanEntry>,
    },
}

impl<S: Snapshot> fmt::Debug for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut de = f.debug_struct("ResolvedTsTask");
        match self {
            Task::RegionDestroyed(ref region) => de
                .field("name", &"region_destroyed")
                .field("region", &region)
                .finish(),
            Task::RegionUpdated(ref region) => de
                .field("name", &"region_updated")
                .field("region", &region)
                .finish(),
            Task::RegionRoleChanged {
                ref region,
                ref role,
            } => de
                .field("name", &"region_role_changed")
                .field("region", &region)
                .field("role", &role)
                .finish(),
            Task::RegionError {
                ref region,
                ref observe_id,
                ref error,
            } => de
                .field("name", &"region_error")
                .field("region", &region)
                .field("observe_id", &observe_id)
                .field("error", &error)
                .finish(),
            Task::AdvanceResolvedTs {
                ref regions,
                ref ts,
            } => de
                .field("name", &"advance_resolved_ts")
                .field("regions", &regions)
                .field("ts", &ts)
                .finish(),
            Task::ChangeLog { .. } => de.field("name", &"change_log").finish(),
            Task::ScanLocks {
                ref region_id,
                ref observe_id,
                ..
            } => de
                .field("name", &"scan_locks")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .finish(),
        }
    }
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine, S: CmdSinker> Runnable for Endpoint<T, E, S> {
    type Task = Task<E::Snapshot>;

    fn run(&mut self, task: Task<E::Snapshot>) {
        debug!("run cdc task"; "task" => ?task);
        match task {
            Task::RegionDestroyed(ref region) => self.region_destroyed(region),
            Task::RegionUpdated(region) => self.region_updated(region),
            Task::RegionRoleChanged { region, role } => self.region_role_changed(region, role),
            Task::RegionError {
                region,
                observe_id,
                error,
            } => self.region_error(region, observe_id, error),
            Task::AdvanceResolvedTs { regions, ts } => self.advance_resolved_ts(regions, ts),
            Task::ChangeLog {
                cmd_batch,
                snapshot,
            } => self.handle_change_log(cmd_batch),
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
            } => self.handle_scan_locks(region_id, observe_id, entries),
        }
    }
}
