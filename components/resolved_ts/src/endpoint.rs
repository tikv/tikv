// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use engine_traits::KvEngine;
use kvproto::metapb::Region;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::ObserveID;
use raftstore::store::util::RemoteLease;
use raftstore::Error as RaftStoreError;
use tikv_util::worker::{Runnable, RunnableWithTimer, ScheduleError, Scheduler};
use txn_types::{Key, Lock, TimeStamp};

use crate::cmd::{ChangeLog, ChangeRow};
use crate::errors::Result;
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
    resolver: Resolver,
    resolver_status: ResolverStatus,
    // indicate have met an error, need to try to scan lock again
    error: Option<RaftStoreError>,
}

impl ObserveRegion {
    fn new(meta: Region) -> Self {
        ObserveRegion {
            resolver: Resolver::new(meta.id),
            meta,
            observe_id: ObserveID::new(),
            lease: None,
            resolver_status: ResolverStatus::Pending {
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
            error: None,
        }
    }

    fn handle_change_log(&mut self, change_logs: &[ChangeLog]) {
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
                            self.resolver.track_lock(lock.ts, key)
                        }
                        ChangeRow::Commit {
                            key,
                            commit_ts,
                            write,
                            ..
                        } => self.resolver.untrack_lock(write.start_ts, *commit_ts, key),
                    })
                }
            }
        }
    }
}

pub trait CmdSinker: Send {
    fn sink_cmd(&mut self, observe_id: ObserveID, change_logs: Vec<ChangeLog>);
}

pub struct Endpoint<T, E, S> {
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task>,
    sinker: S,
    _phantom: PhantomData<(T, E)>,
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine, S: CmdSinker> Endpoint<T, E, S> {
    fn region_create(&mut self, region: Region) {
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

    fn region_destroy(&mut self, region: &Region) {
        let observe_region = self.regions.remove(&region.id).unwrap();
        if let ResolverStatus::Pending { cancelled, .. } = observe_region.resolver_status {
            cancelled.store(true, Ordering::Release);
        }
    }

    fn region_updated(&mut self, region: Region) {
        self.region_destroy(&region);
        self.region_create(region);
    }

    fn advance_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp) {}

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
                    observe_region.handle_change_log(&change_logs);
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

    fn scan_locks(&mut self, region_id: u64, observe_id: ObserveID, entries: Vec<ScanEntry>) {}
}

#[derive(Debug)]
pub enum Task {
    RegionCreated(Region),
    RegionDestroyed(Region),
    RegionUpdated(Region),
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog(Vec<CmdBatch>),
    ScanLocks {
        region_id: u64,
        observe_id: ObserveID,
        entries: Vec<ScanEntry>,
    },
}

impl fmt::Display for Task {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T: 'static + RaftStoreRouter<E>, E: KvEngine, S: CmdSinker> Runnable for Endpoint<T, E, S> {
    type Task = Task;

    fn run(&mut self, task: Task) {
        debug!("run cdc task"; "task" => ?task);
        match task {
            Task::RegionCreated(_) => {}
            Task::RegionDestroyed(_) => {}
            Task::RegionUpdated { .. } => {}
            Task::AdvanceResolvedTs { regions, ts } => {}
            Task::ChangeLog { .. } => {}
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
            } => {}
        }
    }
}
