// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, Snapshot};
use grpcio::Environment;
use kvproto::errorpb::Error as ErrorHeader;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raft::StateRole;
use raftstore::coprocessor::CmdBatch;
use raftstore::router::RaftStoreRouter;
use raftstore::store::fsm::{ObserveID, StoreMeta};
use raftstore::store::util;
use raftstore::store::RegionSnapshot;
use security::SecurityManager;
use tikv::config::CdcConfig;
use tikv_util::worker::{Runnable, Scheduler};
use txn_types::{Key, TimeStamp};

use crate::advance::AdvanceTsWorker;
use crate::cmd::{ChangeLog, ChangeRow};
use crate::errors::{Error, Result};
use crate::resolver::Resolver;
use crate::scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool};
use crate::sinker::{CmdSinker, SinkCmd};

enum ResolverStatus {
    Pending {
        locks: Vec<PendingLock>,
        cancelled: Arc<AtomicBool>,
    },
    Ready,
}

#[allow(dead_code)]
enum PendingLock {
    Track {
        key: Key,
        start_ts: TimeStamp,
    },
    Untrack {
        key: Key,
        start_ts: Option<TimeStamp>,
        commit_ts: Option<TimeStamp>,
    },
}

// Records information related to observed region.
// observe_id is used for avoid ABA problems in incremental scan task, advance resolved ts task,
// and command observing.
struct ObserveRegion {
    meta: Region,
    observe_id: ObserveID,
    // TODO: Get lease from raftstore.
    // lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(meta: Region, resolved_ts: Arc<AtomicU64>) -> Self {
        ObserveRegion {
            resolver: Resolver::from_shared(meta.id, resolved_ts),
            meta,
            observe_id: ObserveID::new(),
            resolver_status: ResolverStatus::Pending {
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        }
    }

    fn track_change_log(&mut self, change_logs: &[ChangeLog]) -> Result<()> {
        match self.resolver_status {
            ResolverStatus::Pending { ref mut locks, .. } => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => return Err(Error::Request(e.clone())),
                        ChangeLog::Rows { rows, .. } => rows.iter().for_each(|row| match row {
                            ChangeRow::Prewrite { key, start_ts, .. } => {
                                locks.push(PendingLock::Track {
                                    key: key.clone(),
                                    start_ts: *start_ts,
                                })
                            }
                            ChangeRow::Commit {
                                key,
                                start_ts,
                                commit_ts,
                                ..
                            } => locks.push(PendingLock::Untrack {
                                key: key.clone(),
                                start_ts: *start_ts,
                                commit_ts: *commit_ts,
                            }),
                            _ => (),
                        }),
                    }
                }
            }
            ResolverStatus::Ready => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => return Err(Error::Request(e.clone())),
                        ChangeLog::Rows { rows, .. } => rows.iter().for_each(|row| match row {
                            ChangeRow::Prewrite { key, start_ts, .. } => {
                                self.resolver.track_lock(*start_ts, key.to_raw().unwrap())
                            }
                            ChangeRow::Commit { key, .. } => {
                                self.resolver.untrack_lock(&key.to_raw().unwrap())
                            }
                            _ => (),
                        }),
                    }
                }
            }
        }
        Ok(())
    }

    fn track_scan_locks(&mut self, entries: Vec<ScanEntry>) {
        for es in entries {
            match es {
                ScanEntry::Lock(locks) => {
                    if let ResolverStatus::Ready = self.resolver_status {
                        panic!("region {:?} resolver has ready", self.meta.id)
                    }
                    for (key, lock) in locks {
                        self.resolver.track_lock(lock.ts, key.to_raw().unwrap());
                    }
                }
                ScanEntry::None => {
                    let status =
                        std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready);
                    match status {
                        ResolverStatus::Pending { locks, .. } => {
                            locks.into_iter().for_each(|lock| match lock {
                                PendingLock::Track { key, start_ts } => {
                                    self.resolver.track_lock(start_ts, key.to_raw().unwrap())
                                }
                                PendingLock::Untrack { key, .. } => {
                                    self.resolver.untrack_lock(&key.to_raw().unwrap())
                                }
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

pub struct Endpoint<T, E: KvEngine, C> {
    store_meta: Arc<Mutex<StoreMeta>>,
    regions: HashMap<u64, ObserveRegion>,
    // raft_router: T,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    sinker: C,
    advance_worker: AdvanceTsWorker<T, E>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E, C> Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    pub fn new(
        cfg: &CdcConfig,
        scheduler: Scheduler<Task<E::Snapshot>>,
        raft_router: T,
        store_meta: Arc<Mutex<StoreMeta>>,
        pd_client: Arc<dyn PdClient>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sinker: C,
    ) -> Self {
        let advance_worker = AdvanceTsWorker::new(
            pd_client,
            scheduler.clone(),
            raft_router.clone(),
            store_meta.clone(),
            concurrency_manager,
            env,
            security_mgr,
            cfg.min_ts_interval.0,
            cfg.hibernate_regions_compatible,
        );
        let scanner_pool = ScannerPool::new(2, raft_router);
        let ep = Self {
            scheduler,
            // raft_router,
            store_meta,
            advance_worker,
            scanner_pool,
            sinker,
            regions: HashMap::default(),
            _phantom: PhantomData::default(),
        };
        ep.register_advance_event();
        ep
    }

    fn register_region(&mut self, region: Region) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let observe_region = {
            let store_meta = self.store_meta.lock().unwrap();
            if let Some(peer_properties) = store_meta.peer_properties.get(&region_id) {
                info!(
                    "register observe region";
                    "store id" => ?store_meta.store_id.clone(),
                    "region" => ?region
                );
                ObserveRegion::new(region.clone(), peer_properties.clone())
            } else {
                warn!(
                    "try register unexit region";
                    "store id" => ?store_meta.store_id.clone(),
                    "region" => ?region,
                );
                return;
            }
        };
        let observe_id = observe_region.observe_id;
        let cancelled = match observe_region.resolver_status {
            ResolverStatus::Pending { ref cancelled, .. } => cancelled.clone(),
            ResolverStatus::Ready => panic!("resolved ts illeagal created observe region"),
        };
        self.regions.insert(region_id, observe_region);

        let scan_task = self.build_scan_task(region, observe_id, cancelled);
        self.scanner_pool.spawn_task(scan_task);
    }

    fn build_scan_task(
        &self,
        region: Region,
        observe_id: ObserveID,
        cancelled: Arc<AtomicBool>,
    ) -> ScanTask {
        let scheduler = self.scheduler.clone();
        let scheduler_error = self.scheduler.clone();
        let region_id = region.id;
        ScanTask {
            id: observe_id,
            tag: String::new(),
            mode: ScanMode::LockOnly,
            region,
            checkpoint_ts: TimeStamp::zero(),
            is_cancelled: Box::new(move || cancelled.load(Ordering::Acquire)),
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
            on_error: Some(Box::new(move |observe_id, _region, e| {
                let error = e.extract_error_header();
                scheduler_error
                    .schedule(Task::RegionError {
                        region_id,
                        observe_id,
                        error,
                    })
                    .unwrap();
            })),
        }
    }

    fn deregister_region(&mut self, region_id: u64) {
        if let Some(observe_region) = self.regions.remove(&region_id) {
            let ObserveRegion {
                observe_id,
                resolver_status,
                ..
            } = observe_region;
            info!("deregister observe region"; "store_id" => ?self.store_meta.lock().unwrap().store_id, "region_id" => region_id, "observe_id" => ?observe_id);
            if let ResolverStatus::Pending { cancelled, .. } = resolver_status {
                cancelled.store(true, Ordering::Release);
            }
        } else {
            warn!("deregister unregister region"; "region_id" => region_id);
        }
    }

    // This function is corresponding to RegionDestroyed event that can be only scheduled by observer.
    // To prevent destroying region for wrong peer, it should check the region epoch at first.
    fn region_destroyed(&mut self, region: Region) {
        if let Some(observe_region) = self.regions.get(&region.id) {
            if util::compare_region_epoch(
                observe_region.meta.get_region_epoch(),
                &region,
                true,
                true,
                false,
            )
            .is_ok()
            {
                self.deregister_region(region.id);
            } else {
                warn!(
                    "resolved ts destroy region failed due to epoch not match";
                    "region_id" => region.id,
                    "current_epoch" => ?observe_region.meta.get_region_epoch(),
                    "request_epoch" => ?region.get_region_epoch(),
                )
            }
        }
    }

    // Start to advance resolved ts after peer becomes leader.
    // Stop to advance resolved ts after peer steps down to follower or candidate.
    // Do not need to check observe id because we expect all role change events are scheduled in order.
    fn region_role_changed(&mut self, region: Region, role: StateRole) {
        match role {
            StateRole::Leader => self.register_region(region),
            _ => self.deregister_region(region.id),
        }
    }

    // Deregister current observed region and try to register it again.
    // Call after the version of region epoch changed.
    fn region_error(&mut self, region_id: u64, observe_id: ObserveID, error: ErrorHeader) {
        if let Some(observe_region) = self.regions.get(&region_id) {
            if observe_region.observe_id != observe_id {
                warn!("resolved ts deregister region failed due to observe_id not match");
                return;
            }
            info!("region met error, try to register again"; "region_id" => region_id, "error" => ?error);
            self.deregister_region(region_id);
            let region;
            {
                let meta = self.store_meta.lock().unwrap();
                match meta.regions.get(&region_id) {
                    Some(r) => region = r.clone(),
                    None => return,
                }
            }
            self.register_region(region);
        }
    }

    // Try to advance resolved ts.
    // Must ensure all regions are leaders at the point of ts.
    fn advance_resolved_ts(&mut self, regions: Vec<u64>, ts: TimeStamp) {
        if regions.is_empty() {
            return;
        }

        let mut min_ts = TimeStamp::max();
        for region_id in regions.iter().copied() {
            if let Some(observe_region) = self.regions.get_mut(&region_id) {
                if let ResolverStatus::Ready = observe_region.resolver_status {
                    let resolved_ts = observe_region.resolver.resolve(ts);
                    if resolved_ts < min_ts {
                        min_ts = resolved_ts;
                    }
                }
            }
        }
        self.sinker.sink_resolved_ts(regions, ts);
    }

    // Tracking or untracking locks with incoming commands that corresponding observe id is valid.
    #[allow(clippy::drop_ref)]
    fn handle_change_log(
        &mut self,
        cmd_batch: Vec<CmdBatch>,
        snapshot: RegionSnapshot<E::Snapshot>,
    ) {
        let logs = cmd_batch
            .into_iter()
            .map(|batch| {
                if !batch.is_empty() {
                    if let Some(observe_region) = self.regions.get_mut(&batch.region_id) {
                        let observe_id = batch.cdc_id;
                        let region_id = observe_region.meta.id;
                        if observe_region.observe_id == observe_id {
                            let logs = ChangeLog::encode_change_log(region_id, batch);
                            if let Err(e) = observe_region.track_change_log(&logs) {
                                drop(observe_region);
                                self.region_error(region_id, observe_id, e.extract_error_header())
                            }
                            return Some(SinkCmd {
                                region_id,
                                observe_id,
                                logs,
                            });
                        } else {
                            debug!("resolved ts CmdBatch discarded";
                                "region_id" => batch.region_id,
                                "observe_id" => ?batch.cdc_id,
                                "current" => ?observe_region.observe_id,
                            );
                        }
                    }
                }
                None
            })
            .filter_map(|v| v)
            .collect();
        self.sinker.sink_cmd(logs, snapshot);
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

    fn register_advance_event(&self) {
        let regions = self.regions.keys().into_iter().copied().collect();
        self.advance_worker.register_advance_event(regions);
    }
}

pub enum Task<S: Snapshot> {
    RegionDestroyed(Region),
    RegionRoleChanged {
        region: Region,
        role: StateRole,
    },
    RegionError {
        region_id: u64,
        observe_id: ObserveID,
        error: ErrorHeader,
    },
    RegisterAdvanceEvent,
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
        snapshot: RegionSnapshot<S>,
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
            Task::RegionRoleChanged {
                ref region,
                ref role,
            } => de
                .field("name", &"region_role_changed")
                .field("region", &region)
                .field("role", &role)
                .finish(),
            Task::RegionError {
                ref region_id,
                ref observe_id,
                ref error,
            } => de
                .field("name", &"region_error")
                .field("region_id", &region_id)
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
            Task::RegisterAdvanceEvent => de.field("name", &"register_advance_event").finish(),
        }
    }
}

impl<S: Snapshot> fmt::Display for Task<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl<T, E, C> Runnable for Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    type Task = Task<E::Snapshot>;

    fn run(&mut self, task: Task<E::Snapshot>) {
        debug!("run resolved-ts task"; "task" => ?task);
        match task {
            Task::RegionDestroyed(region) => self.region_destroyed(region),
            Task::RegionRoleChanged { region, role } => self.region_role_changed(region, role),
            Task::RegionError {
                region_id,
                observe_id,
                error,
            } => self.region_error(region_id, observe_id, error),
            Task::AdvanceResolvedTs { regions, ts } => self.advance_resolved_ts(regions, ts),
            Task::ChangeLog {
                cmd_batch,
                snapshot,
            } => self.handle_change_log(cmd_batch, snapshot),
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
            } => self.handle_scan_locks(region_id, observe_id, entries),
            Task::RegisterAdvanceEvent => self.register_advance_event(),
        }
    }
}
