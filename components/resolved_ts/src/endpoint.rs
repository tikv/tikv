// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt,
    marker::PhantomData,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_traits::{KvEngine, Snapshot};
use grpcio::Environment;
use kvproto::{metapb::Region, raft_cmdpb::AdminCmdType};
use online_config::{self, ConfigChange, ConfigManager, OnlineConfig};
use pd_client::PdClient;
use raftstore::{
    coprocessor::{CmdBatch, ObserveHandle, ObserveID},
    router::RaftStoreRouter,
    store::{
        fsm::StoreMeta,
        util::{self, RegionReadProgress, RegionReadProgressRegistry},
        RegionSnapshot,
    },
};
use security::SecurityManager;
use tikv::config::ResolvedTsConfig;
use tikv_util::worker::{Runnable, RunnableWithTimer, Scheduler};
use txn_types::{Key, TimeStamp};

use crate::{
    advance::AdvanceTsWorker,
    cmd::{ChangeLog, ChangeRow},
    metrics::*,
    resolver::Resolver,
    scanner::{ScanEntry, ScanMode, ScanTask, ScannerPool},
    sinker::{CmdSinker, SinkCmd},
};

enum ResolverStatus {
    Pending {
        tracked_index: u64,
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
// observe_id is used for avoiding ABA problems in incremental scan task, advance resolved ts task,
// and command observing.
struct ObserveRegion {
    meta: Region,
    handle: ObserveHandle,
    // TODO: Get lease from raftstore.
    // lease: Option<RemoteLease>,
    resolver: Resolver,
    resolver_status: ResolverStatus,
}

impl ObserveRegion {
    fn new(meta: Region, rrp: Arc<RegionReadProgress>) -> Self {
        ObserveRegion {
            resolver: Resolver::with_read_progress(meta.id, Some(rrp)),
            meta,
            handle: ObserveHandle::new(),
            resolver_status: ResolverStatus::Pending {
                tracked_index: 0,
                locks: vec![],
                cancelled: Arc::new(AtomicBool::new(false)),
            },
        }
    }

    fn track_change_log(&mut self, change_logs: &[ChangeLog]) -> std::result::Result<(), String> {
        match &mut self.resolver_status {
            ResolverStatus::Pending {
                locks,
                tracked_index,
                ..
            } => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => {
                            debug!(
                                "skip change log error";
                                "region" => self.meta.id,
                                "error" => ?e,
                            );
                            continue;
                        }
                        ChangeLog::Admin(req_type) => {
                            // TODO: for admin cmd that won't change the region meta like peer list and key range
                            // (i.e. `CompactLog`, `ComputeHash`) we may not need to return error
                            return Err(format!(
                                "region met admin command {:?} while initializing resolver",
                                req_type
                            ));
                        }
                        ChangeLog::Rows { rows, index } => {
                            rows.iter().for_each(|row| match row {
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
                                // One pc command do not contains any lock, so just skip it
                                ChangeRow::OnePc { .. } => {}
                            });
                            assert!(
                                *tracked_index < *index,
                                "region {}, tracked_index: {}, incoming index: {}",
                                self.meta.id,
                                *tracked_index,
                                *index
                            );
                            *tracked_index = *index;
                        }
                    }
                }
            }
            ResolverStatus::Ready => {
                for log in change_logs {
                    match log {
                        ChangeLog::Error(e) => {
                            debug!(
                                "skip change log error";
                                "region" => self.meta.id,
                                "error" => ?e,
                            );
                            continue;
                        }
                        ChangeLog::Admin(req_type) => match req_type {
                            AdminCmdType::Split
                            | AdminCmdType::BatchSplit
                            | AdminCmdType::PrepareMerge
                            | AdminCmdType::RollbackMerge
                            | AdminCmdType::CommitMerge => {
                                info!(
                                    "region met split/merge command, stop tracking since key range changed, wait for re-register";
                                    "req_type" => ?req_type,
                                );
                                // Stop tracking so that `tracked_index` larger than the split/merge command index won't be published
                                // untill `RegionUpdate` event trigger the region re-register and re-scan the new key range
                                self.resolver.stop_tracking();
                            }
                            _ => {
                                debug!(
                                    "skip change log admin";
                                    "region" => self.meta.id,
                                    "req_type" => ?req_type,
                                );
                            }
                        },
                        ChangeLog::Rows { rows, index } => {
                            rows.iter().for_each(|row| match row {
                                ChangeRow::Prewrite { key, start_ts, .. } => self
                                    .resolver
                                    .track_lock(*start_ts, key.to_raw().unwrap(), Some(*index)),
                                ChangeRow::Commit { key, .. } => self
                                    .resolver
                                    .untrack_lock(&key.to_raw().unwrap(), Some(*index)),
                                // One pc command do not contains any lock, so just skip it
                                ChangeRow::OnePc { .. } => {}
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn track_scan_locks(&mut self, entries: Vec<ScanEntry>, apply_index: u64) {
        for es in entries {
            match es {
                ScanEntry::Lock(locks) => {
                    if let ResolverStatus::Ready = self.resolver_status {
                        panic!("region {:?} resolver has ready", self.meta.id)
                    }
                    for (key, lock) in locks {
                        self.resolver
                            .track_lock(lock.ts, key.to_raw().unwrap(), Some(apply_index));
                    }
                }
                ScanEntry::None => {
                    // Update the `tracked_index` to the snapshot's `apply_index`
                    self.resolver.update_tracked_index(apply_index);
                    let pending_tracked_index =
                        match std::mem::replace(&mut self.resolver_status, ResolverStatus::Ready) {
                            ResolverStatus::Pending {
                                locks,
                                tracked_index,
                                ..
                            } => {
                                locks.into_iter().for_each(|lock| match lock {
                                    PendingLock::Track { key, start_ts } => {
                                        self.resolver.track_lock(
                                            start_ts,
                                            key.to_raw().unwrap(),
                                            Some(tracked_index),
                                        )
                                    }
                                    PendingLock::Untrack { key, .. } => self
                                        .resolver
                                        .untrack_lock(&key.to_raw().unwrap(), Some(tracked_index)),
                                });
                                tracked_index
                            }
                            ResolverStatus::Ready => {
                                panic!("region {:?} resolver has ready", self.meta.id)
                            }
                        };
                    info!(
                        "Resolver initialized";
                        "region" => self.meta.id,
                        "observe_id" => ?self.handle.id,
                        "snapshot_index" => apply_index,
                        "pending_data_index" => pending_tracked_index,
                    );
                }
                ScanEntry::TxnEntry(_) => panic!("unexpected entry type"),
            }
        }
    }
}

pub struct Endpoint<T, E: KvEngine, C> {
    store_id: Option<u64>,
    cfg: ResolvedTsConfig,
    cfg_version: usize,
    store_meta: Arc<Mutex<StoreMeta>>,
    region_read_progress: RegionReadProgressRegistry,
    regions: HashMap<u64, ObserveRegion>,
    scanner_pool: ScannerPool<T, E>,
    scheduler: Scheduler<Task<E::Snapshot>>,
    sinker: C,
    advance_worker: AdvanceTsWorker<E>,
    _phantom: PhantomData<(T, E)>,
}

impl<T, E, C> Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    pub fn new(
        cfg: &ResolvedTsConfig,
        scheduler: Scheduler<Task<E::Snapshot>>,
        raft_router: T,
        store_meta: Arc<Mutex<StoreMeta>>,
        pd_client: Arc<dyn PdClient>,
        concurrency_manager: ConcurrencyManager,
        env: Arc<Environment>,
        security_mgr: Arc<SecurityManager>,
        sinker: C,
    ) -> Self {
        let (region_read_progress, store_id) = {
            let meta = store_meta.lock().unwrap();
            (meta.region_read_progress.clone(), meta.store_id)
        };
        let advance_worker = AdvanceTsWorker::new(
            pd_client,
            scheduler.clone(),
            store_meta.clone(),
            region_read_progress.clone(),
            concurrency_manager,
            env,
            security_mgr,
        );
        let scanner_pool = ScannerPool::new(cfg.scan_lock_pool_size, raft_router);
        let ep = Self {
            store_id,
            cfg: cfg.clone(),
            cfg_version: 0,
            scheduler,
            store_meta,
            region_read_progress,
            advance_worker,
            scanner_pool,
            sinker,
            regions: HashMap::default(),
            _phantom: PhantomData::default(),
        };
        ep.register_advance_event(ep.cfg_version);
        ep
    }

    fn register_region(&mut self, region: Region) {
        let region_id = region.get_id();
        assert!(self.regions.get(&region_id).is_none());
        let observe_region = {
            if let Some(read_progress) = self.region_read_progress.get(&region_id) {
                info!(
                    "register observe region";
                    "region" => ?region
                );
                ObserveRegion::new(region.clone(), read_progress)
            } else {
                warn!(
                    "try register unexit region";
                    "region" => ?region,
                );
                return;
            }
        };
        let observe_handle = observe_region.handle.clone();
        let cancelled = match observe_region.resolver_status {
            ResolverStatus::Pending { ref cancelled, .. } => cancelled.clone(),
            ResolverStatus::Ready => panic!("resolved ts illeagal created observe region"),
        };
        self.regions.insert(region_id, observe_region);

        let scan_task = self.build_scan_task(region, observe_handle, cancelled);
        self.scanner_pool.spawn_task(scan_task);
        RTS_SCAN_TASKS.with_label_values(&["total"]).inc();
    }

    fn build_scan_task(
        &self,
        region: Region,
        observe_handle: ObserveHandle,
        cancelled: Arc<AtomicBool>,
    ) -> ScanTask {
        let scheduler = self.scheduler.clone();
        let scheduler_error = self.scheduler.clone();
        let region_id = region.id;
        let observe_id = observe_handle.id;
        ScanTask {
            handle: observe_handle,
            tag: String::new(),
            mode: ScanMode::LockOnly,
            region,
            checkpoint_ts: TimeStamp::zero(),
            is_cancelled: Box::new(move || cancelled.load(Ordering::Acquire)),
            send_entries: Box::new(move |entries, apply_index| {
                scheduler
                    .schedule(Task::ScanLocks {
                        region_id,
                        observe_id,
                        entries,
                        apply_index,
                    })
                    .unwrap_or_else(|e| warn!("schedule resolved ts task failed"; "err" => ?e));
                RTS_SCAN_TASKS.with_label_values(&["finish"]).inc();
            }),
            on_error: Some(Box::new(move |observe_id, _region, e| {
                scheduler_error
                    .schedule(Task::ReRegisterRegion {
                        region_id,
                        observe_id,
                        cause: format!("met error while handle scan task {:?}", e),
                    })
                    .unwrap_or_else(|schedule_err| warn!("schedule re-register task failed"; "err" => ?schedule_err, "re-register cause" => ?e));
                RTS_SCAN_TASKS.with_label_values(&["abort"]).inc();
            })),
        }
    }

    fn deregister_region(&mut self, region_id: u64) {
        if let Some(observe_region) = self.regions.remove(&region_id) {
            let ObserveRegion {
                handle,
                resolver_status,
                ..
            } = observe_region;

            info!(
                "deregister observe region";
                "store_id" => ?self.get_or_init_store_id(),
                "region_id" => region_id,
                "observe_id" => ?handle.id
            );
            // Stop observing data
            handle.stop_observing();
            // Stop scanning data
            if let ResolverStatus::Pending { cancelled, .. } = resolver_status {
                cancelled.store(true, Ordering::Release);
            }
        } else {
            debug!("deregister unregister region"; "region_id" => region_id);
        }
    }

    fn region_updated(&mut self, incoming_region: Region) {
        let region_id = incoming_region.get_id();
        if let Some(obs_region) = self.regions.get_mut(&region_id) {
            if obs_region.meta.get_region_epoch().get_version()
                == incoming_region.get_region_epoch().get_version()
            {
                // only peer list change, no need to re-register region
                obs_region.meta = incoming_region;
                return;
            }
            // TODO: may not need to re-register region for some cases:
            // - `Split/BatchSplit`, which can be handled by remove out-of-range locks from the `Resolver`'s lock heap
            // - `PrepareMerge` and `RollbackMerge`, the key range is unchanged
            self.deregister_region(region_id);
            self.register_region(incoming_region);
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

    // Deregister current observed region and try to register it again.
    fn re_register_region(&mut self, region_id: u64, observe_id: ObserveID, cause: String) {
        if let Some(observe_region) = self.regions.get(&region_id) {
            if observe_region.handle.id != observe_id {
                warn!("resolved ts deregister region failed due to observe_id not match");
                return;
            }

            info!(
                "register region again";
                "region_id" => region_id,
                "observe_id" => ?observe_id,
                "cause" => cause
            );
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
        for region_id in regions.iter() {
            if let Some(observe_region) = self.regions.get_mut(region_id) {
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
        snapshot: Option<RegionSnapshot<E::Snapshot>>,
    ) {
        let size = cmd_batch.iter().map(|b| b.size()).sum::<usize>();
        RTS_CHANNEL_PENDING_CMD_BYTES.sub(size as i64);
        let logs = cmd_batch
            .into_iter()
            .filter_map(|batch| {
                if !batch.is_empty() {
                    if let Some(observe_region) = self.regions.get_mut(&batch.region_id) {
                        let observe_id = batch.rts_id;
                        let region_id = observe_region.meta.id;
                        if observe_region.handle.id == observe_id {
                            let logs = ChangeLog::encode_change_log(region_id, batch);
                            if let Err(e) = observe_region.track_change_log(&logs) {
                                drop(observe_region);
                                self.re_register_region(region_id, observe_id, e)
                            }
                            return Some(SinkCmd {
                                region_id,
                                observe_id,
                                logs,
                            });
                        } else {
                            debug!("resolved ts CmdBatch discarded";
                                "region_id" => batch.region_id,
                                "observe_id" => ?batch.rts_id,
                                "current" => ?observe_region.handle.id,
                            );
                        }
                    }
                }
                None
            })
            .collect();
        match snapshot {
            Some(snap) => self.sinker.sink_cmd_with_old_value(logs, snap),
            None => self.sinker.sink_cmd(logs),
        }
    }

    fn handle_scan_locks(
        &mut self,
        region_id: u64,
        observe_id: ObserveID,
        entries: Vec<ScanEntry>,
        apply_index: u64,
    ) {
        match self.regions.get_mut(&region_id) {
            Some(observe_region) => {
                if observe_region.handle.id == observe_id {
                    observe_region.track_scan_locks(entries, apply_index);
                }
            }
            None => {
                debug!("scan locks region not exist"; "region_id" => region_id, "observe_id" => ?observe_id);
            }
        }
    }

    fn register_advance_event(&self, cfg_version: usize) {
        // Ignore advance event that registered with previous `advance_ts_interval` config
        if self.cfg_version != cfg_version {
            return;
        }
        let regions = self.regions.keys().into_iter().copied().collect();
        self.advance_worker.advance_ts_for_regions(regions);
        self.advance_worker
            .register_next_event(self.cfg.advance_ts_interval.0, self.cfg_version);
    }

    fn handle_change_config(&mut self, change: ConfigChange) {
        let prev = format!("{:?}", self.cfg);
        let prev_advance_ts_interval = self.cfg.advance_ts_interval;
        self.cfg.update(change);
        if self.cfg.advance_ts_interval != prev_advance_ts_interval {
            // Increase the `cfg_version` to reject advance event that registered before
            self.cfg_version += 1;
            // Advance `resolved-ts` immediately after `advance_ts_interval` changed
            self.register_advance_event(self.cfg_version);
        }
        info!(
            "resolved-ts config changed";
            "prev" => prev,
            "current" => ?self.cfg,
        );
    }

    fn get_or_init_store_id(&mut self) -> Option<u64> {
        self.store_id.or_else(|| {
            let meta = self.store_meta.lock().unwrap();
            self.store_id = meta.store_id;
            meta.store_id
        })
    }
}

pub enum Task<S: Snapshot> {
    RegionUpdated(Region),
    RegionDestroyed(Region),
    RegisterRegion {
        region: Region,
    },
    DeRegisterRegion {
        region_id: u64,
    },
    ReRegisterRegion {
        region_id: u64,
        observe_id: ObserveID,
        cause: String,
    },
    RegisterAdvanceEvent {
        cfg_version: usize,
    },
    AdvanceResolvedTs {
        regions: Vec<u64>,
        ts: TimeStamp,
    },
    ChangeLog {
        cmd_batch: Vec<CmdBatch>,
        snapshot: Option<RegionSnapshot<S>>,
    },
    ScanLocks {
        region_id: u64,
        observe_id: ObserveID,
        entries: Vec<ScanEntry>,
        apply_index: u64,
    },
    ChangeConfig {
        change: ConfigChange,
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
            Task::RegisterRegion { ref region } => de
                .field("name", &"register_region")
                .field("region", &region)
                .finish(),
            Task::DeRegisterRegion { ref region_id } => de
                .field("name", &"deregister_region")
                .field("region_id", &region_id)
                .finish(),
            Task::ReRegisterRegion {
                ref region_id,
                ref observe_id,
                ref cause,
            } => de
                .field("name", &"re_register_region")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("cause", &cause)
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
                ref apply_index,
                ..
            } => de
                .field("name", &"scan_locks")
                .field("region_id", &region_id)
                .field("observe_id", &observe_id)
                .field("apply_index", &apply_index)
                .finish(),
            Task::RegisterAdvanceEvent { .. } => {
                de.field("name", &"register_advance_event").finish()
            }
            Task::ChangeConfig { ref change } => de
                .field("name", &"change_config")
                .field("change", &change)
                .finish(),
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
            Task::RegionUpdated(region) => self.region_updated(region),
            Task::RegisterRegion { region } => self.register_region(region),
            Task::DeRegisterRegion { region_id } => self.deregister_region(region_id),
            Task::ReRegisterRegion {
                region_id,
                observe_id,
                cause,
            } => self.re_register_region(region_id, observe_id, cause),
            Task::AdvanceResolvedTs { regions, ts } => self.advance_resolved_ts(regions, ts),
            Task::ChangeLog {
                cmd_batch,
                snapshot,
            } => self.handle_change_log(cmd_batch, snapshot),
            Task::ScanLocks {
                region_id,
                observe_id,
                entries,
                apply_index,
            } => self.handle_scan_locks(region_id, observe_id, entries, apply_index),
            Task::RegisterAdvanceEvent { cfg_version } => self.register_advance_event(cfg_version),
            Task::ChangeConfig { change } => self.handle_change_config(change),
        }
    }
}

pub struct ResolvedTsConfigManager<S: Snapshot>(Scheduler<Task<S>>);

impl<S: Snapshot> ResolvedTsConfigManager<S> {
    pub fn new(scheduler: Scheduler<Task<S>>) -> ResolvedTsConfigManager<S> {
        ResolvedTsConfigManager(scheduler)
    }
}

impl<S: Snapshot> ConfigManager for ResolvedTsConfigManager<S> {
    fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
        if let Err(e) = self.0.schedule(Task::ChangeConfig { change }) {
            error!("failed to schedule ChangeConfig task"; "err" => ?e);
        }
        Ok(())
    }
}

const METRICS_FLUSH_INTERVAL: u64 = 10_000; // 10s

impl<T, E, C> RunnableWithTimer for Endpoint<T, E, C>
where
    T: 'static + RaftStoreRouter<E>,
    E: KvEngine,
    C: CmdSinker<E::Snapshot>,
{
    fn on_timeout(&mut self) {
        let store_id = self.get_or_init_store_id();
        let (mut oldest_ts, mut oldest_region, mut zero_ts_count) = (u64::MAX, 0, 0);
        let (mut oldest_leader_ts, mut oldest_leader_region) = (u64::MAX, 0);
        self.region_read_progress.with(|registry| {
            for (region_id, read_progress) in registry {
                let (peers, leader_info) = read_progress.dump_leader_info();
                let leader_store_id = crate::util::find_store_id(&peers, leader_info.peer_id);
                let ts = leader_info.get_read_state().get_safe_ts();
                if ts == 0 {
                    zero_ts_count += 1;
                    continue;
                }
                if ts < oldest_ts {
                    oldest_ts = ts;
                    oldest_region = *region_id;
                }

                if let (Some(store_id), Some(leader_store_id)) = (store_id, leader_store_id) {
                    if leader_store_id == store_id && ts < oldest_leader_ts {
                        oldest_leader_ts = ts;
                        oldest_leader_region = *region_id;
                    }
                }
            }
        });
        let mut lock_heap_size = 0;
        let (mut resolved_count, mut unresolved_count) = (0, 0);
        for observe_region in self.regions.values() {
            match &observe_region.resolver_status {
                ResolverStatus::Pending { locks, .. } => {
                    for l in locks {
                        match l {
                            PendingLock::Track { key, .. } => lock_heap_size += key.len(),
                            PendingLock::Untrack { key, .. } => lock_heap_size += key.len(),
                        }
                    }
                    unresolved_count += 1;
                }
                ResolverStatus::Ready { .. } => {
                    lock_heap_size += observe_region.resolver.size();
                    resolved_count += 1;
                }
            }
        }
        RTS_MIN_RESOLVED_TS_REGION.set(oldest_region as i64);
        RTS_MIN_RESOLVED_TS.set(oldest_ts as i64);
        RTS_ZERO_RESOLVED_TS.set(zero_ts_count as i64);
        RTS_MIN_RESOLVED_TS_GAP.set(
            TimeStamp::physical_now().saturating_sub(TimeStamp::from(oldest_ts).physical()) as i64,
        );

        RTS_MIN_LEADER_RESOLVED_TS_REGION.set(oldest_leader_region as i64);
        RTS_MIN_LEADER_RESOLVED_TS.set(oldest_leader_ts as i64);
        RTS_MIN_LEADER_RESOLVED_TS_GAP.set(
            TimeStamp::physical_now().saturating_sub(TimeStamp::from(oldest_leader_ts).physical())
                as i64,
        );

        RTS_LOCK_HEAP_BYTES_GAUGE.set(lock_heap_size as i64);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["resolved"])
            .set(resolved_count as _);
        RTS_REGION_RESOLVE_STATUS_GAUGE_VEC
            .with_label_values(&["unresolved"])
            .set(unresolved_count as _);
    }

    fn get_interval(&self) -> Duration {
        Duration::from_millis(METRICS_FLUSH_INTERVAL)
    }
}
