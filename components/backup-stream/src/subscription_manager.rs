// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc, time::Duration};

use engine_traits::KvEngine;
use futures::FutureExt;
use kvproto::metapb::Region;
use raft::StateRole;
use raftstore::{
    coprocessor::{ObserveHandle, RegionInfoProvider},
    router::CdcHandle,
    store::fsm::ChangeObserver,
};
use rand::Rng;
use tikv::storage::Statistics;
use tikv_util::{
    box_err, debug, info, memory::MemoryQuota, sys::thread::ThreadBuildWrapper, time::Instant,
    warn, worker::Scheduler,
};
<<<<<<< HEAD
use tokio::sync::mpsc::{channel, error::SendError, Receiver, Sender};
=======
use tokio::sync::mpsc::{channel, error::SendError, Receiver, Sender, WeakSender};
use tracing::instrument;
use tracing_active_tree::root;
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
use txn_types::TimeStamp;

use crate::{
    annotate,
    endpoint::{BackupStreamResolver, ObserveOp},
    errors::{Error, ReportableResult, Result},
    event_loader::InitialDataLoader,
    future,
    metadata::{store::MetaStore, CheckpointProvider, MetadataClient},
    metrics,
    router::{Router, TaskSelector},
    subscription_track::{CheckpointType, Ref, RefMut, ResolveResult, SubscriptionTracer},
    try_send,
    utils::{self, CallbackWaitGroup, Work},
    Task,
};

type ScanPool = tokio::runtime::Runtime;

// The retry parameters for failed to get last checkpoint ts.
// When PD is temporarily disconnected, we may need this retry.
// The total duration of retrying is about 345s ( 20 * 16 + 15 ),
// which is longer than the RPO promise.
const TRY_START_OBSERVE_MAX_RETRY_TIME: u8 = 24;
const RETRY_AWAIT_BASIC_DURATION: Duration = Duration::from_secs(1);
const RETRY_AWAIT_MAX_DURATION: Duration = Duration::from_secs(16);
const OOM_BACKOFF_BASE: Duration = Duration::from_secs(60);
const OOM_BACKOFF_JITTER_SECS: u64 = 60;

fn backoff_for_start_observe(failed_for: u8) -> Duration {
    let res = Ord::min(
        RETRY_AWAIT_BASIC_DURATION * (1 << failed_for),
        RETRY_AWAIT_MAX_DURATION,
    );
    fail::fail_point!("subscribe_mgr_retry_start_observe_delay", |v| {
        v.and_then(|x| x.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(res)
    });
    res
}

/// a request for doing initial scanning.
struct ScanCmd {
    region: Region,
    handle: ObserveHandle,
    last_checkpoint: TimeStamp,

    // This channel will be used to send the result of the initial scanning.
    // NOTE: perhaps we can make them an closure so it will be more flexible.
    // but for now there isn't requirement of that.
    feedback_channel: Sender<ObserveOp>,
    _work: Work,
}

/// The response of requesting resolve the new checkpoint of regions.
pub struct ResolvedRegions {
    items: Vec<ResolveResult>,
    checkpoint: TimeStamp,
}

impl ResolvedRegions {
    /// Compose the calculated global checkpoint and region checkpoints.
    /// Note: Maybe we can compute the global checkpoint internal and getting
    /// the interface clear. However we must take the `min_ts` or we cannot
    /// provide valid global checkpoint if there isn't any region checkpoint.
    pub fn new(checkpoint: TimeStamp, checkpoints: Vec<ResolveResult>) -> Self {
        Self {
            items: checkpoints,
            checkpoint,
        }
    }

    /// take the region checkpoints from the structure.
    #[deprecated = "please use `take_resolve_result` instead."]
    pub fn take_region_checkpoints(&mut self) -> Vec<(Region, TimeStamp)> {
        std::mem::take(&mut self.items)
            .into_iter()
            .map(|x| (x.region, x.checkpoint))
            .collect()
    }

    /// take the resolve result from this struct.
    pub fn take_resolve_result(&mut self) -> Vec<ResolveResult> {
        std::mem::take(&mut self.items)
    }

    /// get the global checkpoint.
    pub fn global_checkpoint(&self) -> TimeStamp {
        self.checkpoint
    }
}

/// returns whether the error should be retried.
/// for some errors, like `epoch not match` or `not leader`,
/// implies that the region is drifting, and no more need to be observed by us.
fn should_retry(err: &Error) -> bool {
    match err.without_context() {
        Error::RaftRequest(pbe) => {
            !(pbe.has_epoch_not_match()
                || pbe.has_not_leader()
                || pbe.get_message().contains("stale observe id")
                || pbe.has_region_not_found())
        }
        Error::RaftStore(raftstore::Error::RegionNotFound(_))
        | Error::RaftStore(raftstore::Error::NotLeader(..))
        | Error::ObserveCanceled(..)
        | Error::RaftStore(raftstore::Error::EpochNotMatch(..)) => false,
        _ => true,
    }
}

/// the abstraction over a "DB" which provides the initial scanning.
#[async_trait::async_trait]
trait InitialScan: Clone + Sync + Send + 'static {
    async fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        handle: ObserveHandle,
    ) -> Result<Statistics>;

    fn handle_fatal_error(&self, region: &Region, err: Error);
}

#[async_trait::async_trait]
impl<E, RT> InitialScan for InitialDataLoader<E, RT>
where
    E: KvEngine,
    RT: CdcHandle<E> + Sync + 'static,
{
    async fn do_initial_scan(
        &self,
        region: &Region,
        start_ts: TimeStamp,
        handle: ObserveHandle,
    ) -> Result<Statistics> {
        let region_id = region.get_id();
        let h = handle.clone();
        // Note: we have external retry at `ScanCmd::exec_by_with_retry`, should we keep
        // retrying here?
        let snap = self
            .observe_over_with_retry(region, move || {
                ChangeObserver::from_pitr(region_id, handle.clone())
            })
            .await?;
        #[cfg(feature = "failpoints")]
        fail::fail_point!("scan_after_get_snapshot");
        let stat = self.do_initial_scan(region, h, start_ts, snap).await?;
        Ok(stat)
    }

    fn handle_fatal_error(&self, region: &Region, err: Error) {
        try_send!(
            self.scheduler,
            Task::FatalError(
                TaskSelector::ByRange(
                    region.get_start_key().to_owned(),
                    region.get_end_key().to_owned()
                ),
                Box::new(err),
            )
        );
    }
}

impl ScanCmd {
    /// execute the initial scanning via the specificated [`InitialDataLoader`].
    async fn exec_by(&self, initial_scan: impl InitialScan) -> Result<()> {
        let Self {
            region,
            handle,
            last_checkpoint,
            ..
        } = self;
        let begin = Instant::now_coarse();
        let stat = initial_scan
            .do_initial_scan(region, *last_checkpoint, handle.clone())
            .await?;
        info!("initial scanning finished!"; "takes" => ?begin.saturating_elapsed(), "from_ts" => %last_checkpoint, utils::slog_region(region));
        utils::record_cf_stat("lock", &stat.lock);
        utils::record_cf_stat("write", &stat.write);
        utils::record_cf_stat("default", &stat.data);
        Ok(())
    }
<<<<<<< HEAD

    /// execute the command, when meeting error, retrying.
    async fn exec_by_with_retry(self, init: impl InitialScan) {
        let mut retry_time = INITIAL_SCAN_FAILURE_MAX_RETRY_TIME;
        loop {
            match self.exec_by(init.clone()).await {
                Err(err) if should_retry(&err) && retry_time > 0 => {
                    tokio::time::sleep(Duration::from_millis(500)).await;
                    warn!("meet retryable error"; "err" => %err, "retry_time" => retry_time);
                    retry_time -= 1;
                    continue;
                }
                Err(err) if retry_time == 0 => {
                    init.handle_fatal_error(&self.region, err.context("retry time exceeds"));
                    break;
                }
                // Errors which `should_retry` returns false means they can be ignored.
                Err(_) | Ok(_) => break,
            }
        }
    }
=======
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
}

async fn scan_executor_loop(init: impl InitialScan, mut cmds: Receiver<ScanCmd>) {
    while let Some(cmd) = cmds.recv().await {
        debug!("handling initial scan request"; utils::slog_region(&cmd.region));
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .dec();
        #[cfg(feature = "failpoints")]
        {
            let sleep = (|| {
                fail::fail_point!("execute_scan_command_sleep_100", |_| { 100 });
                0
            })();
            tokio::time::sleep(std::time::Duration::from_secs(sleep)).await;
        }

        let init = init.clone();
        tokio::task::spawn(async move {
            metrics::PENDING_INITIAL_SCAN_LEN
                .with_label_values(&["executing"])
                .inc();
            let res = cmd.exec_by(init).await;
            cmd.feedback_channel
                .send(ObserveOp::NotifyStartObserveResult {
                    region: cmd.region,
                    handle: cmd.handle,
                    err: res.map_err(Box::new).err(),
                })
                .await
                .map_err(|err| {
                    Error::Other(box_err!(
                        "failed to send result, are we shutting down? {}",
                        err
                    ))
                })
                .report_if_err("exec initial scan");
            metrics::PENDING_INITIAL_SCAN_LEN
                .with_label_values(&["executing"])
                .dec();
        });
    }
}

/// spawn the executors in the scan pool.
fn spawn_executors(
    init: impl InitialScan + Send + Sync + 'static,
    number: usize,
) -> ScanPoolHandle {
    let (tx, rx) = tokio::sync::mpsc::channel(MESSAGE_BUFFER_SIZE);
    let pool = create_scan_pool(number);
<<<<<<< HEAD
    pool.spawn(async move {
        scan_executor_loop(init, rx).await;
    });
    ScanPoolHandle { tx, _pool: pool }
=======
    let handle = pool.handle().clone();
    handle.spawn(async move {
        scan_executor_loop(init, rx).await;
        // The behavior of log backup is undefined while TiKV shutting down.
        // (Recording the logs doesn't require any local persisted information.)
        // So it is OK to make works in the pool fully asynchronous (i.e. We
        // don't syncing it with shutting down.). This trick allows us get rid
        // of the long long panic information during testing.
        tokio::task::block_in_place(move || drop(pool));
    });
    ScanPoolHandle { tx }
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
}

struct ScanPoolHandle {
    // Theoretically, we can get rid of the sender, and spawn a new task via initial loader in each
    // thread. But that will make `SubscribeManager` holds a reference to the implementation of
    // `InitialScan`, which will get the type information a mass.
    tx: Sender<ScanCmd>,
}

impl ScanPoolHandle {
    async fn request(&self, cmd: ScanCmd) -> std::result::Result<(), SendError<ScanCmd>> {
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .inc();
        self.tx.send(cmd).await
    }
}

/// The default channel size.
const MESSAGE_BUFFER_SIZE: usize = 32768;

/// The operator for region subscription.
/// It make a queue for operations over the `SubscriptionTracer`, generally,
/// we should only modify the `SubscriptionTracer` itself (i.e. insert records,
/// remove records) at here. So the order subscription / desubscription won't be
/// broken.
pub struct RegionSubscriptionManager<S, R> {
    // Note: these fields appear everywhere, maybe make them a `context` type?
    regions: R,
    meta_cli: MetadataClient<S>,
    range_router: Router,
    scheduler: Scheduler<Task>,
    subs: SubscriptionTracer,

    failure_count: HashMap<u64, u8>,
    memory_manager: Arc<MemoryQuota>,

    messenger: WeakSender<ObserveOp>,
    scan_pool_handle: ScanPoolHandle,
    scans: Arc<CallbackWaitGroup>,
}

/// Create a pool for doing initial scanning.
fn create_scan_pool(num_threads: usize) -> ScanPool {
    tokio::runtime::Builder::new_multi_thread()
        .after_start_wrapper(move || {
            file_system::set_io_type(file_system::IoType::Replication);
        })
        .before_stop_wrapper(|| {})
        .thread_name("log-backup-scan")
        .enable_time()
        .worker_threads(num_threads)
        .build()
        .unwrap()
}

impl<S, R> RegionSubscriptionManager<S, R>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
{
    /// create a [`RegionSubscriptionManager`].
    ///
    /// # returns
    ///
    /// a two-tuple, the first is the handle to the manager, the second is the
    /// operator loop future.
    pub fn start<E, HInit, HChkLd>(
        initial_loader: InitialDataLoader<E, HInit>,
        regions: R,
        meta_cli: MetadataClient<S>,
        scan_pool_size: usize,
        resolver: BackupStreamResolver<HChkLd, E>,
    ) -> (Sender<ObserveOp>, future![()])
    where
        E: KvEngine,
        HInit: CdcHandle<E> + Sync + 'static,
        HChkLd: CdcHandle<E> + 'static,
    {
        let (tx, rx) = channel(MESSAGE_BUFFER_SIZE);
        let scan_pool_handle = spawn_executors(initial_loader.clone(), scan_pool_size);
        let op = Self {
            regions,
            meta_cli,
            range_router: initial_loader.sink.clone(),
            scheduler: initial_loader.scheduler.clone(),
            subs: initial_loader.tracing,
            messenger: tx.downgrade(),
            scan_pool_handle,
            scans: CallbackWaitGroup::new(),
            failure_count: HashMap::new(),
            memory_manager: Arc::clone(&initial_loader.quota),
        };
        let fut = op.region_operator_loop(rx, resolver);
        (tx, fut)
    }

    /// wait initial scanning get finished.
    pub fn wait(&self, timeout: Duration) -> future![bool] {
        tokio::time::timeout(timeout, self.scans.wait()).map(|result| result.is_err())
    }

    fn issue_fatal_of(&self, region: &Region, err: Error) {
        try_send!(
            self.scheduler,
            Task::FatalError(
                TaskSelector::ByRange(region.start_key.to_owned(), region.end_key.to_owned()),
                Box::new(err)
            )
        );
    }

    /// the handler loop.
    async fn region_operator_loop<E, RT>(
        mut self,
        mut message_box: Receiver<ObserveOp>,
        mut resolver: BackupStreamResolver<RT, E>,
    ) where
        E: KvEngine,
        RT: CdcHandle<E> + 'static,
    {
        while let Some(op) = message_box.recv().await {
            // Skip some trivial resolve commands.
            if !matches!(op, ObserveOp::ResolveRegions { .. }) {
                info!("backup stream: on_modify_observe"; "op" => ?op);
            }
            match op {
                ObserveOp::Start { region, handle } => {
                    fail::fail_point!("delay_on_start_observe");
                    self.start_observe(region, handle).await;
                    metrics::INITIAL_SCAN_REASON
                        .with_label_values(&["leader-changed"])
                        .inc();
                }
                ObserveOp::Stop { ref region } => {
                    self.subs.deregister_region_if(region, |_, _| true);
                }
                ObserveOp::Destroy { ref region } => {
                    self.subs.deregister_region_if(region, |old, new| {
                        raftstore::store::util::compare_region_epoch(
                            old.meta.get_region_epoch(),
                            new,
                            true,
                            true,
                            false,
                        )
                        .map_err(|err| warn!("check epoch and stop failed."; utils::slog_region(region), "err" => %err))
                        .is_ok()
                    });
                }
                ObserveOp::RefreshResolver { ref region } => self.refresh_resolver(region).await,
                ObserveOp::NotifyStartObserveResult {
                    region,
                    handle,
                    err,
                } => {
                    self.on_observe_result(region, handle, err).await;
                }
                ObserveOp::ResolveRegions { callback, min_ts } => {
                    let now = Instant::now();
                    let timedout = self.wait(Duration::from_secs(5)).await;
                    if timedout {
                        warn!("waiting for initial scanning done timed out, forcing progress!"; 
                            "take" => ?now.saturating_elapsed(), "timedout" => %timedout);
                    }
                    let regions = resolver.resolve(self.subs.current_regions(), min_ts).await;
                    let cps = self.subs.resolve_with(min_ts, regions);
                    let min_region = cps.iter().min_by_key(|rs| rs.checkpoint);
                    // If there isn't any region observed, the `min_ts` can be used as resolved ts
                    // safely.
                    let rts = min_region.map(|rs| rs.checkpoint).unwrap_or(min_ts);
                    if min_region
                        .map(|mr| mr.checkpoint_type != CheckpointType::MinTs)
                        .unwrap_or(false)
                    {
                        info!("getting non-trivial checkpoint"; "defined_by_region" => ?min_region);
                    }
                    callback(ResolvedRegions::new(rts, cps));
                }
                ObserveOp::HighMemUsageWarning { region_id } => {
                    self.on_high_memory_usage(region_id).await;
                }
            }
        }
    }

    async fn on_observe_result(
        &mut self,
        region: Region,
        handle: ObserveHandle,
        err: Option<Box<Error>>,
    ) {
        let err = match err {
            None => {
                self.failure_count.remove(&region.id);
                let sub = self.subs.get_subscription_of(region.id);
                if let Some(mut sub) = sub {
                    if sub.value().handle.id == handle.id {
                        sub.value_mut().resolver.phase_one_done();
                    }
                }
                return;
            }
            Some(err) => {
                if !should_retry(&err) {
                    self.failure_count.remove(&region.id);
                    self.subs
                        .deregister_region_if(&region, |sub, _| sub.handle.id == handle.id);
                    return;
                }
                err
            }
        };

        let region_id = region.id;
        match self.retry_observe(region.clone(), handle).await {
            Ok(has_resent_req) => {
                if !has_resent_req {
                    self.failure_count.remove(&region_id);
                }
            }
            Err(e) => {
                self.issue_fatal_of(
                    &region,
                    e.context(format_args!(
                        "retry encountered error, origin error is {}",
                        err
                    )),
                );
                self.failure_count.remove(&region_id);
            }
        }
    }

    async fn on_high_memory_usage(&mut self, inconsistent_region_id: u64) {
        let mut lame_region = Region::new();
        lame_region.set_id(inconsistent_region_id);
        let mut act_region = None;
        self.subs.deregister_region_if(&lame_region, |act, _| {
            act_region = Some(act.meta.clone());
            true
        });
        let delay = OOM_BACKOFF_BASE
            + Duration::from_secs(rand::thread_rng().gen_range(0..OOM_BACKOFF_JITTER_SECS));
        info!("log backup triggering high memory usage."; 
            "region" => %inconsistent_region_id, 
            "mem_usage" => %self.memory_manager.used_ratio(), 
            "mem_max" => %self.memory_manager.capacity());
        if let Some(region) = act_region {
            self.schedule_start_observe(delay, region, None);
        }
    }

    fn schedule_start_observe(
        &self,
        backoff: Duration,
        region: Region,
        handle: Option<ObserveHandle>,
    ) {
        let tx = self.messenger.upgrade();
        let region_id = region.id;
        if tx.is_none() {
            warn!(
                "log backup subscription manager: cannot upgrade self-sender, are we shutting down?"
            );
            return;
        }
        let tx = tx.unwrap();
        // tikv_util::Instant cannot be converted to std::time::Instant :(
        let start = std::time::Instant::now();
        let scheduled = async move {
            tokio::time::sleep_until((start + backoff).into()).await;
            let handle = handle.unwrap_or_else(|| ObserveHandle::new());
            if let Err(err) = tx.send(ObserveOp::Start { region, handle }).await {
                warn!("log backup failed to schedule start observe."; "err" => %err);
            }
        };
        tokio::spawn(root!("scheduled_subscription"; scheduled; "after" = ?backoff, region_id));
    }

    #[instrument(skip_all, fields(id = region.id))]
    async fn refresh_resolver(&self, region: &Region) {
        let need_refresh_all = !self.subs.try_update_region(region);

        if need_refresh_all {
            let canceled = self.subs.deregister_region_if(region, |_, _| true);
            let handle = ObserveHandle::new();
            if canceled {
                if let Some(for_task) = self.find_task_by_region(region) {
                    metrics::INITIAL_SCAN_REASON
                        .with_label_values(&["region-changed"])
                        .inc();
                    let r = async {
                        self.subs.add_pending_region(region);
                        self.observe_over_with_initial_data_from_checkpoint(
                            region,
                            self.get_last_checkpoint_of(&for_task, region).await?,
                            handle.clone(),
                        )
                        .await;
                        Result::Ok(())
                    }
                    .await;
                    if let Err(e) = r {
                        warn!("failed to refresh region: will retry."; "err" => %e, utils::slog_region(region));
                        try_send!(
                            self.scheduler,
                            Task::ModifyObserve(ObserveOp::NotifyStartObserveResult {
                                region: region.clone(),
                                handle,
                                err: Some(Box::new(e)),
                            })
                        );
                    }
                } else {
                    warn!(
                        "BUG: the region {:?} is register to no task but being observed",
                        utils::debug_region(region)
                    );
                }
            }
        }
    }

    async fn try_start_observe(&self, region: &Region, handle: ObserveHandle) -> Result<()> {
        match self.find_task_by_region(region) {
            None => {
                warn!(
                    "the region is register to no task but being observed: maybe stale, skipping";
                    utils::slog_region(region),
                    "task_status" => ?self.range_router,
                );
            }

            Some(for_task) => {
                // the extra failpoint is used to pause the thread.
                // once it triggered "pause" it cannot trigger early return then.
                fail::fail_point!("try_start_observe0");
                fail::fail_point!("try_start_observe", |_| {
                    Err(Error::Other(box_err!("Nature is boring")))
                });
                let tso = self.get_last_checkpoint_of(&for_task, region).await?;
                self.observe_over_with_initial_data_from_checkpoint(region, tso, handle.clone())
                    .await;
            }
        }
        Ok(())
    }

<<<<<<< HEAD
    async fn start_observe(&self, region: Region) {
        self.start_observe_with_failure_count(region, 0).await
    }

    async fn start_observe_with_failure_count(&self, region: Region, has_failed_for: u8) {
        let handle = ObserveHandle::new();
        let schd = self.scheduler.clone();
=======
    async fn start_observe(&self, region: Region, handle: ObserveHandle) {
        match self.is_available(&region, &handle).await {
            Ok(false) => {
                warn!("stale start observe command."; utils::slog_region(&region), "handle" => ?handle);
                return;
            }
            Err(err) => {
                self.issue_fatal_of(&region, err.context("failed to check stale"));
                return;
            }
            _ => {}
        }
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
        self.subs.add_pending_region(&region);
        let res = self.try_start_observe(&region, handle.clone()).await;
        if let Err(err) = res {
            warn!("failed to start observe, would retry"; "err" => %err, utils::slog_region(&region));
<<<<<<< HEAD
            tokio::spawn(async move {
                #[cfg(not(feature = "failpoints"))]
                let delay = backoff_for_start_observe(has_failed_for);
                #[cfg(feature = "failpoints")]
                let delay = (|| {
                    fail::fail_point!("subscribe_mgr_retry_start_observe_delay", |v| {
                        let dur = v
                            .expect("should provide delay time (in ms)")
                            .parse::<u64>()
                            .expect("should be number (in ms)");
                        Duration::from_millis(dur)
                    });
                    backoff_for_start_observe(has_failed_for)
                })();
                tokio::time::sleep(delay).await;
                try_send!(
                    schd,
                    Task::ModifyObserve(ObserveOp::NotifyFailToStartObserve {
                        region,
                        handle,
                        err: Box::new(err),
                        has_failed_for: has_failed_for + 1
                    })
                )
            });
=======
            try_send!(
                self.scheduler,
                Task::ModifyObserve(ObserveOp::NotifyStartObserveResult {
                    region,
                    handle,
                    err: Some(Box::new(err)),
                })
            );
>>>>>>> 66301257e4 (log_backup: stop task while memory out of quota (#16008))
        }
    }

    #[instrument(skip_all)]
    async fn is_available(&self, region: &Region, handle: &ObserveHandle) -> Result<bool> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.regions
            .find_region_by_id(
                region.get_id(),
                Box::new(move |item| {
                    tx.send(item)
                        .expect("BUG: failed to send to newly created channel.");
                }),
            )
            .map_err(|err| {
                annotate!(
                    err,
                    "failed to send request to region info accessor, server maybe too too too busy. (region id = {})",
                    region.get_id()
                )
            })?;
        let new_region_info = rx
            .await
            .map_err(|err| annotate!(err, "BUG?: unexpected channel message dropped."))?;
        if new_region_info.is_none() {
            metrics::SKIP_RETRY
                .with_label_values(&["region-absent"])
                .inc();
            return Ok(false);
        }
        let new_region_info = new_region_info.unwrap();
        if new_region_info.role != StateRole::Leader {
            metrics::SKIP_RETRY.with_label_values(&["not-leader"]).inc();
            return Ok(false);
        }
        if raftstore::store::util::is_epoch_stale(
            region.get_region_epoch(),
            new_region_info.region.get_region_epoch(),
        ) {
            metrics::SKIP_RETRY
                .with_label_values(&["epoch-not-match"])
                .inc();
            return Ok(false);
        }
        // Note: we may fail before we insert the region info to the subscription map.
        // At that time, the command isn't steal and we should retry it.
        let mut exists = false;
        let removed = self.subs.deregister_region_if(region, |old, _| {
            exists = true;
            let should_remove = old.handle().id == handle.id;
            if !should_remove {
                warn!("stale retry command"; utils::slog_region(region), "handle" => ?handle, "old_handle" => ?old.handle());
            }
            should_remove
        });
        if !removed && exists {
            metrics::SKIP_RETRY
                .with_label_values(&["stale-command"])
                .inc();
            return Ok(false);
        }
        Ok(true)
    }

    async fn retry_observe(&mut self, region: Region, handle: ObserveHandle) -> Result<bool> {
        let failure_count = self.failure_count.entry(region.id).or_insert(0);
        *failure_count += 1;
        let failure_count = *failure_count;

        info!("retry observe region"; "region" => %region.get_id(), "failure_count" => %failure_count, "handle" => ?handle);
        if failure_count > TRY_START_OBSERVE_MAX_RETRY_TIME {
            return Err(Error::Other(
                format!(
                    "retry time exceeds for region {:?}",
                    utils::debug_region(&region)
                )
                .into(),
            ));
        }

        let should_retry = self.is_available(&region, &handle).await?;
        if !should_retry {
            return Ok(false);
        }
        self.schedule_start_observe(backoff_for_start_observe(failure_count), region, None);
        metrics::INITIAL_SCAN_REASON
            .with_label_values(&["retry"])
            .inc();
        Ok(true)
    }

    async fn get_last_checkpoint_of(&self, task: &str, region: &Region) -> Result<TimeStamp> {
        fail::fail_point!("get_last_checkpoint_of", |hint| Err(Error::Other(
            box_err!(
                "get_last_checkpoint_of({}, {:?}) failed because {:?}",
                task,
                region,
                hint
            )
        )));
        let meta_cli = self.meta_cli.clone();
        let cp = meta_cli.get_region_checkpoint(task, region).await?;
        debug!("got region checkpoint"; "region_id" => %region.get_id(), "checkpoint" => ?cp);
        if matches!(cp.provider, CheckpointProvider::Global) {
            metrics::STORE_CHECKPOINT_TS
                .with_label_values(&[task])
                .set(cp.ts.into_inner() as _);
        }
        Ok(cp.ts)
    }

    async fn spawn_scan(&self, cmd: ScanCmd) {
        // we should not spawn initial scanning tasks to the tokio blocking pool
        // because it is also used for converting sync File I/O to async. (for now!)
        // In that condition, if we blocking for some resources(for example, the
        // `MemoryQuota`) at the block threads, we may meet some ghosty
        // deadlock.
        let s = self.scan_pool_handle.request(cmd).await;
        if let Err(err) = s {
            let region_id = err.0.region.get_id();
            annotate!(err, "BUG: scan_pool closed")
                .report(format!("during initial scanning for region {}", region_id));
        }
    }

    async fn observe_over_with_initial_data_from_checkpoint(
        &self,
        region: &Region,
        last_checkpoint: TimeStamp,
        handle: ObserveHandle,
    ) {
        self.subs
            .register_region(region, handle.clone(), Some(last_checkpoint));
        let feedback_channel = match self.messenger.upgrade() {
            Some(ch) => ch,
            None => {
                warn!("log backup subscription manager is shutting down, aborting new scan."; 
                    utils::slog_region(region), "handle" => ?handle.id);
                return;
            }
        };
        self.spawn_scan(ScanCmd {
            region: region.clone(),
            handle,
            last_checkpoint,
            feedback_channel,
            _work: self.scans.clone().work(),
        })
        .await
    }

    fn find_task_by_region(&self, r: &Region) -> Option<String> {
        self.range_router
            .find_task_by_range(&r.start_key, &r.end_key)
    }
}

#[cfg(test)]
mod test {
    use std::{
        collections::HashMap,
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    };

    use engine_test::{kv::KvTestEngine, raft::RaftTestEngine};
    use kvproto::{
        brpb::{Noop, StorageBackend, StreamBackupTaskInfo},
        metapb::{Region, RegionEpoch},
    };
    use raftstore::{
        coprocessor::{ObserveHandle, RegionInfoCallback, RegionInfoProvider},
        router::{CdcRaftRouter, ServerRaftStoreRouter},
        RegionInfo,
    };
    use tikv::{config::BackupStreamConfig, storage::Statistics};
    use tikv_util::{info, memory::MemoryQuota, worker::dummy_scheduler};
    use tokio::{sync::mpsc::Sender, task::JoinHandle};
    use txn_types::TimeStamp;

    use super::{spawn_executors, InitialScan, RegionSubscriptionManager};
    use crate::{
        errors::Error,
        metadata::{store::SlashEtcStore, MetadataClient, StreamTask},
        router::{Router, RouterInner},
        subscription_manager::{OOM_BACKOFF_BASE, OOM_BACKOFF_JITTER_SECS},
        subscription_track::{CheckpointType, SubscriptionTracer},
        utils::CallbackWaitGroup,
        BackupStreamResolver, ObserveOp, Task,
    };

    #[derive(Clone, Copy)]
    struct FuncInitialScan<F>(F)
    where
        F: Fn(&Region, TimeStamp, ObserveHandle) -> crate::errors::Result<Statistics>
            + Clone
            + Sync
            + Send
            + 'static;

    #[async_trait::async_trait]
    impl<F> InitialScan for FuncInitialScan<F>
    where
        F: Fn(&Region, TimeStamp, ObserveHandle) -> crate::errors::Result<Statistics>
            + Clone
            + Sync
            + Send
            + 'static,
    {
        async fn do_initial_scan(
            &self,
            region: &Region,
            start_ts: txn_types::TimeStamp,
            handle: raftstore::coprocessor::ObserveHandle,
        ) -> crate::errors::Result<tikv::storage::Statistics> {
            (self.0)(region, start_ts, handle)
        }

        fn handle_fatal_error(&self, region: &Region, err: crate::errors::Error) {
            panic!("fatal {:?} {}", region, err)
        }
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn test_message_delay_and_exit() {
        use std::time::Duration;

        use futures::executor::block_on;

        use super::ScanCmd;
        use crate::{subscription_manager::spawn_executors, utils::CallbackWaitGroup};

        fn should_finish_in(f: impl FnOnce() + Send + 'static, d: std::time::Duration) {
            let (tx, rx) = futures::channel::oneshot::channel();
            std::thread::spawn(move || {
                f();
                tx.send(()).unwrap();
            });
            let pool = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let _e = pool.handle().enter();
            pool.block_on(tokio::time::timeout(d, rx)).unwrap().unwrap();
        }

        let pool = spawn_executors(FuncInitialScan(|_, _, _| Ok(Statistics::default())), 1);
        let wg = CallbackWaitGroup::new();
        let (tx, _) = tokio::sync::mpsc::channel(1);
        fail::cfg("execute_scan_command_sleep_100", "return").unwrap();
        for _ in 0..100 {
            let wg = wg.clone();
            assert!(
                block_on(pool.request(ScanCmd {
                    region: Default::default(),
                    handle: Default::default(),
                    last_checkpoint: Default::default(),
                    feedback_channel: tx.clone(),
                    // Note: Maybe make here a Box<dyn FnOnce()> or some other trait?
                    _work: wg.work(),
                }))
                .is_ok()
            )
        }

        should_finish_in(move || drop(pool), Duration::from_secs(5));
    }

    #[test]
    fn test_backoff_for_start_observe() {
        assert_eq!(
            super::backoff_for_start_observe(0),
            super::RETRY_AWAIT_BASIC_DURATION
        );
        assert_eq!(
            super::backoff_for_start_observe(1),
            super::RETRY_AWAIT_BASIC_DURATION * 2
        );
        assert_eq!(
            super::backoff_for_start_observe(2),
            super::RETRY_AWAIT_BASIC_DURATION * 4
        );
        assert_eq!(
            super::backoff_for_start_observe(3),
            super::RETRY_AWAIT_BASIC_DURATION * 8
        );
        assert_eq!(
            super::backoff_for_start_observe(4),
            super::RETRY_AWAIT_MAX_DURATION
        );
        assert_eq!(
            super::backoff_for_start_observe(5),
            super::RETRY_AWAIT_MAX_DURATION
        );
    }

    struct Suite {
        rt: tokio::runtime::Runtime,
        bg_tasks: Vec<JoinHandle<()>>,
        cancel: Arc<AtomicBool>,

        events: Arc<Mutex<Vec<ObserveEvent>>>,
        task_start_ts: TimeStamp,
        handle: Option<Sender<ObserveOp>>,
        regions: RegionMem,
        subs: SubscriptionTracer,
    }

    #[derive(Debug, Eq, PartialEq)]
    enum ObserveEvent {
        Start(u64),
        Stop(u64),
        StartResult(u64, bool),
        HighMemUse(u64),
    }

    impl ObserveEvent {
        fn of(op: &ObserveOp) -> Option<Self> {
            match op {
                ObserveOp::Start { region, .. } => Some(Self::Start(region.id)),
                ObserveOp::Stop { region } => Some(Self::Stop(region.id)),
                ObserveOp::NotifyStartObserveResult { region, err, .. } => {
                    Some(Self::StartResult(region.id, err.is_none()))
                }
                ObserveOp::HighMemUsageWarning {
                    region_id: inconsistent_region_id,
                } => Some(Self::HighMemUse(*inconsistent_region_id)),

                _ => None,
            }
        }
    }

    #[derive(Clone, Default)]
    struct RegionMem {
        regions: Arc<Mutex<HashMap<u64, RegionInfo>>>,
    }

    impl RegionInfoProvider for RegionMem {
        fn find_region_by_id(
            &self,
            region_id: u64,
            callback: RegionInfoCallback<Option<RegionInfo>>,
        ) -> raftstore::coprocessor::Result<()> {
            let rs = self.regions.lock().unwrap();
            let info = rs.get(&region_id).cloned();
            drop(rs);
            callback(info);
            Ok(())
        }
    }

    impl Suite {
        fn new(init: impl InitialScan) -> Self {
            let task_name = "test";
            let task_start_ts = TimeStamp::new(42);
            let pool = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let regions = RegionMem::default();
            let meta_cli = SlashEtcStore::default();
            let meta_cli = MetadataClient::new(meta_cli, 1);
            let (scheduler, mut output) = dummy_scheduler();
            let subs = SubscriptionTracer::default();
            let memory_manager = Arc::new(MemoryQuota::new(1024));
            let (tx, mut rx) = tokio::sync::mpsc::channel(8);
            let router = RouterInner::new(scheduler.clone(), BackupStreamConfig::default().into());
            let mut task = StreamBackupTaskInfo::new();
            task.set_name(task_name.to_owned());
            task.set_storage({
                let nop = Noop::new();
                let mut backend = StorageBackend::default();
                backend.set_noop(nop);
                backend
            });
            task.set_start_ts(task_start_ts.into_inner());
            let mut task_wrapped = StreamTask::default();
            task_wrapped.info = task;
            pool.block_on(meta_cli.insert_task_with_range(&task_wrapped, &[(b"", b"\xFF\xFF")]))
                .unwrap();
            pool.block_on(router.register_task(
                task_wrapped,
                vec![(vec![], vec![0xff, 0xff])],
                1024 * 1024,
            ))
            .unwrap();
            let subs_mgr = RegionSubscriptionManager {
                regions: regions.clone(),
                meta_cli,
                range_router: Router(Arc::new(router)),
                scheduler,
                subs: subs.clone(),
                failure_count: Default::default(),
                memory_manager,
                messenger: tx.downgrade(),
                scan_pool_handle: spawn_executors(init, 2),
                scans: CallbackWaitGroup::new(),
            };
            let events = Arc::new(Mutex::new(vec![]));
            let ob_events = Arc::clone(&events);
            let (ob_tx, ob_rx) = tokio::sync::mpsc::channel(1);
            let mut bg_tasks = vec![];
            bg_tasks.push(pool.spawn(async move {
                while let Some(item) = rx.recv().await {
                    if let Some(record) = ObserveEvent::of(&item) {
                        ob_events.lock().unwrap().push(record);
                    }
                    ob_tx.send(item).await.unwrap();
                }
            }));
            let self_tx = tx.clone();
            let canceled = Arc::new(AtomicBool::new(false));
            let cancel = canceled.clone();
            bg_tasks.push(pool.spawn_blocking(move || {
                loop {
                    match output.recv_timeout(Duration::from_millis(10)) {
                        Ok(Some(item)) => match item {
                            Task::ModifyObserve(ob) => tokio::runtime::Handle::current()
                                .block_on(self_tx.send(ob))
                                .unwrap(),
                            Task::FatalError(select, err) => {
                                panic!(
                                    "Background handler received fatal error {err} for {select:?}!"
                                )
                            }
                            _ => {}
                        },
                        Ok(None) => return,
                        Err(_) => {
                            if canceled.load(Ordering::SeqCst) {
                                return;
                            }
                        }
                    }
                }
            }));
            bg_tasks.push(
                pool.spawn(subs_mgr.region_operator_loop::<KvTestEngine, CdcRaftRouter<
                    ServerRaftStoreRouter<KvTestEngine, RaftTestEngine>,
                >>(ob_rx, BackupStreamResolver::Nop)),
            );

            Self {
                rt: pool,
                events,
                regions,
                handle: Some(tx),
                task_start_ts,
                bg_tasks,
                cancel,
                subs,
            }
        }

        fn run(&self, op: ObserveOp) {
            self.rt
                .block_on(self.handle.as_ref().unwrap().send(op))
                .unwrap()
        }

        fn start_region(&self, region: Region) {
            self.regions.regions.lock().unwrap().insert(
                region.id,
                RegionInfo {
                    region: region.clone(),
                    role: raft::StateRole::Leader,
                    buckets: 0,
                },
            );
            self.run(ObserveOp::Start {
                region,
                handle: ObserveHandle::new(),
            });
        }

        fn region(
            &self,
            id: u64,
            version: u64,
            conf_ver: u64,
            start_key: &[u8],
            end_key: &[u8],
        ) -> Region {
            let mut region = Region::default();
            region.set_id(id);
            region.set_region_epoch({
                let mut rp = RegionEpoch::new();
                rp.set_conf_ver(conf_ver);
                rp.set_version(version);
                rp
            });
            region.set_start_key(start_key.to_vec());
            region.set_end_key(end_key.to_vec());
            region
        }

        fn wait_shutdown(&mut self) {
            drop(self.handle.take());
            self.cancel.store(true, Ordering::SeqCst);
            self.rt
                .block_on(futures::future::try_join_all(std::mem::take(
                    &mut self.bg_tasks,
                )))
                .unwrap();
        }

        #[track_caller]
        fn wait_initial_scan_all_finish(&self, expected_region: usize) {
            info!("[TEST] Start waiting initial scanning finish.");
            self.rt.block_on(async move {
                let max_wait = Duration::from_secs(1);
                let start = Instant::now();
                loop {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    if start.elapsed() > max_wait {
                        panic!(
                            "wait initial scan takes too long! events = {:?}",
                            self.events
                        );
                    }
                    self.handle
                        .as_ref()
                        .unwrap()
                        .send(ObserveOp::ResolveRegions {
                            callback: Box::new(move |result| {
                                let no_initial_scan = result.items.iter().all(|r| {
                                    r.checkpoint_type != CheckpointType::StartTsOfInitialScan
                                });
                                let all_region_done = result.items.len() == expected_region;
                                tx.send(no_initial_scan && all_region_done).unwrap()
                            }),
                            min_ts: self.task_start_ts.next(),
                        })
                        .await
                        .unwrap();
                    if rx.await.unwrap() {
                        info!("[TEST] Finish waiting initial scanning finish.");
                        return;
                    }
                    // Advance the global timer in case of someone is waiting for timer.
                    tokio::time::advance(Duration::from_secs(16)).await;
                }
            })
        }

        fn advance_ms(&self, n: u64) {
            self.rt
                .block_on(tokio::time::advance(Duration::from_millis(n)))
        }
    }

    #[test]
    fn test_basic_retry() {
        test_util::init_log_for_test();
        use ObserveEvent::*;
        let failed = Arc::new(AtomicBool::new(false));
        let mut suite = Suite::new(FuncInitialScan(move |r, _, _| {
            if r.id != 1 || failed.load(Ordering::SeqCst) {
                return Ok(Statistics::default());
            }
            failed.store(true, Ordering::SeqCst);
            Err(Error::OutOfQuota { region_id: r.id })
        }));
        let _guard = suite.rt.enter();
        tokio::time::pause();
        suite.start_region(suite.region(1, 1, 1, b"a", b"b"));
        suite.start_region(suite.region(2, 1, 1, b"b", b"c"));
        suite.wait_initial_scan_all_finish(2);
        suite.wait_shutdown();
        assert_eq!(
            &*suite.events.lock().unwrap(),
            &[
                Start(1),
                Start(2),
                StartResult(1, false),
                StartResult(2, true),
                Start(1),
                StartResult(1, true)
            ]
        );
    }

    #[test]
    fn test_on_high_mem() {
        let mut suite = Suite::new(FuncInitialScan(|_, _, _| Ok(Statistics::default())));
        let _guard = suite.rt.enter();
        tokio::time::pause();
        suite.start_region(suite.region(1, 1, 1, b"a", b"b"));
        suite.start_region(suite.region(2, 1, 1, b"b", b"c"));
        suite.advance_ms(0);
        let mut rs = suite.subs.current_regions();
        rs.sort();
        assert_eq!(rs, [1, 2]);
        suite.wait_initial_scan_all_finish(2);
        suite.run(ObserveOp::HighMemUsageWarning { region_id: 1 });
        suite.advance_ms(0);
        assert_eq!(suite.subs.current_regions(), [2]);
        suite.advance_ms(
            (OOM_BACKOFF_BASE + Duration::from_secs(OOM_BACKOFF_JITTER_SECS + 1)).as_millis() as _,
        );
        suite.wait_initial_scan_all_finish(2);
        suite.wait_shutdown();
        let mut rs = suite.subs.current_regions();
        rs.sort();
        assert_eq!(rs, [1, 2]);

        use ObserveEvent::*;
        assert_eq!(
            &*suite.events.lock().unwrap(),
            &[
                Start(1),
                Start(2),
                StartResult(1, true),
                StartResult(2, true),
                HighMemUse(1),
                Start(1),
                StartResult(1, true),
            ]
        );
    }
}
