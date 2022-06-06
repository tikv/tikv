use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use crossbeam::channel::{Receiver as SyncReceiver, Sender as SyncSender};
use engine_traits::KvEngine;
use error_code::{backup_stream::OBSERVE_CANCELED, ErrorCodeExt};
use futures::FutureExt;
use kvproto::metapb::Region;
use pd_client::PdClient;
use raft::StateRole;
use raftstore::{
    coprocessor::{ObserveHandle, RegionInfoProvider},
    router::RaftStoreRouter,
    store::fsm::ChangeObserver,
};
use tikv_util::{info, time::Instant, warn, worker::Scheduler};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use txn_types::TimeStamp;
use yatp::task::callback::Handle as YatpHandle;

use crate::{
    annotate,
    endpoint::ObserveOp,
    errors::{Error, Result},
    event_loader::InitialDataLoader,
    future,
    metadata::{store::MetaStore, MetadataClient},
    metrics,
    observer::BackupStreamObserver,
    router::Router,
    subscription_track::SubscriptionTracer,
    try_send,
    utils::{self, CallbackWaitGroup, Work},
    Task,
};

type ScanPool = yatp::ThreadPool<yatp::task::callback::TaskCell>;

/// a request for doing initial scanning.
struct ScanCmd {
    region: Region,
    handle: ObserveHandle,
    last_checkpoint: TimeStamp,
    work: Work,
}

impl ScanCmd {
    /// execute the initial scanning via the specificated [`InitialDataLoader`].
    fn exec_by<E, R, RT>(self, init: InitialDataLoader<E, R, RT>) -> Result<()>
    where
        E: KvEngine,
        R: RegionInfoProvider + Clone + 'static,
        RT: RaftStoreRouter<E>,
    {
        let Self {
            region,
            handle,
            last_checkpoint,
            work,
        } = self;
        let begin = Instant::now_coarse();
        let region_id = region.get_id();
        let snap = init.observe_over_with_retry(&region, move || {
            ChangeObserver::from_pitr(region_id, handle.clone())
        })?;
        let stat = init.do_initial_scan(&region, last_checkpoint, snap, move || drop(work))?;
        info!("initial scanning of leader transforming finished!"; "takes" => ?begin.saturating_elapsed(), "region" => %region.get_id(), "from_ts" => %last_checkpoint);
        utils::record_cf_stat("lock", &stat.lock);
        utils::record_cf_stat("write", &stat.write);
        utils::record_cf_stat("default", &stat.data);
        Ok(())
    }
}

fn scan_executor_loop<E, R, RT>(init: InitialDataLoader<E, R, RT>, cmds: SyncReceiver<ScanCmd>)
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E>,
{
    while let Ok(cmd) = cmds.recv() {
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .dec();
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["executing"])
            .inc();
        let region_id = cmd.region.get_id();
        if let Err(err) = cmd.exec_by(init.clone()) {
            if err.error_code() != OBSERVE_CANCELED {
                err.report(format!("during initial scanning of region {}", region_id));
            }
        }
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["executing"])
            .dec();
    }
}

/// spawn the executors in the scan pool.
/// we make workers thread instead of spawn scan task directly into the pool because the [`InitialDataLoader`] isn't `Sync` hence
/// we must use it very carefully or rustc (along with tokio) would complain that we made a `!Send` future.
/// so we have moved the data loader to the synchronous context so its reference won't be shared between threads any more.
fn spawn_executors<E, R, RT>(
    init: InitialDataLoader<E, R, RT>,
    number: usize,
) -> SyncSender<ScanCmd>
where
    E: KvEngine,
    R: RegionInfoProvider + Clone + 'static,
    RT: RaftStoreRouter<E> + 'static,
{
    let (tx, rx) = crossbeam::channel::bounded(MESSAGE_BUFFER_SIZE);
    let pool = Arc::new(create_scan_pool(number));
    for _ in 0..number {
        let init = init.clone();
        let rx = rx.clone();
        let pool_handle = pool.clone();
        pool.spawn(move |_: &mut YatpHandle<'_>| {
            tikv_alloc::add_thread_memory_accessor();
            let _io_guard = file_system::WithIOType::new(file_system::IOType::Replication);
            scan_executor_loop(init, rx);
            tikv_alloc::remove_thread_memory_accessor();
            // This won't make background thread deadlock,
            // because `shutdown` won't join the handle if the target of which is current thread.
            drop(pool_handle);
        })
    }
    tx
}

/// The default channel size.
const MESSAGE_BUFFER_SIZE: usize = 4096;

/// The operator for region subscription.
/// It make a queue for operations over the `SubscriptionTracer`, generally,
/// we should only modify the `SubscriptionTracer` itself (i.e. insert records, remove records) at here.
/// So the order subscription / unsubscription won't be broken.
pub struct RegionSubscriptionManager<S, R, PDC> {
    // Note: these fields appear everywhere, maybe make them a `context` type?
    regions: R,
    meta_cli: MetadataClient<S>,
    pd_client: Arc<PDC>,
    range_router: Router,
    scheduler: Scheduler<Task>,
    observer: BackupStreamObserver,
    subs: SubscriptionTracer,

    messenger: Sender<ObserveOp>,
    scan_pool_handle: SyncSender<ScanCmd>,
    scans: Arc<CallbackWaitGroup>,
}

impl<S, R, PDC> Clone for RegionSubscriptionManager<S, R, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    PDC: PdClient + 'static,
{
    fn clone(&self) -> Self {
        Self {
            regions: self.regions.clone(),
            meta_cli: self.meta_cli.clone(),
            // We should manually call Arc::clone here or rustc complains that `PDC` isn't `Clone`.
            pd_client: Arc::clone(&self.pd_client),
            range_router: self.range_router.clone(),
            scheduler: self.scheduler.clone(),
            observer: self.observer.clone(),
            subs: self.subs.clone(),
            messenger: self.messenger.clone(),
            scan_pool_handle: self.scan_pool_handle.clone(),
            scans: CallbackWaitGroup::new(),
        }
    }
}

/// Create a yatp pool for doing initial scanning.
fn create_scan_pool(num_threads: usize) -> ScanPool {
    yatp::Builder::new("log-backup-scan")
        .max_thread_count(num_threads)
        .build_callback_pool()
}

impl<S, R, PDC> RegionSubscriptionManager<S, R, PDC>
where
    S: MetaStore + 'static,
    R: RegionInfoProvider + Clone + 'static,
    PDC: PdClient + 'static,
{
    /// create a [`RegionSubscriptionManager`].
    ///
    /// # returns
    ///
    /// a two-tuple, the first is the handle to the manager, the second is the operator loop future.
    pub fn start<E, RT>(
        initial_loader: InitialDataLoader<E, R, RT>,
        observer: BackupStreamObserver,
        meta_cli: MetadataClient<S>,
        pd_client: Arc<PDC>,
        scan_pool_size: usize,
    ) -> (Self, future![()])
    where
        E: KvEngine,
        RT: RaftStoreRouter<E> + 'static,
    {
        let (tx, rx) = channel(MESSAGE_BUFFER_SIZE);
        let scan_pool_handle = spawn_executors(initial_loader.clone(), scan_pool_size);
        let op = Self {
            regions: initial_loader.regions.clone(),
            meta_cli,
            pd_client,
            range_router: initial_loader.sink.clone(),
            scheduler: initial_loader.scheduler.clone(),
            observer,
            subs: initial_loader.tracing.clone(),
            messenger: tx,
            scan_pool_handle,
            scans: CallbackWaitGroup::new(),
        };
        let fut = op.clone().region_operator_loop(rx);
        (op, fut)
    }

    /// send an operation request to the manager.
    /// the returned future would be resolved after send is success.
    /// the opeartion would be executed asynchronously.
    pub async fn request(&self, op: ObserveOp) {
        if let Err(err) = self.messenger.send(op).await {
            annotate!(err, "BUG: region operator channel closed.")
                .report("when executing region op");
        }
    }

    /// wait initial scanning get finished.
    pub fn wait(&self, timeout: Duration) -> future![bool] {
        tokio::time::timeout(timeout, self.scans.wait()).map(|result| result.is_err())
    }

    /// the handler loop.
    async fn region_operator_loop(self, mut message_box: Receiver<ObserveOp>) {
        while let Some(op) = message_box.recv().await {
            info!("backup stream: on_modify_observe"; "op" => ?op);
            match op {
                ObserveOp::Start { region } => {
                    #[cfg(feature = "failpoints")]
                    fail::fail_point!("delay_on_start_observe");
                    self.start_observe(region).await;
                    metrics::INITIAL_SCAN_REASON
                        .with_label_values(&["leader-changed"])
                        .inc();
                    crate::observer::IN_FLIGHT_START_OBSERVE_MESSAGE.fetch_sub(1, Ordering::SeqCst);
                }
                ObserveOp::Stop { ref region } => {
                    self.subs.deregister_region_if(region, |_, _| true);
                }
                ObserveOp::Destroy { ref region } => {
                    let stopped = self.subs.deregister_region_if(region, |old, new| {
                        raftstore::store::util::compare_region_epoch(
                            old.meta.get_region_epoch(),
                            new,
                            true,
                            true,
                            false,
                        )
                        .map_err(|err| warn!("check epoch and stop failed."; "err" => %err))
                        .is_ok()
                    });
                    if stopped {
                        self.subs.destroy_stopped_region(region.get_id());
                    }
                }
                ObserveOp::RefreshResolver { ref region } => self.refresh_resolver(&region).await,
                ObserveOp::NotifyFailToStartObserve {
                    region,
                    handle,
                    err,
                } => {
                    info!("retry observe region"; "region" => %region.get_id(), "err" => %err);
                    // No need for retrying observe canceled.
                    if err.error_code() == error_code::backup_stream::OBSERVE_CANCELED {
                        return;
                    }
                    match self.retry_observe(region, handle).await {
                        Ok(()) => {}
                        Err(e) => {
                            self.fatal(
                                e,
                                format!("While retring to observe region, origin error is {}", err),
                            );
                        }
                    }
                }
                ObserveOp::ResolveRegions { callback, min_ts } => {
                    let now = Instant::now();
                    let timedout = self.wait(Duration::from_secs(30)).await;
                    if timedout {
                        warn!("waiting for initial scanning done timed out, forcing progress(with risk of data loss)!"; 
                            "take" => ?now.saturating_elapsed(), "timedout" => %timedout);
                    }
                    let rts = self.subs.resolve_with(min_ts);
                    self.subs.warn_if_gap_too_huge(rts);
                    callback(rts);
                }
            }
        }
    }

    fn fatal(&self, err: Error, message: String) {
        try_send!(self.scheduler, Task::FatalError(message, Box::new(err)));
    }

    async fn refresh_resolver(&self, region: &Region) {
        let need_refresh_all = !self.subs.try_update_region(region);

        if need_refresh_all {
            let canceled = self.subs.deregister_region_if(region, |_, _| true);
            let handle = ObserveHandle::new();
            if canceled {
                let for_task = self.find_task_by_region(region).unwrap_or_else(|| {
                    panic!(
                        "BUG: the region {:?} is register to no task but being observed",
                        region
                    )
                });
                metrics::INITIAL_SCAN_REASON
                    .with_label_values(&["region-changed"])
                    .inc();
                let r = async {
                    Result::Ok(self.observe_over_with_initial_data_from_checkpoint(
                        region,
                        self.get_last_checkpoint_of(&for_task, region).await?,
                        handle.clone(),
                    ))
                }
                .await;
                if let Err(e) = r {
                    try_send!(
                        self.scheduler,
                        Task::ModifyObserve(ObserveOp::NotifyFailToStartObserve {
                            region: region.clone(),
                            handle,
                            err: Box::new(e)
                        })
                    );
                }
            }
        }
    }

    async fn try_start_observe(&self, region: &Region, handle: ObserveHandle) -> Result<()> {
        match self.find_task_by_region(&region) {
            None => {
                warn!(
                    "the region {:?} is register to no task but being observed (start_key = {}; end_key = {}; task_stat = {:?}): maybe stale, aborting",
                    region,
                    utils::redact(&region.get_start_key()),
                    utils::redact(&region.get_end_key()),
                    self.range_router
                );
            }

            Some(for_task) => {
                let tso = self.get_last_checkpoint_of(&for_task, region).await?;
                self.observe_over_with_initial_data_from_checkpoint(&region, tso, handle.clone());
            }
        }
        Ok(())
    }

    async fn start_observe(&self, region: Region) {
        let handle = ObserveHandle::new();
        if let Err(err) = self.try_start_observe(&region, handle.clone()).await {
            try_send!(
                self.scheduler,
                Task::ModifyObserve(ObserveOp::NotifyFailToStartObserve {
                    region,
                    handle,
                    err: Box::new(err)
                })
            );
        }
    }

    async fn retry_observe(&self, region: Region, handle: ObserveHandle) -> Result<()> {
        let (tx, rx) = crossbeam::channel::bounded(1);
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
            .recv()
            .map_err(|err| annotate!(err, "BUG?: unexpected channel message dropped."))?;
        if new_region_info.is_none() {
            metrics::SKIP_RETRY
                .with_label_values(&["region-absent"])
                .inc();
            return Ok(());
        }
        let new_region_info = new_region_info.unwrap();
        if new_region_info.role != StateRole::Leader {
            metrics::SKIP_RETRY.with_label_values(&["not-leader"]).inc();
            return Ok(());
        }
        let removed = self.subs.deregister_region_if(&region, |old, _| {
            let should_remove = old.handle().id == handle.id;
            if !should_remove {
                warn!("stale retry command"; "region" => ?region, "handle" => ?handle, "old_handle" => ?old.handle());
            }
            should_remove
        });
        if !removed {
            metrics::SKIP_RETRY
                .with_label_values(&["stale-command"])
                .inc();
            return Ok(());
        }
        metrics::INITIAL_SCAN_REASON
            .with_label_values(&["retry"])
            .inc();
        self.start_observe(region).await;
        Ok(())
    }

    async fn get_last_checkpoint_of(&self, task: &str, region: &Region) -> Result<TimeStamp> {
        let meta_cli = self.meta_cli.clone();
        let cp = meta_cli.get_region_checkpoint(task, region).await?;
        info!("got region checkpoint"; "region_id" => %region.get_id(), "checkpoint" => ?cp);
        Ok(cp.ts)
    }

    fn spawn_scan(&self, cmd: ScanCmd) {
        metrics::PENDING_INITIAL_SCAN_LEN
            .with_label_values(&["queuing"])
            .inc();
        // we should not spawn initial scanning tasks to the tokio blocking pool
        // beacuse it is also used for converting sync File I/O to async. (for now!)
        // In that condition, if we blocking for some resouces(for example, the `MemoryQuota`)
        // at the block threads, we may meet some ghosty deadlock.
        let s = self.scan_pool_handle.send(cmd);
        if let Err(err) = s {
            let region_id = err.0.region.get_id();
            annotate!(err, "BUG: scan_pool closed")
                .report(format!("during initial scanning for region {}", region_id));
        }
    }

    fn observe_over_with_initial_data_from_checkpoint(
        &self,
        region: &Region,
        last_checkpoint: TimeStamp,
        handle: ObserveHandle,
    ) {
        self.subs
            .register_region(region, handle.clone(), Some(last_checkpoint));
        self.spawn_scan(ScanCmd {
            region: region.clone(),
            handle,
            last_checkpoint,
            work: self.scans.clone().work(),
        })
    }

    fn find_task_by_region(&self, r: &Region) -> Option<String> {
        self.range_router
            .find_task_by_range(&r.start_key, &r.end_key)
    }
}
