// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    fmt::Display,
    future::Future,
    result,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    thread::Builder,
    time::Instant,
};

use engine_rocks::{
    raw::{CompactOptions, DBBottommostLevelCompaction},
    util::get_cf_handle,
    RocksEngine,
};
use engine_traits::{CfNamesExt, CfOptionsExt, Engines, KvEngine, RaftEngine};
use futures::{
    channel::mpsc,
    executor::{ThreadPool, ThreadPoolBuilder},
    stream::{AbortHandle, Aborted},
    FutureExt, SinkExt, StreamExt,
};
use grpcio::{
    ClientStreamingSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink,
    UnarySink, WriteFlags,
};
use kvproto::{raft_serverpb::StoreIdent, recoverdatapb::*};
use raftstore::{
    router::RaftStoreRouter,
    store::{
        fsm::RaftRouter,
        msg::{PeerMsg, SignificantMsg},
        snapshot_backup::{SnapshotBrWaitApplyRequest, SyncReport},
        transport::SignificantRouter,
        SnapshotBrWaitApplySyncer,
    },
};
use thiserror::Error;
use tikv_util::sys::thread::{StdThreadBuildWrapper, ThreadBuildWrapper};
use tokio::sync::oneshot::{self, Sender};

use crate::{
    data_resolver::DataResolverManager,
    leader_keeper::LeaderKeeper,
    metrics::{CURRENT_WAIT_APPLY_LEADER, REGION_EVENT_COUNTER},
    region_meta_collector::RegionMetaCollector,
};

pub type Result<T> = result::Result<T, Error>;

#[allow(dead_code)]
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("{0:?}")]
    Grpc(#[from] grpcio::Error),

    #[error("Engine {0:?}")]
    Engine(#[from] engine_traits::Error),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

/// Service handles the recovery messages from backup restore.
#[derive(Clone)]
pub struct RecoveryService<EK, ER>
where
    EK: KvEngine<DiskEngine = RocksEngine>,
    ER: RaftEngine,
{
    engines: Engines<EK, ER>,
    router: RaftRouter<EK, ER>,
    threads: ThreadPool,

    /// The handle to last call of recover region RPC.
    ///
    /// We need to make sure the execution of keeping leader exits before next
    /// `RecoverRegion` rpc gets in. Or the previous call may stuck at keep
    /// leader forever, once the second caller request the leader to be at
    /// another store.
    // NOTE: Perhaps it would be better to abort the procedure as soon as the client
    // stream has been closed, but yet it seems there isn't such hook like
    // `on_client_go` for us, and the current implementation only start
    // work AFTER the client closes their sender part(!)
    last_recovery_region_rpc: Arc<Mutex<Option<RecoverRegionState>>>,
}

struct RecoverRegionState {
    start_at: Instant,
    finished: Arc<AtomicBool>,
    abort: AbortHandle,
}

impl RecoverRegionState {
    /// Create the state by wrapping a execution of recover region.
    fn wrap_task<F: Future<Output = T>, T>(
        task: F,
    ) -> (Self, impl Future<Output = std::result::Result<T, Aborted>>) {
        let finished = Arc::new(AtomicBool::new(false));
        let (cancelable_task, abort) = futures::future::abortable(task);
        let state = Self {
            start_at: Instant::now(),
            finished: Arc::clone(&finished),
            abort,
        };
        (state, async move {
            let res = cancelable_task.await;
            finished.store(true, Ordering::SeqCst);
            res
        })
    }
}

impl<EK, ER> RecoveryService<EK, ER>
where
    EK: KvEngine<DiskEngine = RocksEngine>,
    ER: RaftEngine,
{
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a
    /// `thread pool`.
    pub fn new(engines: Engines<EK, ER>, router: RaftRouter<EK, ER>) -> RecoveryService<EK, ER> {
        let props = tikv_util::thread_group::current_properties();
        let threads = ThreadPoolBuilder::new()
            .pool_size(4)
            .name_prefix("recovery-service")
            .with_sys_and_custom_hooks(
                move || {
                    tikv_util::thread_group::set_properties(props.clone());
                },
                || {},
            )
            .create()
            .unwrap();

        // config rocksdb l0 to optimize the restore
        // also for massive data applied during the restore, it easy to reach the write
        // stop
        let db: &RocksEngine = engines.kv.get_disk_engine();
        for cf_name in db.cf_names() {
            Self::set_db_options(cf_name, db.clone()).expect("set db option failure");
        }

        RecoveryService {
            engines,
            router,
            threads,
            last_recovery_region_rpc: Arc::default(),
        }
    }

    pub fn set_db_options(cf_name: &str, engine: RocksEngine) -> Result<()> {
        let level0_stop_writes_trigger: u32 = 1 << 30;
        let level0_slowdown_writes_trigger: u32 = 1 << 30;
        let opts = [
            (
                "level0_stop_writes_trigger".to_owned(),
                level0_stop_writes_trigger.to_string(),
            ),
            (
                "level0_slowdown_writes_trigger".to_owned(),
                level0_slowdown_writes_trigger.to_string(),
            ),
        ];

        let tmp_opts: Vec<_> = opts.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        engine.set_options_cf(cf_name, tmp_opts.as_slice()).unwrap();
        Ok(())
    }

    // return cluster id and store id for registry the store to PD
    fn get_store_id(&self) -> Result<u64> {
        let res = self
            .engines
            .kv
            .get_msg::<StoreIdent>(keys::STORE_IDENT_KEY)
            .unwrap();
        if res.is_none() {
            return Ok(0);
        }

        let ident = res.unwrap();
        let store_id = ident.get_store_id();
        if store_id == 0 {
            error!("invalid store to report");
        }
        Ok(store_id)
    }

    fn abort_last_recover_region(&self, place: impl Display) {
        let mut last_state_lock = self.last_recovery_region_rpc.lock().unwrap();
        Self::abort_last_recover_region_of(place, &mut last_state_lock)
    }

    fn replace_last_recover_region(&self, place: impl Display, new_state: RecoverRegionState) {
        let mut last_state_lock = self.last_recovery_region_rpc.lock().unwrap();
        Self::abort_last_recover_region_of(place, &mut last_state_lock);
        *last_state_lock = Some(new_state);
    }

    fn abort_last_recover_region_of(
        place: impl Display,
        last_state_lock: &mut Option<RecoverRegionState>,
    ) {
        if let Some(last_state) = last_state_lock.take() {
            info!("Another task enter, checking last task.";
                "finished" => ?last_state.finished,
                "start_before" => ?last_state.start_at.elapsed(),
                "abort_by" => %place,
            );
            if !last_state.finished.load(Ordering::SeqCst) {
                last_state.abort.abort();
                warn!("Last task not finished, aborting it.");
            }
        }
    }

    // a new wait apply syncer share with all regions,
    // when all region reached the target index, share reference decreased to 0,
    // trigger closure to send finish info back.
    pub fn wait_apply_last(router: RaftRouter<EK, ER>, sender: Sender<SyncReport>) {
        let wait_apply = SnapshotBrWaitApplySyncer::new(0, sender);
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(Box::new(SignificantMsg::SnapshotBrWaitApply(
                SnapshotBrWaitApplyRequest::relaxed(wait_apply.clone()),
            )))
        });
    }
}

/// This may a temp solution, in future, we may move forward to FlashBack
/// delete data Compact the cf[start..end) in the db.
/// purpose of it to resolve compaction filter gc after restore cluster
fn compact(engine: RocksEngine) -> Result<()> {
    let mut handles = Vec::new();
    for cf_name in engine.cf_names() {
        let cf = cf_name.to_owned().clone();
        let kv_db = engine.clone();
        let h = Builder::new()
            .name(format!("compact-{}", cf))
            .spawn_wrapper(move || {
                info!("recovery starts manual compact"; "cf" => cf.clone());

                let db = kv_db.as_inner();
                let handle = get_cf_handle(db, cf.as_str()).unwrap();
                let mut compact_opts = CompactOptions::new();
                compact_opts.set_max_subcompactions(64);
                compact_opts.set_exclusive_manual_compaction(false);
                compact_opts.set_bottommost_level_compaction(DBBottommostLevelCompaction::Skip);
                db.compact_range_cf_opt(handle, &compact_opts, None, None);

                info!("recovery finishes manual compact"; "cf" => cf);
            })
            .expect("failed to spawn compaction thread");
        handles.push(h);
    }
    for h in handles {
        h.join()
            .unwrap_or_else(|e| error!("thread handle join error"; "error" => ?e));
    }
    Ok(())
}

impl<EK, ER> RecoverData for RecoveryService<EK, ER>
where
    EK: KvEngine<DiskEngine = RocksEngine>,
    ER: RaftEngine,
{
    // 1. br start to ready region meta
    fn read_region_meta(
        &mut self,
        ctx: RpcContext<'_>,
        _req: ReadRegionMetaRequest,
        mut sink: ServerStreamingSink<RegionMeta>,
    ) {
        let (tx, rx) = mpsc::unbounded();
        // tx only clone once within RegionMetaCollector, so that it drop automatically
        // when work thread done
        let meta_collector = RegionMetaCollector::new(self.engines.clone(), tx);
        info!("start to collect region meta");
        meta_collector.start_report();
        let send_task = async move {
            let mut s = rx.map(|resp| Ok((resp, WriteFlags::default())));
            sink.send_all(&mut s).await?;
            sink.close().await?;
            Ok(())
        }
        .map(|res: Result<()>| match res {
            Ok(_) => {
                debug!("collect region meta done");
            }
            Err(e) => {
                error!("rcollect region meta failure"; "error" => ?e);
            }
        });

        // Hacking: Sometimes, the client may omit the RPC call to `recover_region` if
        // no leader should be register to some (unfortunate) store. So we abort
        // last recover region here too, anyway this RPC implies a consequent
        // `recover_region` for now.
        self.abort_last_recover_region(format_args!("read_region_meta by {}", ctx.peer()));
        self.threads.spawn_ok(send_task);
    }

    // 2. br start to recover region
    // assign region leader and wait leader apply to last log
    fn recover_region(
        &mut self,
        ctx: RpcContext<'_>,
        mut stream: RequestStream<RecoverRegionRequest>,
        sink: ClientStreamingSink<RecoverRegionResponse>,
    ) {
        let mut raft_router = Mutex::new(self.router.clone());
        let store_id = self.get_store_id();
        info!("start to recover the region");
        let task = async move {
            let mut leaders = Vec::new();
            while let Some(req) = stream.next().await {
                let req = req.map_err(|e| eprintln!("rpc recv fail: {}", e)).unwrap();
                if req.as_leader {
                    REGION_EVENT_COUNTER.promote_to_leader.inc();
                    leaders.push(req.region_id);
                } else {
                    REGION_EVENT_COUNTER.keep_follower.inc();
                }
            }

            let mut lk = LeaderKeeper::new(&raft_router, leaders.clone());
            // We must use the tokio runtime here because there isn't a `block_in_place`
            // like thing in the futures executor. It simply panics when block
            // on the block_on context.
            // It is also impossible to directly `await` here, because that will make
            // borrowing to the raft router crosses the await point.
            lk.elect_and_wait_all_ready().await;
            info!("all region leader assigned done"; "count" => %leaders.len());
            drop(lk);

            let now = Instant::now();
            // wait apply to the last log
            let mut rx_apply = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                let (tx, rx) = oneshot::channel();
                REGION_EVENT_COUNTER.start_wait_leader_apply.inc();
                let wait_apply = SnapshotBrWaitApplySyncer::new(region_id, tx);
                if let Err(e) = raft_router.get_mut().unwrap().significant_send(
                    region_id,
                    SignificantMsg::SnapshotBrWaitApply(SnapshotBrWaitApplyRequest::relaxed(
                        wait_apply.clone(),
                    )),
                ) {
                    error!(
                        "failed to send wait apply";
                        "region_id" => region_id,
                        "err" => ?e,
                    );
                }
                rx_apply.push(rx);
            }

            // leader apply to last log
            for (rid, rx) in leaders.iter().zip(rx_apply) {
                CURRENT_WAIT_APPLY_LEADER.set(*rid as _);
                match rx.await {
                    Ok(_) => {
                        debug!("leader apply to last log"; "region_id" => rid);
                    }
                    Err(e) => {
                        error!("leader failed to apply to last log"; "error" => ?e);
                    }
                }
                REGION_EVENT_COUNTER.finish_wait_leader_apply.inc();
            }
            CURRENT_WAIT_APPLY_LEADER.set(0);

            info!(
                "all region leader apply to last log";
                "spent_time" => now.elapsed().as_secs(), "count" => %leaders.len(),
            );

            let mut resp = RecoverRegionResponse::default();
            match store_id {
                Ok(id) => resp.set_store_id(id),
                Err(e) => error!("failed to get store id"; "error" => ?e),
            };

            resp
        };

        let (state, task) = RecoverRegionState::wrap_task(task);
        self.replace_last_recover_region(format!("recover_region by {}", ctx.peer()), state);
        self.threads.spawn_ok(async move {
            let res = match task.await {
                Ok(resp) => sink.success(resp),
                Err(Aborted) => sink.fail(RpcStatus::new(RpcStatusCode::ABORTED)),
            };
            if let Err(err) = res.await {
                warn!("failed to response recover region rpc"; "err" => %err);
            }
        });
    }

    // 3. ensure all region peer/follower apply to last
    fn wait_apply(
        &mut self,
        _ctx: RpcContext<'_>,
        _req: WaitApplyRequest,
        sink: UnarySink<WaitApplyResponse>,
    ) {
        let router = self.router.clone();
        info!("wait_apply start");
        let task = async move {
            let now = Instant::now();
            let (tx, rx) = oneshot::channel();
            RecoveryService::wait_apply_last(router, tx);
            match rx.await {
                Ok(id) => {
                    info!("follower apply to last log"; "report" => ?id);
                }
                Err(e) => {
                    error!("follower failed to apply to last log"; "error" => ?e);
                }
            }
            info!(
                "all region apply to last log";
                "spent_time" => now.elapsed().as_secs(),
            );
            let resp = WaitApplyResponse::default();
            let _ = sink.success(resp).await;
        };

        self.threads.spawn_ok(task);
    }

    // 4.resolve kv data to a backup resolved-tss
    fn resolve_kv_data(
        &mut self,
        _ctx: RpcContext<'_>,
        req: ResolveKvDataRequest,
        mut sink: ServerStreamingSink<ResolveKvDataResponse>,
    ) {
        // implement a resolve/delete data funciton
        let resolved_ts = req.get_resolved_ts();
        let (tx, rx) = mpsc::unbounded();
        let resolver = DataResolverManager::new(
            self.engines.kv.get_disk_engine().clone(),
            tx,
            resolved_ts.into(),
        );
        info!("start to resolve kv data");
        resolver.start();
        let db = self.engines.kv.get_disk_engine().clone();
        let store_id = self.get_store_id();
        let send_task = async move {
            let id = store_id?;
            let mut s = rx.map(|mut resp| {
                // TODO: a metric need here
                resp.set_store_id(id);
                Ok((resp, WriteFlags::default()))
            });
            sink.send_all(&mut s).await?;
            compact(db.clone())?;
            sink.close().await?;
            Ok(())
        }
        .map(|res: Result<()>| match res {
            Ok(_) => {
                info!("resolve kv data done");
            }
            Err(e) => {
                error!("resolve kv data error"; "error" => ?e);
            }
        });

        self.threads.spawn_ok(send_task);
    }
}

#[cfg(test)]
mod test {
    use std::{sync::atomic::Ordering, time::Duration};

    use futures::never::Never;

    use super::RecoverRegionState;

    #[test]
    fn test_state() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let (state, task) = RecoverRegionState::wrap_task(futures::future::pending::<Never>());
        let hnd = rt.spawn(task);
        state.abort.abort();
        rt.block_on(async { tokio::time::timeout(Duration::from_secs(10), hnd).await })
            .unwrap()
            .unwrap()
            .unwrap_err();

        let (state, task) = RecoverRegionState::wrap_task(futures::future::ready(42));
        assert_eq!(state.finished.load(Ordering::SeqCst), false);
        assert_eq!(rt.block_on(task), Ok(42));
        assert_eq!(state.finished.load(Ordering::SeqCst), true);
    }
}
