// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{error::Error as StdError, result, thread::Builder, time::Instant};

use engine_rocks::{
    raw::{CompactOptions, DBBottommostLevelCompaction},
    util::get_cf_handle,
    RocksEngine,
};
use engine_traits::{CfNamesExt, CfOptionsExt, Engines, KvEngine, Peekable, RaftEngine};
use futures::{
    channel::mpsc,
    executor::{ThreadPool, ThreadPoolBuilder},
    FutureExt, SinkExt, StreamExt,
};
use grpcio::{
    ClientStreamingSink, RequestStream, RpcContext, ServerStreamingSink, UnarySink, WriteFlags,
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
pub struct RecoveryService<ER: RaftEngine> {
    engines: Engines<RocksEngine, ER>,
    router: RaftRouter<RocksEngine, ER>,
    threads: ThreadPool,
}

impl<ER: RaftEngine> RecoveryService<ER> {
    /// Constructs a new `Service` with `Engines`, a `RaftStoreRouter` and a
    /// `thread pool`.
    pub fn new(
        engines: Engines<RocksEngine, ER>,
        router: RaftRouter<RocksEngine, ER>,
    ) -> RecoveryService<ER> {
        let props = tikv_util::thread_group::current_properties();
        let threads = ThreadPoolBuilder::new()
            .pool_size(4)
            .name_prefix("recovery-service")
            .after_start_wrapper(move || {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
            })
            .before_stop_wrapper(|| tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();

        // config rocksdb l0 to optimize the restore
        // also for massive data applied during the restore, it easy to reach the write
        // stop
        let db = engines.kv.clone();
        for cf_name in db.cf_names() {
            Self::set_db_options(cf_name, db.clone()).expect("set db option failure");
        }

        RecoveryService {
            engines,
            router,
            threads,
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

    // a new wait apply syncer share with all regions,
    // when all region reached the target index, share reference decreased to 0,
    // trigger closure to send finish info back.
    pub fn wait_apply_last<EK: KvEngine>(router: RaftRouter<EK, ER>, sender: Sender<SyncReport>) {
        let wait_apply = SnapshotBrWaitApplySyncer::new(0, sender);
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::SnapshotBrWaitApply(
                SnapshotBrWaitApplyRequest::relaxed(wait_apply.clone()),
            ))
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
                tikv_alloc::add_thread_memory_accessor();
                let db = kv_db.as_inner();
                let handle = get_cf_handle(db, cf.as_str()).unwrap();
                let mut compact_opts = CompactOptions::new();
                compact_opts.set_max_subcompactions(64);
                compact_opts.set_exclusive_manual_compaction(false);
                compact_opts.set_bottommost_level_compaction(DBBottommostLevelCompaction::Skip);
                db.compact_range_cf_opt(handle, &compact_opts, None, None);
                tikv_alloc::remove_thread_memory_accessor();

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

impl<ER: RaftEngine> RecoverData for RecoveryService<ER> {
    // 1. br start to ready region meta
    fn read_region_meta(
        &mut self,
        _ctx: RpcContext<'_>,
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

        self.threads.spawn_ok(send_task);
    }

    // 2. br start to recover region
    // assign region leader and wait leader apply to last log
    fn recover_region(
        &mut self,
        _ctx: RpcContext<'_>,
        mut stream: RequestStream<RecoverRegionRequest>,
        sink: ClientStreamingSink<RecoverRegionResponse>,
    ) {
        let raft_router = self.router.clone();
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

            let mut lk = LeaderKeeper::new(raft_router.clone(), leaders.clone());
            // We must use the tokio runtime here because there isn't a `block_in_place`
            // like thing in the futures executor. It simply panics when block
            // on the block_on context.
            // It is also impossible to directly `await` here, because that will make
            // borrowing to the raft router crosses the await point.
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("failed to build temporary tokio runtime.")
                .block_on(lk.elect_and_wait_all_ready());
            info!("all region leader assigned done"; "count" => %leaders.len());

            let now = Instant::now();
            // wait apply to the last log
            let mut rx_apply = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                let (tx, rx) = oneshot::channel();
                REGION_EVENT_COUNTER.start_wait_leader_apply.inc();
                let wait_apply = SnapshotBrWaitApplySyncer::new(region_id, tx);
                if let Err(e) = raft_router.significant_send(
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

            let _ = sink.success(resp).await;
        };

        self.threads.spawn_ok(task);
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
        let resolver = DataResolverManager::new(self.engines.kv.clone(), tx, resolved_ts.into());
        info!("start to resolve kv data");
        resolver.start();
        let db = self.engines.kv.clone();
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
