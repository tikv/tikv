// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    result,
    sync::mpsc::{sync_channel, SyncSender},
    thread::Builder,
    time::Instant,
};

use engine_rocks::{
    raw::{CompactOptions, DBBottommostLevelCompaction},
    util::get_cf_handle,
    RocksEngine,
};
use engine_traits::{CfNamesExt, CfOptionsExt, Engines, Peekable, RaftEngine};
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
        msg::{Callback, CasualMessage, PeerMsg, SignificantMsg},
        transport::SignificantRouter,
        SnapshotRecoveryWaitApplySyncer,
    },
};
use thiserror::Error;
use tikv_util::sys::thread::{StdThreadBuildWrapper, ThreadBuildWrapper};

use crate::{data_resolver::DataResolverManager, region_meta_collector::RegionMetaCollector};

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
    pub fn wait_apply_last(router: RaftRouter<RocksEngine, ER>, sender: SyncSender<u64>) {
        let wait_apply = SnapshotRecoveryWaitApplySyncer::new(0, sender);
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::SnapshotRecoveryWaitApply(
                wait_apply.clone(),
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
                info!("collect region meta done");
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
                    leaders.push(req.region_id);
                }
            }

            let mut rxs = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                if let Err(e) = raft_router.send_casual_msg(region_id, CasualMessage::Campaign) {
                    // TODO: retry may necessay
                    warn!("region fails to campaign: ";
                    "region_id" => region_id, 
                    "err" => ?e);
                    continue;
                } else {
                    info!("region starts to campaign";
                    "region_id" => region_id);
                }

                let (tx, rx) = sync_channel(1);
                let callback = Callback::read(Box::new(move |_| {
                    if tx.send(1).is_err() {
                        error!("response failed"; "region_id" => region_id);
                    }
                }));
                if let Err(e) = raft_router
                    .significant_send(region_id, SignificantMsg::LeaderCallback(callback))
                {
                    warn!("LeaderCallback failed"; "err" => ?e, "region_id" => region_id);
                }
                rxs.push(Some(rx));
            }

            // leader is campaign and be ensured as leader
            for (_rid, rx) in leaders.iter().zip(rxs) {
                if let Some(rx) = rx {
                    match rx.recv() {
                        Ok(_id) => {
                            info!("leader is assigned for region");
                        }
                        Err(e) => {
                            error!("check leader failed"; "error" => ?e);
                        }
                    }
                }
            }

            info!("all region leader assigned done");

            let now = Instant::now();
            // wait apply to the last log
            let mut rx_apply = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                let (tx, rx) = sync_channel(1);
                let wait_apply = SnapshotRecoveryWaitApplySyncer::new(region_id, tx.clone());
                if let Err(e) = raft_router.significant_send(
                    region_id,
                    SignificantMsg::SnapshotRecoveryWaitApply(wait_apply.clone()),
                ) {
                    error!(
                        "failed to send wait apply";
                        "region_id" => region_id,
                        "err" => ?e,
                    );
                }
                rx_apply.push(Some(rx));
            }

            // leader apply to last log
            for (_rid, rx) in leaders.iter().zip(rx_apply) {
                if let Some(rx) = rx {
                    match rx.recv() {
                        Ok(region_id) => {
                            info!("leader apply to last log"; "error" => region_id);
                        }
                        Err(e) => {
                            error!("leader failed to apply to last log"; "error" => ?e);
                        }
                    }
                }
            }

            info!(
                "all region leader apply to last log";
                "spent_time" => now.elapsed().as_secs(),
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
            let (tx, rx) = sync_channel(1);
            RecoveryService::wait_apply_last(router, tx.clone());
            match rx.recv() {
                Ok(id) => {
                    info!("follower apply to last log"; "error" => id);
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
