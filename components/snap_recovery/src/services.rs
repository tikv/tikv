// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    error::Error as StdError,
    sync::mpsc::{sync_channel, SyncSender},
    time::Instant,
};

use engine_rocks::RocksEngine;
use engine_traits::{CfNamesExt, CfOptionsExt, Engines, Iterable, Peekable, RaftEngine, CF_RAFT};
use futures::{
    channel::mpsc,
    executor::{ThreadPool, ThreadPoolBuilder},
    stream::{self},
    FutureExt, SinkExt, StreamExt,
};
use grpcio::{
    ClientStreamingSink, Error as gRPCError, RequestStream, RpcContext, ServerStreamingSink,
    UnarySink, WriteFlags,
};
use kvproto::{
    raft_serverpb::{PeerState, RaftApplyState, RaftLocalState, RegionLocalState, StoreIdent},
    recoverdatapb::*,
};
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

use crate::data_resolver::DataResolverManager;
// TODO: ERROR need more specific
#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument {0:?}")]
    InvalidArgument(String),

    #[error("Not Found {0:?}")]
    NotFound(String),

    #[error("Grpc Eroor {0:?}")]
    GrpcError(gRPCError),

    #[error("{0:?}")]
    Other(#[from] Box<dyn StdError + Sync + Send>),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(PartialEq, Debug, Default)]
pub struct LocalRegion {
    pub raft_local_state: RaftLocalState,
    pub raft_apply_state: RaftApplyState,
    pub region_local_state: RegionLocalState,
}

impl LocalRegion {
    fn new(
        raft_local: RaftLocalState,
        raft_apply: RaftApplyState,
        region_local: RegionLocalState,
    ) -> Self {
        LocalRegion {
            raft_local_state: raft_local,
            raft_apply_state: raft_apply,
            region_local_state: region_local,
        }
    }

    // fetch local region info into a gRPC message structure RegionMeta
    fn to_region_meta(&self) -> RegionMeta {
        let mut region_meta = RegionMeta::default();
        region_meta.region_id = self.region_local_state.get_region().id;
        region_meta.peer_id = self
            .region_local_state
            .get_region()
            .get_peers()
            .to_vec()
            .iter()
            .max_by_key(|p| p.id)
            .unwrap()
            .get_id();
        region_meta.version = self
            .region_local_state
            .get_region()
            .get_region_epoch()
            .version;
        region_meta.tombstone = self.region_local_state.state == PeerState::Tombstone;
        region_meta.start_key = self
            .region_local_state
            .get_region()
            .get_start_key()
            .to_owned();
        region_meta.end_key = self
            .region_local_state
            .get_region()
            .get_end_key()
            .to_owned();
        region_meta.last_log_term = self.raft_local_state.get_hard_state().term;
        region_meta.last_index = self.raft_local_state.last_index;

        return region_meta;
    }
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
            .after_start(move |_| {
                tikv_util::thread_group::set_properties(props.clone());
                tikv_alloc::add_thread_memory_accessor();
            })
            .before_stop(move |_| tikv_alloc::remove_thread_memory_accessor())
            .create()
            .unwrap();
        RecoveryService {
            engines,
            router,
            threads,
        }
    }

    // a new wait apply syncer share with all regions,
    // when all region reached the target index, share reference decreased to 0,
    // trigger closure to send finish info back.
    pub fn wait_apply_last(router: RaftRouter<RocksEngine, ER>, sender: SyncSender<u64>) {
        // PR https://github.com/tikv/tikv/pull/13374
        let wait_apply = SnapshotRecoveryWaitApplySyncer::new(0, sender.clone());
        // ensure recovery cmd be executed so the leader apply to last index
        router.broadcast_normal(|| {
            PeerMsg::SignificantMsg(SignificantMsg::SnapshotRecoveryWaitApply(
                wait_apply.clone(),
            ))
        });
    }

    fn set_db_options(&self, cf_name: &str) -> Result<()> {
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
        self.engines
            .kv
            .set_options_cf(cf_name, tmp_opts.as_slice())
            .unwrap();
        Ok(())
    }

    // the function is to read region meta from rocksdb and raft engine
    fn get_local_region_meta(&self) -> Vec<RegionMeta> {
        // read the local region info
        let local_regions = self.get_all_regions().unwrap();
        let region_metas = local_regions
            .iter()
            .map(|x| x.to_region_meta())
            .collect::<Vec<_>>();

        info!("region metas to report, total {}", region_metas.len());
        return region_metas;
    }

    /// Get all regions holding region meta data from raft CF in KV storage.
    pub fn get_all_regions(&self) -> Result<Vec<LocalRegion>> {
        let db = &self.engines.kv;
        let cf = CF_RAFT;
        let start_key = keys::REGION_META_MIN_KEY;
        let end_key = keys::REGION_META_MAX_KEY;
        let mut regions = Vec::with_capacity(1024);
        box_try!(db.scan(cf, start_key, end_key, false, |key, _| {
            let (id, suffix) = box_try!(keys::decode_region_meta_key(key));
            if suffix != keys::REGION_STATE_SUFFIX {
                return Ok(true);
            }
            regions.push(id);
            Ok(true)
        }));

        // TODO: shall be const as macro and check if 1024*1024 is good
        let mut region_objects = Vec::with_capacity(1024 * 1024);
        for region_id in regions {
            let region_state_key = keys::region_state_key(region_id);
            let region_state = box_try!(
                self.engines
                    .kv
                    .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            );
            // skip tombstone
            if region_state.clone().unwrap().get_state() == PeerState::Tombstone {
                continue;
            }

            let raft_state = box_try!(self.engines.raft.get_raft_state(region_id));
            let apply_state_key = keys::apply_state_key(region_id);
            let apply_state = box_try!(
                self.engines
                    .kv
                    .get_msg_cf::<RaftApplyState>(CF_RAFT, &apply_state_key)
            );

            region_objects.push(LocalRegion::new(
                raft_state.unwrap(),
                apply_state.unwrap(),
                region_state.unwrap(),
            ));
        }
        Ok(region_objects)
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
}
impl<ER: RaftEngine> RecoverData for RecoveryService<ER> {
    fn read_region_meta(
        &mut self,
        ctx: RpcContext,
        _req: ReadRegionMetaRequest,
        mut sink: ServerStreamingSink<RegionMeta>,
    ) {
        let region_meta = self.get_local_region_meta();
        let db = self.engines.kv.clone();
        for cf_name in db.cf_names() {
            self.set_db_options(cf_name).expect("set db option failure");
        }

        let task = async move {
            let mut metas = stream::iter(
                region_meta
                    .into_iter()
                    .map(|x| Ok((x, WriteFlags::default()))),
            );

            // TODO Error handling necessary
            if let Err(e) = sink.send_all(&mut metas).await {
                error!("send meta error: {:?}", e);
                return;
            }

            let _ = sink.close().await;
        };
        ctx.spawn(task);
    }

    fn recover_region(
        &mut self,
        _ctx: RpcContext,
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
                    warn!("region {} fails to campaign: {}", region_id, e);
                    continue;
                } else {
                    info!("region {} starts to campaign", region_id);
                }

                let (tx, rx) = sync_channel::<u64>(1);
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
                    let _ = rx.recv().unwrap();
                }
            }
            info!("all region state adjustments are done");

            let now = Instant::now();
            // wait apply to the last log
            let mut rx_apply = Vec::with_capacity(leaders.len());
            for &region_id in &leaders {
                let (tx, rx) = sync_channel::<u64>(1);
                let wait_apply = SnapshotRecoveryWaitApplySyncer::new(region_id, tx.clone());
                raft_router
                    .significant_send(
                        region_id,
                        SignificantMsg::SnapshotRecoveryWaitApply(wait_apply.clone()),
                    )
                    .unwrap();
                rx_apply.push(Some(rx));
            }

            // leader is campaign and be ensured as leader
            for (_rid, rx) in leaders.iter().zip(rx_apply) {
                if let Some(rx) = rx {
                    let _ = rx.recv().unwrap();
                }
            }
            info!(
                "all leader region apply to last log takes {}",
                now.elapsed().as_secs()
            );
            let mut resp = RecoverRegionResponse::default();
            resp.set_store_id(store_id.unwrap());
            let _ = sink.success(resp).await;
        };

        self.threads.spawn_ok(task);
    }

    fn wait_apply(
        &mut self,
        _ctx: RpcContext,
        _req: WaitApplyRequest,
        sink: UnarySink<WaitApplyResponse>,
    ) {
        let router = self.router.clone();
        info!("wait_apply start");
        let task = async move {
            let now = Instant::now();
            let (tx, rx) = sync_channel::<u64>(1);
            RecoveryService::wait_apply_last(router, tx.clone());
            let _ = rx.recv().unwrap();
            info!(
                "all region apply to last log takes {}",
                now.elapsed().as_secs()
            );
            let resp = WaitApplyResponse::default();
            let _ = sink.success(resp).await;
        };

        self.threads.spawn_ok(task);
    }

    fn resolve_kv_data(
        &mut self,
        _ctx: RpcContext,
        req: ResolveKvDataRequest,
        mut sink: ServerStreamingSink<ResolveKvDataResponse>,
    ) {
        // implement a resolve/delete data funciton
        let resolved_ts = req.get_resolved_ts();
        let (tx, rx) = mpsc::unbounded();
        let resolver = DataResolverManager::new(self.engines.kv.clone(), tx, resolved_ts.into());
        info!("start to resolve kv data");
        resolver.start();

        let store_id = self.get_store_id();
        let send_task = async move {
            let id = store_id.expect("failed to get store id").clone();
            let mut s = rx.map(|mut resp| {
                // TODO: a metric need here
                resp.set_store_id(id);
                Ok((resp, WriteFlags::default()))
            });
            sink.send_all(&mut s).await?;
            sink.close().await?;
            Ok(())
        }
        .map(|res: std::result::Result<(), gRPCError>| match res {
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
