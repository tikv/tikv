// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use collections::HashSet;
use crossbeam::channel::TrySendError;
use engine_rocks::{RocksEngine, RocksSnapshot};
use engine_traits::{KvEngine, ALL_CFS, CF_DEFAULT};
use futures::future::FutureExt;
use kvproto::{
    kvrpcpb::{Context, ExtraOp as TxnExtraOp},
    metapb::Region,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, Response},
    raft_serverpb::RaftMessage,
};
use raftstore::{
    router::{LocalReadRouter, RaftStoreRouter},
    store::{
        cmd_resp, Callback, CasualMessage, CasualRouter, PeerMsg, ProposalRouter, RaftCmdExtraOpts,
        RaftCommand, ReadResponse, RegionSnapshot, SignificantMsg, SignificantRouter, StoreMsg,
        StoreRouter, WriteResponse,
    },
    Result,
};
use tempfile::{Builder, TempDir};
use tikv::{
    server::raftkv::{CmdRes, RaftKv},
    storage::{
        kv::{Callback as EngineCallback, Modify, SnapContext, WriteData},
        Engine,
    },
};
use tikv_util::{store::new_peer, time::ThreadReadId};
use txn_types::Key;

use crate::test;

#[derive(Clone)]
struct SyncBenchRouter {
    db: RocksEngine,
    region: Region,
}

impl SyncBenchRouter {
    fn new(region: Region, db: RocksEngine) -> SyncBenchRouter {
        SyncBenchRouter { db, region }
    }
}

impl SyncBenchRouter {
    fn invoke(&self, cmd: RaftCommand<RocksSnapshot>) {
        let mut response = RaftCmdResponse::default();
        cmd_resp::bind_term(&mut response, 1);
        match cmd.callback {
            Callback::Read { cb, .. } => {
                let snapshot = self.db.snapshot(None);
                let region = Arc::new(self.region.to_owned());
                cb(ReadResponse {
                    response,
                    snapshot: Some(RegionSnapshot::from_snapshot(Arc::new(snapshot), region)),
                    txn_extra_op: TxnExtraOp::Noop,
                })
            }
            Callback::Write { cb, .. } => {
                let mut resp = Response::default();
                let cmd_type = cmd.request.get_requests()[0].get_cmd_type();
                resp.set_cmd_type(cmd_type);
                response.mut_responses().push(resp);
                cb(WriteResponse { response })
            }
            _ => unreachable!(),
        }
    }
}

impl CasualRouter<RocksEngine> for SyncBenchRouter {
    fn send(&self, _: u64, _: CasualMessage<RocksEngine>) -> Result<()> {
        Ok(())
    }
}

impl SignificantRouter<RocksEngine> for SyncBenchRouter {
    fn significant_send(&self, _: u64, _: SignificantMsg<RocksSnapshot>) -> Result<()> {
        Ok(())
    }
}

impl ProposalRouter<RocksSnapshot> for SyncBenchRouter {
    fn send(
        &self,
        _: RaftCommand<RocksSnapshot>,
    ) -> std::result::Result<(), TrySendError<RaftCommand<RocksSnapshot>>> {
        Ok(())
    }
}
impl StoreRouter<RocksEngine> for SyncBenchRouter {
    fn send(&self, _: StoreMsg<RocksEngine>) -> Result<()> {
        Ok(())
    }
}

impl RaftStoreRouter<RocksEngine> for SyncBenchRouter {
    /// Sends RaftMessage to local store.
    fn send_raft_msg(&self, _: RaftMessage) -> Result<()> {
        Ok(())
    }

    fn broadcast_normal(&self, _: impl FnMut() -> PeerMsg<RocksEngine>) {}

    fn send_command(
        &self,
        req: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
        extra_opts: RaftCmdExtraOpts,
    ) -> Result<()> {
        self.invoke(RaftCommand::new_ext(req, cb, extra_opts));
        Ok(())
    }
}

impl LocalReadRouter<RocksEngine> for SyncBenchRouter {
    fn read(
        &mut self,
        _: Option<ThreadReadId>,
        req: RaftCmdRequest,
        cb: Callback<RocksSnapshot>,
    ) -> Result<()> {
        self.send_command(req, cb, RaftCmdExtraOpts::default())
    }

    fn release_snapshot_cache(&mut self) {}
}

fn new_engine() -> (TempDir, RocksEngine) {
    let dir = Builder::new().prefix("bench_rafkv").tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = engine_rocks::util::new_engine(&path, ALL_CFS).unwrap();
    (dir, db)
}

// The lower limit of time a async_snapshot may take.
#[bench]
fn bench_async_snapshots_noop(b: &mut test::Bencher) {
    let (_dir, db) = new_engine();
    let snapshot = db.snapshot(None);
    let resp = ReadResponse {
        response: RaftCmdResponse::default(),
        snapshot: Some(RegionSnapshot::from_snapshot(
            Arc::new(snapshot),
            Arc::new(Region::default()),
        )),
        txn_extra_op: TxnExtraOp::Noop,
    };

    b.iter(|| {
        let cb1: EngineCallback<RegionSnapshot<RocksSnapshot>> = Box::new(move |res| {
            res.unwrap();
        });
        let cb2: EngineCallback<CmdRes<RocksSnapshot>> = Box::new(move |res| {
            if let Ok(CmdRes::Snap(snap)) = res {
                cb1(Ok(snap));
            }
        });
        let cb: Callback<RocksSnapshot> =
            Callback::read(Box::new(move |resp: ReadResponse<RocksSnapshot>| {
                let res = CmdRes::Snap(resp.snapshot.unwrap());
                cb2(Ok(res));
            }));
        cb.invoke_read(resp.clone());
    });
}

#[bench]
fn bench_async_snapshot(b: &mut test::Bencher) {
    let leader = new_peer(2, 3);
    let mut region = Region::default();
    region.set_id(1);
    region.set_start_key(vec![]);
    region.set_end_key(vec![]);
    region.mut_peers().push(leader.clone());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let mut kv = RaftKv::new(
        SyncBenchRouter::new(region.clone(), db.clone()),
        db,
        Arc::new(RwLock::new(HashSet::default())),
    );

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);
    b.iter(|| {
        let snap_ctx = SnapContext {
            pb_ctx: &ctx,
            ..Default::default()
        };
        let f = kv.async_snapshot(snap_ctx);
        let res = f.map(|res| {
            let _ = test::black_box(res);
        });
        let _ = test::black_box(res);
    });
}

#[bench]
fn bench_async_write(b: &mut test::Bencher) {
    let leader = new_peer(2, 3);
    let mut region = Region::default();
    region.set_id(1);
    region.set_start_key(vec![]);
    region.set_end_key(vec![]);
    region.mut_peers().push(leader.clone());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let kv = RaftKv::new(
        SyncBenchRouter::new(region.clone(), db.clone()),
        db,
        Arc::new(RwLock::new(HashSet::default())),
    );

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);
    b.iter(|| {
        let f = tikv_kv::write(
            &kv,
            &ctx,
            WriteData::from_modifies(vec![Modify::Delete(
                CF_DEFAULT,
                Key::from_encoded(b"fooo".to_vec()),
            )]),
            None,
        );
        let res = f.map(|res| {
            let _ = test::black_box(res);
        });
        let _ = test::black_box(res);
    });
}
