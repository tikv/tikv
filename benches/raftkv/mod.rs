// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use rocksdb::DB;
use tempdir::TempDir;
use test;

use kvproto::kvrpcpb::Context;
use kvproto::metapb::Region;
use kvproto::raft_cmdpb::{RaftCmdResponse, Response};

use tikv::raftstore::store::{
    cmd_resp, engine, util, Callback, Msg, ReadResponse, RegionSnapshot, SignificantMsg,
    WriteResponse,
};
use tikv::raftstore::Result;
use tikv::server::transport::RaftStoreRouter;
use tikv::storage::engine::raftkv::CmdRes;
use tikv::storage::engine::{
    Callback as EngineCallback, CbContext, Modify, Result as EngineResult,
};
use tikv::storage::types::Key;
use tikv::storage::{Engine, RaftKv, ALL_CFS, CF_DEFAULT};
use tikv::util::rocksdb;

#[derive(Clone)]
struct SyncBenchRouter {
    db: Arc<DB>,
    region: Region,
}

impl SyncBenchRouter {
    fn new(region: Region, db: Arc<DB>) -> SyncBenchRouter {
        SyncBenchRouter { db, region }
    }
}

impl SyncBenchRouter {
    fn invoke(&self, msg: Msg) {
        let mut response = RaftCmdResponse::new();
        cmd_resp::bind_term(&mut response, 1);
        if let Msg::RaftCmd {
            request, callback, ..
        } = msg
        {
            match callback {
                Callback::Read(cb) => {
                    let snapshot = engine::Snapshot::new(Arc::clone(&self.db));
                    let region = self.region.to_owned();
                    cb(ReadResponse {
                        response,
                        snapshot: Some(RegionSnapshot::from_snapshot(snapshot.into_sync(), region)),
                    })
                }
                Callback::Write(cb) => {
                    let mut resp = Response::new();
                    let cmd_type = request.get_requests()[0].get_cmd_type();
                    resp.set_cmd_type(cmd_type);
                    response.mut_responses().push(resp);
                    cb(WriteResponse { response })
                }
                _ => unreachable!(),
            }
        }
    }
}

impl RaftStoreRouter for SyncBenchRouter {
    fn send(&self, msg: Msg) -> Result<()> {
        self.invoke(msg);
        Ok(())
    }

    fn try_send(&self, msg: Msg) -> Result<()> {
        self.invoke(msg);
        Ok(())
    }

    fn significant_send(&self, _: SignificantMsg) -> Result<()> {
        Ok(())
    }
}

fn new_engine() -> (TempDir, Arc<DB>) {
    let dir = TempDir::new("bench_rafkv").unwrap();
    let path = dir.path().to_str().unwrap().to_string();
    let db = rocksdb::new_engine(&path, ALL_CFS, None).unwrap();
    (dir, Arc::new(db))
}

// The lower limit of time a async_snapshot may take.
#[bench]
fn bench_async_snapshots_noop(b: &mut test::Bencher) {
    let (_dir, db) = new_engine();
    let snapshot = engine::Snapshot::new(Arc::clone(&db));
    let resp = ReadResponse {
        response: RaftCmdResponse::new(),
        snapshot: Some(RegionSnapshot::from_snapshot(
            snapshot.into_sync(),
            Region::new(),
        )),
    };

    b.iter(|| {
        let cb1: EngineCallback<RegionSnapshot> =
            Box::new(move |(_, res): (CbContext, EngineResult<RegionSnapshot>)| {
                assert!(res.is_ok());
            });
        let cb2: EngineCallback<CmdRes> =
            Box::new(move |(ctx, res): (CbContext, EngineResult<CmdRes>)| {
                if let Ok(CmdRes::Snap(snap)) = res {
                    cb1((ctx, Ok(snap)));
                }
            });
        let cb: Callback = Callback::Read(Box::new(move |resp: ReadResponse| {
            let res = CmdRes::Snap(resp.snapshot.unwrap());
            cb2((CbContext::new(), Ok(res)));
        }));
        cb.invoke_read(resp.clone());
    });
}

#[bench]
fn bench_async_snapshot(b: &mut test::Bencher) {
    let leader = util::new_peer(2, 3);
    let mut region = Region::new();
    region.set_id(1);
    region.set_start_key(vec![]);
    region.set_end_key(vec![]);
    region.mut_peers().push(leader.clone());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let kv = RaftKv::new(SyncBenchRouter::new(region.clone(), db));

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());
    b.iter(|| {
        let on_finished: EngineCallback<RegionSnapshot> = Box::new(move |results| {
            test::black_box(results);
        });
        kv.async_snapshot(&ctx, on_finished).unwrap();
    });
}

#[bench]
fn bench_async_write(b: &mut test::Bencher) {
    let leader = util::new_peer(2, 3);
    let mut region = Region::new();
    region.set_id(1);
    region.set_start_key(vec![]);
    region.set_end_key(vec![]);
    region.mut_peers().push(leader.clone());
    region.mut_region_epoch().set_version(2);
    region.mut_region_epoch().set_conf_ver(5);
    let (_tmp, db) = new_engine();
    let kv = RaftKv::new(SyncBenchRouter::new(region.clone(), db));

    let mut ctx = Context::new();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader.clone());
    b.iter(|| {
        let on_finished: EngineCallback<()> = Box::new(|_| {
            test::black_box(());
        });
        kv.async_write(
            &ctx,
            vec![Modify::Delete(
                CF_DEFAULT,
                Key::from_encoded(b"fooo".to_vec()),
            )],
            on_finished,
        ).unwrap();
    });
}
