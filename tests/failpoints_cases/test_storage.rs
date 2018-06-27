// Copyright 2017 PingCAP, Inc.
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

use fail;
use kvproto::kvrpcpb::Context;
use raftstore::server::new_server_cluster;
use std::sync::mpsc::channel;
use std::thread;
use std::time::Duration;
use storage::util::new_raft_engine;
use tikv::server::readpool::{self, ReadPool};
use tikv::storage;
use tikv::storage::config::Config;
use tikv::storage::*;
use tikv::util::worker::FutureWorker;
use tikv::util::HandyRwLock;

#[test]
fn test_storage_1gc() {
    let _guard = ::setup();
    let snapshot_fp = "raftkv_async_snapshot_finish";
    let batch_snapshot_fp = "raftkv_async_batch_snapshot_finish";
    let (_cluster, engine, ctx) = new_raft_engine(3, "");
    let pd_worker = FutureWorker::new("test future worker");
    let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
        || storage::ReadPoolContext::new(pd_worker.scheduler())
    });
    let config = Config::default();
    let mut storage = Storage::from_engine(engine.clone(), &config, read_pool).unwrap();
    storage.start(&config).unwrap();
    fail::cfg(snapshot_fp, "pause").unwrap();
    fail::cfg(batch_snapshot_fp, "pause").unwrap();
    let (tx1, rx1) = channel();
    storage
        .async_gc(ctx.clone(), 1, box move |res: storage::Result<()>| {
            assert!(res.is_ok());
            tx1.send(1).unwrap();
        })
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Old GC command is blocked at snapshot stage, the other one will get ServerIsBusy error.
    let (tx2, rx2) = channel();
    storage
        .async_gc(Context::new(), 1, box move |res: storage::Result<()>| {
            match res {
                Err(storage::Error::SchedTooBusy) => {}
                _ => panic!("expect too busy"),
            }
            tx2.send(1).unwrap();
        })
        .unwrap();

    rx2.recv().unwrap();
    fail::remove(snapshot_fp);
    fail::remove(batch_snapshot_fp);
    rx1.recv().unwrap();
}

#[test]
fn test_scheduler_leader_change_twice() {
    let _guard = ::setup();
    let snapshot_fp = "scheduler_async_snapshot_finish";
    let mut cluster = new_server_cluster(0, 2);
    cluster.run();
    let region0 = cluster.get_region(b"");
    let peers = region0.get_peers();
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    let config = Config::default();
    let pd_worker = FutureWorker::new("test future worker");
    let read_pool = ReadPool::new("readpool", &readpool::Config::default_for_test(), || {
        || storage::ReadPoolContext::new(pd_worker.scheduler())
    });

    let engine0 = cluster.sim.rl().storages[&peers[0].get_id()].clone();
    let mut storage0 = Storage::from_engine(engine0.clone(), &config, read_pool).unwrap();
    storage0.start(&config).unwrap();

    let mut ctx0 = Context::new();
    ctx0.set_region_id(region0.get_id());
    ctx0.set_region_epoch(region0.get_region_epoch().clone());
    ctx0.set_peer(peers[0].clone());
    let (prewrite_tx, prewrite_rx) = channel();
    fail::cfg(snapshot_fp, "pause").unwrap();
    storage0
        .async_prewrite(
            ctx0,
            vec![Mutation::Put((make_key(b"k"), b"v".to_vec()))],
            b"k".to_vec(),
            10,
            Options::default(),
            box move |res: storage::Result<_>| {
                prewrite_tx.send(res).unwrap();
            },
        )
        .unwrap();
    // Sleep to make sure the failpoint is triggered.
    thread::sleep(Duration::from_millis(2000));
    // Transfer leader twice, then unblock snapshot.
    cluster.must_transfer_leader(region0.get_id(), peers[1].clone());
    cluster.must_transfer_leader(region0.get_id(), peers[0].clone());
    fail::remove(snapshot_fp);

    match prewrite_rx.recv_timeout(Duration::from_secs(5)).unwrap() {
        Err(storage::Error::Txn(txn::Error::Engine(engine::Error::Request(ref e))))
        | Err(storage::Error::Engine(engine::Error::Request(ref e))) => {
            assert!(e.has_stale_command(), "{:?}", e);
        }
        res => {
            panic!("expect stale command, but got {:?}", res);
        }
    }
}
