// Copyright 2019 PingCAP, Inc.
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

use std::path::Path;
use std::sync::{Arc, RwLock};

use kvproto::{metapb, raft_serverpb};
use tempdir::TempDir;

use test_raftstore::*;
use tikv::import::SSTImporter;
use tikv::pd::PdClient;
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv::raftstore::store::{fsm, keys, Engines, Peekable, SnapManager};
use tikv::server::Node;
use tikv::storage::ALL_CFS;
use tikv::util::rocksdb_util;
use tikv::util::worker::{FutureWorker, Worker};

fn test_boostrap_half_way_failure(fp: &str) {
    // create a node
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let cfg = new_tikv_config(0);

    let (_, system) = fsm::create_raft_batch_system(&cfg.raft_store);
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp_path = TempDir::new("test_cluster").unwrap();
    let engine = Arc::new(
        rocksdb_util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap(),
    );
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine = Arc::new(
        rocksdb_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap(),
    );
    let engines = Engines::new(Arc::clone(&engine), Arc::clone(&raft_engine));
    let tmp_mgr = TempDir::new("test_cluster").unwrap();

    let mut node = Node::new(system, &cfg.server, &cfg.raft_store, Arc::clone(&pd_client));
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap(), Some(node.get_router()));
    let pd_worker = FutureWorker::new("test-pd-worker");
    let local_reader = Worker::new("test-local-reader");
    let coprocessor_host = CoprocessorHost::new(cfg.coprocessor.clone(), node.get_router());
    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(SSTImporter::new(dir).unwrap())
    };

    fail::cfg(fp, "return").unwrap();
    // try to start this node, return after write store ident.
    let _ = node.start(
        engines.clone(),
        simulate_trans.clone(),
        snap_mgr.clone(),
        pd_worker,
        local_reader,
        coprocessor_host,
        importer,
    );
    node.stop();
    fail::remove(fp);

    let (_, system) = fsm::create_raft_batch_system(&cfg.raft_store);
    let mut node = Node::new(system, &cfg.server, &cfg.raft_store, Arc::clone(&pd_client));
    let pd_worker = FutureWorker::new("test-pd-worker");
    let local_reader = Worker::new("test-local-reader");
    let coprocessor_host = CoprocessorHost::new(cfg.coprocessor, node.get_router());
    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(SSTImporter::new(dir).unwrap())
    };
    node.start(
        engines.clone(),
        simulate_trans,
        snap_mgr,
        pd_worker,
        local_reader,
        coprocessor_host,
        importer,
    )
    .unwrap();
    node.stop();

    // Check whether it bootstrap cluster successfully.
    assert!(engines
        .kv
        .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());
    assert!(pd_client.is_cluster_bootstrapped().unwrap());
    let ident = engines
        .kv
        .get_msg::<raft_serverpb::StoreIdent>(keys::STORE_IDENT_KEY)
        .unwrap()
        .unwrap();
    let store_id = ident.get_store_id();
    let sim = Arc::new(RwLock::new(NodeCluster::new(Arc::clone(&pd_client))));
    let mut cluster = Cluster::new(0, 5, sim, pd_client);
    cluster.create_engines();
    cluster.dbs[0] = engines.clone();
    cluster.paths[0] = tmp_path;
    cluster.engines.insert(store_id, engines);
    cluster.start();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    must_get_equal(&cluster.get_engine(store_id), k, v);
    for id in cluster.engines.keys() {
        if *id == store_id {
            continue;
        }
        must_get_equal(&cluster.get_engine(*id), k, v);
    }
}

#[test]
fn test_boostrap_half_way_failure_after_bootstrap_store() {
    let _guard = crate::setup();

    let fp = "node_after_bootstrap_store";
    test_boostrap_half_way_failure(fp);
}

#[test]
fn test_boostrap_half_way_failure_after_prepare_bootstrap_cluster() {
    let _guard = crate::setup();

    let fp = "node_after_prepare_bootstrap_cluster";
    test_boostrap_half_way_failure(fp);
}

#[test]
fn test_boostrap_half_way_failure_after_bootstrap_cluster() {
    let _guard = crate::setup();

    let fp = "node_after_bootstrap_cluster";
    test_boostrap_half_way_failure(fp);
}
