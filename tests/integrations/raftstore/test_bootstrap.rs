// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::{Arc, Mutex};

use tempfile::Builder;

use kvproto::metapb;
use kvproto::raft_serverpb::RegionLocalState;

use engine::*;
use test_raftstore::*;
use tikv::import::SSTImporter;
use tikv::raftstore::coprocessor::CoprocessorHost;
use tikv::raftstore::store::fsm::store::StoreMeta;
use tikv::raftstore::store::{bootstrap_store, fsm, SnapManager};
use tikv::server::Node;
use tikv_util::worker::FutureWorker;

fn test_bootstrap_idempotent<T: Simulator>(cluster: &mut Cluster<T>) {
    // assume that there is a node  bootstrap the cluster and add region in pd successfully
    cluster.add_first_region().unwrap();
    // now at same time start the another node, and will recive cluster is not bootstrap
    // it will try to bootstrap with a new region, but will failed
    // the region number still 1
    cluster.start().unwrap();
    cluster.check_regions_number(1);
    cluster.shutdown();
    sleep_ms(500);
    cluster.start().unwrap();
    cluster.check_regions_number(1);
}

#[test]
fn test_node_bootstrap_with_prepared_data() {
    // create a node
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let cfg = new_tikv_config(0);

    let (_, system) = fsm::create_raft_batch_system(&cfg.raft_store);
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp_path = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let engine = Arc::new(
        rocks::util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CFS, None).unwrap(),
    );
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine = Arc::new(
        rocks::util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None).unwrap(),
    );
    let shared_block_cache = false;
    let engines = Engines::new(
        Arc::clone(&engine),
        Arc::clone(&raft_engine),
        shared_block_cache,
    );
    let tmp_mgr = Builder::new().prefix("test_cluster").tempdir().unwrap();

    let mut node = Node::new(system, &cfg.server, &cfg.raft_store, Arc::clone(&pd_client));
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap(), Some(node.get_router()));
    let pd_worker = FutureWorker::new("test-pd-worker");

    // assume there is a node has bootstrapped the cluster and add region in pd successfully
    bootstrap_with_first_region(Arc::clone(&pd_client)).unwrap();

    // now another node at same time begin bootstrap node, but panic after prepared bootstrap
    // now rocksDB must have some prepare data
    bootstrap_store(&engines, 0, 1).unwrap();
    let region = node.prepare_bootstrap_cluster(&engines, 1).unwrap();
    assert!(engine
        .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_some());
    let region_state_key = keys::region_state_key(region.get_id());
    assert!(engine
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        .unwrap()
        .is_some());

    // Create coprocessor.
    let coprocessor_host = CoprocessorHost::new(cfg.coprocessor, node.get_router());

    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(SSTImporter::new(dir).unwrap())
    };

    // try to restart this node, will clear the prepare data
    node.start(
        engines,
        simulate_trans,
        snap_mgr,
        pd_worker,
        Arc::new(Mutex::new(StoreMeta::new(0))),
        coprocessor_host,
        importer,
    )
    .unwrap();
    assert!(Arc::clone(&engine)
        .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
        .unwrap()
        .is_none());
    assert!(engine
        .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
        .unwrap()
        .is_none());
    assert_eq!(pd_client.get_regions_number() as u32, 1);
    node.stop();
}

#[test]
fn test_node_bootstrap_idempotent() {
    let mut cluster = new_node_cluster(0, 3);
    test_bootstrap_idempotent(&mut cluster);
}
