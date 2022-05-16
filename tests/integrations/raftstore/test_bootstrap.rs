// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use concurrency_manager::ConcurrencyManager;
use engine_rocks::{Compat, RocksEngine};
use engine_traits::{Engines, Peekable, ALL_CFS, CF_RAFT};
use kvproto::{kvrpcpb::ApiVersion, metapb, raft_serverpb::RegionLocalState};
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{bootstrap_store, fsm, fsm::store::StoreMeta, AutoSplitController, SnapManager},
};
use resource_metering::CollectorRegHandle;
use tempfile::Builder;
use test_raftstore::*;
use tikv::{import::SstImporter, server::Node};
use tikv_util::{
    config::VersionTrack,
    worker::{dummy_scheduler, Builder as WorkerBuilder, LazyWorker},
};

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
        engine_rocks::raw_util::new_engine(tmp_path.path().to_str().unwrap(), None, ALL_CFS, None)
            .unwrap(),
    );
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine = Arc::new(
        engine_rocks::raw_util::new_engine(tmp_path_raft.to_str().unwrap(), None, &[], None)
            .unwrap(),
    );
    let engines = Engines::new(
        RocksEngine::from_db(Arc::clone(&engine)),
        RocksEngine::from_db(Arc::clone(&raft_engine)),
    );
    let tmp_mgr = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let bg_worker = WorkerBuilder::new("background").thread_count(2).create();
    let mut node = Node::new(
        system,
        &cfg.server,
        Arc::new(VersionTrack::new(cfg.raft_store.clone())),
        cfg.storage.api_version(),
        Arc::clone(&pd_client),
        Arc::default(),
        bg_worker,
        None,
    );
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap());
    let pd_worker = LazyWorker::new("test-pd-worker");

    // assume there is a node has bootstrapped the cluster and add region in pd successfully
    bootstrap_with_first_region(Arc::clone(&pd_client)).unwrap();

    // now another node at same time begin bootstrap node, but panic after prepared bootstrap
    // now rocksDB must have some prepare data
    bootstrap_store(&engines, 0, 1).unwrap();
    let region = node.prepare_bootstrap_cluster(&engines, 1).unwrap();
    assert!(
        engine
            .c()
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_some()
    );
    let region_state_key = keys::region_state_key(region.get_id());
    assert!(
        engine
            .c()
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            .unwrap()
            .is_some()
    );

    // Create coprocessor.
    let coprocessor_host = CoprocessorHost::new(node.get_router(), cfg.coprocessor);

    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(SstImporter::new(&cfg.import, dir, None, cfg.storage.api_version()).unwrap())
    };
    let (split_check_scheduler, _) = dummy_scheduler();

    node.try_bootstrap_store(engines.clone()).unwrap();
    // try to restart this node, will clear the prepare data
    node.start(
        engines,
        simulate_trans,
        snap_mgr,
        pd_worker,
        Arc::new(Mutex::new(StoreMeta::new(0))),
        coprocessor_host,
        importer,
        split_check_scheduler,
        AutoSplitController::default(),
        ConcurrencyManager::new(1.into()),
        CollectorRegHandle::new_for_test(),
    )
    .unwrap();
    assert!(
        Arc::clone(&engine)
            .c()
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
            .c()
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            .unwrap()
            .is_none()
    );
    assert_eq!(pd_client.get_regions_number() as u32, 1);
    node.stop();
}

#[test]
fn test_node_bootstrap_idempotent() {
    let mut cluster = new_node_cluster(0, 3);
    test_bootstrap_idempotent(&mut cluster);
}

#[test]
fn test_node_switch_api_version() {
    // V1 and V1ttl are impossible to switch between because of config check.
    let cases = [
        (ApiVersion::V1, ApiVersion::V1),
        (ApiVersion::V1, ApiVersion::V2),
        (ApiVersion::V1ttl, ApiVersion::V1ttl),
        (ApiVersion::V1ttl, ApiVersion::V2),
        (ApiVersion::V2, ApiVersion::V1),
        (ApiVersion::V2, ApiVersion::V1ttl),
        (ApiVersion::V2, ApiVersion::V2),
    ];
    for (from_api, to_api) in cases {
        // With TiDB data
        {
            // Bootstrap with `from_api`
            let mut cluster = new_node_cluster(0, 1);
            cluster.cfg.storage.set_api_version(from_api);
            cluster.start().unwrap();

            // Write TiDB data
            cluster.put(b"m_tidb_data", b"").unwrap();
            cluster.shutdown();

            // Should switch to `to_api`
            cluster.cfg.storage.set_api_version(to_api);
            cluster.start().unwrap();
            cluster.shutdown();
        }

        // With non-TiDB data.
        {
            // Bootstrap with `from_api`
            let mut cluster = new_node_cluster(0, 1);
            cluster.cfg.storage.set_api_version(from_api);
            cluster.start().unwrap();

            // Write non-TiDB data
            cluster.put(b"k1", b"").unwrap();
            cluster.shutdown();

            cluster.cfg.storage.set_api_version(to_api);
            if from_api == to_api {
                cluster.start().unwrap();
                cluster.shutdown();
            } else {
                // Should not be able to switch to `to_api`.
                assert!(cluster.start().is_err());
            }
        }
    }
}
