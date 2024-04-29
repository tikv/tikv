// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    path::Path,
    sync::{atomic::AtomicU64, mpsc::sync_channel, Arc, Mutex},
    time::Duration,
};

use concurrency_manager::ConcurrencyManager;
use engine_rocks::RocksEngine;
use engine_traits::{
    DbOptionsExt, Engines, MiscExt, Peekable, RaftEngine, RaftEngineReadOnly, ALL_CFS, CF_DEFAULT,
    CF_LOCK, CF_RAFT, CF_WRITE,
};
use health_controller::HealthController;
use kvproto::{kvrpcpb::ApiVersion, metapb, raft_serverpb::RegionLocalState};
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{bootstrap_store, fsm, fsm::store::StoreMeta, AutoSplitController, SnapManager},
};
use raftstore_v2::router::PeerMsg;
use resource_metering::CollectorRegHandle;
use service::service_manager::GrpcServiceManager;
use tempfile::Builder;
use test_pd_client::{bootstrap_with_first_region, TestPdClient};
use test_raftstore::*;
use tikv::{import::SstImporter, server::MultiRaftServer};
use tikv_util::{
    config::VersionTrack,
    worker::{dummy_scheduler, Builder as WorkerBuilder, LazyWorker},
};

fn test_bootstrap_idempotent<T: Simulator<RocksEngine>>(cluster: &mut Cluster<RocksEngine, T>) {
    // assume that there is a node  bootstrap the cluster and add region in pd
    // successfully
    cluster.add_first_region().unwrap();
    // now at same time start the another node, and will receive `cluster is not
    // bootstrap` it will try to bootstrap with a new region, but will failed
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

    let (_, system) = fsm::create_raft_batch_system(&cfg.raft_store, &None);
    let simulate_trans =
        SimulateTransport::<_, RocksEngine>::new(ChannelTransport::<RocksEngine>::new());
    let tmp_path = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let engine =
        engine_rocks::util::new_engine(tmp_path.path().to_str().unwrap(), ALL_CFS).unwrap();
    let tmp_path_raft = tmp_path.path().join(Path::new("raft"));
    let raft_engine =
        engine_rocks::util::new_engine(tmp_path_raft.to_str().unwrap(), &[CF_DEFAULT]).unwrap();
    let engines = Engines::new(engine.clone(), raft_engine);
    let tmp_mgr = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let bg_worker = WorkerBuilder::new("background").thread_count(2).create();
    let mut node = MultiRaftServer::new(
        system,
        &cfg.server,
        Arc::new(VersionTrack::new(cfg.raft_store.clone())),
        cfg.storage.api_version(),
        Arc::clone(&pd_client),
        Arc::default(),
        bg_worker,
        HealthController::new(),
        None,
    );
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap());
    let pd_worker = LazyWorker::new("test-pd-worker");

    // assume there is a node has bootstrapped the cluster and add region in pd
    // successfully
    bootstrap_with_first_region(Arc::clone(&pd_client)).unwrap();

    // now another node at same time begin bootstrap node, but panic after prepared
    // bootstrap now rocksDB must have some prepare data
    bootstrap_store(&engines, 0, 1).unwrap();
    let region = node.prepare_bootstrap_cluster(&engines, 1).unwrap();
    assert!(
        engine
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_some()
    );
    let region_state_key = keys::region_state_key(region.get_id());
    assert!(
        engine
            .get_msg_cf::<RegionLocalState>(CF_RAFT, &region_state_key)
            .unwrap()
            .is_some()
    );

    // Create coprocessor.
    let coprocessor_host = CoprocessorHost::new(node.get_router(), cfg.coprocessor);

    let importer = {
        let dir = tmp_path.path().join("import-sst");
        Arc::new(
            SstImporter::new(&cfg.import, dir, None, cfg.storage.api_version(), false).unwrap(),
        )
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
        None,
        GrpcServiceManager::dummy(),
        Arc::new(AtomicU64::new(0)),
    )
    .unwrap();
    assert!(
        engine
            .get_msg::<metapb::Region>(keys::PREPARE_BOOTSTRAP_KEY)
            .unwrap()
            .is_none()
    );
    assert!(
        engine
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
                cluster.start().unwrap_err();
            }
        }
    }
}

#[test]
fn test_flush_before_stop() {
    use test_raftstore_v2::*;

    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    let region = cluster.get_region(b"");
    cluster.must_split(&region, b"k020");

    let region = cluster.get_region(b"k40");
    cluster.must_split(&region, b"k040");

    let region = cluster.get_region(b"k60");
    cluster.must_split(&region, b"k070");

    fail::cfg("flush_before_close_threshold", "return(10)").unwrap();

    for i in 0..100 {
        let key = format!("k{:03}", i);
        cluster.must_put_cf(CF_WRITE, key.as_bytes(), b"val");
        cluster.must_put_cf(CF_LOCK, key.as_bytes(), b"val");
    }

    let router = cluster.get_router(1).unwrap();
    let raft_engine = cluster.get_raft_engine(1);

    let mut rxs = vec![];
    raft_engine
        .for_each_raft_group::<raftstore::Error, _>(&mut |id| {
            let (tx, rx) = sync_channel(1);
            rxs.push(rx);
            let msg = PeerMsg::FlushBeforeClose { tx };
            router.force_send(id, msg).unwrap();

            Ok(())
        })
        .unwrap();

    for rx in rxs {
        rx.recv().unwrap();
    }

    raft_engine
        .for_each_raft_group::<raftstore::Error, _>(&mut |id| {
            let admin_flush = raft_engine.get_flushed_index(id, CF_RAFT).unwrap().unwrap();
            assert!(admin_flush >= 40);
            Ok(())
        })
        .unwrap();
}

// test flush_before_close will not flush forever
#[test]
fn test_flush_before_stop2() {
    use test_raftstore_v2::*;

    let mut cluster = new_server_cluster(0, 3);
    cluster.run();

    fail::cfg("flush_before_close_threshold", "return(10)").unwrap();
    fail::cfg("on_flush_completed", "return").unwrap();

    for i in 0..20 {
        let key = format!("k{:03}", i);
        cluster.must_put_cf(CF_WRITE, key.as_bytes(), b"val");
        cluster.must_put_cf(CF_LOCK, key.as_bytes(), b"val");
    }

    let router = cluster.get_router(1).unwrap();
    let raft_engine = cluster.get_raft_engine(1);

    let (tx, rx) = sync_channel(1);
    let msg = PeerMsg::FlushBeforeClose { tx };
    router.force_send(1, msg).unwrap();

    rx.recv().unwrap();

    let admin_flush = raft_engine.get_flushed_index(1, CF_RAFT).unwrap().unwrap();
    assert!(admin_flush < 10);
}

// We cannot use a flushed index to call `maybe_advance_admin_flushed`
// consider a case:
// 1. lock `k` with index 6
// 2. on_applied_res => lockcf's last_modified = 6
// 3. flush lock cf => lockcf's flushed_index = 6
// 4. batch {unlock `k`, write `k`} with index 7 (last_modified is updated in
//    store but RocksDB is modified in apply. So,
// before on_apply_res, the last_modified is not updated.)
//
// flush-before-close:
// 5. pick write cf to flush => writecf's flushed_index = 7
//
// 6. maybe_advance_admin_flushed(7): as lockcf's last_modified = flushed_index,
// it will not block advancing admin index
// 7. admin index 7 is persisted. => we may loss `unlock k`
#[test]
fn test_flush_index_exceed_last_modified() {
    let mut cluster = test_raftstore_v2::new_node_cluster(0, 1);
    cluster.run();

    let key = b"key1";
    cluster.must_put_cf(CF_LOCK, key, b"v");
    cluster.must_put_cf(CF_WRITE, b"dummy", b"v");

    fail::cfg("before_report_apply_res", "return").unwrap();
    let reg = cluster.tablet_registries.get(&1).unwrap();
    {
        let mut cache = reg.get(1).unwrap();
        let tablet = cache.latest().unwrap();
        tablet
            .set_db_options(&[("avoid_flush_during_shutdown", "true")])
            .unwrap();

        // previous flush before strategy is flush oldest one by one, where freshness
        // comparison is in second, so sleep a second
        std::thread::sleep(Duration::from_millis(1000));
        tablet.flush_cf(CF_LOCK, true).unwrap();
    }

    cluster
        .batch_put(
            key,
            vec![
                new_put_cf_cmd(CF_WRITE, key, b"value"),
                new_delete_cmd(CF_LOCK, key),
            ],
        )
        .unwrap();

    fail::cfg("flush_before_close_threshold", "return(1)").unwrap();
    let router = cluster.get_router(1).unwrap();
    let (tx, rx) = sync_channel(1);
    let msg = PeerMsg::FlushBeforeClose { tx };
    router.force_send(1, msg).unwrap();
    rx.recv().unwrap();

    assert!(cluster.get_cf(CF_WRITE, b"key1").is_some());
    assert!(cluster.get_cf(CF_LOCK, b"key1").is_none());
    cluster.stop_node(1);

    fail::remove("before_report_apply_res");
    cluster.start().unwrap();

    assert!(cluster.get_cf(CF_WRITE, b"key1").is_some());
    assert!(cluster.get_cf(CF_LOCK, b"key1").is_none());
}
