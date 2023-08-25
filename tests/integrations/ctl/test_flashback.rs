// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::{mpsc, Arc},
    time::Duration,
};

use engine_traits::{Peekable, RaftEngine, RaftEngineReadOnly, RaftLogBatch, SyncMutable, CF_RAFT};
use kvproto::{
    metapb, raft_serverpb,
};
use pd_client::{PdClient, RegionStat, RpcClient};
use raftstore::store::RAFT_INIT_LOG_TERM;
use security::{SecurityConfig, SecurityManager};
use test_pd::{
    mocker::Service as PDService, util::new_client_with_update_interval, Server as MockServer,
};
use test_raftstore::{
    must_new_cluster_kv_client_and_debug_client, write_and_read_key, must_kv_read_equal,
};
use tikv_util::config::ReadableDuration;
use tokio::runtime::Builder;

fn new_test_server_and_client(
    update_interval: ReadableDuration,
) -> (MockServer<PDService>, RpcClient) {
    let server = MockServer::new(1);
    let eps = server.bind_addrs();
    let client = new_client_with_update_interval(eps, None, update_interval);
    (server, client)
}

#[test]
fn test_flashback() {
    let (cluster, kv_client, debug_client, ctx) = must_new_cluster_kv_client_and_debug_client();
    let (mut server, pd_client) = new_test_server_and_client(ReadableDuration::millis(100));
    let store_id = pd_client.alloc_id().unwrap();
    let mut store = metapb::Store::default();
    store.set_id(store_id);

    let region_id = 1;
    let region = cluster.get_region_by_id(region_id);
    let peer = region.get_peers()[0].clone();
    pd_client
        .bootstrap_cluster(store.clone(), region.clone())
        .unwrap();

    let poller = Builder::new_multi_thread()
        .thread_name(thd_name!("poller"))
        .worker_threads(1)
        .build()
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let f = pd_client.handle_region_heartbeat_response(store_id, move |resp| {
        let _ = tx.send(resp);
    });
    poller.spawn(f);
    poller.spawn(pd_client.region_heartbeat(
        RAFT_INIT_LOG_TERM,
        region.clone(),
        peer.clone(),
        RegionStat::default(),
        None,
    ));
    rx.recv_timeout(Duration::from_secs(3)).unwrap();

    let region_key = region.get_start_key();
    let region_info = pd_client.get_region_info(region_key).unwrap();
    assert_eq!(region_info.region, region);
    assert_eq!(region_info.leader.unwrap(), peer);

    // let mut ts = 0;
    // // Need to write many batches.
    // for i in 0..2000 {
    //     let v = format!("value@{}", i).into_bytes();
    //     let k = format!("key@{}", i % 1000).into_bytes();
    //     write_and_read_key(&kv_client, &ctx, &mut ts, k.clone(), v.clone());
    // }

    let mut ts = 0;
    let k1 = b"k1".to_vec();
    let v = b"v1".to_vec();
    write_and_read_key(&kv_client, &ctx, &mut ts, k1.clone(), v.clone());
    let v2 = b"v2".to_vec();
    write_and_read_key(&kv_client, &ctx, &mut ts, k1.clone(), v2.clone());
    let k2 = b"k2".to_vec();
    write_and_read_key(&kv_client, &ctx, &mut ts, k2.clone(), v.clone());
    write_and_read_key(&kv_client, &ctx, &mut ts, k2.clone(), v2.clone());

    

    // put region info.
    let raft_engine = cluster.get_raft_engine(store_id);
    let kv_engine = cluster.get_engine(store_id);

    let mut raft_state = raft_serverpb::RaftLocalState::default();
    raft_state.set_last_index(42);
    let mut lb = raft_engine.log_batch(0);
    lb.put_raft_state(region_id, &raft_state).unwrap();
    raft_engine.consume(&mut lb, false).unwrap();
    assert_eq!(
        raft_engine.get_raft_state(region_id).unwrap().unwrap(),
        raft_state
    );

    let apply_state_key = keys::apply_state_key(region_id);
    let mut apply_state = raft_serverpb::RaftApplyState::default();
    apply_state.set_applied_index(42);
    kv_engine
        .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .get_msg_cf::<raft_serverpb::RaftApplyState>(CF_RAFT, &apply_state_key)
            .unwrap()
            .unwrap(),
        apply_state
    );

    let region_state_key = keys::region_state_key(region_id);
    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(raft_serverpb::PeerState::Tombstone);
    region_state.set_region(region.clone());
    kv_engine
        .put_msg_cf(CF_RAFT, &region_state_key, &region_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .get_msg_cf::<raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key)
            .unwrap()
            .unwrap(),
        region_state
    );


    let mgr = Arc::new(SecurityManager::new(&SecurityConfig::default()).unwrap());
    tikv_ctl::run::flashback_whole_cluster(
        &pd_client,
        &cluster.cfg,
        mgr,
        Vec::default(),
        0,
        Vec::default(),
        Vec::default(),
        Some(debug_client),
    );
    ts += 2;
    // must_kv_read_equal(&kv_client, ctx, b"key@1".to_vec(), b"value@1".to_vec(), ts);
    must_kv_read_equal(&kv_client, ctx, b"k2".to_vec(), b"v2".to_vec(), ts);
    
    server.stop();
}
