// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::{Duration, Instant};

use engine_traits::CF_DEFAULT;
use futures::executor::block_on;
use pd_client::PdClient;
use raftstore_v2::{
    router::{PeerMsg, StoreMsg, StoreTick},
    SimpleWriteEncoder,
};
use tikv_util::config::ReadableDuration;

use crate::cluster::{v2_default_config, Cluster};

#[test]
fn test_fake_store_heartbeat() {
    fail::cfg("mock_collect_tick_interval", "return(0)").unwrap();

    let cluster = Cluster::with_config_and_extra_setting(v2_default_config(), |config| {
        config.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
        config.inspect_interval = ReadableDuration::millis(10);
    });
    let store_id = cluster.node(0).id();
    let router = &cluster.routers[0];
    // Report store heartbeat to pd.
    router
        .store_router()
        .send_control(StoreMsg::Tick(StoreTick::PdStoreHeartbeat))
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    let prev_stats = block_on(cluster.node(0).pd_client().get_store_stats_async(store_id)).unwrap();
    if prev_stats.get_start_time() > 0 {
        assert_ne!(prev_stats.get_capacity(), 0);
        assert_ne!(prev_stats.get_used_size(), 0);
        assert_eq!(prev_stats.get_keys_written(), 0);
    }
    // Inject failpoints to trigger reporting fake store heartbeat to pd.
    fail::cfg("mock_slowness_last_tick_unfinished", "return(0)").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(1));
    let after_stats =
        block_on(cluster.node(0).pd_client().get_store_stats_async(store_id)).unwrap();
    assert_ne!(after_stats.get_capacity(), 0);
    assert_ne!(after_stats.get_used_size(), 0);
    assert_eq!(after_stats.get_keys_written(), 0);
    if after_stats.get_start_time() == 0 {
        // It means that current store_heartbeat is timeout, and triggers a fake
        // heartbeat.
        assert!(after_stats.get_is_busy());
    } else {
        // Normal.
        assert!(!after_stats.get_is_busy());
    }

    fail::remove("mock_slowness_last_tick_unfinished");
    fail::remove("mock_collect_tick_interval");
}

#[test]
fn test_store_heartbeat_after_replay() {
    let mut cluster = Cluster::with_config_and_extra_setting(v2_default_config(), |config| {
        config.pd_store_heartbeat_tick_interval = ReadableDuration::millis(10);
        config.inspect_interval = ReadableDuration::millis(10);
    });

    let router = &mut cluster.routers[0];
    let header = Box::new(router.new_request_for(2).take_header());
    let mut put = SimpleWriteEncoder::with_capacity(64);
    router.wait_applied_to_current_term(2, Duration::from_secs(3));

    fail::cfg("on_handle_apply", "return").unwrap();
    put.put(CF_DEFAULT, b"key", b"value");
    let (msg, mut sub0) = PeerMsg::simple_write(header.clone(), put.encode());
    router.send(2, msg).unwrap();
    assert!(block_on(sub0.wait_proposed()));
    assert!(block_on(sub0.wait_committed()));
    cluster.routers.remove(0);
    let node = &mut cluster.nodes[0];
    let state = node.running_state().unwrap();
    let prev_transport = state.transport.clone();
    let cfg = state.cfg.clone();
    let cop_cfg = state.cop_cfg.clone();
    node.stop();
    std::thread::sleep(Duration::from_secs(1));
    let before_restart = chrono::Local::now().timestamp() as u32;
    std::thread::sleep(Duration::from_secs(1));
    cluster
        .routers
        .insert(0, node.start(cfg, cop_cfg, prev_transport));

    // There should be no store heartbeats before replay is finished.
    let mut timer = Instant::now();
    while timer.elapsed() < Duration::from_secs(3) {
        let stats = block_on(node.pd_client().get_store_stats_async(1)).unwrap();
        assert!(stats.get_start_time() < before_restart);
        std::thread::sleep(Duration::from_millis(100));
    }

    std::thread::sleep(Duration::from_secs(1));
    // Unblock replay.
    fail::remove("on_handle_apply");

    let router = &mut cluster.routers[0];
    let mut put1 = SimpleWriteEncoder::with_capacity(64);
    put1.put(CF_DEFAULT, b"key1", b"value");
    let (msg1, _) = PeerMsg::simple_write(header.clone(), put1.encode());
    router.send(2, msg1).unwrap();
    // There should be store heartbeats after replay is finished.
    timer = Instant::now();
    let mut heartbeat_sent = false;

    while timer.elapsed() < Duration::from_secs(3) {
        let stats = block_on(cluster.node(0).pd_client().get_store_stats_async(1)).unwrap();
        if stats.get_start_time() > before_restart {
            heartbeat_sent = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(heartbeat_sent);
}
