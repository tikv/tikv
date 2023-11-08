// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use futures::executor::block_on;
use pd_client::PdClient;
use raftstore_v2::router::{StoreMsg, StoreTick};
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
