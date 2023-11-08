// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::RaftEngineReadOnly;
use raftstore::store::RAFT_INIT_LOG_INDEX;
use tikv_util::store::new_peer;

use crate::cluster::{split_helper::split_region_and_refresh_bucket, Cluster};

/// Test refresh bucket.
#[test]
fn test_refresh_bucket() {
    let mut cluster = Cluster::default();
    let store_id = cluster.node(0).id();
    let raft_engine = cluster.node(0).running_state().unwrap().raft_engine.clone();
    let router = &mut cluster.routers[0];

    let region_2 = 2;
    let region = router.region_detail(region_2);
    let peer = region.get_peers()[0].clone();
    router.wait_applied_to_current_term(region_2, Duration::from_secs(3));

    // Region 2 ["", ""]
    //   -> Region 2    ["", "k22"]
    //      Region 1000 ["k22", ""] peer(1, 10)
    let region_state = raft_engine
        .get_region_state(region_2, u64::MAX)
        .unwrap()
        .unwrap();
    assert_eq!(region_state.get_tablet_index(), RAFT_INIT_LOG_INDEX);

    // to simulate the delay of set_apply_scheduler
    fail::cfg("delay_set_apply_scheduler", "sleep(1000)").unwrap();
    split_region_and_refresh_bucket(
        router,
        region,
        peer,
        1000,
        new_peer(store_id, 10),
        b"k22",
        false,
    );

    for _i in 1..100 {
        std::thread::sleep(Duration::from_millis(50));
        let meta = router
            .must_query_debug_info(1000, Duration::from_secs(1))
            .unwrap();
        if !meta.bucket_keys.is_empty() {
            assert_eq!(meta.bucket_keys.len(), 4); // include region start/end keys
            assert_eq!(meta.bucket_keys[1], b"1".to_vec());
            assert_eq!(meta.bucket_keys[2], b"2".to_vec());
            return;
        }
    }
    panic!("timeout for updating buckets"); // timeout
}
