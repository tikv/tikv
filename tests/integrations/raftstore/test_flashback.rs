/*
 * @Descripttion: 
 * @Author: HuSharp
 * @Date: 2022-08-26 17:59:33
 * @LastEditTime: 2022-08-26 18:26:48
 * @@Email: ihusharp@gmail.com
 */
// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use test_raftstore::*;
#[test]
fn test_flahsback_for_parameters() {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();

    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let region = cluster.get_region(b"k1");
    cluster.call_prepare_flashback(region.get_id(), 1);
    sleep_ms(100);
    
    assert!(
        cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .get(&region.get_id())
            .unwrap()
            .flashback_pending_ignore
    );
    assert!(
        cluster.store_metas[&1]
            .lock()
            .unwrap()
            .flashback_state
            .is_some()
    );

    cluster.call_finish_flashback(region.get_id(), 1);
    sleep_ms(100);
    
    assert!(
        !cluster.store_metas[&1]
            .lock()
            .unwrap()
            .readers
            .get(&region.get_id())
            .unwrap()
            .flashback_pending_ignore
    );
    assert!(
        cluster.store_metas[&1]
            .lock()
            .unwrap()
            .flashback_state
            .is_none()
    );
}