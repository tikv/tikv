// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;
use test_raftstore::*;
use tikv_util::time::Instant;

/// store meta lock test when destroy peer.
#[test]
fn test_meta_lock_when_destroy_peer() {
    // 3 nodes cluster.
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = cluster.pd_client.clone();
    pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    let region_1 = cluster.get_region(b"k1");
    let peer_1_1 = new_peer(1, 1);
    pd_client.must_add_peer(r1, new_peer(2, 1002));
    pd_client.must_add_peer(r1, new_peer(3, 1003));

    cluster.must_transfer_leader(r1, peer_1_1.clone());
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");
    let ch = async_read_on_peer(
        &mut cluster,
        peer_1_1.clone(),
        region_1.clone(),
        b"k1",
        true,
        false,
    );
    assert!(ch.recv_timeout(Duration::from_secs(1)).is_ok());
    fail::cfg("seek_first_log_index_duration", "sleep(10000)").unwrap();
    cluster.must_transfer_leader(r1, new_peer(2, 1002));
    let t = Instant::now();
    pd_client.must_remove_peer(r1, peer_1_1.clone());
    println!("remove peer takes: {:?}", t.saturating_elapsed());
    fail::cfg("must_track_version_changed", "return").unwrap();

    let t = Instant::now();
    let ch = async_read_on_peer(
        &mut cluster,
        peer_1_1.clone(),
        region_1.clone(),
        b"k1",
        true,
        false, // both true or false are tested, and the results are the same.
    );
    println!("first async read takes {:?}", t.saturating_elapsed());
    let t = Instant::now();
    // assert!(ch.recv_timeout(Duration::from_secs(3)).is_ok());
    let resp = ch.recv_timeout(Duration::from_secs(3)).unwrap();
    println!("async read ch recv call takes {:?}", t.saturating_elapsed());
    println!("async follower read resp is {:?}", resp);

    let t = Instant::now();
    let ch = async_read_on_peer(
        &mut cluster,
        peer_1_1.clone(),
        region_1.clone(),
        b"k1",
        true,
        false, // both true or false are tested, and the results are the same.
    );
    println!("second async read takes {:?}", t.saturating_elapsed());
    let resp = ch.recv_timeout(Duration::from_secs(3)).unwrap();
    println!("async follower read resp is {:?}", resp);

    fail::remove("seek_first_log_index_duration");
}
