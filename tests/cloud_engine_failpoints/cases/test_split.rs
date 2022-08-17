// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use pd_client::PdClient;
use raftstore::store::util::find_peer;
use test_cloud_server::{try_wait, ServerCluster};

use super::{i_to_key, i_to_val};

#[test]
fn test_remove_peer_after_split() {
    test_util::init_log_for_test();
    let mut cluster = ServerCluster::new(vec![1, 2, 3], |_, _| {});
    let mut client = cluster.new_client();
    client.put_kv(0..100, i_to_key, i_to_val);
    let pd_client = cluster.get_pd_client();

    // Splits the region and blocks flushing initial, so that the parent region always has dependents.
    let fp = "kvengine_flush_initial";
    fail::cfg(fp, "pause").unwrap();
    let region = pd_client.get_region(&[]).unwrap();
    client.split(&i_to_key(50));
    // Try to destroy the parent region.
    let store_id = cluster.get_store_id(3);
    let peer = find_peer(&region, store_id).unwrap();
    pd_client.must_remove_peer(region.get_id(), peer.clone());
    // And then move the region back.
    let mut new_peer = peer.clone();
    new_peer.set_id(pd_client.alloc_id().unwrap());
    pd_client.must_add_peer(region.get_id(), new_peer.clone());
    // The new peer can't be created and is pending because the old peer is not destroyed yet.
    assert!(!try_wait(
        || {
            let pending_peers = pd_client.get_pending_peers();
            !pending_peers.contains_key(&new_peer.get_id())
        },
        1
    ));
    // Should be able to recover splitted regions.
    cluster.stop_node(3);
    cluster.start_node(3, |_, _| {});
    // Should delay destroying the parent region even if the node is restarted.
    assert!(!try_wait(
        || {
            let pending_peers = pd_client.get_pending_peers();
            !pending_peers.contains_key(&new_peer.get_id())
        },
        1
    ));
    fail::remove(fp);
    // After all splitted regions finish flushing initial, the old peer is destroyed and new peer
    // can be created.
    pd_client.must_none_pending_peer(new_peer);

    cluster.stop();
}
