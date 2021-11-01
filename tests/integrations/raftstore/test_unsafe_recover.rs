// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::iter::FromIterator;
use std::sync::Arc;
use std::thread;

use futures::executor::block_on;
use kvproto::metapb;
use pd_client::PdClient;
use test_raftstore::*;

#[test]
fn test_unsafe_recover_update_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let election_timeout = configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    // Wait for the leader lease to expire.
    thread::sleep(election_timeout * 2);

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut update = metapb::Region::default();
    update.set_id(1);
    update.set_end_key(b"anykey2".to_vec());
    for p in region.get_peers() {
        if p.get_store_id() == nodes[0] {
            update.mut_peers().push(p.clone());
        }
    }
    update.mut_region_epoch().set_version(1);
    update.mut_region_epoch().set_conf_ver(1);
    // Removes the boostrap region, since it overlaps with any regions we create.
    cluster.must_update_region_for_unsafe_recover(nodes[0], &update);
    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    assert_eq!(region.get_end_key(), b"anykey2");
}

#[test]
fn test_unsafe_recover_create_region() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.run();
    let nodes = Vec::from_iter(cluster.get_node_ids());
    assert_eq!(nodes.len(), 3);

    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer number check.
    pd_client.disable_default_operator();

    let election_timeout = configure_for_lease_read(&mut cluster, None, None);
    cluster.stop_node(nodes[1]);
    cluster.stop_node(nodes[2]);
    // Wait for the leader lease to expire.
    thread::sleep(election_timeout * 2);

    let region = block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut update = metapb::Region::default();
    update.set_id(1);
    update.set_end_key(b"anykey".to_vec());
    for p in region.get_peers() {
        if p.get_store_id() == nodes[0] {
            update.mut_peers().push(p.clone());
        }
    }
    update.mut_region_epoch().set_version(1);
    update.mut_region_epoch().set_conf_ver(1);
    // Removes the boostrap region, since it overlaps with any regions we create.
    cluster.must_update_region_for_unsafe_recover(nodes[0], &update);
    block_on(pd_client.get_region_by_id(1)).unwrap().unwrap();
    let mut create = metapb::Region::default();
    create.set_id(101);
    create.set_start_key(b"anykey".to_vec());
    let mut peer = metapb::Peer::default();
    peer.set_id(102);
    peer.set_store_id(nodes[0]);
    create.mut_peers().push(peer);
    cluster.must_recreate_region_for_unsafe_recover(nodes[0], &create);
    let region = pd_client.get_region(b"anykey1").unwrap();
    assert_eq!(create.get_id(), region.get_id());
}
