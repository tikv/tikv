// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashSet, time::Duration};

use kvproto::{kvrpcpb::KeyRange, raft_cmdpb::Request};
use test_raftstore::*;
use tidb_query_datatype::codec::table;
use txn_types::Key;

/// A transactional snapshot read as `raftkv::async_snapshot` builds it: `Snap`
/// with the ranges in `read_index`.
fn snap_read(table_id: i64) -> Request {
    let mut req = new_snap_cmd();
    let mut range = KeyRange::default();
    range.set_start_key(Key::from_raw(&table::encode_row_key(table_id, 1)).into_encoded());
    req.mut_read_index().set_start_ts(10);
    req.mut_read_index().mut_key_ranges().push(range);
    req
}

/// Attribution happens in `Peer::propose`, so drive a read through it rather
/// than calling the marking function directly.
#[test]
fn test_snap_read_from_blamed_group_marks_its_table() {
    let mut cluster = new_node_cluster(0, 1);
    cluster.cfg.raft_store.enable_noisy_tenant_demotion = true;
    cluster.cfg.raft_store.store_batch_system.low_priority_pool_size = Some(1);
    cluster.run();
    cluster.must_put(b"k1", b"v1");

    let manager = cluster.resource_manager.clone().unwrap();
    manager.set_noisy_groups_for_test(HashSet::from(["noisy".to_string()]));

    let region = cluster.get_region(b"k1");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let mut read = |group: &str, table_id: i64| {
        // read_quorum sends the read through `Peer::propose` as a ReadIndex
        // instead of serving it from the local reader.
        let mut req = new_request(
            region.get_id(),
            region.get_region_epoch().clone(),
            vec![snap_read(table_id)],
            true,
        );
        req.mut_header().set_peer(leader.clone());
        req.mut_header().set_resource_group_name(group.to_string());
        let resp = cluster
            .call_command_on_leader(req, Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
    };
    read("noisy", 104);
    read("quiet", 105);

    manager.publish_noisy_tables_for_test(true);
    assert!(
        manager.is_noisy_table(104),
        "the blamed group's Snap read marks the table its ranges name"
    );
    assert!(!manager.is_noisy_table(105), "a group nobody blames marks nothing");
}
