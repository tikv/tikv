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
    cluster
        .cfg
        .raft_store
        .store_batch_system
        .low_priority_pool_size = 1;
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
    assert!(
        !manager.is_noisy_table(105),
        "a group nobody blames marks nothing"
    );
}

/// Encoded `t{table_id}` prefix: the key the TiDB schema splits regions at.
fn table_boundary(table_id: i64) -> Vec<u8> {
    let mut raw = table::TABLE_PREFIX.to_vec();
    // Memcomparable i64, as `codec::number::encode_i64` writes it.
    raw.extend_from_slice(&(table_id as u64 ^ (1 << 63)).to_be_bytes());
    Key::from_raw(&raw).into_encoded()
}

fn row_key(table_id: i64, handle: i64) -> Vec<u8> {
    Key::from_raw(&table::encode_row_key(table_id, handle)).into_encoded()
}

fn wait_until(what: &str, mut cond: impl FnMut() -> bool) {
    for _ in 0..200 {
        if cond() {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("timed out waiting for {}", what);
}

/// The real `RaftPoller` on the low pool: a demoted region is still polled
/// there, serves reads, writes and a leader change, and comes back to the
/// normal pool when the tables are released.
#[test]
fn test_demoted_region_runs_on_low_pool_and_recovers() {
    use raftstore::store::metrics::{
        NOISY_REGION_TO_LOW, NOISY_REGION_TO_NORMAL, STORE_POOL_MSGS_VEC,
    };

    let mut cluster = new_node_cluster(0, 3);
    cluster.cfg.raft_store.enable_noisy_tenant_demotion = true;
    cluster
        .cfg
        .raft_store
        .store_batch_system
        .low_priority_pool_size = 1;
    // Keep the region awake so every peer gets a poll round after a publish.
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.run();

    // Carve out a region holding exactly table 104 so the span check has
    // something to match and the rest of the keyspace stays undemoted.
    let region = cluster.get_region(b"");
    cluster.must_split(&region, &table_boundary(104));
    let region = cluster.get_region(&row_key(104, 1));
    cluster.must_split(&region, &table_boundary(105));
    let region = cluster.get_region(&row_key(104, 1));
    let region_id = region.get_id();
    cluster.must_put(&row_key(104, 1), b"v1");

    let manager = cluster.resource_manager.clone().unwrap();
    manager.set_noisy_groups_for_test(HashSet::from(["noisy".to_string()]));
    let low_msgs = STORE_POOL_MSGS_VEC.with_label_values(&["low"]);
    let to_low_before = NOISY_REGION_TO_LOW.get();
    let to_normal_before = NOISY_REGION_TO_NORMAL.get();
    let low_msgs_before = low_msgs.get();

    // A blamed read through `Peer::propose` marks the table; publishing moves
    // the region's three peers to the low pool on their next poll round.
    let leader = cluster.leader_of_region(region_id).unwrap();
    let mut req = new_request(
        region_id,
        region.get_region_epoch().clone(),
        vec![snap_read(104)],
        true,
    );
    req.mut_header().set_peer(leader.clone());
    req.mut_header()
        .set_resource_group_name("noisy".to_string());
    let resp = cluster
        .call_command_on_leader(req, Duration::from_secs(5))
        .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    manager.publish_noisy_tables_for_test(true);
    assert!(manager.is_noisy_table(104));
    wait_until("all three peers to be demoted", || {
        NOISY_REGION_TO_LOW.get() - to_low_before >= 3
    });

    // Polled on the low pool: a poller reporting the wrong priority would
    // reschedule the FSM forever and these would time out.
    cluster.must_put(&row_key(104, 2), b"v2");
    assert_eq!(cluster.must_get(&row_key(104, 2)), Some(b"v2".to_vec()));
    assert_eq!(cluster.must_get(&row_key(104, 1)), Some(b"v1".to_vec()));
    let other = region
        .get_peers()
        .iter()
        .find(|p| p.get_id() != leader.get_id())
        .unwrap()
        .clone();
    cluster.must_transfer_leader(region_id, other);
    cluster.must_put(&row_key(104, 3), b"v3");
    assert_eq!(cluster.must_get(&row_key(104, 3)), Some(b"v3".to_vec()));
    // The thread-local counter flushes about once a second.
    wait_until("low-pool messages to be counted", || {
        low_msgs.get() > low_msgs_before
    });

    // Releasing the tables promotes the peers back; the region keeps working.
    manager.publish_noisy_tables_for_test(false);
    assert!(!manager.is_noisy_table(104));
    wait_until("all three peers to be promoted", || {
        NOISY_REGION_TO_NORMAL.get() - to_normal_before >= 3
    });
    cluster.must_put(&row_key(104, 4), b"v4");
    assert_eq!(cluster.must_get(&row_key(104, 4)), Some(b"v4".to_vec()));
}
