// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Duration;

use engine_traits::{RaftEngine, RaftEngineDebug};
use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::MessageType;
use raftstore::store::*;
use test_raftstore::*;

fn delete_old_data<E: RaftEngineDebug>(engine: &E, id: u64) {
    let mut deleter = engine.log_batch(0);
    let mut last_index = 0;
    engine
        .scan_entries(id, |e| {
            last_index = e.get_index();
            Ok(true)
        })
        .unwrap();
    let state = RaftLocalState {
        last_index,
        ..Default::default()
    };
    engine
        .clean(id, 0 /*first_index*/, &state, &mut deleter)
        .unwrap();
    engine.consume(&mut deleter, true /*sync*/).unwrap();
}

/// Allow lost situation.
#[derive(PartialEq, Eq, Clone, Copy)]
enum DataLost {
    /// The leader loses commit index.
    ///
    /// A leader can't lost both the committed entries and commit index
    /// at the same time.
    LeaderCommit,
    /// A follower loses commit index.
    FollowerCommit,
    /// All the nodes loses data.
    ///
    /// Typically, both leader and followers lose commit index.
    AllLost,
}

fn test<A, C>(cluster: &mut Cluster<NodeCluster>, action: A, check: C, mode: DataLost)
where
    A: FnOnce(&mut Cluster<NodeCluster>),
    C: FnOnce(&mut Cluster<NodeCluster>),
{
    let filter = match mode {
        DataLost::AllLost | DataLost::LeaderCommit => RegionPacketFilter::new(1, 1)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
        DataLost::FollowerCommit => RegionPacketFilter::new(1, 3)
            .msg_type(MessageType::MsgAppendResponse)
            .direction(Direction::Recv),
    };
    cluster.add_send_filter(CloneFilterFactory(filter));

    let ids = if mode == DataLost::AllLost {
        vec![1, 2, 3]
    } else {
        vec![1]
    };
    let last_index: Vec<_> = ids
        .iter()
        .map(|id| cluster.raft_local_state(1, *id).get_last_index())
        .collect();

    action(cluster);

    let snaps: Vec<_> = ids
        .iter()
        .zip(last_index)
        .map(|(id, index)| {
            cluster.wait_last_index(1, *id, index + 1, Duration::from_secs(3));
            cluster.get_raft_engine(*id).dump_all_data(*id)
        })
        .collect();

    cluster.clear_send_filters();
    check(cluster);
    for id in &ids {
        cluster.stop_node(*id);
    }
    // Simulate data lost in raft cf.
    for (id, mut batch) in ids.iter().zip(snaps) {
        delete_old_data(&cluster.get_raft_engine(*id), *id);
        cluster
            .get_raft_engine(*id)
            .consume(&mut batch, true /*sync*/)
            .unwrap();
    }
    for id in &ids {
        cluster.run_node(*id).unwrap();
    }

    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    }
}

/// Test whether system can recover from mismatched raft state and apply state.
///
/// If TiKV is shutdown, apply state may go ahead of raft
/// state. TiKV should be able to recognize the situation and start normally.
fn test_early_apply(mode: DataLost) {
    let mut cluster = new_node_cluster(0, 3);
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    if mode == DataLost::LeaderCommit || mode == DataLost::AllLost {
        cluster.must_transfer_leader(1, new_peer(1, 1));
    } else {
        cluster.must_transfer_leader(1, new_peer(3, 3));
    }
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    test(
        &mut cluster,
        |c| {
            c.async_put(b"k2", b"v2").unwrap();
        },
        |c| must_get_equal(&c.get_engine(1), b"k2", b"v2"),
        mode,
    );
    let region = cluster.get_region(b"");
    test(
        &mut cluster,
        |c| {
            c.split_region(&region, b"k2", Callback::None);
        },
        |c| c.wait_region_split(&region),
        mode,
    );
    if mode != DataLost::LeaderCommit && mode != DataLost::AllLost {
        test(
            &mut cluster,
            |c| {
                c.async_remove_peer(1, new_peer(1, 1)).unwrap();
            },
            |c| must_get_none(&c.get_engine(1), b"k2"),
            mode,
        );
    }
}

/// Tests whether the cluster can recover from leader lost its commit index.
#[test]
fn test_leader_early_apply() {
    test_early_apply(DataLost::LeaderCommit)
}

/// Tests whether the cluster can recover from follower lost its commit index.
#[test]
fn test_follower_commit_early_apply() {
    test_early_apply(DataLost::FollowerCommit)
}

/// Tests whether the cluster can recover from all nodes lost their commit index.
#[test]
fn test_all_node_crash() {
    test_early_apply(DataLost::AllLost)
}

/// Tests if apply index inside raft is updated correctly.
///
/// If index is not updated, raft will reject to campaign on timeout.
#[test]
fn test_update_internal_apply_index() {
    let mut cluster = new_node_cluster(0, 4);
    cluster.pd_client.disable_default_operator();
    // So compact log will not be triggered automatically.
    configure_for_request_snapshot(&mut cluster);
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(3, 3));
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v1");

    let filter = RegionPacketFilter::new(1, 3)
        .msg_type(MessageType::MsgAppendResponse)
        .direction(Direction::Recv);
    cluster.add_send_filter(CloneFilterFactory(filter));
    let last_index = cluster.raft_local_state(1, 1).get_last_index();
    cluster.async_remove_peer(1, new_peer(4, 4)).unwrap();
    cluster.async_put(b"k2", b"v2").unwrap();
    let mut snaps = Vec::new();
    for id in 1..3 {
        cluster.wait_last_index(1, id, last_index + 2, Duration::from_secs(3));
        snaps.push((id, cluster.get_raft_engine(id).dump_all_data(id)));
    }
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(1), b"k2", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k2", b"v2");

    // Simulate data lost in raft cf.
    for (id, mut batch) in snaps {
        cluster.stop_node(id);
        delete_old_data(&cluster.get_raft_engine(id), id);
        cluster
            .get_raft_engine(id)
            .consume(&mut batch, true /*sync*/)
            .unwrap();
        cluster.run_node(id).unwrap();
    }

    let region = cluster.get_region(b"k1");
    // Issues a heartbeat to followers so they will re-commit the logs.
    let resp = read_on_peer(
        &mut cluster,
        new_peer(3, 3),
        region,
        b"k1",
        true,
        Duration::from_secs(3),
    )
    .unwrap();
    assert!(!resp.get_header().has_error(), "{:?}", resp);
    cluster.stop_node(3);
    cluster.must_put(b"k3", b"v3");
}
