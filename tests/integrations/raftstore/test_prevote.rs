// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicBool, mpsc, Arc};
use std::thread;
use std::time::Duration;

use raft::eraftpb::MessageType;
use tikv_util::HandyRwLock;

use test_raftstore::*;

enum FailureType<'a> {
    Partition(&'a [u64], &'a [u64]),
    Reboot(&'a [u64]),
}

fn attach_prevote_notifiers<T: Simulator>(cluster: &Cluster<T>, peer: u64) -> mpsc::Receiver<()> {
    // Setup a notifier
    let (tx, rx) = mpsc::channel();
    let response_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));
    let request_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVote,
        tx.clone(),
        Arc::from(AtomicBool::new(true)),
    ));

    cluster.sim.wl().add_send_filter(peer, response_notifier);
    cluster.sim.wl().add_send_filter(peer, request_notifier);

    rx
}

// Validate that prevote is used in elections after partition or reboot of some nodes.
fn test_prevote<T: Simulator>(
    cluster: &mut Cluster<T>,
    failure_type: FailureType<'_>,
    leader_after_failure_id: impl Into<Option<u64>>,
    detect_during_failure: impl Into<Option<(u64, bool)>>,
    detect_during_recovery: impl Into<Option<(u64, bool)>>,
) {
    cluster.cfg.raft_store.prevote = true;
    // To stable the test, we use a large election timeout to make
    // leader's readiness get handle within an election timeout
    configure_for_lease_read(cluster, Some(10), Some(50));

    let leader_id = 1;
    let detect_during_failure = detect_during_failure.into();
    let detect_during_recovery = detect_during_recovery.into();

    // We must start the cluster before adding send filters, otherwise it panics.
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(leader_id, 1));
    cluster.must_put(b"k1", b"v1");

    // Determine how to fail.
    let rx = if let Some((id, _)) = detect_during_failure {
        let rx = attach_prevote_notifiers(cluster, id);
        debug!("Attached failure prevote notifier.");
        Some(rx)
    } else {
        None
    };

    match failure_type {
        FailureType::Partition(majority, minority) => {
            cluster.partition(majority.to_vec(), minority.to_vec());
        }
        FailureType::Reboot(peers) => {
            peers.iter().for_each(|&peer| cluster.stop_node(peer));
        }
    };

    if let (Some(rx), Some((_, should_detect))) = (rx, detect_during_failure) {
        // Once we see a response on the wire we know a prevote round is happening.
        let received = rx.recv_timeout(Duration::from_secs(5));
        debug!("Done with failure prevote notifier, got {:?}", received);
        assert_eq!(
            received.is_ok(),
            should_detect,
            "Sends a PreVote or PreVoteResponse during failure.",
        );
    }

    // Let the cluster recover.
    match failure_type {
        FailureType::Partition(_, _) => {
            cluster.clear_send_filters();
        }
        FailureType::Reboot(peers) => {
            cluster.clear_send_filters();
            peers.iter().for_each(|&peer| {
                cluster.run_node(peer).unwrap();
            });
        }
    };

    // Prepare to listen.
    let rx = if let Some((id, _)) = detect_during_recovery {
        let rx = attach_prevote_notifiers(cluster, id);
        debug!("Attached recovery prevote notifier.");
        Some(rx)
    } else {
        None
    };

    if let Some(leader_id) = leader_after_failure_id.into() {
        cluster.must_transfer_leader(1, new_peer(leader_id, 1));
    };

    // Once we see a response on the wire we know a prevote round is happening.
    if let (Some(rx), Some((_, should_detect))) = (rx, detect_during_failure) {
        let received = rx.recv_timeout(Duration::from_secs(5));
        debug!("Done with recovery prevote notifier, got {:?}", received);

        assert_eq!(
            received.is_ok(),
            should_detect,
            "Sends a PreVote or PreVoteResponse during recovery.",
        );
    };

    cluster.must_put(b"k3", b"v3");
    assert_eq!(cluster.must_get(b"k1"), Some(b"v1".to_vec()));
}

#[test]
fn test_prevote_partition_leader_in_majority_detect_in_majority() {
    let mut cluster = new_server_cluster(0, 5);
    // Since the leader is in the majority and not rebooted, it sees no prevote.
    test_prevote(
        &mut cluster,
        FailureType::Partition(&[1, 2, 3], &[4, 5]),
        None,
        (1, false),
        (1, false),
    );
}

// TODO: Enable detect after failure when we can reliably capture the prevote.
#[test]
fn test_prevote_partition_leader_in_majority_detect_in_minority() {
    let mut cluster = new_server_cluster(0, 5);
    // The follower is in the minority and is part of a prevote process. On rejoin it adopts the
    // old leader.
    test_prevote(
        &mut cluster,
        FailureType::Partition(&[1, 2, 3], &[4, 5]),
        None,
        (4, true),
        None,
    );
}

// TODO: Enable detect after failure when we can reliably capture the prevote.
#[test]
fn test_prevote_partition_leader_in_minority_detect_in_majority() {
    let mut cluster = new_server_cluster(0, 5);
    // The follower is in the minority and is part of a prevote process. On rejoin it adopts the
    // old leader.
    test_prevote(
        &mut cluster,
        FailureType::Partition(&[1, 2], &[3, 4, 5]),
        None,
        (4, true),
        None,
    );
}

// TODO: Enable detect after failure when we can reliably capture the prevote.
#[test]
fn test_prevote_partition_leader_in_minority_detect_in_minority() {
    let mut cluster = new_server_cluster(0, 5);
    // The follower is in the minority and is part of a prevote process. On rejoin it adopts the
    // old leader.
    test_prevote(
        &mut cluster,
        FailureType::Partition(&[1, 2, 3], &[3, 4, 5]),
        None,
        (4, true),
        None,
    );
}

#[test]
fn test_prevote_reboot_majority_followers() {
    let mut cluster = new_server_cluster(0, 5);
    // A prevote round will start, but nothing will succeed.
    test_prevote(
        &mut cluster,
        FailureType::Reboot(&[3, 4, 5]),
        1,
        (1, true),
        None,
    );
}

#[test]
fn test_prevote_reboot_minority_followers() {
    let mut cluster = new_server_cluster(0, 5);
    // A prevote round will start, but nothing will succeed until recovery.
    test_prevote(
        &mut cluster,
        FailureType::Reboot(&[4, 5]),
        None,
        (2, false),
        (2, false),
    );
}

// Test isolating a minority of the cluster and make sure that the remove themselves.
#[cfg(feature = "protobuf_codec")]
fn test_pair_isolated<T: Simulator>(cluster: &mut Cluster<T>) {
    let region = 1;
    let pd_client = Arc::clone(&cluster.pd_client);

    // Given some nodes A, B, C, D, E, we partition the cluster such that D, E are isolated from the rest.
    cluster.run();
    // Choose a predictable leader so we don't accidentally partition the leader.
    cluster.must_transfer_leader(region, new_peer(1, 1));
    cluster.partition(vec![1, 2, 3], vec![4, 5]);

    // Then, add a policy to PD that it should ask the Raft leader to remove the peer from the group.
    pd_client.must_remove_peer(region, new_peer(4, 4));
    pd_client.must_remove_peer(region, new_peer(5, 5));

    // Verify the nodes have self removed.
    cluster.must_remove_region(4, region);
    cluster.must_remove_region(5, region);
}

// FIXME(nrc) failing on CI only
#[cfg(feature = "protobuf_codec")]
#[test]
fn test_server_pair_isolated() {
    let mut cluster = new_server_cluster(0, 5);
    test_pair_isolated(&mut cluster);
}

fn test_isolated_follower_leader_does_not_change<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.must_put(b"k1", b"v1");
    let region_status = new_status_request(1, new_peer(1, 1), new_region_leader_cmd());
    let resp = cluster
        .call_command(region_status.clone(), Duration::from_secs(5))
        .unwrap();
    let term = resp.get_header().get_current_term();
    // Isolate peer5.
    cluster.partition(vec![1, 2, 3, 4], vec![5]);
    let election_timeout = cluster.cfg.raft_store.raft_base_tick_interval.0
        * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    // Peer5 should not increase its term.
    thread::sleep(election_timeout * 2);
    // Now peer5 can send messages to others
    cluster.clear_send_filters();
    thread::sleep(election_timeout * 2);
    cluster.must_put(b"k1", b"v1");
    // Peer1 is still the leader.
    let leader = cluster.leader_of_region(1);
    assert_eq!(leader, Some(new_peer(1, 1)));
    // And the term is not changed.
    let resp = cluster
        .call_command(region_status, Duration::from_secs(5))
        .unwrap();
    let current_term = resp.get_header().get_current_term();
    assert_eq!(term, current_term);
}

#[test]
fn test_server_isolated_follower_leader_does_not_change() {
    let mut cluster = new_server_cluster(0, 5);
    test_isolated_follower_leader_does_not_change(&mut cluster);
}

fn test_create_peer_from_pre_vote<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");

    let rx = attach_prevote_notifiers(cluster, 1);
    cluster.add_send_filter(IsolationFilterFactory::new(2));
    pd_client.must_add_peer(r1, new_peer(2, 2));

    if rx.recv_timeout(Duration::from_secs(3)).is_err() {
        panic!("peer 1 should send pre vote");
    }

    // The peer 2 should be created.
    cluster.clear_send_filters();
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
}

#[test]
fn test_node_create_peer_from_pre_vote() {
    let mut cluster = new_node_cluster(0, 2);
    cluster.cfg.raft_store.prevote = true;
    test_create_peer_from_pre_vote(&mut cluster);
}
