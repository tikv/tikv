// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{atomic::AtomicBool, mpsc, Arc};
use std::time::Duration;
use std::thread;

use super::cluster::{Cluster, Simulator};
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;
use raft::eraftpb::MessageType;

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

    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(peer, response_notifier);
    cluster
        .sim
        .write()
        .unwrap()
        .add_send_filter(peer, request_notifier);

    rx
}

// Validate that prevote is used in elections after partition or reboot of some nodes.
fn test_prevote<T: Simulator>(
    cluster: &mut Cluster<T>,
    failure_type: FailureType,
    leader_after_failure_id: impl Into<Option<u64>>,
    detect_during_failure: (u64, bool),
    detect_during_recovery: (u64, bool),
) {
    cluster.cfg.raft_store.prevote = true;

    let leader_id = 1;

    // We must start the cluster before adding send filters, otherwise it panics.
    cluster.run();

    cluster.must_transfer_leader(1, new_peer(leader_id, 1));
    cluster.must_put(b"k1", b"v1");

    // Determine how to fail.
    let rx = attach_prevote_notifiers(cluster, detect_during_failure.0);
    match failure_type {
        FailureType::Partition(majority, minority) => {
            cluster.partition(majority.clone().to_vec(), minority.clone().to_vec());
        }
        FailureType::Reboot(peers) => {
            peers.iter().for_each(|&peer| cluster.stop_node(peer));
        }
    };

    // Once we see a response on the wire we know a prevote round is happening.
    let received = rx.recv_timeout(Duration::from_secs(5));
    assert_eq!(
        received.is_ok(),
        detect_during_failure.1,
        "Sends a PreVote or PreVoteResponse during failure.",
    );

    if let Some(leader_id) = leader_after_failure_id.into() {
        cluster.must_transfer_leader(1, new_peer(leader_id, 1));
    }


    // Let the cluster recover.
    match failure_type {
        FailureType::Partition(_, _) => {
            cluster.clear_send_filters();
        }
        FailureType::Reboot(peers) => {
            cluster.clear_send_filters();
            peers.iter().for_each(|&peer| cluster.run_node(peer));
        }
    };

    // Prepare to listen.
    let rx = attach_prevote_notifiers(cluster, detect_during_recovery.0);

    // Once we see a response on the wire we know a prevote round is happening.
    let received = rx.recv_timeout(Duration::from_secs(5));

    cluster.must_put(b"k3", b"v3");
    assert_eq!(cluster.must_get(b"k1"), Some(b"v1".to_vec()));

    assert_eq!(
        received.is_ok(),
        detect_during_recovery.1,
        "Sends a PreVote or PreVoteResponse during recovery.",
    );
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
        (4, false),
    );
}

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
        (4, false),
    );
}

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
        (4, false),
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
        (1, false),
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
fn test_pair_isolated<T: Simulator>(cluster: &mut Cluster<T>) {
    let region = 1;
    let pd_client = Arc::clone(&cluster.pd_client);

    // Given some nodes A, B, C, D, E, we partition the cluster such that D, E are isolated from the rest.
    cluster.run();
    // Choose a predictable leader so we don't accidently partition the leader.
    cluster.must_transfer_leader(region, new_peer(1, 1));
    cluster.partition(vec![1, 2, 3], vec![4, 5]);

    // Then, add a policy to PD that it should ask the Raft leader to remove the peer from the group.
    pd_client.must_remove_peer(region, new_peer(4, 4));
    pd_client.must_remove_peer(region, new_peer(5, 5));

    // Verify the nodes have self removed.
    cluster.must_remove_region(4, region);
    cluster.must_remove_region(5, region);
}

#[test]
fn test_server_pair_isolated() {
    let mut cluster = new_server_cluster(0, 5);
    test_pair_isolated(&mut cluster);
}

fn test_isolated_follower_leader_does_not_change<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.run();
    cluster.must_transfer_leader(1, new_peer(1, 1));
    cluster.partition(vec![1,2,3,4], vec![5]);
    cluster.must_put(b"k1", b"v1");
    let election_timeout = cluster.cfg.raft_store.raft_base_tick_interval.0 * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    thread::sleep(election_timeout * 2);
    cluster.clear_send_filters();
    cluster.leader_of_region(1);
}

#[test]
fn test_server_isolated_follower_leader_does_not_change() {
    let mut cluster = new_server_cluster(0, 5);
    test_isolated_follower_leader_does_not_change(&mut cluster);
}
