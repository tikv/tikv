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

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use raft::eraftpb::MessageType;

use test_raftstore::*;
use tikv::util::config::*;

fn test_basic_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.raft_heartbeat_ticks = 20;
    let reserved_time = Duration::from_millis(
        cluster.cfg.raft_store.raft_base_tick_interval.as_millis()
            * cluster.cfg.raft_store.raft_heartbeat_ticks as u64,
    );
    cluster.run();

    // transfer leader to (2, 2) first to make address resolve happen early.
    cluster.must_transfer_leader(1, new_peer(2, 2));
    cluster.must_transfer_leader(1, new_peer(1, 1));

    let mut region = cluster.get_region(b"k3");

    // ensure follower has latest entries before transfer leader.
    cluster.must_put(b"k1", b"v1");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // check if transfer leader is fast enough.
    let leader = cluster.leader_of_region(1).unwrap();
    let admin_req = new_transfer_leader_cmd(new_peer(2, 2));
    let mut req = new_admin_request(1, region.get_region_epoch(), admin_req);
    req.mut_header().set_peer(leader);
    cluster.call_command(req, Duration::from_secs(3)).unwrap();
    thread::sleep(reserved_time);
    assert_eq!(
        cluster.query_leader(2, 1, Duration::from_secs(5)),
        Some(new_peer(2, 2))
    );

    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_put_cmd(b"k3", b"v3")],
        false,
    );
    req.mut_header().set_peer(new_peer(2, 2));
    // transfer leader to (1, 1)
    cluster.must_transfer_leader(1, new_peer(1, 1));
    // send request to old leader (2, 2) directly and verify it fails
    let resp = cluster.call_command(req, Duration::from_secs(5)).unwrap();
    assert!(resp.get_header().get_error().has_not_leader());
}

#[test]
fn test_server_basic_transfer_leader() {
    let mut cluster = new_server_cluster(0, 3);
    test_basic_transfer_leader(&mut cluster);
}

#[test]
fn test_node_basic_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    test_basic_transfer_leader(&mut cluster);
}

fn test_pd_transfer_leader<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    cluster.run();

    cluster.must_put(b"k", b"v");

    // call command on this leader directly, must successfully.
    let mut region = cluster.get_region(b"");
    let mut req = new_request(
        region.get_id(),
        region.take_region_epoch(),
        vec![new_get_cmd(b"k")],
        false,
    );

    for id in 1..4 {
        // select a new leader to transfer
        pd_client.transfer_leader(region.get_id(), new_peer(id, id));

        for _ in 0..100 {
            // reset leader and wait transfer successfully.
            cluster.reset_leader_of_region(1);

            sleep_ms(20);

            if let Some(leader) = cluster.leader_of_region(1) {
                if leader.get_id() == id {
                    break;
                }
            }
        }

        assert_eq!(cluster.leader_of_region(1), Some(new_peer(id, id)));
        req.mut_header().set_peer(new_peer(id, id));
        debug!("requesting {:?}", req);
        let resp = cluster
            .call_command(req.clone(), Duration::from_secs(5))
            .unwrap();
        assert!(!resp.get_header().has_error(), "{:?}", resp);
        assert_eq!(resp.get_responses()[0].get_get().get_value(), b"v");
    }
}

#[test]
fn test_server_pd_transfer_leader() {
    let mut cluster = new_server_cluster(0, 3);
    test_pd_transfer_leader(&mut cluster);
}

#[test]
fn test_node_pd_transfer_leader() {
    let mut cluster = new_node_cluster(0, 3);
    test_pd_transfer_leader(&mut cluster);
}

fn test_transfer_leader_during_snapshot<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = Arc::clone(&cluster.pd_client);
    // Disable default max peer count check.
    pd_client.disable_default_operator();
    cluster.cfg.raft_store.raft_log_gc_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.raft_store.raft_log_gc_count_limit = 2;
    cluster.cfg.raft_store.merge_max_log_gap = 1;

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));

    for i in 0..1024 {
        let key = format!("{:01024}", i);
        let value = format!("{:01024}", i);
        cluster.must_put(key.as_bytes(), value.as_bytes());
    }

    cluster.must_transfer_leader(r1, new_peer(1, 1));

    // hook transport and drop all snapshot packet, so follower's status
    // will stay at snapshot.
    cluster.add_send_filter(DefaultFilterFactory::<SnapshotFilter>::default());
    // don't allow leader transfer succeed if it is actually triggered.
    cluster.add_send_filter(CloneFilterFactory(
        RegionPacketFilter::new(1, 2)
            .msg_type(MessageType::MsgTimeoutNow)
            .direction(Direction::Recv),
    ));

    pd_client.must_add_peer(r1, new_peer(3, 3));
    // a just added peer needs wait a couple of ticks, it'll communicate with leader
    // before getting snapshot
    sleep_ms(1000);

    let epoch = cluster.get_region_epoch(1);
    let put = new_request(1, epoch, vec![new_put_cmd(b"k1", b"v1")], false);
    cluster.transfer_leader(r1, new_peer(2, 2));
    let resp = cluster.call_command_on_leader(put, Duration::from_secs(5));
    // if it's transferring leader, resp will timeout.
    assert!(resp.is_ok(), "{:?}", resp);
}

#[test]
fn test_server_transfer_leader_during_snapshot() {
    let mut cluster = new_server_cluster(0, 3);
    test_transfer_leader_during_snapshot(&mut cluster);
}

#[test]
fn test_node_transfer_leader_during_snapshot() {
    let mut cluster = new_node_cluster(0, 3);
    test_transfer_leader_during_snapshot(&mut cluster);
}
