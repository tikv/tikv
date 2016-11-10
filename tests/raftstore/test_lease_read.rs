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

//! A module contains test cases for lease read on Raft leader.

use std::thread;
use std::time::{Instant, Duration};

use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::CmdType;
use tikv::raftstore::{Error, Result};
use tikv::util::escape;

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::transport_simulate::*;
use super::util::*;

// Perform a local read on the specified peer.
fn do_local_read_on_peer<T: Simulator>(cluster: &mut Cluster<T>,
                                       peer: Peer,
                                       region: Region,
                                       key: &[u8],
                                       timeout: Duration)
                                       -> Result<Vec<u8>> {
    let mut request = new_request(region.get_id(),
                                  region.get_region_epoch().clone(),
                                  vec![new_get_cmd(key)],
                                  false);
    request.mut_header().set_peer(peer);
    let mut resp = try!(cluster.call_command(request, timeout));
    if resp.get_header().has_error() {
        return Err(Error::Other(box_err!(resp.mut_header().take_error().take_message())));
    }
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CmdType::Get);
    let mut get = resp.mut_responses()[0].take_get();
    assert!(get.has_value());
    Ok(get.take_value())
}

// A helper function for testing the lease reads on a raft leader with slow ticks.
// The leader with slow ticks could not tell that its lease has expired without delay.
// It will still serve the lease read, and cause external inconsistency.
// When `calibrate_tick` is set to `true`, the system's monotonic clocktime is used to
// calibrate ticks. So the leader could reject lease read when the time passes its lease,
// even when it has slow ticks.
fn test_lease_read<T: Simulator>(cluster: &mut Cluster<T>, calibrate_tick: bool) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    if !calibrate_tick {
        // Disable calibrating tick with clocktime.
        cluster.cfg.raft_store.calibrate_tick_with_clocktime = false;
    }
    let election_timeout = Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval) *
                           cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let mut slow_tick_config = cluster.cfg.clone();
    // Increase tick interval to simulate the slow tick.
    slow_tick_config.raft_store.raft_base_tick_interval = 100;
    let slow_election_timeout = Duration::from_millis(slow_tick_config.raft_store
        .raft_base_tick_interval) *
                                slow_tick_config.raft_store.raft_election_timeout_ticks as u32;
    let slow_node_id = 3u64;
    let slow_store_id = 3u64;
    let slow_peer = new_peer(slow_store_id, slow_node_id);
    cluster.set_node_config(slow_node_id, slow_tick_config);
    cluster.run();

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `slow_peer` to become leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, slow_peer.clone());
    assert_eq!(cluster.leader_of_region(region_id), Some(slow_peer.clone()));

    // Isolate `slow_peer` from other peers.
    cluster.add_send_filter(IsolationFilterFactory::new(slow_store_id));

    // After a long time passed, a new leader should be elected.
    if calibrate_tick {
        thread::sleep(slow_election_timeout * 2);
    } else {
        thread::sleep(election_timeout * 2);
    }
    let leader_store_id;
    let timeout = Duration::from_secs(3);
    let start_time = Instant::now();
    loop {
        cluster.reset_leader_of_region(region_id);
        if let Some(p) = cluster.leader_of_region(region_id) {
            if p != slow_peer {
                leader_store_id = p.get_store_id();
                break;
            }
        };
        if start_time.elapsed() >= timeout {
            panic!("failed to elect leader in time");
        }
    }

    // Write a new value for `key`, and verify it takes effect.
    let engine = cluster.get_engine(leader_store_id);
    must_get_equal(&engine, key, b"v1");
    cluster.must_put(key, b"v2");
    assert_eq!(b"v2".to_vec(), cluster.must_get(b"k").unwrap());
    must_get_equal(&engine, b"k", b"v2");

    // Check that the value for `key` in `slow_peer` db doesn't change by previous modify.
    let slow_peer_engine = cluster.get_engine(slow_store_id);
    must_get_equal(&slow_peer_engine, b"k", b"v1");

    // And then ensure the local read returns the stale value.
    match do_local_read_on_peer(cluster, slow_peer.clone(), region, key, timeout) {
        Ok(value) => {
            if calibrate_tick {
                panic!("should not read out value {} for key {} if calibrating ticks",
                       escape(&value),
                       escape(key))
            } else if value != b"v1" {
                panic!("key {}, expect value {}, got {}",
                       escape(key),
                       escape(b"v1"),
                       escape(&value))
            }
        }
        Err(e) => {
            if !calibrate_tick {
                panic!("failed to do local read for key {}, err {:?}",
                       escape(key),
                       e)
            }
        }
    }
}

#[test]
fn test_node_stale_lease_read() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_read(&mut cluster, false);
}

#[test]
fn test_server_stale_lease_read() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_lease_read(&mut cluster, false);
}

#[test]
fn test_node_consistent_lease_read() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_read(&mut cluster, true);
}

#[test]
fn test_server_consistent_lease_read() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_lease_read(&mut cluster, true);
}
