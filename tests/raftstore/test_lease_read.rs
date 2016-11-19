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
use std::time::Duration;

use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::CmdType;
use kvproto::raft_serverpb::RaftLocalState;
use tikv::raftstore::{Error, Result};
use tikv::raftstore::store::keys;
use tikv::raftstore::store::engine::Peekable;
use tikv::storage;
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

// A helper function for testing the lease reads and lease renewing.
// The leader keep a record of its leader lease, and use the system's
// monotonic raw clocktime to check whether its lease has been expired.
// If the leader lease is valid, the leader serves read requests as the lease reads.
// Otherwise, it handles read requests as consistent reads, which would be replicated
// to the raft quorum.
// When the leader performs consistent read or write, it will try to renew its lease.
fn test_renew_lease<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    // Increase raft tick interval to make this test case running reliably.
    cluster.cfg.raft_store.raft_base_tick_interval = 50;

    let election_timeout = Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval) *
                           cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let test_node_id = 1u64;
    let test_store_id = 1u64;
    let test_peer = new_peer(test_store_id, test_node_id);
    cluster.run();

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `test_peer` to become leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, test_peer.clone());
    assert_eq!(cluster.leader_of_region(region_id), Some(test_peer.clone()));
    let engine = cluster.get_engine(test_store_id);
    let state_key = keys::raft_state_key(region_id);
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    let last_index = state.get_last_index();

    // Issue a read request and check the value on response.
    let timeout = Duration::from_secs(1);
    match do_local_read_on_peer(cluster, test_peer.clone(), region.clone(), key, timeout) {
        Ok(value) => {
            if value != b"v1" {
                panic!("key {}, expect value {}, got {}",
                       escape(key),
                       escape(b"v1"),
                       escape(&value))
            }
        }
        Err(e) => {
            panic!("failed to do local read for key {}, err {:?}",
                   escape(key),
                   e)
        }
    }
    // Check the leader did a local read.
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index);

    // Wait for the leader lease to expire.
    thread::sleep(election_timeout);

    // Issue a read request and check the value on response.
    let timeout = Duration::from_secs(1);
    match do_local_read_on_peer(cluster, test_peer.clone(), region.clone(), key, timeout) {
        Ok(value) => {
            if value != b"v1" {
                panic!("key {}, expect value {}, got {}",
                       escape(key),
                       escape(b"v1"),
                       escape(&value))
            }
        }
        Err(e) => {
            panic!("failed to do local read for key {}, err {:?}",
                   escape(key),
                   e)
        }
    }

    // Check the leader did a consistent read and renewed its lease.
    assert_eq!(cluster.leader_of_region(region_id), Some(test_peer.clone()));
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);

    // Wait for the leader lease to expire.
    thread::sleep(election_timeout);

    // Issue a write request.
    cluster.must_put(key, b"v2");

    // Check the leader has renewed its lease so that it could do lease read.
    assert_eq!(cluster.leader_of_region(region_id), Some(test_peer.clone()));
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 2);

    // Issue a read request and check the value on response.
    let timeout = Duration::from_secs(1);
    match do_local_read_on_peer(cluster, test_peer.clone(), region, key, timeout) {
        Ok(value) => {
            if value != b"v2" {
                panic!("key {}, expect value {}, got {}",
                       escape(key),
                       escape(b"v1"),
                       escape(&value))
            }
        }
        Err(e) => {
            panic!("failed to do local read for key {}, err {:?}",
                   escape(key),
                   e)
        }
    }
    // Check the leader did a local read.
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 2);
}

#[test]
fn test_one_node_renew_lease() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

#[test]
fn test_one_server_renew_lease() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    test_renew_lease(&mut cluster);
}

#[test]
fn test_node_renew_lease() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

#[test]
fn test_server_renew_lease() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_renew_lease(&mut cluster);
}

// A helper function for testing the lease reads when the lease has expired.
// When the leader lease is expired, there may be new leader elected and
// the old leader will fail to renew its lease.
fn test_lease_expired<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    // Increase raft tick interval to make this test case running reliably.
    cluster.cfg.raft_store.raft_base_tick_interval = 50;

    let election_timeout = Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval) *
                           cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let test_node_id = 3u64;
    let test_store_id = 3u64;
    let test_peer = new_peer(test_store_id, test_node_id);
    cluster.run();

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `test_peer` to become leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, test_peer.clone());
    assert_eq!(cluster.leader_of_region(region_id), Some(test_peer.clone()));

    // Isolate `test_peer` from other peers.
    cluster.add_send_filter(IsolationFilterFactory::new(test_store_id));

    // Wait for the leader lease to expire and a new leader is elected.
    thread::sleep(election_timeout * 2);

    // Issue a read request and check the value on response.
    let timeout = Duration::from_secs(1);
    if let Ok(value) = do_local_read_on_peer(cluster, test_peer.clone(), region, key, timeout) {
        panic!("key {}, expect error but got {}",
               escape(key),
               escape(&value))
    }

    // Recover the network for `test_peer`.
    cluster.clear_send_filters();

    // Check a new leader elected for the cluster.
    cluster.reset_leader_of_region(region_id);
    assert_ne!(cluster.leader_of_region(region_id), Some(test_peer.clone()));
}

#[test]
fn test_node_lease_expired() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_expired(&mut cluster);
}

#[test]
fn test_server_lease_expired() {
    let count = 3;
    let mut cluster = new_server_cluster(0, count);
    test_lease_expired(&mut cluster);
}
