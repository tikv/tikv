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
use std::sync::{Arc, RwLock};
use std::sync::atomic::*;
use std::time::*;
use std::collections::HashSet;

use time::Duration as TimeDuration;

use kvproto::eraftpb::{MessageType, ConfChangeType};
use kvproto::metapb::{Peer, Region};
use kvproto::raft_cmdpb::CmdType;
use kvproto::raft_serverpb::{RaftMessage, RaftLocalState};
use tikv::raftstore::{Error, Result};
use tikv::raftstore::store::keys;
use tikv::raftstore::store::engine::Peekable;
use tikv::storage;
use tikv::util::{escape, HandyRwLock};

use super::cluster::{Cluster, Simulator};
use super::node::new_node_cluster;
use super::transport_simulate::*;
use super::util::*;

#[derive(Clone, Default)]
struct LeaseReadFilter {
    ctx: Arc<RwLock<HashSet<Vec<u8>>>>,
    take: bool,
}

impl Filter<RaftMessage> for LeaseReadFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut ctx = self.ctx.wl();
        for m in msgs {
            let msg = m.mut_message();
            if msg.get_msg_type() == MessageType::MsgHeartbeat && !msg.get_context().is_empty() {
                ctx.insert(msg.get_context().to_owned());
            }
            if self.take {
                msg.take_context();
            }
        }
        Ok(())
    }
}

// Issue a read request on the specified peer.
fn read_on_peer<T: Simulator>(cluster: &mut Cluster<T>,
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

fn must_read_on_peer<T: Simulator>(cluster: &mut Cluster<T>,
                                   peer: Peer,
                                   region: Region,
                                   key: &[u8],
                                   value: &[u8]) {
    let timeout = Duration::from_secs(1);
    match read_on_peer(cluster, peer, region, key, timeout) {
        Ok(v) => {
            if v != value {
                panic!("read key {}, expect value {}, got {}",
                       escape(key),
                       escape(value),
                       escape(&v))
            }
        }
        Err(e) => panic!("failed to read for key {}, err {:?}", escape(key), e),
    }
}

fn must_error_read_on_peer<T: Simulator>(cluster: &mut Cluster<T>,
                                         peer: Peer,
                                         region: Region,
                                         key: &[u8],
                                         timeout: Duration) {
    if let Ok(value) = read_on_peer(cluster, peer, region, key, timeout) {
        panic!("key {}, expect error but got {}",
               escape(key),
               escape(&value));
    }
}

// A helper function for testing the lease reads and lease renewing.
// The leader keeps a record of its leader lease, and uses the system's
// monotonic raw clocktime to check whether its lease has expired.
// If the leader lease has not expired, when the leader receives a read request
//   1. with `read_quorum == false`, the leader will serve it by reading local data.
//      This way of handling request is called "lease read".
//   2. with `read_quorum == true`, the leader will serve it by doing index read (see raft's doc).
//      This way of handling request is called "index read".
// If the leader lease has expired, leader will serve both kinds of requests by index read, and
// propose an no-op entry to raft quorum to renew the lease.
// No matter what status the leader lease is, a write request is always served by writing a Raft
// log to the Raft quorum. It is called "consistent write". All writes are consistent writes.
// Every time the leader performs a consistent read/write, it will try to renew its lease.
fn test_renew_lease<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    // Increase the Raft tick interval to make this test case running reliably.
    cluster.cfg.raft_store.raft_base_tick_interval = 50;
    // Use large election timeout to make leadership stable.
    cluster.cfg.raft_store.raft_election_timeout_ticks = 10000;

    let max_lease = Duration::from_secs(2);
    cluster.cfg.raft_store.raft_store_max_leader_lease = TimeDuration::from_std(max_lease).unwrap();

    let node_id = 1u64;
    let store_id = 1u64;
    let peer = new_peer(store_id, node_id);
    cluster.pd_client.disable_default_rule();
    let region_id = cluster.run_conf_change();
    for id in 2..cluster.engines.len() as u64 + 1 {
        cluster.pd_client.must_add_peer(region_id, new_peer(id, id));
    }

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer` to become leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, peer.clone());
    let engine = cluster.get_engine(store_id);
    let state_key = keys::raft_state_key(region_id);
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    let last_index = state.get_last_index();

    let detector = LeaseReadFilter::default();
    cluster.add_send_filter(CloneFilterFactory(detector.clone()));

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 0);

    let mut expect_lease_read = 0;

    if cluster.engines.len() > 1 {
        // Wait for the leader lease to expire.
        thread::sleep(max_lease);

        // Issue a read request and check the value on response.
        must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");

        // Check if the leader does a index read and renewed its lease.
        assert_eq!(cluster.leader_of_region(region_id), Some(peer.clone()));
        expect_lease_read += 1;
        assert_eq!(detector.ctx.rl().len(), expect_lease_read);
    }

    // Wait for the leader lease to expire.
    thread::sleep(max_lease);

    // Issue a write request.
    cluster.must_put(key, b"v2");

    // Check if the leader has renewed its lease so that it can do lease read.
    assert_eq!(cluster.leader_of_region(region_id), Some(peer.clone()));
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v2");

    // Check if the leader does a local read.
    assert_eq!(detector.ctx.rl().len(), expect_lease_read);
}

#[test]
fn test_one_node_renew_lease() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

#[test]
fn test_node_renew_lease() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_renew_lease(&mut cluster);
}

// A helper function for testing the lease reads when the lease has expired.
// If the leader lease has expired, there may be new leader elected and
// the old leader will fail to renew its lease.
fn test_lease_expired<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer number check.
    pd_client.disable_default_rule();

    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    // Increase the Raft tick interval to make this test case running reliably.
    cluster.cfg.raft_store.raft_base_tick_interval = 50;

    let election_timeout = Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval) *
                           cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    let node_id = 3u64;
    let store_id = 3u64;
    let peer = new_peer(store_id, node_id);
    cluster.run();

    // Write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer` to become leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, peer.clone());

    // Isolate the leader `peer` from other peers.
    cluster.add_send_filter(IsolationFilterFactory::new(store_id));

    // Wait for the leader lease to expire and a new leader is elected.
    thread::sleep(election_timeout * 2);

    // Issue a read request and check the value on response.
    must_error_read_on_peer(cluster,
                            peer.clone(),
                            region.clone(),
                            key,
                            Duration::from_secs(1));
}

#[test]
fn test_node_lease_expired() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_expired(&mut cluster);
}

// A helper function for testing the leader holds unsafe lease during the leader transfer
// procedure, so it will not do lease read.
// Since raft will not propose any request during leader transfer procedure, consistent read/write
// could not be performed neither.
// When leader transfer procedure aborts later, the leader would use and update the lease as usual.
fn test_lease_unsafe_during_leader_transfers<T: Simulator>(cluster: &mut Cluster<T>) {
    // Avoid triggering the log compaction in this test case.
    cluster.cfg.raft_store.raft_log_gc_threshold = 100;
    // Increase the Raft tick interval to make this test case running reliably.
    cluster.cfg.raft_store.raft_base_tick_interval = 50;

    let base_tick = Duration::from_millis(cluster.cfg.raft_store.raft_base_tick_interval);
    let election_timeout = base_tick * cluster.cfg.raft_store.raft_election_timeout_ticks as u32;
    cluster.cfg.raft_store.raft_store_max_leader_lease = TimeDuration::from_std(election_timeout)
        .unwrap();

    let store_id = 1u64;
    let peer = new_peer(store_id, 1);
    let peer3_store_id = 3u64;
    let peer3 = new_peer(peer3_store_id, 3);
    cluster.run();

    let detector = LeaseReadFilter::default();
    cluster.add_send_filter(CloneFilterFactory(detector.clone()));

    // write the initial value for a key.
    let key = b"k";
    cluster.must_put(key, b"v1");
    // Force `peer1` to became leader.
    let region = cluster.get_region(key);
    let region_id = region.get_id();
    cluster.must_transfer_leader(region_id, peer.clone());

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");

    let engine = cluster.get_engine(store_id);
    let state_key = keys::raft_state_key(region_id);
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    let last_index = state.get_last_index();

    // Check if the leader does a local read.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index);
    assert_eq!(detector.ctx.rl().len(), 0);

    // Drop MsgTimeoutNow to `peer3` so that the leader transfer procedure would abort later.
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(region_id, peer3_store_id)
        .msg_type(MessageType::MsgTimeoutNow)
        .direction(Direction::Recv)));

    // Issue a transfer leader request to transfer leader from `peer` to `peer3`.
    cluster.transfer_leader(region_id, peer3);

    // Delay a while to ensure transfer leader procedure is triggered inside raft module.
    thread::sleep(election_timeout / 2);

    // Issue a read request and it will fall back to read index.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 1);

    // And read index should not update lease.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 2);

    // Make sure the leader transfer procedure timeouts.
    thread::sleep(election_timeout * 2);

    // Then the leader transfer procedure aborts, now the leader could do lease read or consistent
    // read/write and renew/reuse the lease as usual.

    // Issue a read request and check the value on response.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    assert_eq!(detector.ctx.rl().len(), 3);

    // Check if the leader also propose an entry to renew its lease.
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);

    // wait some time for the proposal to be applied.
    thread::sleep(election_timeout / 2);

    // Check if the leader does a local read.
    must_read_on_peer(cluster, peer.clone(), region.clone(), key, b"v1");
    let state: RaftLocalState = engine.get_msg_cf(storage::CF_RAFT, &state_key).unwrap().unwrap();
    assert_eq!(state.get_last_index(), last_index + 1);
    assert_eq!(detector.ctx.rl().len(), 3);
}

#[test]
fn test_node_lease_unsafe_during_leader_transfers() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    test_lease_unsafe_during_leader_transfers(&mut cluster);
}

/// test whether the read index callback will be handled when a region is destroyed.
/// If it's not handled properly, it will cause dead lock in transaction scheduler.
#[test]
fn test_node_callback_when_destroyed() {
    let count = 3;
    let mut cluster = new_node_cluster(0, count);
    cluster.run();
    cluster.must_put(b"k1", b"v1");
    let leader = cluster.leader_of_region(1).unwrap();
    let cc = new_change_peer_request(ConfChangeType::RemoveNode, leader.clone());
    let epoch = cluster.get_region_epoch(1);
    let req = new_admin_request(1, &epoch, cc);
    // so the leader can't commit the conf change yet.
    let block = Arc::new(AtomicBool::new(true));
    cluster.add_send_filter(CloneFilterFactory(RegionPacketFilter::new(1, leader.get_store_id())
        .msg_type(MessageType::MsgAppendResponse)
        .direction(Direction::Recv)
        .when(block.clone())));
    let mut filter = LeaseReadFilter::default();
    filter.take = true;
    // so the leader can't perform read index.
    cluster.add_send_filter(CloneFilterFactory(filter.clone()));
    // it always timeout, no need to wait.
    let _ = cluster.call_command_on_leader(req, Duration::from_millis(500));

    let get = new_get_cmd(b"k1");
    let req = new_request(1, epoch, vec![get], true);
    block.store(false, Ordering::SeqCst);
    let resp = cluster.call_command_on_leader(req, Duration::from_secs(3)).unwrap();
    assert!(!filter.ctx.rl().is_empty(),
            "read index should be performed");
    assert!(resp.get_header().get_error().has_region_not_found(),
            "{:?}",
            resp);
}
