// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::atomic::AtomicBool;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use engine_traits::CF_WRITE;
use kvproto::metapb::Region;
use kvproto::raft_serverpb::RaftMessage;
use pd_client::PdClient;
use raft::eraftpb::MessageType;
use raftstore::store::util::is_vote_msg;
use raftstore::Result;
use tikv_util::HandyRwLock;

use test_raftstore::*;
use tikv_util::config::{ReadableDuration, ReadableSize};

#[test]
fn test_follower_slow_split() {
    let mut cluster = new_node_cluster(0, 3);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();
    cluster.run();
    let region = cluster.get_region(b"");

    // Only need peer 1 and 3. Stop node 2 to avoid extra vote messages.
    cluster.must_transfer_leader(1, new_peer(1, 1));
    pd_client.must_remove_peer(1, new_peer(2, 2));
    cluster.stop_node(2);

    // Use a channel to retrieve start_key and end_key in pre-vote messages.
    let (range_tx, range_rx) = mpsc::channel();
    let prevote_filter = PrevoteRangeFilter {
        // Only send 1 pre-vote message to peer 3 so if peer 3 drops it,
        // it needs to start a new election.
        filter: RegionPacketFilter::new(1000, 1) // new region id is 1000
            .msg_type(MessageType::MsgRequestPreVote)
            .direction(Direction::Send)
            .allow(1),
        tx: Mutex::new(range_tx),
    };
    cluster
        .sim
        .wl()
        .add_send_filter(1, Box::new(prevote_filter));

    // Ensure pre-vote response is really sended.
    let (tx, rx) = mpsc::channel();
    let prevote_resp_notifier = Box::new(MessageTypeNotifier::new(
        MessageType::MsgRequestPreVoteResponse,
        tx,
        Arc::from(AtomicBool::new(true)),
    ));
    cluster.sim.wl().add_send_filter(3, prevote_resp_notifier);

    // After split, pre-vote message should be sent to peer 2.
    fail::cfg("apply_before_split_1_3", "pause").unwrap();
    cluster.must_split(&region, b"k2");
    let range = range_rx.recv_timeout(Duration::from_millis(100)).unwrap();
    assert_eq!(range.0, b"");
    assert_eq!(range.1, b"k2");

    // After the follower split success, it will response to the pending vote.
    fail::cfg("apply_before_split_1_3", "off").unwrap();
    assert!(rx.recv_timeout(Duration::from_millis(100)).is_ok());
}

fn gen_split_region() -> (Region, Region, Region) {
    let mut cluster = new_server_cluster(0, 2);
    let region_max_size = 50000;
    let region_split_size = 30000;
    cluster.cfg.raft_store.split_region_check_tick_interval = ReadableDuration::millis(20);
    cluster.cfg.coprocessor.region_max_size = ReadableSize(region_max_size);
    cluster.cfg.coprocessor.region_split_size = ReadableSize(region_split_size);

    let mut range = 1..;
    cluster.run();
    let pd_client = Arc::clone(&cluster.pd_client);
    let region = pd_client.get_region(b"").unwrap();
    let last_key = put_till_size(&mut cluster, region_split_size, &mut range);
    let target = pd_client.get_region(&last_key).unwrap();

    assert_eq!(region, target);

    let max_key = put_cf_till_size(&mut cluster, CF_WRITE, region_max_size, &mut range);

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();
    if left == right {
        cluster.wait_region_split_max_cnt(&region, 20, 10, false);
    }

    let left = pd_client.get_region(b"").unwrap();
    let right = pd_client.get_region(&max_key).unwrap();

    (region, left, right)
}

#[test]
fn test_pause_split_when_snap_gen_will_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    fail::cfg(is_generating_snapshot, "return()").unwrap();

    let (region, left, right) = gen_split_region();

    assert_ne!(left, right);
    assert_eq!(region.get_start_key(), left.get_start_key());
    assert_eq!(region.get_end_key(), right.get_end_key());

    fail::remove(is_generating_snapshot);
}

#[test]
fn test_pause_split_when_snap_gen_never_split() {
    let is_generating_snapshot = "is_generating_snapshot";
    let region_split_skip_max_count = "region_split_skip_max_count";
    fail::cfg(region_split_skip_max_count, "return()").unwrap();
    fail::cfg(is_generating_snapshot, "return()").unwrap();

    let (region, left, right) = gen_split_region();

    assert_eq!(region, left);
    assert_eq!(left, right);

    fail::remove(is_generating_snapshot);
    fail::remove(region_split_skip_max_count);
}

// Filter prevote message and record the range.
struct PrevoteRangeFilter {
    filter: RegionPacketFilter,
    tx: Mutex<mpsc::Sender<(Vec<u8>, Vec<u8>)>>,
}

impl Filter for PrevoteRangeFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        self.filter.before(msgs)?;
        if let Some(msg) = msgs.iter().filter(|m| is_vote_msg(m.get_message())).last() {
            let start_key = msg.get_start_key().to_owned();
            let end_key = msg.get_end_key().to_owned();
            let tx = self.tx.lock().unwrap();
            let _ = tx.send((start_key, end_key));
        }
        Ok(())
    }
}

#[test]
fn test_split_not_to_split_exist_region() {
    let mut cluster = new_node_cluster(0, 4);
    configure_for_merge(&mut cluster);
    cluster.cfg.raft_store.right_derive_when_split = true;
    cluster.cfg.raft_store.apply_batch_system.max_batch_size = 1;
    cluster.cfg.raft_store.apply_batch_system.pool_size = 2;
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let r1 = cluster.run_conf_change();
    pd_client.must_add_peer(r1, new_peer(2, 2));
    pd_client.must_add_peer(r1, new_peer(3, 3));

    let mut region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");
    cluster.put(b"k1", b"v1").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    region = pd_client.get_region(b"k1").unwrap();
    let peer_1 = find_peer(&region, 1).cloned().unwrap();
    cluster.must_transfer_leader(region.get_id(), peer_1);

    let peer_3 = find_peer(&region, 3).cloned().unwrap();
    assert_eq!(peer_3.get_id(), 1003);
    let on_handle_apply_1003_fp = "on_handle_apply_1003";
    fail::cfg(on_handle_apply_1003_fp, "pause").unwrap();

    // [-∞, k1), [k1, k2), [k2, +∞)
    cluster.must_split(&region, b"k1");

    pd_client.must_remove_peer(region.get_id(), peer_3);
    pd_client.must_add_peer(region.get_id(), new_peer(4, 4));

    let left_region = pd_client.get_region(b"k0").unwrap();
    let left_peer_3 = find_peer(&left_region, 3).cloned().unwrap();
    pd_client.must_remove_peer(left_region.get_id(), left_peer_3);
    pd_client.must_add_peer(left_region.get_id(), new_peer(4, 5));

    pd_client.must_merge(region.get_id(), left_region.get_id());

    let right_region = pd_client.get_region(b"k2").unwrap();
    let right_peer_3 = find_peer(&right_region, 3).cloned().unwrap();
    pd_client.must_remove_peer(right_region.get_id(), right_peer_3);
    pd_client.must_add_peer(right_region.get_id(), new_peer(4, 6));

    pd_client.must_merge(right_region.get_id(), left_region.get_id());

    region = pd_client.get_region(b"k1").unwrap();
    cluster.must_split(&region, b"k2");

    let peer_4 = find_peer(&region, 4).cloned().unwrap();
    pd_client.must_remove_peer(region.get_id(), peer_4);
    pd_client.must_add_peer(region.get_id(), new_peer(3, 7));

    cluster.put(b"k2", b"v2").unwrap();
    must_get_equal(&cluster.get_engine(3), b"k2", b"v2");

    fail::remove(on_handle_apply_1003_fp);

    must_get_none(&cluster.get_engine(3), b"k1");
}
