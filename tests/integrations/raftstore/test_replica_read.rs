// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::mem;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Duration;

use kvproto::raft_serverpb::RaftMessage;
use raft::eraftpb::MessageType;
use test_raftstore::*;
use tikv::raftstore::store::Callback;
use tikv::raftstore::Result;
use tikv_util::config::*;
use tikv_util::HandyRwLock;

#[test]
fn test_replica_read_not_applied() {
    let mut cluster = new_node_cluster(0, 3);

    // Increase the election tick to make this test case running reliably.
    configure_for_lease_read(&mut cluster, Some(50), Some(10_000));
    let max_lease = Duration::from_secs(2);
    cluster.cfg.raft_store.raft_store_max_leader_lease = ReadableDuration(max_lease);

    cluster.pd_client.disable_default_operator();
    let r1 = cluster.run_conf_change();
    cluster.must_put(b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");
    cluster.pd_client.must_add_peer(r1, new_peer(3, 3));
    must_get_equal(&cluster.get_engine(3), b"k1", b"v1");

    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Add a filter to forbid peer 2 and 3 to know the last entry is committed.
    let committed_indices = Arc::new(Mutex::new(HashMap::default()));
    let filter = Box::new(CommitToFilter::new(committed_indices));
    cluster.sim.wl().add_send_filter(1, filter);

    cluster.must_put(b"k1", b"v2");
    must_get_equal(&cluster.get_engine(1), b"k1", b"v2");
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Add a filter to forbid the new leader to commit its first entry.
    let dropped_msgs = Arc::new(Mutex::new(Vec::new()));
    let filter = Box::new(
        RegionPacketFilter::new(1, 2)
            .direction(Direction::Recv)
            .msg_type(MessageType::MsgAppendResponse)
            .reserve_dropped(Arc::clone(&dropped_msgs)),
    );
    cluster.sim.wl().add_recv_filter(2, filter);

    cluster.must_transfer_leader(1, new_peer(2, 2));

    // Use a macro instead of a closure to avoid any capture of local variables.
    macro_rules! read_on {
        ($req: expr, $id:expr, $store_id:expr) => {{
            let mut req = $req.clone();
            req.mut_header().set_peer(new_peer($store_id, $id));
            let (tx, rx) = mpsc::sync_channel(1);
            let sim = cluster.sim.wl();
            let cb = Callback::Read(Box::new(move |resp| drop(tx.send(resp.response))));
            sim.async_command_on_node($id, req, cb).unwrap();
            rx
        }};
    }

    let r1 = cluster.get_region(b"k1");
    let mut read_request = new_request(
        r1.get_id(),
        r1.get_region_epoch().clone(),
        vec![new_get_cmd(b"k1")],
        true, // read quorum
    );
    read_request.mut_header().set_replica_read(true);

    // Read index on follower should be blocked instead of get an old value.
    let resp1_ch = read_on!(read_request, 3, 3);
    assert!(resp1_ch.recv_timeout(Duration::from_secs(3)).is_err());

    // Unpark all append responses so that the new leader can commit its first entry.
    let router = cluster.sim.wl().get_router(2).unwrap();
    for raft_msg in mem::replace(dropped_msgs.lock().unwrap().as_mut(), vec![]) {
        router.send_raft_message(raft_msg).unwrap();
    }

    // However, the old read index request could be blocked in raftstore forever, need more fix.
    // TODO: this needs to be fixed.
    cluster.sim.wl().clear_send_filters(1);
    cluster.sim.wl().clear_recv_filters(2);
    assert!(resp1_ch.recv_timeout(Duration::from_secs(3)).is_err());

    // New read index requests can be resolved quickly, and the previous one will be unblocked.
    let resp2_ch = read_on!(read_request, 3, 3);
    let resp2 = resp2_ch.recv_timeout(Duration::from_secs(3)).unwrap();
    let exp_value = resp2.get_responses()[0].get_get().get_value();
    assert_eq!(exp_value, b"v2");

    let resp1 = resp1_ch.recv_timeout(Duration::from_secs(3)).unwrap();
    let exp_value = resp1.get_responses()[0].get_get().get_value();
    assert_eq!(exp_value, b"v2");
}

#[derive(Default)]
struct CommitToFilter {
    // map[peer_id] -> committed index.
    committed: Arc<Mutex<HashMap<u64, u64>>>,
}

impl CommitToFilter {
    fn new(committed: Arc<Mutex<HashMap<u64, u64>>>) -> Self {
        Self { committed }
    }
}

impl Filter for CommitToFilter {
    fn before(&self, msgs: &mut Vec<RaftMessage>) -> Result<()> {
        let mut committed = self.committed.lock().unwrap();
        for msg in msgs.iter_mut() {
            let cmt = msg.get_message().get_commit();
            if cmt != 0 {
                let to = msg.get_message().get_to();
                committed.insert(to, cmt);
                msg.mut_message().set_commit(0);
            }
        }
        Ok(())
    }

    fn after(&self, _: Result<()>) -> Result<()> {
        Ok(())
    }
}
