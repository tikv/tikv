use std::thread::sleep;
use std::sync::mpsc;
use std::time::{Instant, Duration};

use tikv::util::HandyRwLock;

use super::util;
use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::cluster::{Cluster, Simulator};
use super::transport_simulate::*;
use super::util::*;

fn wait_down_peers<T: Simulator>(cluster: &Cluster<T>, count: u64) {
    for _ in 1..100 {
        if cluster.get_down_peers().len() != count as usize {
            sleep(Duration::from_millis(10));
        } else {
            break;
        }
    }
}

fn test_down_peers<T: Simulator>(cluster: &mut Cluster<T>) {
    cluster.cfg.raft_store.max_peer_down_duration = Duration::from_millis(500);
    cluster.run();

    // Kill 1, 2
    for len in 1..3 {
        let id = len;
        cluster.stop_node(id);
        wait_down_peers(cluster, len);
        let down_peers = cluster.get_down_peers();
        assert!(down_peers.contains_key(&id));
    }

    // Restart 1, 2
    cluster.run_node(1);
    cluster.run_node(2);
    wait_down_peers(cluster, 0);

    cluster.stop_node(1);

    cluster.must_put(b"k1", b"v1");
    // max peer down duration is 500 millis, but we only report down time in seconds,
    // so sleep 1 second to make the old down second is always larger than new down second
    // by at lease 1 second.
    util::sleep_ms(1000);

    wait_down_peers(cluster, 1);
    let down_secs = cluster.get_down_peers()[&1].get_down_seconds();
    let timer = Instant::now();
    let leader = cluster.leader_of_region(1).unwrap();
    let new_leader = if leader.get_id() == 2 {
        util::new_peer(3, 3)
    } else {
        util::new_peer(2, 2)
    };

    cluster.must_transfer_leader(1, new_leader);
    // new leader should reset all down peer list.
    wait_down_peers(cluster, 0);
    wait_down_peers(cluster, 1);
    assert!(cluster.get_down_peers()[&1].get_down_seconds() <
            down_secs + timer.elapsed().as_secs());

    // Ensure that node will not reuse the previous peer heartbeats.
    cluster.must_transfer_leader(1, leader);
    wait_down_peers(cluster, 0);
    wait_down_peers(cluster, 1);
    assert!(cluster.get_down_peers()[&1].get_down_seconds() < timer.elapsed().as_secs() + 1);
}

#[test]
fn test_node_down_peers() {
    let mut cluster = new_node_cluster(0, 5);
    test_down_peers(&mut cluster);
}

#[test]
fn test_server_down_peers() {
    let mut cluster = new_server_cluster(0, 5);
    test_down_peers(&mut cluster);
}

fn test_pending_peers<T: Simulator>(cluster: &mut Cluster<T>) {
    let pd_client = cluster.pd_client.clone();
    // Disable default max peer count check.
    pd_client.disable_default_rule();

    let region_id = cluster.run_conf_change();

    cluster.must_put(b"k1", b"v1");

    let (tx, _) = mpsc::channel();
    cluster.sim.wl().add_recv_filter(2, box DropSnapshotFilter::new(tx));

    pd_client.must_add_peer(region_id, new_peer(2, 2));

    let mut tried_times = 0;
    loop {
        tried_times += 1;
        if tried_times > 100 {
            panic!("can't get pending peer after {} tries.", tried_times);
        }
        let pending_peers = cluster.pd_client.get_pending_peers();
        if pending_peers.is_empty() {
            sleep(Duration::from_millis(100));
        } else {
            assert_eq!(pending_peers[&2], new_peer(2, 2));
            break;
        }
    }

    cluster.sim.wl().clear_recv_filters(2);
    cluster.must_put(b"k2", b"v2");

    tried_times = 0;
    loop {
        tried_times += 1;
        let pending_peers = cluster.pd_client.get_pending_peers();
        if !pending_peers.is_empty() {
            sleep(Duration::from_millis(100));
        } else {
            return;
        }
        if tried_times > 100 {
            panic!("pending peer {:?} still exists after {} tries.",
                   pending_peers,
                   tried_times);
        }
    }
}

#[test]
fn test_node_pending_peers() {
    let mut cluster = new_node_cluster(0, 3);
    test_pending_peers(&mut cluster);
}

#[test]
fn test_server_pending_peers() {
    let mut cluster = new_server_cluster(0, 3);
    test_pending_peers(&mut cluster);
}
