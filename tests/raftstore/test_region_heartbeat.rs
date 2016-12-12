use std::thread::sleep;
use std::sync::mpsc;
use std::time::{Instant, Duration};

use rand::random;
use kvproto::pdpb;
use tikv::util::HandyRwLock;

use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::cluster::{Cluster, Simulator};
use super::transport_simulate::*;
use super::util::*;

fn wait_down_peers<T: Simulator>(cluster: &Cluster<T>, count: u64) -> u64 {
    let begin = Instant::now();
    for _ in 1..100 {
        if cluster.get_down_peers().len() != count as usize {
            sleep(Duration::from_millis(100));
        } else {
            break;
        }
    }
    begin.elapsed().as_secs()
}

fn check_down_seconds(peer: &pdpb::PeerStats, secs: u64) {
    debug!("down {} secs {}", peer.get_down_seconds(), secs);
    assert!(peer.get_down_seconds() <= secs);
}

fn test_leader_down_and_become_leader_again<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    // Isolate node.
    let node = cluster.leader_of_region(1).unwrap();
    let node_id = node.get_id();
    cluster.add_send_filter(IsolationFilterFactory::new(node_id));
    debug!("node: {:?}", node);

    // Kill another node.
    let next_id = if node_id < count { node_id + 1 } else { 1 };
    cluster.stop_node(next_id);

    // Wait other node to become leader.
    let begin = Instant::now();
    for _ in 1..100 {
        cluster.reset_leader_of_region(1);
        if let Some(peer) = cluster.leader_of_region(1) {
            if peer.get_id() != node_id {
                break;
            }
        }
        sleep(Duration::from_millis(100));
    }
    assert!(node_id != cluster.leader_of_region(1).unwrap().get_id());
    wait_down_peers(cluster, 2);

    // Check node and another are down.
    let down_secs = begin.elapsed().as_secs();
    let down_peers = cluster.get_down_peers();
    debug!("down_secs: {} down_peers: {:?}", down_secs, down_peers);
    check_down_seconds(&down_peers[&node_id], down_secs);
    check_down_seconds(&down_peers[&next_id], down_secs);

    // Restart node and sleep a few seconds.
    let sleep_secs = 3;
    cluster.clear_send_filters();
    wait_down_peers(cluster, 1);
    sleep(Duration::from_secs(sleep_secs));

    // Wait node to become leader again.
    for _ in 1..100 {
        cluster.transfer_leader(1, node.clone());
        if let Some(peer) = cluster.leader_of_region(1) {
            if peer.get_id() == node_id {
                break;
            }
        }
        sleep(Duration::from_millis(100));
    }
    assert!(node_id == cluster.leader_of_region(1).unwrap().get_id());
    wait_down_peers(cluster, 1);

    // Ensure that node will not reuse the previous peer heartbeats.
    let prev_secs = cluster.get_down_peers()[&next_id].get_down_seconds();
    for _ in 1..100 {
        let down_secs = cluster.get_down_peers()[&next_id].get_down_seconds();
        if down_secs != prev_secs {
            assert!(down_secs < sleep_secs);
            break;
        }
        sleep(Duration::from_millis(100));
    }
}

fn test_down_peers<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    cluster.cfg.raft_store.max_peer_down_duration = Duration::from_millis(500);
    cluster.run();

    // Kill 1, 3
    cluster.stop_node(1);
    cluster.stop_node(3);
    let secs = wait_down_peers(cluster, 2);

    // Check 1, 3 are down.
    let down_peers = cluster.get_down_peers();
    check_down_seconds(&down_peers[&1], secs);
    check_down_seconds(&down_peers[&3], secs);

    // Restart 1, 3
    cluster.run_node(1);
    cluster.run_node(3);
    wait_down_peers(cluster, 0);

    for _ in 0..3 {
        let n = random::<u64>() % count + 1;

        // kill n.
        cluster.stop_node(n);
        let secs = wait_down_peers(cluster, 1);

        // Check i is down and others are not down.
        for i in 1..(count + 1) {
            match cluster.get_down_peers().get(&i) {
                Some(peer) => {
                    assert_eq!(i, n);
                    check_down_seconds(peer, secs);
                }
                None => assert!(i != n),
            }
        }

        // Restart n.
        cluster.run_node(n);
        wait_down_peers(cluster, 0);
    }

    test_leader_down_and_become_leader_again(cluster, count);
}

#[test]
fn test_node_down_peers() {
    let mut cluster = new_node_cluster(0, 5);
    test_down_peers(&mut cluster, 5);
}

#[test]
fn test_server_down_peers() {
    let mut cluster = new_server_cluster(0, 5);
    test_down_peers(&mut cluster, 5);
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
