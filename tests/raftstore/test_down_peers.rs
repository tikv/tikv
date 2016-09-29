use std::thread::sleep;
use std::time::{Instant, Duration};

use rand::random;
use kvproto::pdpb;

use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::cluster::{Cluster, Simulator};
use super::transport_simulate::IsolationFilterFactory;

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
    check_down_seconds(down_peers.get(&node_id).unwrap(), down_secs);
    check_down_seconds(down_peers.get(&next_id).unwrap(), down_secs);

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
    let prev_secs = cluster.get_down_peers().get(&next_id).unwrap().get_down_seconds();
    for _ in 1..100 {
        let down_secs = cluster.get_down_peers().get(&next_id).unwrap().get_down_seconds();
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
    check_down_seconds(down_peers.get(&1).unwrap(), secs);
    check_down_seconds(down_peers.get(&3).unwrap(), secs);

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
