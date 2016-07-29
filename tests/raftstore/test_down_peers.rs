use std::thread::sleep;
use std::time::{Instant, Duration};

use rand::random;
use kvproto::pdpb;

use super::node::new_node_cluster;
use super::server::new_server_cluster;
use super::cluster::{Cluster, Simulator};

fn wait_down_peers<T: Simulator>(cluster: &Cluster<T>, count: u64) -> u64 {
    let begin = Instant::now();
    loop {
        if cluster.get_down_peers().len() != count as usize {
            sleep(Duration::from_millis(100));
        } else {
            break;
        }
    }
    begin.elapsed().as_secs()
}

fn check_down_seconds(peer: &pdpb::PeerStats, secs: u64) {
    assert!(peer.get_down_seconds() == secs || secs > 0 && peer.get_down_seconds() == secs - 1);
}

fn test_down_peers<T: Simulator>(cluster: &mut Cluster<T>, count: u64) {
    cluster.cfg.raft_store.max_peer_down_duration = Duration::from_millis(100);
    cluster.run();

    // Kill 1, 3
    cluster.stop_node(1);
    cluster.stop_node(3);
    let secs = wait_down_peers(&cluster, 2);

    // Check 1, 3 are down.
    let down_peers = cluster.get_down_peers();
    check_down_seconds(down_peers.get(&1).unwrap(), secs);
    check_down_seconds(down_peers.get(&3).unwrap(), secs);

    // Restart 1, 3
    cluster.run_node(1);
    cluster.run_node(3);
    wait_down_peers(&cluster, 0);

    for _ in 0..3 {
        let n = random::<u64>() % count + 1;

        // kill n.
        cluster.stop_node(n);
        let secs = wait_down_peers(&cluster, 1);

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
        wait_down_peers(&cluster, 0);
    }
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
