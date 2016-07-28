use std::thread::sleep;
use std::time::Duration;

use rand::random;

use super::node::new_node_cluster;

#[test]
fn test_down_peers() {
    let count = 5 as u64;
    let mut cluster = new_node_cluster(0, count as usize);
    cluster.cfg.raft_store.max_peer_down_duration = Duration::from_millis(100);
    cluster.run();

    cluster.stop_node(1);
    cluster.stop_node(3);
    sleep(Duration::from_millis(2500));
    let down_peers = cluster.get_down_peers();
    let p1 = down_peers.get(&1).unwrap();
    assert!(p1.get_down_seconds() == 1 || p1.get_down_seconds() == 2);
    let p3 = down_peers.get(&3).unwrap();
    assert!(p3.get_down_seconds() == 1 || p3.get_down_seconds() == 2);
    cluster.run_node(1);
    cluster.run_node(3);
    sleep(Duration::from_millis(500));

    for _ in 0..3 {
        let id = random::<u64>() % count + 1;
        cluster.stop_node(id);
        sleep(Duration::from_millis(500));

        let down_peers = cluster.get_down_peers();
        for n in 1..(count + 1) {
            match down_peers.get(&n) {
                Some(peer) => {
                    assert_eq!(n, id);
                    assert_eq!(peer.get_down_seconds(), 0);
                }
                None => assert!(n != id),
            }
        }

        cluster.run_node(id);
        sleep(Duration::from_millis(500));
    }

}
