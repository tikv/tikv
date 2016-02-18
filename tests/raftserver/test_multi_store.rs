use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;

use rocksdb::{DB, WriteBatch};

use tikv::raftserver::store::*;
use tikv::proto::metapb;
use tikv::proto::raft_cmdpb::{StatusCommandType, CommandType};

use super::util::*;
use super::cluster::Cluster;

fn create_region(peer_count: usize) -> metapb::Region {
    let mut region = metapb::Region::new();
    region.set_region_id(1);
    region.set_start_key(keys::MIN_KEY.to_vec());
    region.set_end_key(keys::MAX_KEY.to_vec());

    for i in 0..peer_count {
        let mut peer = metapb::Peer::new();
        peer.set_node_id(i as u64 + 1);
        peer.set_store_id(i as u64 + 1);
        peer.set_peer_id(i as u64 + 1);
        region.mut_peers().push(peer);
    }

    region
}

fn bootstrap_multi_store(engines: &HashMap<u64, Arc<DB>>) {
    let count = engines.len();
    let region = create_region(count);
    // we use cluster id 0, node [1, 5], store [1, 5] and peer [1, 5]
    for (i, engine) in engines {
        let id = *i;
        bootstrap_store(engine.clone(), 0, id, id).unwrap();

        // here we use bootstrap region, but the region here only contains
        // one peer, we will re-construct it later.
        bootstrap_region(engine.clone()).unwrap();

        let batch = WriteBatch::new();
        batch.put_msg(&keys::region_info_key(region.get_start_key()), &region).unwrap();
        engine.write(batch).unwrap();
    }
}

fn get_leader(cluster: &Cluster) -> Option<metapb::Peer> {
    for id in cluster.get_senders().keys() {
        let id = *id;
        let peer = new_peer(id, id, id);
        let region_leader = new_status_request(1, peer.clone(), new_region_leader_cmd());
        let resp = cluster.call_command(region_leader, Duration::from_secs(3)).unwrap();
        assert!(resp.has_status_response());
        assert_eq!(resp.get_status_response().get_cmd_type(),
                   StatusCommandType::RegionLeader);
        let region_leader = resp.get_status_response().get_region_leader();
        if !region_leader.has_leader() {
            sleep_ms(100);
            continue;
        }

        return Some(region_leader.get_leader().clone());
    }

    None
}

#[test]
fn test_multi_store() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let mut cluster = Cluster::new(count);

    bootstrap_multi_store(cluster.get_engines());

    // cache all peers here.
    // we should find a better way to cache peers later.
    let trans = cluster.get_transport();
    for i in 0..count {
        let id = i as u64 + 1;
        trans.write().unwrap().cache_peer(id, new_peer(id, id, id));
    }

    cluster.run_all_stores();

    // Let raft run.
    sleep_ms(500);

    // Get leader first,
    let peer = get_leader(&cluster).unwrap();

    let put = new_request(1,
                          peer.clone(),
                          vec![new_put_cmd(&keys::data_key(b"a1"), b"v1")]);
    let resp = cluster.call_command(put, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);

    let get = new_request(1, peer.clone(), vec![new_get_cmd(&keys::data_key(b"a1"))]);
    let resp = cluster.call_command(get, Duration::from_secs(3)).unwrap();
    assert_eq!(resp.get_responses().len(), 1);
    assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Get);



}
