use std::sync::Arc;
use std::collections::HashMap;

use rocksdb::{DB, WriteBatch};

use tikv::raftserver::store::*;
use tikv::proto::metapb;

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

#[test]
fn test_multi_store() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let mut cluster = Cluster::new(count);

    bootstrap_multi_store(cluster.get_engines());

    cluster.run_all_stores();

    // Let raft run.
    sleep_ms(500);

    // TODO: add more tests later.
}
