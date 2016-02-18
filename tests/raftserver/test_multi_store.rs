use std::thread;
use std::sync::{Arc, RwLock};

use rocksdb::{DB, WriteBatch};

use tikv::raftserver::store::*;
// use tikv::proto::raft_cmdpb::*;
use tikv::proto::metapb;

use super::util::*;

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

fn create_engines(count: usize) -> (Vec<TempDir>, Vec<Arc<DB>>) {
    let mut paths: Vec<TempDir> = vec![];
    for _ in 0..count {
        paths.push(TempDir::new("test_multi_store").unwrap());
    }

    let mut engines: Vec<Arc<DB>> = vec![];

    for item in &paths {
        engines.push(new_engine(item));
    }

    (paths, engines)
}

fn init_multi_store(engines: &[Arc<DB>]) {
    let count = engines.len();
    let region = create_region(count);
    // we use cluster id 0, node [1, 5], store [1, 5] and peer [1, 5]
    for (i, engine) in engines.iter().enumerate() {
        bootstrap_store(engine.clone(), 0, i as u64 + 1, i as u64 + 1).unwrap();

        // here we use bootstrap region, but the region here only contains
        // one peer, we will re-construct it later.
        bootstrap_region(engine.clone()).unwrap();

        let batch = WriteBatch::new();
        batch.put_msg(&keys::region_info_key(region.get_start_key()), &region).unwrap();
        engine.write(batch).unwrap();
    }
}

fn run_store(engine: Arc<DB>,
             trans: Arc<RwLock<StoreTransport>>)
             -> (Sender, thread::JoinHandle<()>) {
    let mut store = new_store(engine, trans);

    let sender = store.get_sender();
    let t = thread::spawn(move || {
        store.run().unwrap();
    });

    (sender, t)
}

#[test]
fn test_multi_store() {
    // init_env_log();

    // test a cluster with five nodes [1, 5], only one region (region 1).
    // every node has a store and a peer with same id as node's.
    let count = 5;
    let (_, engines) = create_engines(count);

    init_multi_store(&engines);

    let trans = StoreTransport::new();

    let mut senders: Vec<Sender> = vec![];
    let mut handles: Vec<thread::JoinHandle<()>> = vec![];
    for engine in &engines {
        let (sender, h) = run_store(engine.clone(), trans.clone());
        senders.push(sender);
        handles.push(h);
    }

    // Let raft run.
    sleep_ms(500);

    // TODO: add tests here.

    for sender in &senders {
        sender.send_quit().unwrap();
    }

    for s in handles {
        s.join().unwrap();
    }
}
