#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use rocksdb::DB;
use tempdir::TempDir;

use tikv::raftserver::store::*;
use super::util::*;
use tikv::proto::raft_cmdpb::*;

// We simulate 3 or 5 nodes, each has a store, the node id and store id are same.
// E,g, for node 1, the node id and store id are both 1.
pub struct Cluster {
    paths: HashMap<u64, TempDir>,
    engines: HashMap<u64, Arc<DB>>,

    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,

    trans: Arc<RwLock<StoreTransport>>,
}

impl Cluster {
    pub fn new(count: usize) -> Cluster {
        let mut c = Cluster {
            paths: HashMap::new(),
            engines: HashMap::new(),
            senders: HashMap::new(),
            handles: HashMap::new(),
            trans: StoreTransport::new(),
        };

        c.create_engines(count);

        c
    }

    fn create_engines(&mut self, count: usize) {
        for i in 0..count {
            self.paths.insert(i as u64 + 1, TempDir::new("test_cluster").unwrap());
        }

        for (i, item) in &self.paths {
            self.engines.insert(*i, new_engine(item));
        }
    }

    pub fn run_store(&mut self, store_id: u64) {
        assert!(!self.handles.contains_key(&store_id));
        assert!(!self.senders.contains_key(&store_id));

        let engine = self.engines.get(&store_id).unwrap();
        let mut store = new_store(engine.clone(), self.trans.clone());

        let sender = store.get_sendch();
        let t = thread::spawn(move || {
            store.run().unwrap();
        });

        self.handles.insert(store_id, t);
        self.senders.insert(store_id, sender);
    }

    pub fn run_all_stores(&mut self) {
        let count = self.engines.len();
        for i in 0..count {
            self.run_store(i as u64 + 1);
        }
    }

    pub fn stop_store(&mut self, store_id: u64) {
        let h = self.handles.remove(&store_id).unwrap();
        let sender = self.senders.remove(&store_id).unwrap();

        self.trans.write().unwrap().remove_sender(store_id);

        sender.send_quit().unwrap();
        h.join().unwrap();
    }

    pub fn get_engines(&self) -> &HashMap<u64, Arc<DB>> {
        &self.engines
    }

    pub fn get_senders(&self) -> &HashMap<u64, SendCh> {
        &self.senders
    }

    pub fn call_command(&self,
                        request: RaftCommandRequest,
                        timeout: Duration)
                        -> Option<(RaftCommandResponse)> {
        let store_id = request.get_header().get_peer().get_store_id();
        let sender = self.senders.get(&store_id).unwrap();

        sender.call_command(request, timeout).unwrap()
    }

    pub fn get_transport(&self) -> Arc<RwLock<StoreTransport>> {
        self.trans.clone()
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        for (id, sender) in self.senders.drain() {
            let h = self.handles.remove(&id).unwrap();

            self.trans.write().unwrap().remove_sender(id);
            sender.send_quit().unwrap();

            h.join().unwrap();
        }
    }
}
