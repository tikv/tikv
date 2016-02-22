#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::Duration;

use rocksdb::DB;
use tempdir::TempDir;

use tikv::raftserver::Result;
use tikv::raftserver::store::*;
use super::util::*;
use tikv::proto::raft_cmdpb::*;
use tikv::proto::metapb;

// We simulate 3 or 5 nodes, each has a store, the node id and store id are same.
// E,g, for node 1, the node id and store id are both 1.
pub struct Cluster {
    id: u64,
    leaders: HashMap<u64, metapb::Peer>,
    paths: HashMap<u64, TempDir>,
    pub engines: HashMap<u64, Arc<DB>>,

    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,

    trans: Arc<RwLock<StoreTransport>>,
}

impl Cluster {
    pub fn new(id: u64, count: usize) -> Cluster {
        let mut c = Cluster {
            id: id,
            leaders: HashMap::new(),
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
                        -> Option<RaftCommandResponse> {
        let store_id = request.get_header().get_peer().get_store_id();
        let sender = self.senders.get(&store_id).unwrap();

        call_command(sender, request, timeout).unwrap()
    }

    pub fn call_command_on_leader(&mut self,
                                  region_id: u64,
                                  mut request: RaftCommandRequest,
                                  timeout: Duration)
                                  -> Option<RaftCommandResponse> {
        request.mut_header().set_peer(self.leader_of_region(region_id).clone().unwrap());
        self.call_command(request, timeout)
    }

    pub fn get_transport(&self) -> Arc<RwLock<StoreTransport>> {
        self.trans.clone()
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        if let Some(l) = self.leaders.get(&region_id) {
            return Some(l.clone());
        }
        let mut leader = None;
        for id in self.get_senders().keys() {
            let id = *id;
            let peer = new_peer(id, id, id);
            let find_leader = new_status_request(region_id, &peer, new_region_leader_cmd());
            let resp = self.call_command(find_leader, Duration::from_secs(3)).unwrap();
            let region_leader = resp.get_status_response().get_region_leader();
            if region_leader.has_leader() {
                leader = Some(region_leader.get_leader().clone());
                break;
            }
            sleep_ms(100);
        }

        if let Some(l) = leader {
            self.leaders.insert(region_id, l);
        }
        self.leaders.get(&region_id).cloned()
    }

    pub fn bootstrap_single_region(&self) -> Result<()> {
        let mut region = metapb::Region::new();
        region.set_region_id(1);
        region.set_start_key(keys::MIN_KEY.to_vec());
        region.set_end_key(keys::MAX_KEY.to_vec());

        for (&id, engine) in &self.engines {
            let peer = new_peer(id, id, id);
            region.mut_peers().push(peer.clone());
            bootstrap_store(engine.clone(), self.id, id, id).unwrap();
        }

        for engine in self.engines.values() {
            try!(write_first_region(&engine, &region));
        }
        Ok(())
    }

    pub fn reset_leader_of_region(&mut self, region_id: u64) {
        self.leaders.remove(&region_id);
    }

    pub fn check_quorum<F: FnMut(&&Arc<DB>) -> bool>(&self, condition: F) -> bool {
        if self.engines.is_empty() {
            return true;
        }
        self.engines.values().filter(condition).count() > self.engines.len() / 2
    }

    pub fn shutdown(&mut self) {
        let keys: Vec<u64> = self.senders.keys().cloned().collect();
        for id in keys {
            self.stop_store(id);
        }
        self.leaders.clear();
    }

    pub fn get(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        let get = new_request(1, vec![new_get_cmd(&keys::data_key(key))]);
        let mut resp = self.call_command_on_leader(1, get, Duration::from_secs(3)).unwrap();
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Get);
        let mut get = resp.mut_responses()[0].take_get();
        if get.has_value() {
            Some(get.take_value())
        } else {
            None
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        let put = new_request(1, vec![new_put_cmd(&keys::data_key(key), value)]);
        let resp = self.call_command_on_leader(1, put, Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error());
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Put);
    }

    pub fn seek(&mut self, key: &[u8]) -> Option<(Vec<u8>, Vec<u8>)> {
        let seek = new_request(1, vec![new_seek_cmd(&keys::data_key(key))]);
        let resp = self.call_command_on_leader(1, seek, Duration::from_secs(3)).unwrap();
        assert!(!resp.get_header().has_error());
        assert_eq!(resp.get_responses().len(), 1);
        let resp = &resp.get_responses()[0];
        assert_eq!(resp.get_cmd_type(), CommandType::Seek);
        if !resp.has_seek() {
            None
        } else {
            Some((resp.get_seek().get_key().to_vec(),
                  resp.get_seek().get_value().to_vec()))
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        let delete = new_request(1, vec![new_delete_cmd(&keys::data_key(key))]);
        let resp = self.call_command_on_leader(1, delete, Duration::from_secs(3)).unwrap();
        assert_eq!(resp.get_responses().len(), 1);
        assert_eq!(resp.get_responses()[0].get_cmd_type(), CommandType::Delete);
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.shutdown();
    }
}
