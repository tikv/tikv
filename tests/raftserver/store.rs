use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rocksdb::DB;

use super::cluster::{Simulator, Cluster};
use tikv::raftserver::store::*;
use tikv::proto::raft_cmdpb::*;
use super::util::*;

// TODO: Now we treat Store as the Node directly, we can use Node instead
// after we implement it later.
pub struct StoreCluster {
    senders: HashMap<u64, SendCh>,
    handles: HashMap<u64, thread::JoinHandle<()>>,

    trans: Arc<RwLock<StoreTransport>>,
}

impl StoreCluster {
    pub fn new() -> StoreCluster {
        StoreCluster {
            senders: HashMap::new(),
            handles: HashMap::new(),
            trans: StoreTransport::new(),
        }
    }
}

impl Simulator for StoreCluster {
    fn run_node(&mut self, node_id: u64, engine: Arc<DB>) {
        assert!(!self.handles.contains_key(&node_id));
        assert!(!self.senders.contains_key(&node_id));

        let cfg = new_store_cfg();

        let mut event_loop = create_event_loop(&cfg).unwrap();

        let mut store = Store::new(&mut event_loop, cfg, engine.clone(), self.trans.clone())
                            .unwrap();

        self.trans.write().unwrap().add_sender(store.get_node_id(), store.get_sendch());

        let sender = store.get_sendch();
        let t = thread::spawn(move || {
            store.run(&mut event_loop).unwrap();
        });

        self.handles.insert(node_id, t);
        self.senders.insert(node_id, sender);
    }

    fn stop_node(&mut self, node_id: u64) {
        let h = self.handles.remove(&node_id).unwrap();
        let sender = self.senders.remove(&node_id).unwrap();

        self.trans.write().unwrap().remove_sender(node_id);

        sender.send_quit().unwrap();
        h.join().unwrap();
    }

    fn get_node_ids(&self) -> Vec<u64> {
        self.senders.keys().cloned().collect()
    }

    fn call_command(&self,
                    request: RaftCommandRequest,
                    timeout: Duration)
                    -> Option<RaftCommandResponse> {
        let node_id = request.get_header().get_peer().get_node_id();
        let sender = self.senders.get(&node_id).unwrap();

        msg::call_command(sender, request, timeout).unwrap()
    }
}

pub fn new_store_cluster(id: u64, count: usize) -> Cluster<StoreCluster> {
    Cluster::new(id, count, StoreCluster::new())
}
