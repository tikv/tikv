use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, RwLock};

use rocksdb::DB;

use pd::{INVALID_ID, Client as PdClient, Error as PdError};
use kvproto::raft_serverpb::StoreIdent;
use kvproto::metapb;
use raftserver::store::{self, Store, Config as StoreConfig, keys, Peekable, Transport};
use raftserver::{Result, other};
use super::config::Config;

pub struct Node<T: PdClient, Trans: Transport + Send + Sync + 'static> {
    cluster_id: u64,
    node_id: u64,
    addr: String,
    store_cfg: StoreConfig,
    store_handles: HashMap<u64, thread::JoinHandle<()>>,

    pd_client: Arc<RwLock<T>>,
    trans: Arc<RwLock<Trans>>,
}

impl<T, Trans> Node<T, Trans>
    where T: PdClient,
          Trans: Transport + Send + Sync + 'static
{
    pub fn new(cfg: &Config,
               pd_client: Arc<RwLock<T>>,
               trans: Arc<RwLock<Trans>>)
               -> Node<T, Trans> {
        Node {
            cluster_id: cfg.cluster_id,
            node_id: INVALID_ID,
            addr: cfg.addr.clone(),
            store_cfg: cfg.store_cfg.clone(),
            store_handles: HashMap::new(),
            pd_client: pd_client.clone(),
            trans: trans.clone(),
        }
    }

    pub fn start(&mut self, engines: Vec<Arc<DB>>) -> Result<()> {
        assert!(!engines.is_empty());

        let bootstrapped = try!(self.pd_client
                                    .read()
                                    .unwrap()
                                    .is_cluster_bootstrapped(self.cluster_id));
        let (node_id, mut store_ids) = try!(self.check_stores(&engines));
        if node_id == INVALID_ID {
            self.node_id = try!(self.pd_client.write().unwrap().alloc_id());
            debug!("alloc node id {:?}", self.node_id);
        } else {
            self.node_id = node_id;
            // We have saved data before, and the cluster must be bootstrapped.
            if !bootstrapped {
                return Err(other(format!("node {} is not empty, but cluster {} is not \
                                          bootstrapped",
                                         self.node_id,
                                         self.cluster_id)));
            }
        }

        for (index, store_id) in store_ids.iter_mut().enumerate() {
            if *store_id == INVALID_ID {
                *store_id = try!(self.bootstrap_store(engines[index].clone()));
                debug!("bootstrap store {} in node {}", store_id, self.node_id);
            }
        }

        if !bootstrapped {
            // cluster is not bootstrapped, and we choose first store to bootstrap
            // first region.
            let region = try!(self.bootstrap_first_region(engines[0].clone(), store_ids[0]));
            try!(self.bootstrap_cluster(engines[0].clone(), &store_ids, region));
        }

        // inform pd.
        try!(self.pd_client.write().unwrap().put_node(self.cluster_id, self.new_node_meta()));

        for (index, store_id) in store_ids.iter().enumerate() {
            try!(self.start_store(*store_id, engines[index].clone()));
            try!(self.pd_client
                     .write()
                     .unwrap()
                     .put_store(self.cluster_id, self.new_store_meta(*store_id)));
        }

        Ok(())
    }

    pub fn get_node_id(&self) -> u64 {
        self.node_id
    }

    // check stores, return node id, corresponding store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    // If all the stores are not bootstrapped, the return node id is INVALID_ID.
    fn check_stores(&self, engines: &[Arc<DB>]) -> Result<(u64, Vec<u64>)> {
        let mut stores: Vec<u64> = Vec::with_capacity(engines.len());
        let mut node_id = INVALID_ID;

        for engine in engines {
            let res = try!(engine.get_msg::<StoreIdent>(&keys::store_ident_key()));
            if res.is_none() {
                stores.push(INVALID_ID);
                continue;
            }

            let ident = res.unwrap();
            if ident.get_cluster_id() != self.cluster_id {
                return Err(other(format!("store ident {:?} has mismatched cluster id with {}",
                                         ident,
                                         self.cluster_id)));
            }

            if node_id == INVALID_ID {
                node_id = ident.get_node_id();
            } else if ident.get_node_id() != node_id {
                return Err(other(format!("store ident {:?} has mismatched node id with {}",
                                         ident,
                                         node_id)));
            }

            let store_id = ident.get_store_id();
            if node_id == INVALID_ID || store_id == INVALID_ID {
                return Err(other(format!("invalid store ident {:?}", ident)));
            }

            stores.push(store_id);
        }

        Ok((node_id, stores))
    }

    fn bootstrap_store(&self, engine: Arc<DB>) -> Result<u64> {
        let store_id = try!(self.pd_client.write().unwrap().alloc_id());
        debug!("alloc store id {} for node {}", store_id, self.node_id);

        try!(store::bootstrap_store(engine, self.cluster_id, self.node_id, store_id));

        Ok(store_id)
    }

    fn bootstrap_first_region(&self, engine: Arc<DB>, store_id: u64) -> Result<metapb::Region> {
        let region_id = try!(self.pd_client.write().unwrap().alloc_id());
        debug!("alloc first region id {} for cluster {}, node {}, store {}",
               region_id,
               self.cluster_id,
               self.node_id,
               store_id);
        let peer_id = try!(self.pd_client.write().unwrap().alloc_id());
        debug!("alloc first peer id {} for first region {}",
               peer_id,
               region_id);

        let region = try!(store::bootstrap_region(engine,
                                                  self.node_id,
                                                  store_id,
                                                  region_id,
                                                  peer_id));
        Ok(region)
    }

    fn new_node_meta(&self) -> metapb::Node {
        let mut node = metapb::Node::new();
        node.set_node_id(self.node_id);
        node.set_address(self.addr.clone());

        node
    }

    fn new_store_meta(&self, store_id: u64) -> metapb::Store {
        let mut store = metapb::Store::new();
        store.set_node_id(self.node_id);
        store.set_store_id(store_id);
        store
    }

    fn new_store_metas(&self, store_ids: &[u64]) -> Vec<metapb::Store> {
        let mut stores: Vec<metapb::Store> = Vec::with_capacity(store_ids.len());
        for store_id in store_ids {
            stores.push(self.new_store_meta(*store_id));
        }
        stores
    }

    fn bootstrap_cluster(&mut self,
                         engine: Arc<DB>,
                         store_ids: &[u64],
                         region: metapb::Region)
                         -> Result<()> {
        let region_id = region.get_region_id();
        match self.pd_client.write().unwrap().bootstrap_cluster(self.cluster_id,
                                                                self.new_node_meta(),
                                                                self.new_store_metas(store_ids),
                                                                region) {
            Err(PdError::ClusterBootstrapped(_)) => {
                error!("cluster {} is already bootstrapped", self.cluster_id);
                try!(store::clear_region(&engine, region_id));
                Ok(())
            }
            // TODO: should we clean region for other errors too?
            Err(e) => Err(other(format!("bootstrap cluster {} err: {:?}", self.cluster_id, e))),
            Ok(_) => {
                info!("bootstrap cluster {} ok", self.cluster_id);
                Ok(())
            }
        }
    }

    fn start_store(&mut self, store_id: u64, engine: Arc<DB>) -> Result<()> {
        if self.store_handles.contains_key(&store_id) {
            return Err(other(format!("duplicated store id {}", store_id)));
        }

        let cfg = self.store_cfg.clone();
        let mut event_loop = try!(store::create_event_loop(&cfg));
        let mut store = try!(Store::new(&mut event_loop, cfg, engine, self.trans.clone()));
        let ch = store.get_sendch();
        self.trans.write().unwrap().add_sendch(store_id, ch);

        let h = thread::spawn(move || {
            if let Err(e) = store.run(&mut event_loop) {
                error!("store {} run err {:?}", store_id, e);
            };
        });

        self.store_handles.insert(store_id, h);
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) -> Result<()> {
        let ch = self.trans.write().unwrap().remove_sendch(store_id);

        if ch.is_none() {
            return Err(other(format!("stop invalid store with id {}", store_id)));
        }

        let h = self.store_handles.remove(&store_id);
        if h.is_none() {
            return Err(other(format!("store {} thread has already gone", store_id)));
        }

        try!(ch.unwrap().send_quit());

        if let Err(e) = h.unwrap().join() {
            return Err(other(format!("join store {} thread err {:?}", store_id, e)));
        }

        Ok(())
    }
}

impl<T, Trans> Drop for Node<T, Trans>
    where T: PdClient,
          Trans: Transport + Send + Sync + 'static
{
    fn drop(&mut self) {
        let ids: Vec<u64> = self.store_handles.keys().cloned().collect();
        for id in ids {
            if let Err(e) = self.stop_store(id) {
                error!("stop store {} err {:?}", id, e);
            }
        }
    }
}
