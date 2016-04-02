use std::thread;
use std::sync::{Arc, RwLock};

use rocksdb::DB;

use pd::{INVALID_ID, PdClient, Error as PdError};
use kvproto::raft_serverpb::StoreIdent;
use kvproto::metapb;
use raftstore::store::{self, Msg, Store, Config as StoreConfig, keys, Peekable, Transport, SendCh};
use super::Result;
use util::HandyRwLock;
use super::config::Config;
use storage::{Storage, Engine, RaftKv};
use super::transport::ServerRaftStoreRouter;

pub fn create_raft_storage<T, Trans>(node: Node<T, Trans>) -> Result<Storage>
    where T: PdClient + 'static,
          Trans: Transport + 'static
{
    let engine = box RaftKv::new(node);
    let store = try!(Storage::from_engine(engine));
    Ok(store)
}

// Node is a wrapper for raft store.
// TODO: we will rename another better name like RaftStore later.
pub struct Node<T: PdClient + 'static, Trans: Transport + 'static> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: StoreConfig,
    store_handle: Option<thread::JoinHandle<()>>,
    ch: Option<SendCh>,

    trans: Arc<RwLock<Trans>>,

    pd_client: Arc<RwLock<T>>,
}

impl<T, Trans> Node<T, Trans>
    where T: PdClient,
          Trans: Transport
{
    pub fn new(cfg: &Config,
               pd_client: Arc<RwLock<T>>,
               trans: Arc<RwLock<Trans>>)
               -> Node<T, Trans> {
        let mut store = metapb::Store::new();
        store.set_id(INVALID_ID);
        if cfg.advertise_addr.is_empty() {
            store.set_address(cfg.addr.clone());
        } else {
            store.set_address(cfg.advertise_addr.clone())
        }

        Node {
            cluster_id: cfg.cluster_id,
            store: store,
            store_cfg: cfg.store_cfg.clone(),
            store_handle: None,
            pd_client: pd_client,
            trans: trans.clone(),
            ch: None,
        }
    }

    pub fn start(&mut self, engine: Arc<DB>) -> Result<()> {
        let bootstrapped = try!(self.pd_client
                                    .read()
                                    .unwrap()
                                    .is_cluster_bootstrapped(self.cluster_id));
        let mut store_id = try!(self.check_store(&engine));
        if store_id == INVALID_ID {
            store_id = try!(self.bootstrap_store(&engine));
        } else if !bootstrapped {
            // We have saved data before, and the cluster must be bootstrapped.
            return Err(box_err!("store {} is not empty, but cluster {} is not bootstrapped",
                                store_id,
                                self.cluster_id));
        }

        self.store.set_id(store_id);

        if !bootstrapped {
            // cluster is not bootstrapped, and we choose first store to bootstrap
            // first region.
            let region = try!(self.bootstrap_first_region(&engine, store_id));
            try!(self.bootstrap_cluster(&engine, region));
        }

        // inform pd.
        try!(self.start_store(store_id, engine));
        try!(self.pd_client
                 .write()
                 .unwrap()
                 .put_store(self.cluster_id, self.store.clone()));


        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn raft_store_router(&self) -> Arc<RwLock<ServerRaftStoreRouter>> {
        // We must start Store thread OK before using this raft handler.
        // TODO: should we return an error? or
        let ch = self.ch.clone().unwrap();
        Arc::new(RwLock::new(ServerRaftStoreRouter::new(self.store.get_id(), ch)))
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, engine: &DB) -> Result<u64> {
        let res = try!(engine.get_msg::<StoreIdent>(&keys::store_ident_key()));
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            return Err(box_err!("store ident {:?} has mismatched cluster id with {}",
                                ident,
                                self.cluster_id));
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }

        Ok(store_id)
    }

    fn bootstrap_store(&self, engine: &DB) -> Result<u64> {
        let store_id = try!(self.pd_client.wl().alloc_id());
        debug!("alloc store id {} ", store_id);

        try!(store::bootstrap_store(engine, self.cluster_id, store_id));

        Ok(store_id)
    }

    fn bootstrap_first_region(&self, engine: &DB, store_id: u64) -> Result<metapb::Region> {
        let region_id = try!(self.pd_client.wl().alloc_id());
        debug!("alloc first region id {} for cluster {}, store {}",
               region_id,
               self.cluster_id,
               store_id);
        let peer_id = try!(self.pd_client.wl().alloc_id());
        debug!("alloc first peer id {} for first region {}",
               peer_id,
               region_id);

        let region = try!(store::bootstrap_region(engine, store_id, region_id, peer_id));
        Ok(region)
    }

    fn bootstrap_cluster(&mut self, engine: &DB, region: metapb::Region) -> Result<()> {
        let region_id = region.get_id();
        match self.pd_client.wl().bootstrap_cluster(self.cluster_id, self.store.clone(), region) {
            Err(PdError::ClusterBootstrapped(_)) => {
                error!("cluster {} is already bootstrapped", self.cluster_id);
                try!(store::clear_region(engine, region_id));
                Ok(())
            }
            // TODO: should we clean region for other errors too?
            Err(e) => Err(box_err!("bootstrap cluster {} err: {:?}", self.cluster_id, e)),
            Ok(_) => {
                info!("bootstrap cluster {} ok", self.cluster_id);
                Ok(())
            }
        }
    }

    fn start_store(&mut self, store_id: u64, engine: Arc<DB>) -> Result<()> {
        let meta = try!(self.pd_client.rl().get_cluster_meta(self.cluster_id));

        if self.store_handle.is_some() {
            return Err(box_err!("{} is already started", store_id));
        }

        let cfg = self.store_cfg.clone();
        let pd_client = self.pd_client.clone();
        let mut event_loop = try!(store::create_event_loop(&cfg));
        let mut store = try!(Store::new(&mut event_loop,
                                        meta,
                                        cfg,
                                        engine,
                                        self.trans.clone(),
                                        pd_client));
        let ch = store.get_sendch();
        self.ch = Some(ch);

        let h = thread::spawn(move || {
            if let Err(e) = store.run(&mut event_loop) {
                error!("store {} run err {:?}", store_id, e);
            };
        });

        self.store_handle = Some(h);
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) -> Result<()> {
        match self.ch.take() {
            None => return Err(box_err!("stop invalid store with id {}", store_id)),
            Some(ch) => try!(ch.send(Msg::Quit)),
        }

        // Handler must exist here.
        let h = self.store_handle.take().unwrap();

        if let Err(e) = h.join() {
            return Err(box_err!("join store {} thread err {:?}", store_id, e));
        }

        Ok(())
    }
}

impl<T, Trans> Drop for Node<T, Trans>
    where T: PdClient,
          Trans: Transport + 'static
{
    fn drop(&mut self) {
        let store_id = self.store.get_id();
        if let Err(e) = self.stop_store(store_id) {
            error!("stop store {} err {:?}", store_id, e);
        }
    }
}
