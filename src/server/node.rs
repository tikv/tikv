// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::thread;
use std::sync::{Arc, mpsc};
use std::sync::mpsc::Receiver;
use std::time::Duration;
use std::process;

use mio::EventLoop;
use rocksdb::DB;

use pd::{INVALID_ID, PdClient, Error as PdError};
use kvproto::raft_serverpb::StoreIdent;
use kvproto::metapb;
use protobuf::RepeatedField;
use util::transport::SendCh;
use raftstore::store::{self, Msg, SnapshotStatusMsg, StoreChannel, Store, Config as StoreConfig,
                       keys, Peekable, Transport, SnapManager};
use super::Result;
use super::config::Config;
use storage::{Storage, RaftKv};
use super::transport::RaftStoreRouter;

const MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT: u64 = 60;
const CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS: u64 = 3;

pub fn create_raft_storage<S>(router: S, kv_db: Arc<DB>, cfg: &Config) -> Result<Storage>
    where S: RaftStoreRouter + 'static
{
    let kv_engine = box RaftKv::new(kv_db, router);
    let store = try!(Storage::from_engine(kv_engine, &cfg.storage));
    Ok(store)
}

fn check_region_epoch(region: &metapb::Region, other: &metapb::Region) -> Result<()> {
    let epoch = region.get_region_epoch();
    let other_epoch = other.get_region_epoch();
    if epoch.get_conf_ver() != other_epoch.get_conf_ver() {
        return Err(box_err!("region conf_ver inconsist: {} with {}",
                            epoch.get_conf_ver(),
                            other_epoch.get_conf_ver()));
    }
    if epoch.get_version() != other_epoch.get_version() {
        return Err(box_err!("region version inconsist: {} with {}",
                            epoch.get_version(),
                            other_epoch.get_version()));
    }
    Ok(())
}

// Node is a wrapper for raft store.
// TODO: we will rename another better name like RaftStore later.
pub struct Node<C: PdClient + 'static> {
    cluster_id: u64,
    store: metapb::Store,
    store_cfg: StoreConfig,
    store_handle: Option<thread::JoinHandle<()>>,
    ch: SendCh<Msg>,

    pd_client: Arc<C>,
}

impl<C> Node<C>
    where C: PdClient
{
    pub fn new<T>(event_loop: &mut EventLoop<Store<T, C>>,
                  cfg: &Config,
                  pd_client: Arc<C>)
                  -> Node<C>
        where T: Transport + 'static
    {
        let mut store = metapb::Store::new();
        store.set_id(INVALID_ID);
        if cfg.advertise_addr.is_empty() {
            store.set_address(cfg.addr.clone());
        } else {
            store.set_address(cfg.advertise_addr.clone())
        }

        let mut labels = Vec::new();
        for (k, v) in &cfg.labels {
            let mut label = metapb::StoreLabel::new();
            label.set_key(k.to_owned());
            label.set_value(v.to_owned());
            labels.push(label);
        }
        store.set_labels(RepeatedField::from_vec(labels));

        let ch = SendCh::new(event_loop.channel(), "raftstore");
        Node {
            cluster_id: cfg.cluster_id,
            store: store,
            store_cfg: cfg.raft_store.clone(),
            store_handle: None,
            pd_client: pd_client,
            ch: ch,
        }
    }

    pub fn start<T>(&mut self,
                    event_loop: EventLoop<Store<T, C>>,
                    raft_engine: Arc<DB>,
                    kv_engine: Arc<DB>,
                    trans: T,
                    snap_mgr: SnapManager,
                    snap_status_receiver: Receiver<SnapshotStatusMsg>)
                    -> Result<()>
        where T: Transport + 'static
    {
        let bootstrapped = try!(self.check_cluster_bootstrapped());
        let mut store_id = try!(self.check_store(&raft_engine));
        if store_id == INVALID_ID {
            store_id = try!(self.bootstrap_store(&raft_engine));
        } else if !bootstrapped {
            // We have saved data before, and the cluster must be bootstrapped.
            return Err(box_err!("store {} is not empty, but cluster {} is not bootstrapped, \
                                 maybe you connected a wrong PD or need to remove the TiKV data \
                                 and start again",
                                store_id,
                                self.cluster_id));
        }

        self.store.set_id(store_id);
        try!(self.check_prepare_bootstrap_cluster(&raft_engine, &kv_engine));
        if !bootstrapped {
            // cluster is not bootstrapped, and we choose first store to bootstrap
            // prepare bootstrap.
            let region = try!(self.prepare_bootstrap_cluster(&raft_engine, &kv_engine, store_id));
            try!(self.bootstrap_cluster(&raft_engine, &kv_engine, region));
        }

        // inform pd.
        try!(self.pd_client
            .put_store(self.store.clone()));
        try!(self.start_store(event_loop,
                              store_id,
                              raft_engine,
                              kv_engine,
                              trans,
                              snap_mgr,
                              snap_status_receiver));
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.store.get_id()
    }

    pub fn get_sendch(&self) -> SendCh<Msg> {
        self.ch.clone()
    }

    // check store, return store id for the engine.
    // If the store is not bootstrapped, use INVALID_ID.
    fn check_store(&self, raft_engine: &DB) -> Result<u64> {
        let res = try!(raft_engine.get_msg::<StoreIdent>(&keys::store_ident_key()));
        if res.is_none() {
            return Ok(INVALID_ID);
        }

        let ident = res.unwrap();
        if ident.get_cluster_id() != self.cluster_id {
            error!("cluster ID mismatch: local_id {} remote_id {}. \
            you are trying to connect to another cluster, please reconnect to the correct PD",
                   ident.get_cluster_id(),
                   self.cluster_id);
            process::exit(1);
        }

        let store_id = ident.get_store_id();
        if store_id == INVALID_ID {
            return Err(box_err!("invalid store ident {:?}", ident));
        }

        Ok(store_id)
    }

    fn alloc_id(&self) -> Result<u64> {
        let id = try!(self.pd_client.alloc_id());
        Ok(id)
    }

    fn bootstrap_store(&self, raft_engine: &DB) -> Result<u64> {
        let store_id = try!(self.alloc_id());
        info!("alloc store id {} ", store_id);

        try!(store::bootstrap_store(raft_engine, self.cluster_id, store_id));

        Ok(store_id)
    }

    pub fn prepare_bootstrap_cluster(&self, raft_engine: &DB, kv_engine: &DB, store_id: u64)
                                     -> Result<metapb::Region> {
        let region_id = try!(self.alloc_id());
        info!("alloc first region id {} for cluster {}, store {}",
              region_id,
              self.cluster_id,
              store_id);
        let peer_id = try!(self.alloc_id());
        info!("alloc first peer id {} for first region {}",
              peer_id,
              region_id);

        let region = try!(store::prepare_bootstrap(raft_engine, kv_engine, store_id, region_id, peer_id));
        Ok(region)
    }

    fn check_prepare_bootstrap_cluster(&self, raft_engine: &DB, kv_engine: &DB) -> Result<()> {
        let res = try!(raft_engine.get_msg::<metapb::Region>(&keys::prepare_bootstrap_key()));
        if res.is_none() {
            return Ok(());
        }

        let first_region = res.unwrap();
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.get_region(b"") {
                Ok(region) => {
                    if region.get_id() == first_region.get_id() {
                        try!(check_region_epoch(&region, &first_region));
                        try!(store::clear_prepare_bootstrap_state(raft_engine));
                    } else {
                        try!(store::clear_prepare_bootstrap(raft_engine,
                                                            kv_engine,
                                                            first_region.get_id()));
                    }
                    return Ok(());
                }

                Err(e) => {
                    warn!("check cluster prepare bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS));
        }
        Err(box_err!("check cluster prepare bootstrapped failed"))
    }

    fn bootstrap_cluster(&mut self, raft_engine: &DB, kv_engine: &DB, region: metapb::Region) -> Result<()> {
        let region_id = region.get_id();
        match self.pd_client.bootstrap_cluster(self.store.clone(), region) {
            Err(PdError::ClusterBootstrapped(_)) => {
                error!("cluster {} is already bootstrapped", self.cluster_id);
                try!(store::clear_prepare_bootstrap(raft_engine, kv_engine, region_id));
                Ok(())
            }
            // TODO: should we clean region for other errors too?
            Err(e) => panic!("bootstrap cluster {} err: {:?}", self.cluster_id, e),
            Ok(_) => {
                try!(store::clear_prepare_bootstrap_state(raft_engine));
                info!("bootstrap cluster {} ok", self.cluster_id);
                Ok(())
            }
        }
    }

    fn check_cluster_bootstrapped(&self) -> Result<bool> {
        for _ in 0..MAX_CHECK_CLUSTER_BOOTSTRAPPED_RETRY_COUNT {
            match self.pd_client.is_cluster_bootstrapped() {
                Ok(b) => return Ok(b),
                Err(e) => {
                    warn!("check cluster bootstrapped failed: {:?}", e);
                }
            }
            thread::sleep(Duration::from_secs(CHECK_CLUSTER_BOOTSTRAPPED_RETRY_SECONDS));
        }
        Err(box_err!("check cluster bootstrapped failed"))
    }

    fn start_store<T>(&mut self,
                      mut event_loop: EventLoop<Store<T, C>>,
                      store_id: u64,
                      raft_db: Arc<DB>,
                      kv_db: Arc<DB>,
                      trans: T,
                      snap_mgr: SnapManager,
                      snapshot_status_receiver: Receiver<SnapshotStatusMsg>)
                      -> Result<()>
        where T: Transport + 'static
    {
        info!("start raft store {} thread", store_id);

        if self.store_handle.is_some() {
            return Err(box_err!("{} is already started", store_id));
        }

        let cfg = self.store_cfg.clone();
        let pd_client = self.pd_client.clone();
        let store = self.store.clone();
        let sender = event_loop.channel();

        let (tx, rx) = mpsc::channel();
        let builder = thread::Builder::new().name(thd_name!(format!("raftstore-{}", store_id)));
        let h = try!(builder.spawn(move || {
            let ch = StoreChannel {
                sender: sender,
                snapshot_status_receiver: snapshot_status_receiver,
            };
            let mut store = match Store::new(ch, store, cfg, raft_db, kv_db, trans, pd_client, snap_mgr) {
                Err(e) => panic!("construct store {} err {:?}", store_id, e),
                Ok(s) => s,
            };
            tx.send(0).unwrap();
            if let Err(e) = store.run(&mut event_loop) {
                error!("store {} run err {:?}", store_id, e);
            };
        }));
        // wait for store to be initialized
        rx.recv().unwrap();

        self.store_handle = Some(h);
        Ok(())
    }

    fn stop_store(&mut self, store_id: u64) -> Result<()> {
        info!("stop raft store {} thread", store_id);
        let h = match self.store_handle.take() {
            None => return Ok(()),
            Some(h) => h,
        };

        box_try!(self.ch.send(Msg::Quit));
        if let Err(e) = h.join() {
            return Err(box_err!("join store {} thread err {:?}", store_id, e));
        }

        Ok(())
    }

    pub fn stop(&mut self) -> Result<()> {
        let store_id = self.store.get_id();
        self.stop_store(store_id)
    }
}

impl<C> Drop for Node<C>
    where C: PdClient
{
    fn drop(&mut self) {
        self.stop().unwrap();
    }
}
#[cfg(test)]
mod tests {
    use raftstore::store::keys;
    use super::check_region_epoch;
    use kvproto::metapb;

    #[test]
    fn test_check_region_epoch() {
        let mut r1 = metapb::Region::new();
        r1.set_id(1);
        r1.set_start_key(keys::EMPTY_KEY.to_vec());
        r1.set_end_key(keys::EMPTY_KEY.to_vec());
        r1.mut_region_epoch().set_version(1);
        r1.mut_region_epoch().set_conf_ver(1);

        let mut r2 = metapb::Region::new();
        r2.set_id(1);
        r2.set_start_key(keys::EMPTY_KEY.to_vec());
        r2.set_end_key(keys::EMPTY_KEY.to_vec());
        r2.mut_region_epoch().set_version(2);
        r2.mut_region_epoch().set_conf_ver(1);

        let mut r3 = metapb::Region::new();
        r3.set_id(1);
        r3.set_start_key(keys::EMPTY_KEY.to_vec());
        r3.set_end_key(keys::EMPTY_KEY.to_vec());
        r3.mut_region_epoch().set_version(1);
        r3.mut_region_epoch().set_conf_ver(2);

        assert!(check_region_epoch(&r1, &r2).is_err());
        assert!(check_region_epoch(&r1, &r3).is_err());
    }
}
