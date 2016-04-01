#![allow(dead_code)]

use std::collections::{HashMap, BTreeMap, HashSet};
use std::vec::Vec;
use std::collections::Bound::{Excluded, Unbounded};
use std::sync::{Mutex, mpsc};

use kvproto::metapb;
use kvproto::pdpb;
use tikv::pd::{PdClient, Result, Error, Key};
use tikv::raftstore::store::keys::{enc_end_key, data_key};

#[derive(Default)]
struct Store {
    store: metapb::Store,
    region_ids: HashSet<u64>,
}

struct Cluster {
    meta: metapb::Cluster,
    stores: HashMap<u64, Store>,
    regions: BTreeMap<Key, metapb::Region>,
    region_id_keys: HashMap<u64, Key>,
}

impl Cluster {
    pub fn new(cluster_id: u64, store: metapb::Store, region: metapb::Region) -> Cluster {
        let mut meta = metapb::Cluster::new();
        meta.set_id(cluster_id);
        meta.set_max_peer_number(5);

        let mut c = Cluster {
            meta: meta,
            stores: HashMap::new(),
            regions: BTreeMap::new(),
            region_id_keys: HashMap::new(),
        };

        // Now, some tests use multi peers in bootstrap,
        // disable this check.
        // TODO: enable this check later.
        // assert_eq!(region.get_peers().len(), 1);
        let store_id = store.get_id();
        let mut s = Store {
            store: store,
            region_ids: HashSet::new(),
        };


        s.region_ids.insert(region.get_id());

        c.stores.insert(store_id, s);

        c.region_id_keys.insert(region.get_id(), enc_end_key(&region));
        c.regions.insert(enc_end_key(&region), region);

        c
    }

    fn put_store(&mut self, store: metapb::Store) -> Result<()> {
        let mut s = self.stores.entry(store.get_id()).or_insert_with(Store::default);
        s.store = store;
        Ok(())
    }

    fn get_store(&self, store_id: u64) -> Result<metapb::Store> {
        Ok(self.stores.get(&store_id).unwrap().store.clone())
    }

    fn get_region(&self, key: Vec<u8>) -> Result<metapb::Region> {
        let (_, region) = self.regions
                              .range::<Key, Key>(Excluded(&key), Unbounded)
                              .next()
                              .unwrap();
        Ok(region.clone())
    }

    fn get_region_by_id(&self, region_id: u64) -> Result<metapb::Region> {
        let key = self.region_id_keys.get(&region_id).unwrap();
        if self.regions.contains_key(key) {
            Ok(self.regions[key].clone())
        } else {
            Err(box_err!("region of {} not exist!!!", region_id))
        }
    }

    fn change_peer(&mut self, region: metapb::Region) -> Result<()> {
        let end_key = enc_end_key(&region);
        if !self.regions.contains_key(&end_key) {
            return Err(box_err!("region {:?} doesn't exist", region));
        }

        for peer in region.get_peers() {
            let store = self.stores.get_mut(&peer.get_store_id()).unwrap();
            store.region_ids.insert(region.get_id());
        }

        assert!(self.regions.insert(end_key, region).is_some());

        Ok(())
    }

    fn split_region(&mut self, left: metapb::Region, right: metapb::Region) -> Result<()> {
        let left_end_key = enc_end_key(&left);
        let right_end_key = enc_end_key(&right);

        assert!(left.get_start_key() < left.get_end_key());
        assert_eq!(left.get_end_key(), right.get_start_key());
        assert!(right.get_end_key().is_empty() || right.get_start_key() < right.get_end_key());

        // origin pre-split region's end key is the same as right end key,
        // and must exists.
        if !self.regions.contains_key(&right_end_key) {
            return Err(box_err!("region {:?} doesn't exist", right));
        }

        if self.regions.contains_key(&left_end_key) {
            return Err(box_err!("region {:?} has already existed", left));
        }

        assert!(self.region_id_keys.insert(left.get_id(), left_end_key.clone()).is_some());
        assert!(self.region_id_keys.insert(right.get_id(), right_end_key.clone()).is_none());

        for peer in right.get_peers() {
            let store = self.stores.get_mut(&peer.get_store_id()).unwrap();
            store.region_ids.insert(right.get_id());
        }

        assert!(self.regions.insert(left_end_key, left).is_none());
        assert!(self.regions.insert(right_end_key, right).is_some());
        debug!("cluster.regions: {:?}", self.regions);

        Ok(())
    }

    fn get_stores(&self) -> Vec<metapb::Store> {
        self.stores.values().map(|s| s.store.clone()).collect()
    }
}

pub struct TestPdClient {
    clusters: HashMap<u64, Cluster>,

    base_id: u64,

    ask_tx: Mutex<mpsc::Sender<pdpb::Request>>,
}

impl TestPdClient {
    pub fn new(tx: mpsc::Sender<pdpb::Request>) -> TestPdClient {
        TestPdClient {
            clusters: HashMap::new(),
            base_id: 1000,
            ask_tx: Mutex::new(tx),
        }
    }

    fn get_cluster(&self, cluster_id: u64) -> Result<&Cluster> {
        match self.clusters.get(&cluster_id) {
            None => Err(Error::ClusterNotBootstrapped(cluster_id)),
            Some(cluster) => Ok(cluster),
        }
    }

    fn get_mut_cluster(&mut self, cluster_id: u64) -> Result<&mut Cluster> {
        match self.clusters.get_mut(&cluster_id) {
            None => Err(Error::ClusterNotBootstrapped(cluster_id)),
            Some(cluster) => Ok(cluster),
        }
    }

    pub fn change_peer(&mut self, cluster_id: u64, region: metapb::Region) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.change_peer(region)
    }

    pub fn split_region(&mut self,
                        cluster_id: u64,
                        left: metapb::Region,
                        right: metapb::Region)
                        -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.split_region(left, right)
    }

    pub fn get_stores(&self, cluster_id: u64) -> Result<Vec<metapb::Store>> {
        let cluster = try!(self.get_cluster(cluster_id));
        Ok(cluster.get_stores())
    }

    pub fn get_region_by_id(&self, cluster_id: u64, region_id: u64) -> Result<metapb::Region> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_region_by_id(region_id)
    }
}

impl PdClient for TestPdClient {
    fn bootstrap_cluster(&mut self,
                         cluster_id: u64,
                         store: metapb::Store,
                         region: metapb::Region)
                         -> Result<()> {
        if self.is_cluster_bootstrapped(cluster_id).unwrap() {
            return Err(Error::ClusterBootstrapped(cluster_id));
        }

        self.clusters.insert(cluster_id, Cluster::new(cluster_id, store, region));

        Ok(())
    }

    fn is_cluster_bootstrapped(&self, cluster_id: u64) -> Result<bool> {
        Ok(self.clusters.contains_key(&cluster_id))
    }

    fn alloc_id(&mut self) -> Result<u64> {
        self.base_id += 1;
        Ok(self.base_id)
    }

    fn put_store(&mut self, cluster_id: u64, store: metapb::Store) -> Result<()> {
        let mut cluster = try!(self.get_mut_cluster(cluster_id));
        cluster.put_store(store)
    }

    fn get_store(&self, cluster_id: u64, store_id: u64) -> Result<metapb::Store> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_store(store_id)
    }


    fn get_region(&self, cluster_id: u64, key: &[u8]) -> Result<metapb::Region> {
        let cluster = try!(self.get_cluster(cluster_id));
        cluster.get_region(data_key(key))
    }

    fn get_cluster_meta(&self, cluster_id: u64) -> Result<metapb::Cluster> {
        let cluster = try!(self.get_cluster(cluster_id));
        Ok(cluster.meta.clone())
    }

    fn ask_change_peer(&self,
                       cluster_id: u64,
                       region: metapb::Region,
                       leader: metapb::Peer)
                       -> Result<()> {
        try!(self.get_cluster(cluster_id));
        let mut req = pdpb::Request::new();
        req.mut_header().set_cluster_id(cluster_id);
        req.set_cmd_type(pdpb::CommandType::AskChangePeer);
        req.mut_ask_change_peer().set_region(region);
        req.mut_ask_change_peer().set_leader(leader);

        self.ask_tx.lock().unwrap().send(req).unwrap();

        Ok(())
    }

    fn ask_split(&self,
                 cluster_id: u64,
                 region: metapb::Region,
                 split_key: &[u8],
                 leader: metapb::Peer)
                 -> Result<()> {
        try!(self.get_cluster(cluster_id));
        let mut req = pdpb::Request::new();
        req.mut_header().set_cluster_id(cluster_id);
        req.set_cmd_type(pdpb::CommandType::AskSplit);
        req.mut_ask_split().set_region(region);
        req.mut_ask_split().set_leader(leader);
        req.mut_ask_split().set_split_key(split_key.to_vec());

        self.ask_tx.lock().unwrap().send(req).unwrap();

        Ok(())
    }
}
