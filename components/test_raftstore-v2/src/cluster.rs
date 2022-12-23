// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry as MapEntry,
    sync::{Arc, Mutex, RwLock},
    time::Duration,
};

use collections::{HashMap, HashSet};
use encryption_export::DataKeyManager;
use engine_rocks::RocksEngine;
use engine_test::raft::RaftTestEngine;
use engine_traits::{KvEngine, RaftEngine, RaftLogBatch, TabletFactory, TabletRegistry};
use file_system::IoRateLimiter;
use futures::executor::block_on;
use keys::data_key;
use kvproto::{
    errorpb::Error as PbError,
    kvrpcpb::ApiVersion,
    metapb::{self, Buckets, PeerRole, RegionEpoch},
    raft_cmdpb::{
        AdminCmdType, CmdType, RaftCmdRequest, RaftCmdResponse, RegionDetailResponse, Request,
        Response, StatusCmdType,
    },
    raft_serverpb::{RaftApplyState, RegionLocalState, StoreIdent},
};
use pd_client::PdClient;
use raftstore::{
    store::{
        cmd_resp, initial_region, util::check_key_in_region, Bucket, BucketRange, Callback,
        RegionSnapshot, WriteResponse, INIT_EPOCH_CONF_VER, INIT_EPOCH_VER,
    },
    Error, Result,
};
use raftstore_v2::{
    create_store_batch_system,
    router::{PeerMsg, QueryResult},
    write_initial_states, Bootstrap, StoreMeta, StoreRouter, StoreSystem,
};
use slog::o;
use tempfile::TempDir;
use test_pd_client::TestPdClient;
use test_raftstore::{
    is_error_response, new_admin_request, new_delete_cmd, new_delete_range_cmd, new_get_cf_cmd,
    new_peer, new_put_cf_cmd, new_region_detail_cmd, new_region_leader_cmd, new_request,
    new_snap_cmd, new_status_request, new_store, new_tikv_config_with_api_ver,
    new_transfer_leader_cmd, sleep_ms, Config, Filter, FilterFactory, PartitionFilterFactory,
};
use tikv::server::Result as ServerResult;
use tikv_util::{
    box_err, box_try, debug, error, safe_panic, thread_group::GroupProperties, time::Instant,
    timer::GLOBAL_TIMER_HANDLE, warn, HandyRwLock,
};

use crate::create_test_engine;

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by pd, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.
pub trait Simulator {
    // Pass 0 to let pd allocate a node id if db is empty.
    // If node id > 0, the node must be created in db already,
    // and the node id must be the same as given argument.
    // Return the node id.
    // TODO: we will rename node name here because now we use store only.
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        store_meta: Arc<Mutex<StoreMeta>>,
        router: StoreRouter<RocksEngine, RaftTestEngine>,
        system: StoreSystem<RocksEngine, RaftTestEngine>,
        raft_engine: RaftTestEngine,
        factory: TabletRegistry<RocksEngine>,
    ) -> ServerResult<u64>;

    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_send_filters(&mut self, node_id: u64);
    fn get_router(&self, node_id: u64) -> Option<StoreRouter<RocksEngine, RaftTestEngine>>;
    fn get_snap_dir(&self, node_id: u64) -> String;

    fn read(&mut self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse>;

    fn snapshot(
        &mut self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> std::result::Result<RegionSnapshot<<RocksEngine as KvEngine>::Snapshot>, RaftCmdResponse>;

    fn async_peer_msg_on_node(&self, node_id: u64, region_id: u64, msg: PeerMsg) -> Result<()>;

    fn call_query(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_query_on_node(node_id, request, timeout)
    }

    fn call_query_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let region_id = request.get_header().get_region_id();
        let (msg, sub) = PeerMsg::raft_query(request);
        match self.async_peer_msg_on_node(node_id, region_id, msg) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }

        // todo(SpadeA): unwrap and timeout
        match block_on(sub.result()).unwrap() {
            QueryResult::Read(_) => unreachable!(),
            QueryResult::Response(resp) => Ok(resp),
        }
    }

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        // let region_id = request.get_header().get_region_id();
        // let (msg, sub) = PeerMsg::raft_command(request);
        // match self.async_peer_msg_on_node(node_id, region_id, msg) {
        //     Ok(()) => {}
        //     Err(e) => {
        //         let mut resp = RaftCmdResponse::default();
        //         resp.mut_header().set_error(e.into());
        //         return Ok(resp);
        //     }
        // }

        // let timeout_f = GLOBAL_TIMER_HANDLE.delay(std::time::Instant::now() +
        // timeout); block_on(async move {
        //     select! {
        //         // todo: unwrap?
        //         res = sub.result().fuse() => Ok(res.unwrap()),
        //         _ = timeout_f.compat().fuse() => Err(Error::Timeout(format!("request
        // timeout for {:?}", timeout))),

        //     }
        // })
        unimplemented!()
    }
}

pub struct Cluster<T: Simulator> {
    pub cfg: Config,
    leaders: HashMap<u64, metapb::Peer>,
    pub count: usize,

    pub paths: Vec<TempDir>,
    pub dbs: Vec<(TabletRegistry<RocksEngine>, RaftTestEngine)>,
    pub tablet_factories: HashMap<u64, TabletRegistry<RocksEngine>>,
    pub raft_engines: HashMap<u64, RaftTestEngine>,
    pub store_metas: HashMap<u64, Arc<Mutex<StoreMeta>>>,
    key_managers: Vec<Option<Arc<DataKeyManager>>>,
    pub io_rate_limiter: Option<Arc<IoRateLimiter>>,
    key_managers_map: HashMap<u64, Option<Arc<DataKeyManager>>>,
    group_props: HashMap<u64, GroupProperties>,

    pub pd_client: Arc<TestPdClient>,

    pub sim: Arc<RwLock<T>>,
}

impl<T: Simulator> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
        api_version: ApiVersion,
    ) -> Cluster<T> {
        Cluster {
            cfg: Config {
                tikv: new_tikv_config_with_api_ver(id, api_version),
                prefer_mem: true,
            },
            count,
            tablet_factories: HashMap::default(),
            key_managers_map: HashMap::default(),
            group_props: HashMap::default(),
            raft_engines: HashMap::default(),
            store_metas: HashMap::default(),
            leaders: HashMap::default(),
            paths: vec![],
            dbs: vec![],
            key_managers: vec![],
            io_rate_limiter: None,
            sim,
            pd_client,
        }
    }

    pub fn id(&self) -> u64 {
        self.cfg.server.cluster_id
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in all stores, then start the cluster.
    pub fn run(&mut self) {
        self.create_engines();
        self.bootstrap_region().unwrap();
        self.start().unwrap();
    }

    // Bootstrap the store with fixed ID (like 1, 2, .. 5) and
    // initialize first region in store 1, then start the cluster.
    pub fn run_conf_change(&mut self) -> u64 {
        self.create_engines();
        let region_id = self.bootstrap_conf_change();
        self.start().unwrap();
        region_id
    }

    pub fn create_engines(&mut self) {
        self.io_rate_limiter = Some(Arc::new(
            self.cfg
                .storage
                .io_rate_limit
                .build(true /* enable_statistics */),
        ));
        for _ in 0..self.count {
            self.create_engine();
        }
    }

    fn create_engine(&mut self) {
        let (reg, raft_engine, key_manager, dir) =
            create_test_engine(self.io_rate_limiter.clone(), &self.cfg);
        self.dbs.push((reg, raft_engine));
        self.key_managers.push(key_manager);
        self.paths.push(dir);
    }

    pub fn start(&mut self) -> ServerResult<()> {
        if self.cfg.raft_store.store_io_pool_size == 0 {
            // v2 always use async write.
            self.cfg.raft_store.store_io_pool_size = 1;
        }

        let node_ids: Vec<u64> = self.tablet_factories.iter().map(|(&id, _)| id).collect();
        for node_id in node_ids {
            self.run_node(node_id)?;
        }

        // Try start new nodes.
        for _ in self.raft_engines.len()..self.count {
            let logger = slog_global::borrow_global().new(o!());
            self.create_engine();
            let (tablet_registry, raft_engine) = self.dbs.last().unwrap().clone();
            let id = Bootstrap::new(&raft_engine, self.id(), &*self.pd_client, logger.clone())
                .bootstrap_store()?;

            let (router, system) = create_store_batch_system(&self.cfg.raft_store, id, logger);

            let key_mgr = self.key_managers.last().unwrap().clone();
            let store_meta = Arc::new(Mutex::new(StoreMeta::default()));

            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            // todo: GroupProperties
            let mut sim = self.sim.wl();
            let node_id = sim.run_node(
                id,
                self.cfg.clone(),
                store_meta.clone(),
                router,
                system,
                raft_engine.clone(),
                tablet_registry.clone(),
            )?;
            assert_eq!(id, node_id);
            self.group_props.insert(node_id, props);
            self.raft_engines.insert(node_id, raft_engine);
            self.tablet_factories.insert(node_id, tablet_registry);
            self.store_metas.insert(node_id, store_meta);
            self.key_managers_map.insert(node_id, key_mgr);
        }

        Ok(())
    }

    pub fn run_node(&mut self, node_id: u64) -> ServerResult<()> {
        debug!("starting node {}", node_id);
        let logger = slog_global::borrow_global().new(o!());
        let factory = self.tablet_factories[&node_id].clone();
        let raft_engine = self.raft_engines[&node_id].clone();
        let cfg = self.cfg.clone();

        // if let Some(labels) = self.labels.get(&node_id) {
        //     cfg.server.labels = labels.to_owned();
        // }
        let (router, system) = create_store_batch_system(&cfg.raft_store, node_id, logger.clone());
        let store_meta = match self.store_metas.entry(node_id) {
            MapEntry::Occupied(o) => {
                let mut meta = o.get().lock().unwrap();
                *meta = StoreMeta::default();
                o.get().clone()
            }
            MapEntry::Vacant(v) => v.insert(Arc::new(Mutex::new(StoreMeta::default()))).clone(),
        };

        let props = GroupProperties::default();
        self.group_props.insert(node_id, props.clone());
        tikv_util::thread_group::set_properties(Some(props));

        debug!("calling run node"; "node_id" => node_id);
        self.sim.wl().run_node(
            node_id,
            cfg,
            store_meta,
            router,
            system,
            raft_engine,
            factory,
        )?;
        debug!("node {} started", node_id);
        Ok(())
    }

    pub fn stop_node(&mut self, node_id: u64) {
        debug!("stopping node {}", node_id);
        self.group_props[&node_id].mark_shutdown();

        match self.sim.write() {
            Ok(mut sim) => sim.stop_node(node_id),
            Err(_) => safe_panic!("failed to acquire write lock."),
        }
        self.pd_client.shutdown_store(node_id);
        debug!("node {} stopped", node_id);
    }

    /// Multiple nodes with fixed node id, like node 1, 2, .. 5,
    /// First region 1 is in all stores with peer 1, 2, .. 5.
    /// Peer 1 is in node 1, store 1, etc.
    ///
    /// Must be called after `create_engines`.
    pub fn bootstrap_region(&mut self) -> Result<()> {
        for (i, (tablet_registry, raft_engine)) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.tablet_factories.insert(id, tablet_registry.clone());
            self.raft_engines.insert(id, raft_engine.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::default()));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
            // todo: sst workers
        }

        let mut region = metapb::Region::default();
        region.set_id(1);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        for (&id, raft_engine) in &self.raft_engines {
            let peer = new_peer(id, id);
            region.mut_peers().push(peer.clone());
            bootstrap_store(raft_engine, self.id(), id).unwrap();
        }

        for raft_engine in self.raft_engines.values() {
            let mut wb = raft_engine.log_batch(10);
            wb.put_prepare_bootstrap_region(&region)?;
            write_initial_states(&mut wb, region.clone())?;
            box_try!(raft_engine.consume(&mut wb, true));
        }

        self.bootstrap_cluster(region);

        Ok(())
    }

    pub fn bootstrap_conf_change(&mut self) -> u64 {
        for (i, (factory, raft_engine)) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.tablet_factories.insert(id, factory.clone());
            self.raft_engines.insert(id, raft_engine.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::default()));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
            // todo: sst workers
        }

        for (&id, raft_engine) in &self.raft_engines {
            bootstrap_store(raft_engine, self.id(), id).unwrap();
        }

        let node_id = 1;
        let region_id = 1;
        let peer_id = 1;

        let region = initial_region(node_id, region_id, peer_id);
        let raft_engine = self.raft_engines[&node_id].clone();
        let mut wb = raft_engine.log_batch(10);
        wb.put_prepare_bootstrap_region(&region).unwrap();
        write_initial_states(&mut wb, region.clone()).unwrap();
        raft_engine.consume(&mut wb, true).unwrap();

        self.bootstrap_cluster(region);

        region_id
    }

    // This is only for fixed id test
    fn bootstrap_cluster(&mut self, region: metapb::Region) {
        self.pd_client
            .bootstrap_cluster(new_store(1, "".to_owned()), region)
            .unwrap();
        for id in self.raft_engines.keys() {
            let store = new_store(*id, "".to_owned());
            // todo: labels
            self.pd_client.put_store(store).unwrap();
        }
    }

    // pub fn get_engine(&self, node_id: u64) -> WrapFactory {
    //     WrapFactory::new(
    //         self.pd_client.clone(),
    //         self.raft_engines[&node_id].clone(),
    //         self.tablet_factories[&node_id].clone(),
    //     )
    // }
}

pub fn bootstrap_store<ER: RaftEngine>(
    raft_engine: &ER,
    cluster_id: u64,
    store_id: u64,
) -> Result<()> {
    let mut ident = StoreIdent::default();

    if !raft_engine.is_empty()? {
        return Err(box_err!("store is not empty and has already had data"));
    }

    ident.set_cluster_id(cluster_id);
    ident.set_store_id(store_id);

    let mut lb = raft_engine.log_batch(1);
    lb.put_store_ident(&ident)?;
    raft_engine.consume(&mut lb, true)?;

    Ok(())
}
