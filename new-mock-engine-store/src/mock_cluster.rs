// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry as MapEntry,
    result,
    sync::{atomic::AtomicU8, Arc, Mutex, RwLock},
    thread,
    time::Duration,
};

use collections::{HashMap, HashSet};
use encryption::DataKeyManager;
// mock cluster
pub use engine_store_ffi::ffi::{
    interfaces_ffi,
    interfaces_ffi::{
        EngineStoreServerHelper, RaftProxyStatus, RaftStoreProxyFFIHelper, RawCppPtr,
    },
    RaftStoreProxy, RaftStoreProxyFFI, UnwrapExternCFunc,
};
pub use engine_store_ffi::TiFlashEngine;
use engine_tiflash::DB;
use engine_traits::{Engines, KvEngine, Peekable, CF_DEFAULT};
use file_system::IoRateLimiter;
use futures::executor::block_on;
use kvproto::{
    errorpb::Error as PbError,
    metapb::{self, PeerRole, RegionEpoch, StoreLabel},
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse, Request, *},
    raft_serverpb::RaftMessage,
};
use pd_client::PdClient;
pub use proxy_server::config::ProxyConfig;
use raftstore::{
    router::RaftStoreRouter,
    store::{
        bootstrap_store,
        fsm::{
            create_raft_batch_system,
            store::{StoreMeta, PENDING_MSG_CAP},
            RaftBatchSystem,
        },
        initial_region,
        msg::StoreTick,
        prepare_bootstrap_cluster, Callback, CasualMessage, CasualRouter, RaftCmdExtraOpts,
        RaftRouter, SnapManager, StoreMsg, StoreRouter, WriteResponse, INIT_EPOCH_CONF_VER,
        INIT_EPOCH_VER,
    },
    Error, Result,
};
use resource_control::ResourceGroupManager;
use tempfile::TempDir;
pub use test_pd_client::TestPdClient;
use test_raftstore::FilterFactory;
pub use test_raftstore::{
    is_error_response, make_cb, new_admin_request, new_delete_cmd, new_peer, new_put_cf_cmd,
    new_put_cmd, new_region_leader_cmd, new_request, new_status_request, new_store,
    new_tikv_config, new_transfer_leader_cmd, sleep_ms,
};
use tikv::{config::TikvConfig, server::Result as ServerResult};
use tikv_util::{
    debug, error, escape, safe_panic,
    sys::SysQuota,
    thread_group::GroupProperties,
    time::{Instant, ThreadReadId},
    warn, HandyRwLock,
};
use tokio::sync::oneshot;
use txn_types::WriteBatchFlags;

pub use crate::config::Config;
use crate::{
    gen_engine_store_server_helper, transport_simulate::Filter, EngineStoreServer,
    EngineStoreServerWrap, MockConfig,
};

pub struct FFIHelperSet {
    pub proxy: Box<engine_store_ffi::ffi::RaftStoreProxy>,
    pub proxy_helper: Box<RaftStoreProxyFFIHelper>,
    pub engine_store_server: Box<EngineStoreServer>,
    // Make interface happy, don't own proxy and server.
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<EngineStoreServerHelper>,
    pub engine_store_server_helper_ptr: isize,
}

pub struct EngineHelperSet {
    pub engine_store_server: Box<EngineStoreServer>,
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<EngineStoreServerHelper>,
}

pub struct TestData {
    pub expected_leader_safe_ts: u64,
    pub expected_self_safe_ts: u64,
}

pub struct Cluster<T: Simulator<TiFlashEngine>> {
    // Helper to set ffi_helper_set.
    pub ffi_helper_lst: Vec<FFIHelperSet>,
    ffi_helper_set: Arc<Mutex<HashMap<u64, FFIHelperSet>>>,

    pub cfg: Config,
    leaders: HashMap<u64, metapb::Peer>,
    pub count: usize,
    pub paths: Vec<TempDir>,
    pub dbs: Vec<Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    pub store_metas: HashMap<u64, Arc<Mutex<StoreMeta>>>,
    pub key_managers: Vec<Option<Arc<DataKeyManager>>>,
    pub io_rate_limiter: Option<Arc<IoRateLimiter>>,
    pub engines: HashMap<u64, Engines<TiFlashEngine, engine_rocks::RocksEngine>>,
    pub key_managers_map: HashMap<u64, Option<Arc<DataKeyManager>>>,
    pub labels: HashMap<u64, HashMap<String, String>>,
    pub group_props: HashMap<u64, GroupProperties>,
    pub sim: Arc<RwLock<T>>,
    pub pd_client: Arc<TestPdClient>,
    pub test_data: TestData,
    resource_manager: Option<Arc<ResourceGroupManager>>,
}

impl<T: Simulator<TiFlashEngine>> std::panic::UnwindSafe for Cluster<T> {}

impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn new(
        id: u64,
        count: usize,
        sim: Arc<RwLock<T>>,
        pd_client: Arc<TestPdClient>,
        proxy_cfg: ProxyConfig,
    ) -> Cluster<T> {
        test_util::init_log_for_test();
        // Force sync to enable Leader run as a Leader, rather than proxy
        fail::cfg("apply_on_handle_snapshot_sync", "return").unwrap();

        Cluster {
            ffi_helper_lst: Vec::default(),
            ffi_helper_set: Arc::new(Mutex::new(HashMap::default())),

            cfg: Config {
                tikv: new_tikv_config(id),
                prefer_mem: true,
                proxy_cfg,
                proxy_compat: false,
                mock_cfg: Default::default(),
            },
            leaders: HashMap::default(),
            count,
            paths: vec![],
            dbs: vec![],
            store_metas: HashMap::default(),
            key_managers: vec![],
            io_rate_limiter: None,
            engines: HashMap::default(),
            key_managers_map: HashMap::default(),
            labels: HashMap::default(),
            group_props: HashMap::default(),
            sim,
            pd_client,
            test_data: TestData {
                expected_leader_safe_ts: 0,
                expected_self_safe_ts: 0,
            },
            resource_manager: Some(Arc::new(ResourceGroupManager::default())),
        }
    }

    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
        node_cfg: TikvConfig,
        cluster_id: isize,
        proxy_compat: bool,
        mock_cfg: MockConfig,
    ) -> (FFIHelperSet, TikvConfig) {
        // We must allocate on heap to avoid move.
        let proxy = Box::new(engine_store_ffi::ffi::RaftStoreProxy::new(
            AtomicU8::new(RaftProxyStatus::Idle as u8),
            key_mgr.clone(),
            match router {
                Some(r) => Some(Box::new(
                    engine_store_ffi::ffi::read_index_helper::ReadIndexClient::new(
                        r.clone(),
                        SysQuota::cpu_cores_quota() as usize * 2,
                    ),
                )),
                None => None,
            },
            engine_store_ffi::ffi::RaftStoreProxyEngine::from_tiflash_engine(engines.kv.clone()),
        ));

        let proxy_ref = proxy.as_ref();
        let mut proxy_helper = Box::new(RaftStoreProxyFFIHelper::new(proxy_ref.into()));
        let mut engine_store_server = Box::new(EngineStoreServer::new(id, Some(engines)));
        engine_store_server.proxy_compat = proxy_compat;
        engine_store_server.mock_cfg = mock_cfg;
        let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
            &mut *engine_store_server,
            Some(&mut *proxy_helper),
            cluster_id,
        ));
        let engine_store_server_helper = Box::new(gen_engine_store_server_helper(
            std::pin::Pin::new(&*engine_store_server_wrap),
        ));

        let engine_store_server_helper_ptr = &*engine_store_server_helper as *const _ as isize;
        proxy
            .kv_engine()
            .write()
            .unwrap()
            .as_mut()
            .unwrap()
            .set_engine_store_server_helper(engine_store_server_helper_ptr);
        let ffi_helper_set = FFIHelperSet {
            proxy,
            proxy_helper,
            engine_store_server,
            engine_store_server_wrap,
            engine_store_server_helper,
            engine_store_server_helper_ptr,
        };
        (ffi_helper_set, node_cfg)
    }

    pub fn make_ffi_helper_set(
        &mut self,
        id: u64,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) -> (FFIHelperSet, TikvConfig) {
        Cluster::<T>::make_ffi_helper_set_no_bind(
            id,
            engines,
            key_mgr,
            router,
            self.cfg.tikv.clone(),
            self as *const Cluster<T> as isize,
            self.cfg.proxy_compat,
            self.cfg.mock_cfg.clone(),
        )
    }

    pub fn iter_ffi_helpers(
        &self,
        store_ids: Option<Vec<u64>>,
        f: &mut dyn FnMut(u64, &engine_store_ffi::TiFlashEngine, &mut FFIHelperSet),
    ) {
        let ids = match store_ids {
            Some(ids) => ids,
            None => self.engines.keys().copied().collect::<Vec<_>>(),
        };
        for id in ids {
            let engine = self.get_tiflash_engine(id);
            let lock = self.ffi_helper_set.lock();
            match lock {
                Ok(mut l) => {
                    let ffiset = l.get_mut(&id).unwrap();
                    f(id, &engine, ffiset);
                }
                Err(_) => std::process::exit(1),
            }
        }
    }

    pub fn access_ffi_helpers(&self, f: &mut dyn FnMut(&mut HashMap<u64, FFIHelperSet>)) {
        let lock = self.ffi_helper_set.lock();
        match lock {
            Ok(mut l) => {
                f(&mut l);
            }
            Err(_) => std::process::exit(1),
        }
    }

    pub fn create_engines(&mut self) {
        self.io_rate_limiter = Some(Arc::new(
            self.cfg
                .storage
                .io_rate_limit
                .build(true /* enable_statistics */),
        ));
        for _ in 0..self.count {
            self.create_engine(None);
        }
    }

    pub fn run(&mut self) {
        self.create_engines();
        self.bootstrap_region().unwrap();
        self.start().unwrap();
    }

    pub fn run_conf_change(&mut self) -> u64 {
        self.create_engines();
        let region_id = self.bootstrap_conf_change();
        // Will not start new nodes in `start`
        self.start().unwrap();
        region_id
    }

    pub fn run_conf_change_no_start(&mut self) -> u64 {
        self.create_engines();
        self.bootstrap_conf_change()
    }

    /// We need to create FFIHelperSet while we create engine.
    /// And later set its `node_id` when we are allocated one when start.
    pub fn create_ffi_helper_set(
        &mut self,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_manager: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) {
        init_global_ffi_helper_set();
        let (mut ffi_helper_set, _node_cfg) =
            self.make_ffi_helper_set(0, engines, key_manager, router);

        // We can not use moved or cloned engines any more.
        let (helper_ptr, ffi_hub) = {
            let helper_ptr = ffi_helper_set
                .proxy
                .kv_engine()
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .engine_store_server_helper();

            let helper = engine_store_ffi::ffi::gen_engine_store_server_helper(helper_ptr);
            let ffi_hub = Arc::new(engine_store_ffi::engine::TiFlashFFIHub {
                engine_store_server_helper: helper,
            });
            (helper_ptr, ffi_hub)
        };
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();
        let proxy_config_set = Arc::new(engine_tiflash::ProxyConfigSet {
            engine_store: self.cfg.proxy_cfg.engine_store.clone(),
        });
        engines.kv.init(
            helper_ptr,
            self.cfg.proxy_cfg.raft_store.snap_handle_pool_size,
            Some(ffi_hub),
            Some(proxy_config_set),
        );

        assert_ne!(engines.kv.engine_store_server_helper, 0);
        self.ffi_helper_lst.push(ffi_helper_set);
    }

    // If index is None, use the last in the list, which is added by
    // create_ffi_helper_set. In most cases, index is `Some(0)`, which means we
    // will use the first.
    pub fn associate_ffi_helper_set(&mut self, index: Option<usize>, node_id: u64) {
        let mut ffi_helper_set = if let Some(i) = index {
            self.ffi_helper_lst.remove(i)
        } else {
            self.ffi_helper_lst.pop().unwrap()
        };
        debug!("set up ffi helper set for {}", node_id);
        ffi_helper_set.engine_store_server.id = node_id;
        self.ffi_helper_set
            .lock()
            .unwrap()
            .insert(node_id, ffi_helper_set);
    }

    // Need self.engines be filled.
    pub fn bootstrap_ffi_helper_set(&mut self) {
        let mut node_ids: Vec<u64> = self.engines.iter().map(|(&id, _)| id).collect();
        // We force iterate engines in sorted order.
        node_ids.sort();
        for (_, node_id) in node_ids.iter().enumerate() {
            let node_id = *node_id;
            // Always at the front of the vector.
            self.associate_ffi_helper_set(Some(0), node_id);
        }
    }

    pub fn create_engine(
        &mut self,
        router: Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) {
        let (engines, key_manager, dir) =
            create_tiflash_test_engine(router.clone(), self.io_rate_limiter.clone(), &self.cfg);

        self.create_ffi_helper_set(engines, &key_manager, &router);
        let ffi_helper_set = self.ffi_helper_lst.last_mut().unwrap();
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();

        // replace self.create_engine
        self.dbs.push(engines.clone());
        self.key_managers.push(key_manager.clone());
        self.paths.push(dir);
    }

    pub fn start(&mut self) -> ServerResult<()> {
        self.start_with(Default::default())
    }

    pub fn start_with(&mut self, skip_set: HashSet<usize>) -> ServerResult<()> {
        // Try recover from last shutdown.
        // `self.engines` is inited in bootstrap_region or bootstrap_conf_change.
        let mut node_ids: Vec<u64> = self.engines.iter().map(|(&id, _)| id).collect();
        // We force iterate engines in sorted order.
        node_ids.sort();
        for (cnt, node_id) in node_ids.iter().enumerate() {
            let node_id = *node_id;
            if skip_set.contains(&cnt) {
                tikv_util::info!("skip start at {} is {}", cnt, node_id);
                continue;
            }
            debug!("recover node"; "node_id" => node_id);
            // Like TiKVServer::init
            self.run_node(node_id)?;
            // Since we use None to create_ffi_helper_set, we must init again.
            let router = self.sim.rl().get_router(node_id).unwrap();
            let mut lock = self.ffi_helper_set.lock().unwrap();
            let ffi_helper_set = lock.get_mut(&node_id).unwrap();
            ffi_helper_set.proxy.set_read_index_client(Some(Box::new(
                engine_store_ffi::ffi::read_index_helper::ReadIndexClient::new(
                    router.clone(),
                    SysQuota::cpu_cores_quota() as usize * 2,
                ),
            )));
        }

        // Try start new nodes.
        // Normally, this branch will not be called, since self.engines are already
        // added in bootstrap_region or bootstrap_conf_change.
        for _ in 0..self.count - self.engines.len() {
            if !skip_set.is_empty() {
                panic!("Error when start with skip set");
            }
            let (router, system) =
                create_raft_batch_system(&self.cfg.raft_store, &self.resource_manager);
            self.create_engine(Some(router.clone()));

            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
            let props = GroupProperties::default();
            tikv_util::thread_group::set_properties(Some(props.clone()));

            let engines = self.dbs.last().unwrap().clone();
            let key_manager = self.key_managers.last().unwrap().clone();
            let node_id = {
                let mut sim = self.sim.wl();
                let cfg = self.cfg.clone();
                // Like TiKVServer::init
                sim.run_node(
                    0,
                    cfg,
                    engines.clone(),
                    store_meta.clone(),
                    key_manager.clone(),
                    router,
                    system,
                )?
            };
            debug!("start new node {}", node_id);
            self.group_props.insert(node_id, props);
            self.engines.insert(node_id, engines.clone());
            self.store_metas.insert(node_id, store_meta);
            self.key_managers_map.insert(node_id, key_manager.clone());
            self.associate_ffi_helper_set(None, node_id);
        }
        assert_eq!(self.count, self.engines.len());
        assert_eq!(self.count, self.dbs.len());
        Ok(())
    }

    pub fn set_expected_safe_ts(&mut self, leader_safe_ts: u64, self_safe_ts: u64) {
        self.test_data.expected_leader_safe_ts = leader_safe_ts;
        self.test_data.expected_self_safe_ts = self_safe_ts;
    }
}

static mut GLOBAL_ENGINE_HELPER_SET: Option<EngineHelperSet> = None;
static START: std::sync::Once = std::sync::Once::new();

pub unsafe fn get_global_engine_helper_set() -> &'static Option<EngineHelperSet> {
    &GLOBAL_ENGINE_HELPER_SET
}

pub fn make_global_ffi_helper_set_no_bind() -> (EngineHelperSet, *const u8) {
    let mut engine_store_server = Box::new(EngineStoreServer::new(99999, None));
    let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
        &mut *engine_store_server,
        None,
        0,
    ));
    let engine_store_server_helper = Box::new(gen_engine_store_server_helper(std::pin::Pin::new(
        &*engine_store_server_wrap,
    )));
    let ptr = &*engine_store_server_helper as *const EngineStoreServerHelper as *mut u8;
    // Will mutate ENGINE_STORE_SERVER_HELPER_PTR
    (
        EngineHelperSet {
            engine_store_server,
            engine_store_server_wrap,
            engine_store_server_helper,
        },
        ptr,
    )
}

pub fn init_global_ffi_helper_set() {
    unsafe {
        START.call_once(|| {
            debug!("init_global_ffi_helper_set");
            assert_eq!(
                engine_store_ffi::ffi::get_engine_store_server_helper_ptr(),
                0
            );
            let (set, ptr) = make_global_ffi_helper_set_no_bind();
            engine_store_ffi::ffi::init_engine_store_server_helper(ptr);
            GLOBAL_ENGINE_HELPER_SET = Some(set);
        });
    }
}

pub fn create_tiflash_test_engine(
    // ref init_tiflash_engines and create_test_engine
    // TODO: pass it in for all cases.
    _router: Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    limiter: Option<Arc<IoRateLimiter>>,
    cfg: &Config,
) -> (
    Engines<TiFlashEngine, engine_rocks::RocksEngine>,
    Option<Arc<DataKeyManager>>,
    TempDir,
) {
    let dir = test_util::temp_dir("test_cluster", cfg.prefer_mem);
    let key_manager = encryption_export::data_key_manager_from_config(
        &cfg.security.encryption,
        dir.path().to_str().unwrap(),
    )
    .unwrap()
    .map(Arc::new);

    let env = engine_rocks::get_env(key_manager.clone(), limiter).unwrap();

    let kv_path = dir.path().join(tikv::config::DEFAULT_ROCKSDB_SUB_DIR);
    let kv_path_str = kv_path.to_str().unwrap();

    let kv_db_opt = cfg.rocksdb.build_opt(
        &cfg.rocksdb.build_resources(env.clone()),
        cfg.storage.engine,
    );

    let cache = cfg.storage.block_cache.build_shared_cache();
    let raft_cfs_opt = cfg.raftdb.build_cf_opts(&cache);

    let kv_cfs_opt = cfg.rocksdb.build_cf_opts(
        &cfg.rocksdb.build_cf_resources(cache),
        None,
        cfg.storage.api_version(),
        cfg.storage.engine,
    );

    let engine = engine_rocks::util::new_engine_opt(kv_path_str, kv_db_opt, kv_cfs_opt).unwrap();
    let engine = TiFlashEngine::from_rocks(engine);

    let raft_path = dir.path().join("raft");
    let raft_path_str = raft_path.to_str().unwrap();

    let raft_db_opt = cfg.raftdb.build_opt(env.clone(), None);

    let raft_engine =
        engine_rocks::util::new_engine_opt(raft_path_str, raft_db_opt, raft_cfs_opt).unwrap();

    // FFI is not usable, until create_engine.
    let engines = Engines::new(engine, raft_engine);
    (engines, key_manager, dir)
}

impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn call_command(
        &self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let mut is_read = false;
        for req in request.get_requests() {
            match req.get_cmd_type() {
                CmdType::Get | CmdType::Snap | CmdType::ReadIndex => {
                    is_read = true;
                }
                _ => (),
            }
        }
        let ret = if is_read {
            self.sim.wl().read(None, request.clone(), timeout)
        } else {
            self.sim.rl().call_command(request.clone(), timeout)
        };
        match ret {
            Err(e) => {
                warn!("failed to call command {:?}: {:?}", request, e);
                Err(e)
            }
            a => a,
        }
    }

    // It's similar to `ask_split`, the difference is the msg, it sends, is
    // `Msg::SplitRegion`, and `region` will not be embedded to that msg.
    // Caller must ensure that the `split_key` is in the `region`.
    pub fn split_region(
        &mut self,
        region: &metapb::Region,
        split_key: &[u8],
        cb: Callback<engine_rocks::RocksSnapshot>,
    ) {
        let leader = self.leader_of_region(region.get_id()).unwrap();
        let router = self.sim.rl().get_router(leader.get_store_id()).unwrap();
        let split_key = split_key.to_vec();
        CasualRouter::send(
            &router,
            region.get_id(),
            CasualMessage::SplitRegion {
                region_epoch: region.get_region_epoch().clone(),
                split_keys: vec![split_key],
                callback: cb,
                source: "test".into(),
            },
        )
        .unwrap();
    }

    pub fn leader_of_region(&mut self, region_id: u64) -> Option<metapb::Peer> {
        let timer = Instant::now_coarse();
        let timeout = Duration::from_secs(5);
        let mut store_ids = None;
        while timer.saturating_elapsed() < timeout {
            match self.voter_store_ids_of_region(region_id) {
                None => thread::sleep(Duration::from_millis(10)),
                Some(ids) => {
                    store_ids = Some(ids);
                    break;
                }
            };
        }
        let store_ids = store_ids?;
        if let Some(l) = self.leaders.get(&region_id) {
            // leader may be stopped in some tests.
            if self.valid_leader_id(region_id, l.get_store_id()) {
                return Some(l.clone());
            }
        }
        self.reset_leader_of_region(region_id);
        let mut leader = None;
        let mut leaders = HashMap::default();

        let node_ids = self.sim.rl().get_node_ids();
        // For some tests, we stop the node but pd still has this information,
        // and we must skip this.
        let alive_store_ids: Vec<_> = store_ids
            .iter()
            .filter(|id| node_ids.contains(id))
            .cloned()
            .collect();
        while timer.saturating_elapsed() < timeout {
            for store_id in &alive_store_ids {
                let l = match self.query_leader(*store_id, region_id, Duration::from_secs(1)) {
                    None => continue,
                    Some(l) => l,
                };
                leaders
                    .entry(l.get_id())
                    .or_insert((l, vec![]))
                    .1
                    .push(*store_id);
            }
            if let Some((_, (l, c))) = leaders.iter().max_by_key(|(_, (_, c))| c.len()) {
                // It may be a step down leader.
                if c.contains(&l.get_store_id()) {
                    leader = Some(l.clone());
                    // Technically, correct calculation should use two quorum when in joint
                    // state. Here just for simplicity.
                    if c.len() > store_ids.len() / 2 {
                        break;
                    }
                }
            }
            debug!("failed to detect leaders"; "leaders" => ?leaders, "store_ids" => ?store_ids);
            sleep_ms(10);
            leaders.clear();
        }

        if let Some(l) = leader {
            self.leaders.insert(region_id, l);
        }

        self.leaders.get(&region_id).cloned()
    }

    // This is only for fixed id test.
    fn bootstrap_cluster(&mut self, region: metapb::Region) {
        self.pd_client
            .bootstrap_cluster(new_store(1, "".to_owned()), region)
            .unwrap();
        for id in self.engines.keys() {
            let mut store = new_store(*id, "".to_owned());
            if let Some(labels) = self.labels.get(id) {
                for (key, value) in labels.iter() {
                    store.labels.push(StoreLabel {
                        key: key.clone(),
                        value: value.clone(),
                        ..Default::default()
                    });
                }
            }
            self.pd_client.put_store(store).unwrap();
        }
    }

    #[allow(clippy::significant_drop_in_scrutinee)]
    pub fn add_send_filter<F: FilterFactory>(&self, factory: F) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            for filter in factory.generate(node_id) {
                sim.add_send_filter(node_id, filter);
            }
        }
    }

    pub fn transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let epoch = self.get_region_epoch(region_id);
        let transfer_leader = new_admin_request(region_id, &epoch, new_transfer_leader_cmd(leader));
        let resp = self
            .call_command_on_leader(transfer_leader, Duration::from_secs(5))
            .unwrap();
        assert_eq!(
            resp.get_admin_response().get_cmd_type(),
            AdminCmdType::TransferLeader,
            "{:?}",
            resp
        );
    }

    // If the resp is "not leader error", get the real leader.
    // Otherwise reset or refresh leader if needed.
    // Returns if the request should retry.
    fn refresh_leader_if_needed(&mut self, resp: &RaftCmdResponse, region_id: u64) -> bool {
        if !is_error_response(resp) {
            return false;
        }

        let err = resp.get_header().get_error();
        if err
            .get_message()
            .contains("peer has not applied to current term")
        {
            // leader peer has not applied to current term
            return true;
        }

        // If command is stale, leadership may have changed.
        // EpochNotMatch is not checked as leadership is checked first in raftstore.
        if err.has_stale_command() {
            self.reset_leader_of_region(region_id);
            return true;
        }

        if !err.has_not_leader() {
            return false;
        }
        let err = err.get_not_leader();
        if !err.has_leader() {
            self.reset_leader_of_region(region_id);
            return true;
        }
        self.leaders.insert(region_id, err.get_leader().clone());
        true
    }

    fn voter_store_ids_of_region(&self, region_id: u64) -> Option<Vec<u64>> {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .map(|region| {
                region
                    .get_peers()
                    .iter()
                    .flat_map(|p| {
                        if p.get_role() != PeerRole::Learner {
                            Some(p.get_store_id())
                        } else {
                            None
                        }
                    })
                    .collect()
            })
    }

    pub fn query_leader(
        &self,
        store_id: u64,
        region_id: u64,
        timeout: Duration,
    ) -> Option<metapb::Peer> {
        // To get region leader, we don't care real peer id, so use 0 instead.
        let peer = new_peer(store_id, 0);
        let find_leader = new_status_request(region_id, peer, new_region_leader_cmd());
        let mut resp = match self.call_command(find_leader, timeout) {
            Ok(resp) => resp,
            Err(err) => {
                error!(
                    "fail to get leader of region {} on store {}, error: {:?}",
                    region_id, store_id, err
                );
                return None;
            }
        };
        let mut region_leader = resp.take_status_response().take_region_leader();
        // NOTE: node id can't be 0.
        if self.valid_leader_id(region_id, region_leader.get_leader().get_store_id()) {
            Some(region_leader.take_leader())
        } else {
            None
        }
    }

    fn valid_leader_id(&self, region_id: u64, leader_id: u64) -> bool {
        let store_ids = match self.voter_store_ids_of_region(region_id) {
            None => return false,
            Some(ids) => ids,
        };
        let node_ids = self.sim.rl().get_node_ids();
        store_ids.contains(&leader_id) && node_ids.contains(&leader_id)
    }

    pub fn run_node(&mut self, node_id: u64) -> ServerResult<()> {
        debug!("starting node {}", node_id);
        let engines = self.engines[&node_id].clone();
        assert_ne!(engines.kv.engine_store_server_helper, 0);

        let key_mgr = self.key_managers_map[&node_id].clone();
        let (router, system) =
            create_raft_batch_system(&self.cfg.raft_store, &self.resource_manager);

        let mut cfg = self.cfg.clone();
        if let Some(labels) = self.labels.get(&node_id) {
            cfg.server.labels = labels.to_owned();
        }
        let store_meta = match self.store_metas.entry(node_id) {
            MapEntry::Occupied(o) => {
                let mut meta = o.get().lock().unwrap();
                *meta = StoreMeta::new(PENDING_MSG_CAP);
                o.get().clone()
            }
            MapEntry::Vacant(v) => v
                .insert(Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP))))
                .clone(),
        };
        let props = GroupProperties::default();
        self.group_props.insert(node_id, props.clone());
        tikv_util::thread_group::set_properties(Some(props));
        debug!("calling run node"; "node_id" => node_id);

        // FIXME: rocksdb event listeners may not work, because we change the router.
        self.sim
            .wl()
            .run_node(node_id, cfg, engines, store_meta, key_mgr, router, system)?;
        debug!("node {} started", node_id);
        Ok(())
    }

    pub fn id(&self) -> u64 {
        self.cfg.server.cluster_id
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

    pub fn get_region_epoch(&self, region_id: u64) -> RegionEpoch {
        block_on(self.pd_client.get_region_by_id(region_id))
            .unwrap()
            .unwrap()
            .take_region_epoch()
    }

    /// Multiple nodes with fixed node id, like node 1, 2, .. 5,
    /// First region 1 is in all stores with peer 1, 2, .. 5.
    /// Peer 1 is in node 1, store 1, etc.
    ///
    /// Must be called after `create_engines`.
    pub fn bootstrap_region(&mut self) -> Result<()> {
        for (i, engines) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.engines.insert(id, engines.clone());
            tikv_util::debug!("bootstrap_region";
                "node_id" => id,
            );
            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
        }

        self.bootstrap_ffi_helper_set();
        let mut region = metapb::Region::default();
        region.set_id(1);
        region.set_start_key(keys::EMPTY_KEY.to_vec());
        region.set_end_key(keys::EMPTY_KEY.to_vec());
        region.mut_region_epoch().set_version(INIT_EPOCH_VER);
        region.mut_region_epoch().set_conf_ver(INIT_EPOCH_CONF_VER);

        for (&id, engines) in &self.engines {
            let peer = new_peer(id, id);
            region.mut_peers().push(peer.clone());
            bootstrap_store(engines, self.id(), id).unwrap();
        }

        for (&id, engines) in &self.engines {
            tikv_util::debug!("prepare_bootstrap_cluster";
                "node_id" => id,
            );
            prepare_bootstrap_cluster(engines, &region)?;
            tikv_util::debug!("prepare_bootstrap_cluster finish";
                "node_id" => id,
            );
        }

        self.bootstrap_cluster(region);

        Ok(())
    }

    // Return first region id.
    pub fn bootstrap_conf_change(&mut self) -> u64 {
        for (i, engines) in self.dbs.iter().enumerate() {
            let id = i as u64 + 1;
            self.engines.insert(id, engines.clone());
            let store_meta = Arc::new(Mutex::new(StoreMeta::new(PENDING_MSG_CAP)));
            self.store_metas.insert(id, store_meta);
            self.key_managers_map
                .insert(id, self.key_managers[i].clone());
        }

        self.bootstrap_ffi_helper_set();
        for (&id, engines) in &self.engines {
            bootstrap_store(engines, self.id(), id).unwrap();
        }

        let node_id = 1;
        let region_id = 1;
        let peer_id = 1;

        let region = initial_region(node_id, region_id, peer_id);
        prepare_bootstrap_cluster(&self.engines[&node_id], &region).unwrap();
        self.bootstrap_cluster(region);
        region_id
    }

    pub fn reset_leader_of_region(&mut self, region_id: u64) {
        self.leaders.remove(&region_id);
    }
    pub fn shutdown(&mut self) {
        debug!("about to shutdown cluster");
        let keys = match self.sim.read() {
            Ok(s) => s.get_node_ids(),
            Err(_) => {
                safe_panic!("failed to acquire read lock");
                // Leave the resource to avoid double panic.
                return;
            }
        };
        for id in keys {
            self.stop_node(id);
        }
        self.leaders.clear();
        self.store_metas.clear();
        debug!("all nodes are shut down.");
    }

    pub fn request(
        &mut self,
        key: &[u8],
        reqs: Vec<Request>,
        read_quorum: bool,
        timeout: Duration,
        panic_when_timeout: bool,
    ) -> RaftCmdResponse {
        let timer = Instant::now();
        let mut tried_times = 0;
        // At least retry once.
        while tried_times < 2 || timer.saturating_elapsed() < timeout {
            tried_times += 1;
            let mut region = self.get_region(key);
            let region_id = region.get_id();
            let req = new_request(
                region_id,
                region.take_region_epoch(),
                reqs.clone(),
                read_quorum,
            );
            let result = self.call_command_on_leader(req, timeout);

            let resp = match result {
                e @ Err(Error::Timeout(_))
                | e @ Err(Error::NotLeader(..))
                | e @ Err(Error::StaleCommand) => {
                    warn!("call command failed, retry it"; "err" => ?e);
                    sleep_ms(100);
                    continue;
                }
                Err(e) => panic!("call command failed {:?}", e),
                Ok(resp) => resp,
            };

            if resp.get_header().get_error().has_epoch_not_match() {
                warn!("seems split, let's retry");
                sleep_ms(100);
                continue;
            }
            if resp
                .get_header()
                .get_error()
                .get_message()
                .contains("merging mode")
            {
                warn!("seems waiting for merge, let's retry");
                sleep_ms(100);
                continue;
            }
            return resp;
        }
        if panic_when_timeout {
            panic!("request timeout");
        }
        RaftCmdResponse::default()
    }

    pub fn must_put(&mut self, key: &[u8], value: &[u8]) {
        self.must_put_cf(CF_DEFAULT, key, value);
    }

    pub fn must_put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) {
        if let Err(e) = self.batch_put(key, vec![new_put_cf_cmd(cf, key, value)]) {
            panic!("has error: {:?}", e);
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> result::Result<(), PbError> {
        self.batch_put(key, vec![new_put_cf_cmd(CF_DEFAULT, key, value)])
            .map(|_| ())
    }

    pub fn batch_put(
        &mut self,
        region_key: &[u8],
        reqs: Vec<Request>,
    ) -> result::Result<RaftCmdResponse, PbError> {
        let resp = self.request(region_key, reqs, false, Duration::from_secs(5), true);
        if resp.get_header().has_error() {
            Err(resp.get_header().get_error().clone())
        } else {
            Ok(resp)
        }
    }

    pub fn must_delete(&mut self, key: &[u8]) {
        self.must_delete_cf(CF_DEFAULT, key)
    }

    pub fn must_delete_cf(&mut self, cf: &str, key: &[u8]) {
        let resp = self.request(
            key,
            vec![new_delete_cmd(cf, key)],
            false,
            Duration::from_secs(5),
            true,
        );
        if resp.get_header().has_error() {
            panic!("response {:?} has error", resp);
        }
    }

    // Get region when the `filter` returns true.
    pub fn get_region_with<F>(&self, key: &[u8], filter: F) -> metapb::Region
    where
        F: Fn(&metapb::Region) -> bool,
    {
        for _ in 0..100 {
            if let Ok(region) = self.pd_client.get_region(key) {
                if filter(&region) {
                    return region;
                }
            }
            // We may meet range gap after split, so here we will
            // retry to get the region again.
            sleep_ms(20);
        }

        panic!("find no region for {}", log_wrappers::hex_encode_upper(key));
    }

    pub fn get_region(&self, key: &[u8]) -> metapb::Region {
        self.get_region_with(key, |_| true)
    }

    pub fn get_tiflash_engine(&self, node_id: u64) -> &TiFlashEngine {
        &self.engines[&node_id].kv
    }

    pub fn get_engines(&self, node_id: u64) -> &Engines<TiFlashEngine, engine_rocks::RocksEngine> {
        &self.engines[&node_id]
    }

    pub fn get_raw_engine(&self, node_id: u64) -> Arc<DB> {
        Arc::clone(self.engines[&node_id].kv.bad_downcast())
    }

    pub fn get_engine(&self, node_id: u64) -> &engine_rocks::RocksEngine {
        &self.get_tiflash_engine(node_id).rocks
    }

    pub fn clear_send_filters(&mut self) {
        let mut sim = self.sim.wl();
        for node_id in sim.get_node_ids() {
            sim.clear_send_filters(node_id);
        }
    }

    pub fn must_transfer_leader(&mut self, region_id: u64, leader: metapb::Peer) {
        let timer = Instant::now();
        loop {
            self.reset_leader_of_region(region_id);
            let cur_leader = self.leader_of_region(region_id);
            if let Some(ref cur_leader) = cur_leader {
                if cur_leader.get_id() == leader.get_id()
                    && cur_leader.get_store_id() == leader.get_store_id()
                {
                    return;
                }
            }
            if timer.saturating_elapsed() > Duration::from_secs(5) {
                panic!(
                    "failed to transfer leader to [{}] {:?}, current leader: {:?}",
                    region_id, leader, cur_leader
                );
            }
            self.transfer_leader(region_id, leader.clone());
        }
    }

    pub fn call_command_on_leader(
        &mut self,
        mut request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let timer = Instant::now();
        let region_id = request.get_header().get_region_id();
        loop {
            let leader = match self.leader_of_region(region_id) {
                None => return Err(Error::NotLeader(region_id, None)),
                Some(l) => l,
            };
            request.mut_header().set_peer(leader);
            let resp = match self.call_command(request.clone(), timeout) {
                e @ Err(_) => return e,
                Ok(resp) => resp,
            };
            if self.refresh_leader_if_needed(&resp, region_id)
                && timer.saturating_elapsed() < timeout
            {
                warn!(
                    "{:?} is no longer leader, let's retry",
                    request.get_header().get_peer()
                );
                continue;
            }
            return Ok(resp);
        }
    }

    pub fn must_split(&mut self, region: &metapb::Region, split_key: &[u8]) {
        let mut try_cnt = 0;
        let split_count = self.pd_client.get_split_count();
        loop {
            debug!("asking split"; "region" => ?region, "key" => ?split_key);
            // In case ask split message is ignored, we should retry.
            if try_cnt % 50 == 0 {
                self.reset_leader_of_region(region.get_id());
                let key = split_key.to_vec();
                let check = Box::new(move |write_resp: WriteResponse| {
                    let mut resp = write_resp.response;
                    if resp.get_header().has_error() {
                        let error = resp.get_header().get_error();
                        if error.has_epoch_not_match()
                            || error.has_not_leader()
                            || error.has_stale_command()
                            || error
                                .get_message()
                                .contains("peer has not applied to current term")
                        {
                            warn!("fail to split: {:?}, ignore.", error);
                            return;
                        }
                        panic!("failed to split: {:?}", resp);
                    }
                    let admin_resp = resp.mut_admin_response();
                    let split_resp = admin_resp.mut_splits();
                    let regions = split_resp.get_regions();
                    assert_eq!(regions.len(), 2);
                    assert_eq!(regions[0].get_end_key(), key.as_slice());
                    assert_eq!(regions[0].get_end_key(), regions[1].get_start_key());
                });
                if self.leader_of_region(region.get_id()).is_some() {
                    self.split_region(region, split_key, Callback::write(check));
                }
            }

            if self.pd_client.check_split(region, split_key)
                && self.pd_client.get_split_count() > split_count
            {
                return;
            }

            if try_cnt > 250 {
                panic!(
                    "region {:?} has not been split by {}",
                    region,
                    log_wrappers::hex_encode_upper(split_key)
                );
            }
            try_cnt += 1;
            sleep_ms(20);
        }
    }

    pub fn must_send_store_heartbeat(&self, node_id: u64) {
        let router = self.sim.rl().get_router(node_id).unwrap();
        StoreRouter::send(&router, StoreMsg::Tick(StoreTick::PdStoreHeartbeat)).unwrap();
    }

    pub async fn send_flashback_msg(
        &mut self,
        region_id: u64,
        store_id: u64,
        cmd_type: AdminCmdType,
        epoch: metapb::RegionEpoch,
        peer: metapb::Peer,
    ) {
        let (result_tx, result_rx) = oneshot::channel();
        let cb = Callback::write(Box::new(move |resp| {
            if resp.response.get_header().has_error() {
                result_tx.send(false).unwrap();
                error!("send flashback msg failed"; "region_id" => region_id);
                return;
            }
            result_tx.send(true).unwrap();
        }));

        let mut admin = AdminRequest::default();
        admin.set_cmd_type(cmd_type);
        let mut req = RaftCmdRequest::default();
        req.mut_header().set_region_id(region_id);
        req.mut_header().set_region_epoch(epoch);
        req.mut_header().set_peer(peer);
        req.set_admin_request(admin);
        req.mut_header()
            .set_flags(WriteBatchFlags::FLASHBACK.bits());

        let router = self.sim.rl().get_router(store_id).unwrap();
        if let Err(e) = router.send_command(
            req,
            cb,
            RaftCmdExtraOpts {
                deadline: None,
                disk_full_opt: kvproto::kvrpcpb::DiskFullOpt::AllowedOnAlmostFull,
            },
        ) {
            panic!("router send failed, error{}", e);
        }

        if !result_rx.await.unwrap() {
            panic!("Flashback call msg failed");
        }
    }
}

// We simulate 3 or 5 nodes, each has a store.
// Sometimes, we use fixed id to test, which means the id
// isn't allocated by pd, and node id, store id are same.
// E,g, for node 1, the node id and store id are both 1.

pub trait Simulator<EK: KvEngine> {
    // Pass 0 to let pd allocate a node id if db is empty.
    // If node id > 0, the node must be created in db already,
    // and the node id must be the same as given argument.
    // Return the node id.
    // TODO: we will rename node name here because now we use store only.
    fn run_node(
        &mut self,
        node_id: u64,
        cfg: Config,
        engines: Engines<EK, engine_rocks::RocksEngine>,
        store_meta: Arc<Mutex<StoreMeta>>,
        key_manager: Option<Arc<DataKeyManager>>,
        router: RaftRouter<EK, engine_rocks::RocksEngine>,
        system: RaftBatchSystem<EK, engine_rocks::RocksEngine>,
    ) -> ServerResult<u64>;
    fn stop_node(&mut self, node_id: u64);
    fn get_node_ids(&self) -> HashSet<u64>;
    fn async_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback<engine_rocks::RocksSnapshot>,
    ) -> Result<()> {
        self.async_command_on_node_with_opts(node_id, request, cb, Default::default())
    }
    fn async_command_on_node_with_opts(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        cb: Callback<engine_rocks::RocksSnapshot>,
        opts: RaftCmdExtraOpts,
    ) -> Result<()>;
    fn send_raft_msg(&mut self, msg: RaftMessage) -> Result<()>;
    fn get_snap_dir(&self, node_id: u64) -> String;
    fn get_snap_mgr(&self, node_id: u64) -> &SnapManager;
    fn get_router(&self, node_id: u64) -> Option<RaftRouter<EK, engine_rocks::RocksEngine>>;
    fn add_send_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_send_filters(&mut self, node_id: u64);
    fn add_recv_filter(&mut self, node_id: u64, filter: Box<dyn Filter>);
    fn clear_recv_filters(&mut self, node_id: u64);

    fn call_command(&self, request: RaftCmdRequest, timeout: Duration) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        self.call_command_on_node(node_id, request, timeout)
    }

    fn read(
        &mut self,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let node_id = request.get_header().get_peer().get_store_id();
        let (cb, rx) = make_cb(&request);
        self.async_read(node_id, batch_id, request, cb);
        rx.recv_timeout(timeout)
            .map_err(|_| Error::Timeout(format!("request timeout for {:?}", timeout)))
    }

    fn async_read(
        &mut self,
        node_id: u64,
        batch_id: Option<ThreadReadId>,
        request: RaftCmdRequest,
        cb: Callback<engine_rocks::RocksSnapshot>,
    );

    fn call_command_on_node(
        &self,
        node_id: u64,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> Result<RaftCmdResponse> {
        let (cb, rx) = make_cb(&request);

        match self.async_command_on_node(node_id, request, cb) {
            Ok(()) => {}
            Err(e) => {
                let mut resp = RaftCmdResponse::default();
                resp.mut_header().set_error(e.into());
                return Ok(resp);
            }
        }
        rx.recv_timeout(timeout)
            .map_err(|e| Error::Timeout(format!("request timeout for {:?}: {:?}", timeout, e)))
    }
}

pub fn must_get(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8], value: Option<&[u8]>) {
    for _ in 1..300 {
        let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
        if let (Some(value), Some(res)) = (value, res.as_ref()) {
            assert_eq!(value, &res[..]);
            return;
        }
        if value.is_none() && res.is_none() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    debug!("last try to get {}", log_wrappers::hex_encode_upper(key));
    let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
    if value.is_none() && res.is_none()
        || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
    {
        return;
    }
    panic!(
        "can't get value {:?} for key {}",
        value.map(escape),
        log_wrappers::hex_encode_upper(key)
    )
}

pub fn must_get_equal(engine: &engine_rocks::RocksEngine, key: &[u8], value: &[u8]) {
    must_get(engine, "default", key, Some(value));
}

pub fn must_get_none(engine: &engine_rocks::RocksEngine, key: &[u8]) {
    must_get(engine, "default", key, None);
}

pub fn must_get_cf_equal(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8], value: &[u8]) {
    must_get(engine, cf, key, Some(value));
}

pub fn must_get_cf_none(engine: &engine_rocks::RocksEngine, cf: &str, key: &[u8]) {
    must_get(engine, cf, key, None);
}
