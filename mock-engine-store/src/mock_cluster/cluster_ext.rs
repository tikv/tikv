// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicU8, Arc, Mutex};

use collections::HashMap;
use encryption::DataKeyManager;
use engine_store_ffi::ffi::{
    interfaces_ffi::{EngineStoreServerHelper, RaftProxyStatus, RaftStoreProxyFFIHelper},
    RaftStoreProxyFFI,
};
use engine_tiflash::DB;
use engine_traits::{Engines, KvEngine};
use raftstore::store::RaftRouter;
use tikv::config::TikvConfig;
use tikv_util::{debug, sys::SysQuota, HandyRwLock};

use crate::{
    config::MockConfig, mock_store::gen_engine_store_server_helper, EngineStoreServer,
    EngineStoreServerWrap, TiFlashEngine,
};

pub struct EngineHelperSet {
    pub engine_store_server: Box<EngineStoreServer>,
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<EngineStoreServerHelper>,
}

pub struct FFIHelperSet {
    pub proxy: Box<engine_store_ffi::ffi::RaftStoreProxy>,
    pub proxy_helper: Box<RaftStoreProxyFFIHelper>,
    pub engine_store_server: Box<EngineStoreServer>,
    // Make interface happy, don't own proxy and server.
    pub engine_store_server_wrap: Box<EngineStoreServerWrap>,
    pub engine_store_server_helper: Box<EngineStoreServerHelper>,
    pub engine_store_server_helper_ptr: isize,
}

#[derive(Debug, Default)]
pub struct TestData {
    pub expected_leader_safe_ts: u64,
    pub expected_self_safe_ts: u64,
}

#[derive(Default)]
pub struct ClusterExt {
    // Helper to set ffi_helper_set.
    pub ffi_helper_lst: Vec<FFIHelperSet>,
    ffi_helper_set: Arc<Mutex<HashMap<u64, FFIHelperSet>>>,
    pub test_data: TestData,
}

impl ClusterExt {
    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
        node_cfg: TikvConfig,
        cluster_id: isize,
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

    pub fn access_ffi_helpers(&self, f: &mut dyn FnMut(&mut HashMap<u64, FFIHelperSet>)) {
        let lock = self.ffi_helper_set.lock();
        match lock {
            Ok(mut l) => {
                f(&mut l);
            }
            Err(_) => std::process::exit(1),
        }
    }

    /// We need to create FFIHelperSet while creating engine.
    /// The FFIHelperSet wil also be stored in ffi_helper_lst.
    pub fn create_ffi_helper_set<T: Simulator<TiFlashEngine>>(
        cluster: &mut Cluster<T>,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_manager: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) {
        init_global_ffi_helper_set();
        // We don't know `node_id` now.
        // It will be allocated when start by register_ffi_helper_set.
        let (mut ffi_helper_set, _node_cfg) =
            cluster.make_ffi_helper_set(0, engines, key_manager, router);

        // We can not use moved or cloned engines any more.
        let (helper_ptr, engine_store_hub) = {
            let helper_ptr = ffi_helper_set
                .proxy
                .kv_engine()
                .write()
                .unwrap()
                .as_mut()
                .unwrap()
                .engine_store_server_helper();

            let helper = engine_store_ffi::ffi::gen_engine_store_server_helper(helper_ptr);
            let engine_store_hub = Arc::new(engine_store_ffi::engine::TiFlashEngineStoreHub {
                engine_store_server_helper: helper,
            });
            (helper_ptr, engine_store_hub)
        };
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();
        let proxy_config_set = Arc::new(engine_tiflash::ProxyEngineConfigSet {
            engine_store: cluster.cfg.proxy_cfg.engine_store.clone(),
        });
        engines.kv.init(
            helper_ptr,
            cluster.cfg.proxy_cfg.raft_store.snap_handle_pool_size,
            Some(engine_store_hub),
            Some(proxy_config_set),
        );

        assert_ne!(engines.kv.proxy_ext.engine_store_server_helper, 0);
        cluster.cluster_ext.ffi_helper_lst.push(ffi_helper_set);
    }
}

use super::{Cluster, Simulator};

impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn make_ffi_helper_set(
        &mut self,
        id: u64,
        engines: Engines<TiFlashEngine, engine_rocks::RocksEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, engine_rocks::RocksEngine>>,
    ) -> (FFIHelperSet, TikvConfig) {
        ClusterExt::make_ffi_helper_set_no_bind(
            id,
            engines,
            key_mgr,
            router,
            self.cfg.tikv.clone(),
            self as *const Cluster<T> as isize,
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
            let lock = self.cluster_ext.ffi_helper_set.lock();
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
        self.cluster_ext.access_ffi_helpers(f)
    }

    pub fn post_node_start(&mut self, node_id: u64) {
        // Since we use None to create_ffi_helper_set, we must init again.
        let router = self.sim.rl().get_router(node_id).unwrap();
        self.iter_ffi_helpers(Some(vec![node_id]), &mut |_, _, ffi: &mut FFIHelperSet| {
            ffi.proxy.set_read_index_client(Some(Box::new(
                engine_store_ffi::ffi::read_index_helper::ReadIndexClient::new(
                    router.clone(),
                    SysQuota::cpu_cores_quota() as usize * 2,
                ),
            )));
        });
    }

    // If index is None, use the last in ffi_helper_lst, which is added by
    // create_ffi_helper_set.
    // Used in two places:
    // 1. bootstrap_ffi_helper_set where all nodes are inited before start. In this
    // case index is `Some(0)`. 2. cluster.start where new nodes are added to
    // the cluster after stared. In this case index is None. This method is
    // weird since we don't know node_id when creating engine.
    pub fn register_ffi_helper_set(&mut self, index: Option<usize>, node_id: u64) {
        let mut ffi_helper_set = if let Some(i) = index {
            self.cluster_ext.ffi_helper_lst.remove(i)
        } else {
            self.cluster_ext.ffi_helper_lst.pop().unwrap()
        };
        debug!("register ffi_helper_set for {}", node_id);
        ffi_helper_set.engine_store_server.id = node_id;
        self.cluster_ext
            .ffi_helper_set
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
            // Always at the front of the vector since iterate from 0.
            self.register_ffi_helper_set(Some(0), *node_id);
        }
    }
}

// TiFlash specific
impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn run_conf_change_no_start(&mut self) -> u64 {
        self.create_engines();
        self.bootstrap_conf_change()
    }

    pub fn set_expected_safe_ts(&mut self, leader_safe_ts: u64, self_safe_ts: u64) {
        self.cluster_ext.test_data.expected_leader_safe_ts = leader_safe_ts;
        self.cluster_ext.test_data.expected_self_safe_ts = self_safe_ts;
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
