// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{atomic::AtomicU8, Arc, Mutex};

use collections::HashMap;
use encryption::DataKeyManager;
use engine_store_ffi::{
    ffi::interfaces_ffi::{EngineStoreServerHelper, RaftProxyStatus, RaftStoreProxyFFIHelper},
    TiFlashEngine,
};
use engine_traits::Engines;
use raftstore::store::RaftRouter;
use tikv::config::TikvConfig;
use tikv_util::{debug, sys::SysQuota};

use super::common::*;
use crate::{
    mock_cluster::config::MockConfig, mock_store::gen_engine_store_server_helper,
    EngineStoreServer, EngineStoreServerWrap,
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

#[allow(clippy::type_complexity)]
#[derive(Default)]
pub struct ClusterExt {
    // Helper to set ffi_helper_set.
    pub ffi_helper_lst: Vec<FFIHelperSet>,
    // This field should be pub(crate).
    pub(crate) ffi_helper_set: Arc<Mutex<HashMap<u64, FFIHelperSet>>>,
    pub test_data: TestData,
    pub cluster_ptr: isize,
    pub pre_run_node_callback: Option<Box<dyn FnMut(&mut crate::mock_cluster::MixedClusterConfig)>>,
}

impl ClusterExt {
    pub fn pre_node_start(&mut self, cfg: &mut crate::mock_cluster::MixedClusterConfig) {
        if let Some(c) = self.pre_run_node_callback.as_mut() {
            c(cfg);
        }
    }

    pub fn post_cluster_start(&mut self) {
        self.iter_ffi_helpers(None, &mut |_, ffi: &mut FFIHelperSet| {
            ffi.proxy.refresh_cluster_raftstore_version(-1);
        });
    }

    pub fn get_cluster_size(&self) -> usize {
        self.ffi_helper_set.lock().expect("poisoned").len()
    }

    pub fn make_ffi_helper_set_no_bind(
        id: u64,
        engines: Engines<TiFlashEngine, ProxyRaftEngine>,
        key_mgr: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, ProxyRaftEngine>>,
        node_cfg: TikvConfig,
        cluster_ptr: isize,
        cluster_ext_ptr: isize,
        mock_cfg: MockConfig,
        pd_client: Option<Arc<dyn pd_client::PdClient>>,
        proxy_cfg: &ProxyConfig,
    ) -> (FFIHelperSet, TikvConfig) {
        // We must allocate on heap to avoid move.

        let proxy_config_str = serde_json::to_string(proxy_cfg).unwrap_or_default();
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
            None,
            pd_client,
            proxy_config_str,
        ));

        let proxy_ref = proxy.as_ref();
        let mut proxy_helper = Box::new(RaftStoreProxyFFIHelper::new(proxy_ref.into()));
        let mut engine_store_server = Box::new(EngineStoreServer::new(id, Some(engines)));
        engine_store_server.mock_cfg = mock_cfg;
        let engine_store_server_wrap = Box::new(EngineStoreServerWrap::new(
            &mut *engine_store_server,
            Some(&mut *proxy_helper),
            cluster_ptr,
            cluster_ext_ptr,
        ));
        let engine_store_server_helper = Box::new(gen_engine_store_server_helper(
            std::pin::Pin::new(&*engine_store_server_wrap),
        ));

        let engine_store_server_helper_ptr = &*engine_store_server_helper as *const _ as isize;
        // We don't set helper ptr inside `raftstore_proxy_engine`.
        // Delay that to `create_ffi_helper_set`.
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
    pub fn create_ffi_helper_set(
        cluster_ext: &mut ClusterExt,
        cluster_ptr: isize,
        cluster_ext_ptr: isize,
        proxy_cfg: &ProxyConfig,
        tikv_cfg: &TikvConfig,
        mock_cfg: MockConfig,
        engines: Engines<TiFlashEngine, ProxyRaftEngine>,
        key_manager: &Option<Arc<DataKeyManager>>,
        router: &Option<RaftRouter<TiFlashEngine, ProxyRaftEngine>>,
        pd_client: Option<Arc<dyn pd_client::PdClient>>,
    ) {
        init_global_ffi_helper_set();
        // We don't know `node_id` now.
        // It will be allocated when start by register_ffi_helper_set.
        let (mut ffi_helper_set, _node_cfg) = ClusterExt::make_ffi_helper_set_no_bind(
            0,
            engines,
            key_manager,
            router,
            tikv_cfg.clone(),
            cluster_ptr,
            cluster_ext_ptr,
            mock_cfg,
            pd_client,
            proxy_cfg,
        );

        // We can not use moved or cloned engines any more.
        let (helper_ptr, engine_store_hub) = {
            let helper_ptr = ffi_helper_set.engine_store_server_helper_ptr;
            let helper = engine_store_ffi::ffi::gen_engine_store_server_helper(helper_ptr);
            let engine_store_hub = Arc::new(engine_store_ffi::engine::TiFlashEngineStoreHub {
                engine_store_server_helper: helper,
            });
            (helper_ptr, engine_store_hub)
        };
        let engines = ffi_helper_set.engine_store_server.engines.as_mut().unwrap();
        let proxy_config_set = Arc::new(engine_tiflash::ProxyEngineConfigSet {
            engine_store: proxy_cfg.engine_store.clone(),
        });
        engines.kv.init(
            helper_ptr,
            proxy_cfg.raft_store.snap_handle_pool_size,
            Some(engine_store_hub),
            Some(proxy_config_set),
        );
        ffi_helper_set.proxy.set_kv_engine(
            engine_store_ffi::ffi::RaftStoreProxyEngine::from_tiflash_engine(engines.kv.clone()),
        );
        assert_ne!(engines.kv.proxy_ext.engine_store_server_helper, 0);
        assert!(engines.kv.element_engine.is_some());
        cluster_ext.ffi_helper_lst.push(ffi_helper_set);
    }

    // Set node_id to ffi helpers which is cached in ffi_helper_lst.
    pub fn register_ffi_helper_set(&mut self, index: Option<usize>, node_id: u64) {
        let mut ffi_helper_set = if let Some(i) = index {
            // The node is added by `bootstrap_ffi_helper_set`.
            // In this case, all nodes are already set up before start.
            // `index` should be `Some(0)`, which means the queue head.
            self.ffi_helper_lst.remove(i)
        } else {
            // The node is added to the cluster after stared.
            // In this case index is None.
            // This method is weird since we don't know node_id when creating engine.
            self.ffi_helper_lst.remove(0)
        };
        debug!("register ffi_helper_set for {}", node_id);
        ffi_helper_set.engine_store_server.id = node_id;
        self.ffi_helper_set
            .lock()
            .unwrap()
            .insert(node_id, ffi_helper_set);
    }

    pub fn iter_ffi_helpers(
        &self,
        store_ids: Option<Vec<u64>>,
        f: &mut dyn FnMut(u64, &mut FFIHelperSet),
    ) -> Vec<u64> {
        let ids = match store_ids {
            Some(ids) => ids,
            None => self
                .ffi_helper_set
                .lock()
                .expect("lock error")
                .keys()
                .copied()
                .collect::<Vec<_>>(),
        };
        for id in ids.iter() {
            let id = *id;
            let lock = self.ffi_helper_set.lock();
            match lock {
                Ok(mut l) => {
                    let ffiset = l.get_mut(&id).unwrap();
                    f(id, ffiset);
                }
                Err(_) => std::process::exit(1),
            }
        }
        ids
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
