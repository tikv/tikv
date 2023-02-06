// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::core::common::*;

pub struct PtrWrapper(pub RawCppPtr);

unsafe impl Send for PtrWrapper {}
unsafe impl Sync for PtrWrapper {}

#[derive(Default, Debug)]
pub struct PrehandleContext {
    // tracer holds ptr of snapshot prehandled by TiFlash side.
    pub tracer: HashMap<SnapKey, Arc<PrehandleTask>>,
}

#[derive(Debug)]
pub struct PrehandleTask {
    pub recv: mpsc::Receiver<PtrWrapper>,
    pub peer_id: u64,
}

impl PrehandleTask {
    pub fn new(recv: mpsc::Receiver<PtrWrapper>, peer_id: u64) -> Self {
        PrehandleTask { recv, peer_id }
    }
}
unsafe impl Send for PrehandleTask {}
unsafe impl Sync for PrehandleTask {}

pub struct PackedEnvs {
    pub engine_store_cfg: crate::EngineStoreConfig,
    pub pd_endpoints: Vec<String>,
    pub snap_handle_pool_size: usize,
}

#[derive(Debug, Default)]
pub struct DebugStruct {}

impl DebugStruct {}

// This object can be safely cloned.
pub struct ProxyForwarder<T: Transport, ER: RaftEngine> {
    pub store_id: u64,
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
    pub engine: TiFlashEngine,
    pub raft_engine: ER,
    pub sst_importer: Arc<SstImporter>,
    pub pre_handle_snapshot_ctx: Arc<Mutex<PrehandleContext>>,
    pub apply_snap_pool: Option<Arc<ThreadPool<TaskCell>>>,
    pub pending_delete_ssts: Arc<RwLock<Vec<SstMetaInfo>>>,
    // TODO should we use a Mutex here?
    pub trans: Arc<Mutex<T>>,
    pub snap_mgr: Arc<SnapManager>,
    pub packed_envs: Arc<PackedEnvs>,
    pub debug_struct: Arc<DebugStruct>,
}

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    pub fn get_cached_manager(&self) -> Arc<CachedRegionInfoManager> {
        self.engine
            .cached_region_info_manager
            .as_ref()
            .unwrap()
            .clone()
    }

    pub fn on_compute_engine_size(&self, store_size: &mut Option<StoreSizeInfo>) {
        let stats = self.engine_store_server_helper.handle_compute_store_stats();
        let _ = store_size.insert(StoreSizeInfo {
            capacity: stats.fs_stats.capacity_size,
            used: stats.fs_stats.used_size,
            avail: stats.fs_stats.avail_size,
        });
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store_id: u64,
        engine: engine_tiflash::RocksEngine,
        raft_engine: ER,
        sst_importer: Arc<SstImporter>,
        trans: T,
        snap_mgr: SnapManager,
        packed_envs: PackedEnvs,
        debug_struct: DebugStruct,
    ) -> Self {
        let engine_store_server_helper =
            gen_engine_store_server_helper(engine.engine_store_server_helper);
        // start thread pool for pre handle snapshot
        let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
            .max_thread_count(packed_envs.snap_handle_pool_size)
            .build_future_pool();

        ProxyForwarder {
            store_id,
            engine_store_server_helper,
            engine,
            raft_engine,
            sst_importer,
            pre_handle_snapshot_ctx: Arc::new(Mutex::new(PrehandleContext::default())),
            apply_snap_pool: Some(Arc::new(snap_pool)),
            pending_delete_ssts: Arc::new(RwLock::new(vec![])),
            trans: Arc::new(Mutex::new(trans)),
            snap_mgr: Arc::new(snap_mgr),
            packed_envs: Arc::new(packed_envs),
            debug_struct: Arc::new(debug_struct),
        }
    }

    pub fn stop(&self) {
        info!("shutdown tiflash observer"; "store_id" => self.store_id);
        self.apply_snap_pool.as_ref().unwrap().shutdown();
    }
}

impl<T: Transport + 'static, ER: RaftEngine> Clone for ProxyForwarder<T, ER> {
    fn clone(&self) -> Self {
        ProxyForwarder {
            store_id: self.store_id,
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            raft_engine: self.raft_engine.clone(),
            sst_importer: self.sst_importer.clone(),
            pre_handle_snapshot_ctx: self.pre_handle_snapshot_ctx.clone(),
            apply_snap_pool: self.apply_snap_pool.clone(),
            pending_delete_ssts: self.pending_delete_ssts.clone(),
            trans: self.trans.clone(),
            snap_mgr: self.snap_mgr.clone(),
            packed_envs: self.packed_envs.clone(),
            debug_struct: self.debug_struct.clone(),
        }
    }
}
