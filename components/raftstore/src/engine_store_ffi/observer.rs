// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{mpsc, Arc, Mutex};

use collections::HashMap;
use engine_tiflash::FsStatsExt;
use sst_importer::SstImporter;
use tikv_util::debug;
use yatp::{
    pool::{Builder, ThreadPool},
    task::future::TaskCell,
};

use crate::{
    coprocessor::{
        AdminObserver, ApplySnapshotObserver, BoxAdminObserver, BoxApplySnapshotObserver,
        BoxQueryObserver, BoxRegionChangeObserver, Cmd, Coprocessor, CoprocessorHost,
        ObserverContext, QueryObserver, RegionChangeEvent, RegionChangeObserver,
    },
    engine_store_ffi::{
        gen_engine_store_server_helper,
        interfaces::root::{DB as ffi_interfaces, DB::EngineStoreApplyRes},
        ColumnFamilyType, EngineStoreServerHelper, RaftCmdHeader, RawCppPtr, TiFlashEngine,
        WriteCmdType, WriteCmds,
    },
    store::SnapKey,
};

impl Into<engine_tiflash::FsStatsExt> for ffi_interfaces::StoreStats {
    fn into(self) -> FsStatsExt {
        FsStatsExt {
            available: self.fs_stats.avail_size,
            capacity: self.fs_stats.capacity_size,
            used: self.fs_stats.used_size,
        }
    }
}

pub struct TiFlashFFIHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
}
unsafe impl Send for TiFlashFFIHub {}
unsafe impl Sync for TiFlashFFIHub {}
impl engine_tiflash::FFIHubInner for TiFlashFFIHub {
    fn get_store_stats(&self) -> engine_tiflash::FsStatsExt {
        self.engine_store_server_helper
            .handle_compute_store_stats()
            .into()
    }
}

pub struct PtrWrapper(RawCppPtr);

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
    fn new(recv: mpsc::Receiver<PtrWrapper>, peer_id: u64) -> Self {
        PrehandleTask { recv, peer_id }
    }
}
unsafe impl Send for PrehandleTask {}
unsafe impl Sync for PrehandleTask {}

pub struct TiFlashObserver {
    pub peer_id: u64,
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
    pub engine: TiFlashEngine,
    pub sst_importer: Arc<SstImporter>,
    pub pre_handle_snapshot_ctx: Arc<Mutex<PrehandleContext>>,
    pub snap_handle_pool_size: usize,
    pub apply_snap_pool: Option<Arc<ThreadPool<TaskCell>>>,
}

impl Clone for TiFlashObserver {
    fn clone(&self) -> Self {
        TiFlashObserver {
            peer_id: self.peer_id,
            engine_store_server_helper: self.engine_store_server_helper,
            engine: self.engine.clone(),
            sst_importer: self.sst_importer.clone(),
            pre_handle_snapshot_ctx: self.pre_handle_snapshot_ctx.clone(),
            snap_handle_pool_size: self.snap_handle_pool_size,
            apply_snap_pool: self.apply_snap_pool.clone(),
        }
    }
}

// TiFlash observer's priority should be higher than all other observers, to avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

impl TiFlashObserver {
    pub fn new(
        peer_id: u64,
        engine: engine_tiflash::RocksEngine,
        sst_importer: Arc<SstImporter>,
        snap_handle_pool_size: usize,
    ) -> Self {
        let engine_store_server_helper =
            gen_engine_store_server_helper(engine.engine_store_server_helper);
        let snap_pool = Builder::new(tikv_util::thd_name!("region-task"))
            .max_thread_count(snap_handle_pool_size)
            .build_future_pool();
        TiFlashObserver {
            peer_id,
            engine_store_server_helper,
            engine,
            sst_importer,
            pre_handle_snapshot_ctx: Arc::new(Mutex::new(PrehandleContext::default())),
            snap_handle_pool_size,
            apply_snap_pool: Some(Arc::new(snap_pool)),
        }
    }

    // TODO(tiflash) open observers when TiKV merged.
    pub fn register_to<E: engine_traits::KvEngine>(
        &self,
        coprocessor_host: &mut CoprocessorHost<E>,
    ) {
        // coprocessor_host.registry.register_query_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxQueryObserver::new(self.clone()),
        // );
        // coprocessor_host.registry.register_admin_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxAdminObserver::new(self.clone()),
        // );
        // coprocessor_host.registry.register_apply_snapshot_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxApplySnapshotObserver::new(self.clone()),
        // );
        // coprocessor_host.registry.register_region_change_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxRegionChangeObserver::new(self.clone()),
        // );
        // coprocessor_host.registry.register_pd_task_observer(
        //     TIFLASH_OBSERVER_PRIORITY,
        //     BoxPdTaskObserver::new(self.clone()),
        // );
    }
}

impl Coprocessor for TiFlashObserver {
    fn stop(&self) {
        self.apply_snap_pool.as_ref().unwrap().shutdown();
    }
}
