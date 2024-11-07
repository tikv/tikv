// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use std::sync::Arc;

use encryption::DataKeyManager;
use engine_traits::RaftEngine;
use kvproto::{
    raft_cmdpb::{AdminRequest, RaftCmdRequest},
    raft_serverpb::{RaftApplyState, RaftMessage},
};
use raft::StateRole;
use raftstore::{
    coprocessor::{
        AdminObserver, ApplyCtxInfo, ApplySnapshotObserver, BoxAdminObserver,
        BoxApplySnapshotObserver, BoxPdTaskObserver, BoxQueryObserver, BoxRaftMessageObserver,
        BoxRegionChangeObserver, BoxRoleObserver, BoxUpdateSafeTsObserver, Cmd, Coprocessor,
        CoprocessorHost, ObserverContext, PdTaskObserver, QueryObserver, RaftMessageObserver,
        RegionChangeEvent, RegionChangeObserver, RegionState, RoleChange, RoleObserver,
        StoreSizeInfo, UpdateSafeTsObserver,
    },
    store::{self, SnapManager, Transport},
};
use sst_importer::SstImporter;

use crate::{
    core::{DebugStruct, PackedEnvs, ProxyForwarder},
    TiFlashEngine,
};

// TiFlash observer's priority should be higher than all other observers, to
// avoid being bypassed.
const TIFLASH_OBSERVER_PRIORITY: u32 = 0;

#[derive(Clone)]
pub struct TiFlashObserver<T: Transport + 'static, ER: RaftEngine> {
    pub forwarder: ProxyForwarder<T, ER>,
}

impl<T: Transport + 'static, ER: RaftEngine> TiFlashObserver<T, ER> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        store_id: u64,
        engine: engine_tiflash::MixedModeEngine,
        raft_engine: ER,
        sst_importer: Arc<SstImporter<TiFlashEngine>>,
        trans: T,
        snap_mgr: SnapManager,
        packed_envs: PackedEnvs,
        debug_struct: DebugStruct,
        key_manager: Option<Arc<DataKeyManager>>,
    ) -> Self {
        TiFlashObserver {
            forwarder: ProxyForwarder::new(
                store_id,
                engine,
                raft_engine,
                sst_importer,
                trans,
                snap_mgr,
                packed_envs,
                debug_struct,
                key_manager,
            ),
        }
    }

    pub fn register_to<E: engine_traits::KvEngine>(
        &self,
        coprocessor_host: &mut CoprocessorHost<E>,
    ) {
        // If a observer is repeatedly registered, it can run repeated logic.
        coprocessor_host.registry.register_admin_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxAdminObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_query_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxQueryObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_apply_snapshot_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxApplySnapshotObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_region_change_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxRegionChangeObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_pd_task_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxPdTaskObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_update_safe_ts_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxUpdateSafeTsObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_role_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxRoleObserver::new(self.clone()),
        );
        coprocessor_host.registry.register_raft_message_observer(
            TIFLASH_OBSERVER_PRIORITY,
            BoxRaftMessageObserver::new(self.clone()),
        );
    }
}

impl<T: Transport + 'static, ER: RaftEngine> Coprocessor for TiFlashObserver<T, ER> {
    fn stop(&self) {
        self.forwarder.stop();
    }
}

impl<T: Transport + 'static, ER: RaftEngine> AdminObserver for TiFlashObserver<T, ER> {
    fn pre_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        req: &AdminRequest,
        index: u64,
        term: u64,
    ) -> bool {
        self.forwarder
            .pre_exec_admin(ob_ctx.region(), req, index, term)
    }

    fn post_exec_admin(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        apply_ctx_info: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        self.forwarder.post_exec_admin(
            ob_ctx.region(),
            cmd,
            apply_state,
            region_state,
            apply_ctx_info,
        )
    }
}

impl<T: Transport + 'static, ER: RaftEngine> QueryObserver for TiFlashObserver<T, ER> {
    fn on_empty_cmd(&self, ob_ctx: &mut ObserverContext<'_>, index: u64, term: u64) {
        self.forwarder.on_empty_cmd(ob_ctx.region(), index, term)
    }

    fn post_exec_query(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        cmd: &Cmd,
        apply_state: &RaftApplyState,
        region_state: &RegionState,
        apply_ctx_info: &mut ApplyCtxInfo<'_>,
    ) -> bool {
        self.forwarder.post_exec_query(
            ob_ctx.region(),
            cmd,
            apply_state,
            region_state,
            apply_ctx_info,
        )
    }
}

impl<T: Transport + 'static, ER: RaftEngine> UpdateSafeTsObserver for TiFlashObserver<T, ER> {
    fn on_update_safe_ts(&self, region_id: u64, self_safe_ts: u64, leader_safe_ts: u64) {
        self.forwarder
            .on_update_safe_ts(region_id, self_safe_ts, leader_safe_ts)
    }
}

impl<T: Transport + 'static, ER: RaftEngine> RegionChangeObserver for TiFlashObserver<T, ER> {
    fn on_region_changed(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        e: RegionChangeEvent,
        r: StateRole,
    ) {
        self.forwarder.on_region_changed(ob_ctx.region(), e, r)
    }

    #[allow(clippy::match_like_matches_macro)]
    fn pre_persist(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        is_finished: bool,
        cmd: Option<&RaftCmdRequest>,
    ) -> bool {
        self.forwarder
            .pre_persist(ob_ctx.region(), is_finished, cmd)
    }

    fn pre_write_apply_state(&self, ob_ctx: &mut ObserverContext<'_>) -> bool {
        self.forwarder.pre_write_apply_state(ob_ctx.region())
    }
}

impl<T: Transport + 'static, ER: RaftEngine> RaftMessageObserver for TiFlashObserver<T, ER> {
    fn on_raft_message(&self, msg: &RaftMessage) -> bool {
        self.forwarder.on_raft_message(msg)
    }
}

impl<T: Transport + 'static, ER: RaftEngine> PdTaskObserver for TiFlashObserver<T, ER> {
    fn on_compute_engine_size(&self, store_size: &mut Option<StoreSizeInfo>) {
        self.forwarder.on_compute_engine_size(store_size)
    }
}

impl<T: Transport + 'static, ER: RaftEngine> ApplySnapshotObserver for TiFlashObserver<T, ER> {
    #[allow(clippy::single_match)]
    fn pre_apply_snapshot(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        self.forwarder
            .pre_apply_snapshot(ob_ctx.region(), peer_id, snap_key, snap)
    }

    fn post_apply_snapshot(
        &self,
        ob_ctx: &mut ObserverContext<'_>,
        peer_id: u64,
        snap_key: &store::SnapKey,
        snap: Option<&store::Snapshot>,
    ) {
        self.forwarder
            .post_apply_snapshot(ob_ctx.region(), peer_id, snap_key, snap)
    }

    fn should_pre_apply_snapshot(&self) -> bool {
        self.forwarder.should_pre_apply_snapshot()
    }

    fn cancel_apply_snapshot(&self, region_id: u64, peer_id: u64) {
        self.forwarder.cancel_apply_snapshot(region_id, peer_id)
    }
}

impl<T: Transport + 'static, ER: RaftEngine> RoleObserver for TiFlashObserver<T, ER> {
    fn on_role_change(&self, ob_ctx: &mut ObserverContext<'_>, r: &RoleChange) {
        self.forwarder.on_role_change(ob_ctx.region(), r)
    }
}
