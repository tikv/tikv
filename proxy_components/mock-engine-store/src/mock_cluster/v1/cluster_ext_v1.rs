// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use collections::HashMap;
use engine_store_ffi::{ffi::RaftStoreProxyFFI, TiFlashEngine};
use engine_tiflash::DB;
use engine_traits::{Engines, KvEngine};
use tikv_util::{sys::SysQuota, HandyRwLock};

use super::{common::*, Cluster, Simulator};

impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn access_ffi_helpers(&self, f: &mut dyn FnMut(&mut HashMap<u64, FFIHelperSet>)) {
        self.cluster_ext.access_ffi_helpers(f)
    }

    pub fn post_node_start(&mut self, node_id: u64) {
        // Since we use None to create_ffi_helper_set, we must init again.
        let router = self.sim.rl().get_router(node_id).unwrap();
        self.cluster_ext
            .iter_ffi_helpers(Some(vec![node_id]), &mut |_, ffi: &mut FFIHelperSet| {
                ffi.proxy.set_read_index_client(Some(Box::new(
                    engine_store_ffi::ffi::read_index_helper::ReadIndexClient::new(
                        router.clone(),
                        SysQuota::cpu_cores_quota() as usize * 2,
                    ),
                )));
            });
    }

    pub fn register_ffi_helper_set(&mut self, index: Option<usize>, node_id: u64) {
        self.cluster_ext.register_ffi_helper_set(index, node_id)
    }

    // Need self.engines be filled.
    pub fn bootstrap_ffi_helper_set(&mut self) {
        let mut node_ids: Vec<u64> = self.engines.iter().map(|(&id, _)| id).collect();
        // We force iterate engines in sorted order.
        node_ids.sort();
        for node_id in node_ids.iter() {
            // Always at the front of the vector since iterate from 0.
            self.register_ffi_helper_set(Some(0), *node_id);
        }
        assert_eq!(self.cluster_ext.ffi_helper_lst.len(), 0);
    }
}

// TiFlash specific
impl<T: Simulator<TiFlashEngine>> Cluster<T> {
    pub fn run_conf_change_no_start(&mut self) -> u64 {
        self.create_engines();
        self.bootstrap_conf_change()
    }

    pub fn get_tiflash_engine(&self, node_id: u64) -> &TiFlashEngine {
        &self.engines[&node_id].kv
    }

    pub fn get_engines(&self, node_id: u64) -> &Engines<TiFlashEngine, ProxyRaftEngine> {
        &self.engines[&node_id]
    }

    pub fn get_raw_engine(&self, node_id: u64) -> Arc<DB> {
        Arc::clone(self.engines[&node_id].kv.bad_downcast())
    }
}
