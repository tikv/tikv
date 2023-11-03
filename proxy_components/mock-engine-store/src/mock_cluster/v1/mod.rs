// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub mod cluster;
pub mod cluster_ext_v1;
pub mod node;
pub mod server;
pub mod transport_simulate;
pub mod util;
// mod common should be private
mod common;

use std::{option::Option, time::Duration, vec::Vec};

pub use cluster::*;
use common::*;
use kvproto::{
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
};
use test_raftstore::RawEngine;
pub use test_raftstore::{
    is_error_response, make_cb, new_admin_request, new_delete_cmd, new_put_cf_cmd,
    new_region_leader_cmd, new_status_request, new_store, new_tikv_config, new_transfer_leader_cmd,
    sleep_ms,
};
pub use util::*;

use super::{mixed_cluster::*, test_utils::*, ClusterExt, FFIHelperSet, MixedClusterConfig};

impl<T: Simulator<TiFlashEngine>> MixedCluster for Cluster<T> {
    fn get_all_store_ids(&self) -> Vec<u64> {
        self.engines.keys().copied().collect::<Vec<u64>>()
    }
    fn get_region(&self, key: &[u8]) -> metapb::Region {
        <Cluster<T>>::get_region(self, key)
    }
    fn get_engine(&self, node_id: u64) -> &impl RawEngine<engine_rocks::RocksEngine> {
        <Cluster<T>>::get_engine(self, node_id)
    }
    fn must_put(&mut self, key: &[u8], value: &[u8]) {
        <Cluster<T>>::must_put(self, key, value);
    }
    fn must_get(&self, node_id: u64, key: &[u8], expected: Option<&[u8]>) {
        match expected {
            Some(v) => must_get_equal(<Cluster<T>>::get_engine(self, node_id), key, v),
            None => must_get_none(<Cluster<T>>::get_engine(self, node_id), key),
        };
    }
    fn must_get_finally(&self, node_id: u64, key: &[u8], value: Option<&[u8]>) {
        use engine_traits::Peekable;
        let engine = <Cluster<T>>::get_engine(self, node_id);
        let cf = "default";
        for _ in 1..300 {
            let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
            if let (Some(value), Some(res)) = (value, res.as_ref()) {
                if value == &res[..] {
                    return;
                }
            }
            if value.is_none() && res.is_none() {
                return;
            }
            std::thread::sleep(Duration::from_millis(20));
        }

        tikv_util::debug!("last try to get {}", log_wrappers::hex_encode_upper(key));
        let res = engine.get_value_cf(cf, &keys::data_key(key)).unwrap();
        if value.is_none() && res.is_none()
            || value.is_some() && res.is_some() && value.unwrap() == &*res.unwrap()
        {
            return;
        }
        panic!(
            "can't get value {:?} for key {}",
            value.map(tikv_util::escape),
            log_wrappers::hex_encode_upper(key)
        )
    }
    fn run_node(&mut self, node_id: u64) {
        <Cluster<T>>::run_node(self, node_id).unwrap();
    }
    fn stop_node(&mut self, node_id: u64) {
        <Cluster<T>>::stop_node(self, node_id);
    }
    fn call_command_on_leader(
        &mut self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> raftstore::Result<RaftCmdResponse> {
        self.call_command_on_leader(request, timeout)
    }
    fn mut_config(&mut self) -> &mut MixedClusterConfig {
        &mut self.cfg
    }
    fn get_config(&self) -> &MixedClusterConfig {
        &self.cfg
    }

    // ClusterExt
    fn get_cluster_ext(&self) -> &ClusterExt {
        &self.cluster_ext
    }
    fn iter_ffi_helpers(
        &self,
        store_ids: Option<Vec<u64>>,
        f: &mut dyn FnMut(u64, &mut FFIHelperSet),
    ) {
        let need_check = store_ids.is_none();
        let ffi_side_ids = self.cluster_ext.iter_ffi_helpers(store_ids, f);
        if need_check {
            let cluster_side_ids = self.engines.keys().copied();
            assert_eq!(cluster_side_ids.len(), ffi_side_ids.len());
        }
    }
}
