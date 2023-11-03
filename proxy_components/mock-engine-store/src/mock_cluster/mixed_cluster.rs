// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{option::Option, time::Duration, vec::Vec};

use kvproto::{
    metapb,
    raft_cmdpb::{RaftCmdRequest, RaftCmdResponse},
};
use test_raftstore::RawEngine;

use super::{ClusterExt, FFIHelperSet, MixedClusterConfig};

/// An union layer between Cluster of different `test_raftstore`s.
/// In test_utils and util functions in proxy_tests, we no longer rely on
/// specific `Cluster`s and `Simulator`s.
// This trait can't be dispatched dynamiclly, since `get_engine`.
pub trait MixedCluster {
    fn get_all_store_ids(&self) -> Vec<u64>;
    fn get_region(&self, key: &[u8]) -> metapb::Region;
    fn get_engine(&self, node_id: u64) -> &impl RawEngine<engine_rocks::RocksEngine>;
    fn must_put(&mut self, key: &[u8], value: &[u8]);
    fn must_get(&self, id: u64, key: &[u8], expected: Option<&[u8]>);
    fn must_get_finally(&self, id: u64, key: &[u8], expected: Option<&[u8]>);
    fn run_node(&mut self, node_id: u64);
    fn stop_node(&mut self, node_id: u64);
    fn call_command_on_leader(
        &mut self,
        request: RaftCmdRequest,
        timeout: Duration,
    ) -> raftstore::Result<RaftCmdResponse>;

    // Proxy specific
    fn get_cluster_ext(&self) -> &ClusterExt;
    fn mut_config(&mut self) -> &mut MixedClusterConfig;
    fn get_config(&self) -> &MixedClusterConfig;
    fn iter_ffi_helpers(
        &self,
        store_ids: Option<Vec<u64>>,
        f: &mut dyn FnMut(u64, &mut FFIHelperSet),
    );
}
