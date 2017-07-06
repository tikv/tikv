// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::{Arc, mpsc};
use tikv::raftstore::store::{keys, Peekable, SnapManager, create_event_loop, bootstrap_store};
use tikv::server::Node;
use tikv::storage::ALL_CFS;
use tikv::util::rocksdb;
use tempdir::TempDir;
use kvproto::metapb;
use kvproto::raft_serverpb::RegionLocalState;
use rocksdb::DBCompressionType;

use super::pd::{TestPdClient, bootstrap_with_first_region};
use super::cluster::{Cluster, Simulator};
use super::node::{ChannelTransport, new_node_cluster};
use super::transport_simulate::SimulateTransport;
use super::util::*;

fn test_bootstrap_idempotent<T: Simulator>(cluster: &mut Cluster<T>) {
    // assume that there is a node  bootstrap the cluster and add region in pd successfully
    cluster.add_first_region().unwrap();
    // now  at same time start the another node, and will recive cluster is not bootstrap
    // it will try to bootstrap with a new region, but will failed
    // the region number still 1
    cluster.start();
    cluster.check_regions_number(1);
    cluster.shutdown();
    sleep_ms(500);
    cluster.start();
    cluster.check_regions_number(1);
}

#[test]
fn test_node_bootstrap_with_prepared_data() {
    // create a node
    let pd_client = Arc::new(TestPdClient::new(0));
    let cfg = new_server_config(0);

    let mut event_loop = create_event_loop(&cfg.raft_store).unwrap();
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp_engine = TempDir::new("test_cluster").unwrap();
    let engine = Arc::new(rocksdb::new_engine(tmp_engine.path().to_str().unwrap(), ALL_CFS)
        .unwrap());
    let tmp_mgr = TempDir::new("test_cluster").unwrap();

    let mut node = Node::new(&mut event_loop, &cfg, pd_client.clone());
    let snap_mgr = SnapManager::new(tmp_mgr.path().to_str().unwrap(),
                                    Some(node.get_sendch()),
                                    cfg.raft_store.use_sst_file_snapshot,
                                    DBCompressionType::DBNo);
    let (_, snapshot_status_receiver) = mpsc::channel();


    // assume there is a node has bootstrapped the cluster and add region in pd successfully
    bootstrap_with_first_region(pd_client.clone()).unwrap();

    // now anthoer node at same time begin bootstrap node, but panic after prepared bootstrap
    // now rocksDB must have some prepare data
    bootstrap_store(&engine, 0, 1).unwrap();
    let region = node.prepare_bootstrap_cluster(&engine, 1).unwrap();
    assert!(engine.get_msg::<metapb::Region>(&keys::prepare_bootstrap_key())
        .unwrap()
        .is_some());
    let region_state_key = keys::region_state_key(region.get_id());
    assert!(engine.get_msg::<RegionLocalState>(&region_state_key).unwrap().is_some());

    // try to restart this node, will clear the prepare data
    node.start(event_loop,
               engine.clone(),
               simulate_trans,
               snap_mgr,
               snapshot_status_receiver)
        .unwrap();
    assert!(engine.clone()
        .get_msg::<metapb::Region>(&keys::prepare_bootstrap_key())
        .unwrap()
        .is_none());
    assert!(engine.get_msg::<RegionLocalState>(&region_state_key).unwrap().is_none());
    assert_eq!(pd_client.get_regions_number() as u32, 1);
}

#[test]
fn test_node_bootstrap_idempotent() {
    let mut cluster = new_node_cluster(0, 3);
    test_bootstrap_idempotent(&mut cluster);
}
