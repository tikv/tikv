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

use super::cluster::{Cluster, Simulator};
use std::sync::{Arc, RwLock};
use super::node::{NodeCluster, new_node_cluster};
use super::util::*;
use tikv::raftstore::store::*;
use futures::Future;
use tikv::pd::PdClient;
use tikv::storage::ALL_CFS;
use super::pd::TestPdClient;
use tikv::util::HandyRwLock;
use tikv::server::Error as ServerError;

fn test_bootstrap_idempotent<T: Simulator>(cluster: &mut Cluster<T>) {
    // assume that there is a node  bootstrap the cluster and add region in pd successfully
    cluster.add_first_region().unwrap();
    // now  at same time start the another node, and will recive cluster is not bootstrap
    // try to bootstrap with a new region
    cluster.start();
    cluster.check_regions_number(1);
    cluster.shutdown();
    sleep_ms(500);
    cluster.start();
    cluster.check_regions_number(1);
}

fn test_bootstrap_with_check_epoch() {
    let pd_client = Arc::new(TestPdClient::new(0));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(0, 1, ALL_CFS, sim.clone(), pd_client);
    let pd_client = cluster.pd_client.clone();
    // firstly bootstrap with region
    let region_id = cluster.run_conf_change();
    let mut region = pd_client.get_region_by_id(region_id).wait().unwrap().unwrap();
    let engine = cluster.get_engine(1);
    // change region epoch
    region.mut_region_epoch().set_version(2);

    // assume cluster prepare meet an error
    // and prepared a differet region in store
    cluster.shutdown();
    assert!(clear_prepare_bootstrap(&engine, region_id).is_ok());
    assert!(write_prepare_bootstrap(&engine, &region).is_ok());
    pd_client.set_cluster_bootstrap(true);

    // to check meet inconsistent epoch when bootstrap，will meet error in here
    let e = sim.wl()
        .run_node_with_handle_error(1, cluster.cfg.clone(), engine.clone())
        .unwrap_err();
    match e {
        ServerError::Other(err) => {
            assert!(err.description().contains("region version inconsist: 1 with 2"))
        }
        _ => panic!("should meet region epoch inconsist error"),
    }
    cluster.check_regions_number(1);
}
#[test]
fn test_node_bootstrap_idempotent() {
    let mut cluster = new_node_cluster(0, 3);
    test_bootstrap_idempotent(&mut cluster);
}

#[test]
fn test_node_bootstrap_witch_check_epoch() {
    test_bootstrap_with_check_epoch();
}
