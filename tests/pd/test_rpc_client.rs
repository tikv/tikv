// Copyright 2016 PingCAP, Inc.
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

use std::env;

use kvproto::metapb;

use tikv::pd::{PdClient, RpcClient};

#[test]
fn test_rpc_client() {
    // We need to set all members in PD_ENDPOINTS to pass this test.
    let endpoints = match env::var("PD_ENDPOINTS") {
        Ok(v) => v,
        Err(_) => return,
    };

    let client = RpcClient::new(&endpoints).unwrap();
    assert!(client.cluster_id != 0);
    assert_eq!(client.cluster_id, client.get_cluster_id().unwrap());

    let store_id = client.alloc_id().unwrap();
    let mut store = metapb::Store::new();
    store.set_id(store_id);
    debug!("bootstrap store {:?}", store);

    let peer_id = client.alloc_id().unwrap();
    let mut peer = metapb::Peer::new();
    peer.set_id(peer_id);
    peer.set_store_id(store_id);

    let region_id = client.alloc_id().unwrap();
    let mut region = metapb::Region::new();
    region.set_id(region_id);
    region.mut_peers().push(peer.clone());
    debug!("bootstrap region {:?}", region);

    client.bootstrap_cluster(store.clone(), region.clone()).unwrap();
    assert_eq!(client.is_cluster_bootstrapped().unwrap(), true);

    let tmp_store = client.get_store(store_id).unwrap();
    assert_eq!(tmp_store.get_id(), store.get_id());

    let tmp_region = client.get_region_by_id(region_id).unwrap().unwrap();
    assert_eq!(tmp_region.get_id(), region.get_id());

    let mut prev_id = 0;
    for _ in 0..100 {
        let client = RpcClient::new(&endpoints).unwrap();
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > prev_id);
        prev_id = alloc_id;
    }
}

#[test]
fn test_rpc_client_safely_new() {
    let endpoints_1 = match env::var("PD_ENDPOINTS") {
        Ok(v) => v,
        Err(_) => return,
    };
    let endpoints_2 = match env::var("PD_ENDPOINTS_SEP") {
        Ok(v) => v,
        Err(_) => return,
    };
    let endpoints = [endpoints_1, endpoints_2];

    assert!(RpcClient::validate_endpoints(&endpoints).is_err());
}
