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

use tikv::pd::{PdClient, RpcClient};

#[test]
fn test_rpc_client() {
    // We need to set all members in PD_ENDPOINTS to pass this test.
    let endpoints = match env::var("PD_ENDPOINTS") {
        Ok(v) => v,
        Err(_) => return,
    };

    let mut id = 0;

    for _ in 0..100 {
        let client = RpcClient::new(&endpoints, 0).unwrap();
        let alloc_id = client.alloc_id().unwrap();
        assert!(alloc_id > id);
        id = alloc_id;
    }
}
