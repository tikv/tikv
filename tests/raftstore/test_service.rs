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

use std::sync::Arc;

use grpc::{ChannelBuilder, Environment};
use tikv::util::HandyRwLock;

use kvproto::tikvpb_grpc::TikvClient;
use kvproto::kvrpcpb::*;

use super::server::*;

#[test]
fn test_grpc_service() {
    let count = 1;
    let mut cluster = new_server_cluster(0, count);
    cluster.run();

    let region_id = 1;
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::new();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);

    let addr = cluster.sim.rl().get_addr(leader.get_store_id());
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env)
        .connect(&format!("{}", addr));
    let client = TikvClient::new(channel);

    // Raw KV
    {
        let (k, v) = (b"key".to_vec(), b"value".to_vec());

        let mut put_req = RawPutRequest::new();
        put_req.set_context(ctx.clone());
        put_req.key = k.clone();
        put_req.value = v.clone();
        let put_resp = client.raw_put(put_req).unwrap();
        assert!(!put_resp.has_region_error(), "{:?}", put_resp.region_error);
        assert!(put_resp.error.is_empty(), "{:?}", put_resp.error);

        let mut get_req = RawGetRequest::new();
        get_req.set_context(ctx.clone());
        get_req.key = k.clone();
        let get_resp = client.raw_get(get_req).unwrap();
        assert!(!get_resp.has_region_error());
        assert!(get_resp.error.is_empty());
        assert_eq!(get_resp.value, v);

        let mut scan_req = RawScanRequest::new();
        scan_req.set_context(ctx.clone());
        scan_req.start_key = k.clone();
        scan_req.limit = 1;
        let scan_resp = client.raw_scan(scan_req).unwrap();
        assert!(!scan_resp.has_region_error());
        assert_eq!(scan_resp.kvs.len(), 1);
        for kv in scan_resp.kvs.into_iter() {
            assert!(!kv.has_error());
            assert_eq!(kv.key, k);
            assert_eq!(kv.value, v);
        }

        let mut delete_req = RawDeleteRequest::new();
        delete_req.set_context(ctx.clone());
        delete_req.key = k.clone();
        let delete_resp = client.raw_delete(delete_req).unwrap();
        assert!(!delete_resp.has_region_error());
        assert!(delete_resp.error.is_empty());
    }
}
