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
    let channel = ChannelBuilder::new(env).connect(&format!("{}", addr));
    let client = TikvClient::new(channel);
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Raw KV
    {
        let mut put_req = RawPutRequest::new();
        put_req.set_context(ctx.clone());
        put_req.key = k.clone();
        put_req.value = v.clone();
        let put_resp = client.raw_put(put_req).unwrap();
        assert!(!put_resp.has_region_error());
        assert!(put_resp.error.is_empty());

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

    // MVCC / TXN
    {
        let mut ts = 0;

        ts += 1;
        let start_version = ts;
        let mut prewrite_req = PrewriteRequest::new();
        prewrite_req.set_context(ctx.clone());
        let mut muts = Mutation::new();
        muts.op = Op::Put;
        muts.key = k.clone();
        muts.value = v.clone();
        prewrite_req.set_mutations(vec![muts].into_iter().collect());
        prewrite_req.primary_lock = k.clone();
        prewrite_req.start_version = start_version;
        prewrite_req.lock_ttl = prewrite_req.start_version + 1;
        let prewrite_resp = client.kv_prewrite(prewrite_req).unwrap();
        assert!(!prewrite_resp.has_region_error());
        assert!(prewrite_resp.errors.is_empty());

        ts += 1;
        let commit_version = ts;
        let mut commit_req = CommitRequest::new();
        commit_req.set_context(ctx.clone());
        commit_req.start_version = start_version;
        commit_req.set_keys(vec![k.clone()].into_iter().collect());
        commit_req.commit_version = commit_version;
        let commit_resp = client.kv_commit(commit_req).unwrap();
        assert!(!commit_resp.has_region_error());
        assert!(!commit_resp.has_error());

        ts += 1;
        let get_version = ts;
        let mut get_req = GetRequest::new();
        get_req.set_context(ctx.clone());
        get_req.key = k.clone();
        get_req.version = get_version;
        let get_resp = client.kv_get(get_req).unwrap();
        assert!(!get_resp.has_region_error());
        assert!(!get_resp.has_error());
        assert_eq!(get_resp.value, v);

        ts += 1;
        let scan_version = ts;
        let mut scan_req = ScanRequest::new();
        scan_req.set_context(ctx.clone());
        scan_req.start_key = k.clone();
        scan_req.limit = 1;
        scan_req.version = scan_version;
        let scan_resp = client.kv_scan(scan_req).unwrap();
        assert!(!scan_resp.has_region_error());
        assert_eq!(scan_resp.pairs.len(), 1);
        for kv in scan_resp.pairs.into_iter() {
            assert!(!kv.has_error());
            assert_eq!(kv.key, k);
            assert_eq!(kv.value, v);
        }
    }
}
