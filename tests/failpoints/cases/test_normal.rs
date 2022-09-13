// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_store_ffi::{KVGetStatus, RaftStoreProxyFFI};
use kvproto::kvrpcpb::ApiVersion;
use mock_engine_store;
use test_raftstore::*;

#[test]
fn test_normal() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(0, 3, sim, pd_client, ApiVersion::V1);
    unsafe {
        test_raftstore::init_cluster_ptr(&cluster);
    }

    // Try to start this node, return after persisted some keys.
    let _ = cluster.start();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);
    for id in cluster.engines.keys() {
        must_get_equal(&cluster.get_engine(*id), k, v);
        // must_get_equal(db, k, v);
    }
    let region_id = cluster.get_region(k).get_id();
    unsafe {
        for (_, ffi_set) in cluster.ffi_helper_set.iter_mut() {
            let f = ffi_set.proxy_helper.fn_get_region_local_state.unwrap();
            let mut state = kvproto::raft_serverpb::RegionLocalState::default();
            let mut error_msg = mock_engine_store::RawCppStringPtrGuard::default();

            assert_eq!(
                f(
                    ffi_set.proxy_helper.proxy_ptr,
                    region_id,
                    &mut state as *mut _ as _,
                    error_msg.as_mut(),
                ),
                KVGetStatus::Ok
            );
            assert!(state.has_region());
            assert_eq!(state.get_state(), kvproto::raft_serverpb::PeerState::Normal);
            assert!(error_msg.as_ref().is_null());

            let mut state = kvproto::raft_serverpb::RegionLocalState::default();
            assert_eq!(
                f(
                    ffi_set.proxy_helper.proxy_ptr,
                    0, // not exist
                    &mut state as *mut _ as _,
                    error_msg.as_mut(),
                ),
                KVGetStatus::NotFound
            );
            assert!(!state.has_region());
            assert!(error_msg.as_ref().is_null());

            ffi_set
                .proxy
                .get_value_cf("none_cf", "123".as_bytes(), |value| {
                    let msg = value.unwrap_err();
                    assert_eq!(msg, "Storage Engine cf none_cf not found");
                });
            ffi_set
                .proxy
                .get_value_cf("raft", "123".as_bytes(), |value| {
                    let res = value.unwrap();
                    assert!(res.is_none());
                });

            ffi_set.proxy.set_kv_engine(None);
            let res = ffi_set.proxy_helper.fn_get_region_local_state.unwrap()(
                ffi_set.proxy_helper.proxy_ptr,
                region_id,
                &mut state as *mut _ as _,
                error_msg.as_mut(),
            );
            assert_eq!(res, KVGetStatus::Error);
            assert!(!error_msg.as_ref().is_null());
            assert_eq!(
                error_msg.as_str(),
                "KV engine is not initialized".as_bytes()
            );
        }
    }

    cluster.shutdown();
}
