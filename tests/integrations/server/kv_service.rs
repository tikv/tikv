// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    char::from_u32,
    collections::HashMap,
    path::Path,
    sync::{atomic::AtomicU64, *},
    thread,
    time::{Duration, Instant},
};

use api_version::{ApiV1, ApiV1Ttl, ApiV2, KvFormat};
use concurrency_manager::ConcurrencyManager;
use engine_traits::{
    MiscExt, Peekable, RaftEngine, RaftEngineReadOnly, RaftLogBatch, SyncMutable, CF_DEFAULT,
    CF_LOCK, CF_RAFT, CF_WRITE,
};
use futures::{executor::block_on, future, SinkExt, StreamExt, TryStreamExt};
use grpcio::*;
use grpcio_health::{proto::HealthCheckRequest, *};
use kvproto::{
    coprocessor::*,
    debugpb,
    kvrpcpb::{Action::MinCommitTsPushed, PrewriteRequestPessimisticAction::*, *},
    metapb, raft_serverpb,
    raft_serverpb::*,
    tikvpb::*,
};
use pd_client::PdClient;
use raft::eraftpb;
use raftstore::{
    coprocessor::CoprocessorHost,
    store::{fsm::store::StoreMeta, AutoSplitController, SnapManager},
};
use resource_metering::CollectorRegHandle;
use service::service_manager::GrpcServiceManager;
use tempfile::Builder;
use test_raftstore::*;
use test_raftstore_macro::test_case;
use tikv::{
    config::QuotaConfig,
    coprocessor::REQ_TYPE_DAG,
    import::{Config as ImportConfig, SstImporter},
    server,
    server::{
        gc_worker::sync_gc,
        service::{batch_commands_request, batch_commands_response},
    },
    storage::{config::EngineType, txn::FLASHBACK_BATCH_SIZE},
};
use tikv_util::{
    config::ReadableSize,
    worker::{dummy_scheduler, LazyWorker},
    HandyRwLock,
};
use txn_types::{Key, Lock, LockType, TimeStamp};

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_rawkv() {
    let (_cluster, client, ctx) = new_cluster();
    let v0 = b"v0".to_vec();
    let v1 = b"v1".to_vec();
    let (k, v) = (b"key".to_vec(), b"v2".to_vec());

    // Raw cas
    let mut cas_req = RawCasRequest::default();
    cas_req.set_context(ctx.clone());
    cas_req.key = k.clone();
    cas_req.value = v0.clone();
    cas_req.previous_not_exist = true;
    let resp = client.raw_compare_and_swap(&cas_req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_succeed());

    // Raw get
    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    let get_resp = client.raw_get(&get_req).unwrap();
    assert_eq!(get_resp.value, v0);

    cas_req.value = v1.clone();
    cas_req.previous_not_exist = false;
    cas_req.previous_value = v0;
    let resp = client.raw_compare_and_swap(&cas_req).unwrap();
    assert!(resp.get_succeed());
    let get_resp = client.raw_get(&get_req).unwrap();
    assert_eq!(get_resp.value, v1);

    // Raw put
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v.clone();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());

    // Raw get
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(get_resp.error.is_empty());
    assert_eq!(get_resp.value, v);

    // Raw scan
    let mut scan_req = RawScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.start_key = k.clone();
    scan_req.limit = 1;
    let scan_resp = client.raw_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.kvs.len(), 1);
    for kv in scan_resp.kvs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }

    // Raw delete
    let mut delete_req = RawDeleteRequest::default();
    delete_req.set_context(ctx);
    delete_req.key = k;
    let delete_resp = client.raw_delete(&delete_req).unwrap();
    assert!(!delete_resp.has_region_error());
    assert!(delete_resp.error.is_empty());
}

#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_rawkv_ttl() {
    let (cluster, leader, ctx) = new_cluster(|cluster| {
        cluster.cfg.storage.enable_ttl = true;
    });

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    let (v0, v1) = (b"v0".to_vec(), b"v1".to_vec());
    let (k, v) = (b"key".to_vec(), b"v2".to_vec());
    // Raw cas
    let mut cas_req = RawCasRequest::default();
    cas_req.set_context(ctx.clone());
    cas_req.key = k.clone();
    cas_req.value = v0.clone();
    cas_req.previous_not_exist = false;
    cas_req.previous_value = v1.clone();
    let resp = client.raw_compare_and_swap(&cas_req).unwrap();
    assert!(!resp.has_region_error());
    assert!(!resp.get_succeed());

    let mut cas_req = RawCasRequest::default();
    cas_req.set_context(ctx.clone());
    cas_req.key = k.clone();
    cas_req.value = v0.clone();
    cas_req.previous_not_exist = true;
    cas_req.previous_value = vec![];
    cas_req.ttl = 100;
    let resp = client.raw_compare_and_swap(&cas_req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_succeed());
    // Raw get
    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert_eq!(get_resp.value, v0);

    // cas a new value
    cas_req.value = v1.clone();
    cas_req.previous_not_exist = false;
    cas_req.previous_value = v0;
    cas_req.ttl = 140;
    let resp = client.raw_compare_and_swap(&cas_req).unwrap();
    assert!(resp.get_succeed());
    let get_resp = client.raw_get(&get_req).unwrap();
    assert_eq!(get_resp.value, v1);

    let mut get_ttl_req = RawGetKeyTtlRequest::default();
    get_ttl_req.set_context(ctx.clone());
    get_ttl_req.key = k.clone();
    let get_ttl_resp = client.raw_get_key_ttl(&get_ttl_req).unwrap();
    assert!(!get_ttl_resp.has_region_error());
    assert!(get_ttl_resp.error.is_empty());
    assert!(get_ttl_resp.ttl > 100);
    let mut delete_req = RawDeleteRequest::default();
    delete_req.set_context(ctx.clone());
    delete_req.key = k.clone();
    let delete_resp = client.raw_delete(&delete_req).unwrap();
    assert!(!delete_resp.has_region_error());
    let get_ttl_resp = client.raw_get_key_ttl(&get_ttl_req).unwrap();
    assert!(!get_ttl_resp.has_region_error());
    assert!(get_ttl_resp.get_not_found());

    // Raw put
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v.clone();
    put_req.ttl = 100;
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());

    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(get_resp.error.is_empty());
    assert_eq!(get_resp.value, v);

    // Raw scan
    let mut scan_req = RawScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.start_key = k.clone();
    scan_req.limit = 1;
    let scan_resp = client.raw_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.kvs.len(), 1);
    for kv in scan_resp.kvs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }

    // Raw get key ttl
    let get_ttl_resp = client.raw_get_key_ttl(&get_ttl_req).unwrap();
    assert!(!get_ttl_resp.has_region_error());
    assert!(get_ttl_resp.error.is_empty());
    assert_ne!(get_ttl_resp.ttl, 0);

    // Raw delete
    let mut delete_req = RawDeleteRequest::default();
    delete_req.set_context(ctx.clone());
    delete_req.key = k.clone();
    let delete_resp = client.raw_delete(&delete_req).unwrap();
    assert!(!delete_resp.has_region_error());
    assert!(delete_resp.error.is_empty());

    // Raw get key ttl
    let mut get_ttl_req = RawGetKeyTtlRequest::default();
    get_ttl_req.set_context(ctx.clone());
    get_ttl_req.key = k.clone();
    let get_ttl_resp = client.raw_get_key_ttl(&get_ttl_req).unwrap();
    assert!(!get_ttl_resp.has_region_error());
    assert!(get_ttl_resp.error.is_empty());
    assert!(get_ttl_resp.not_found);
    assert_eq!(get_ttl_resp.ttl, 0);

    // Raw put and exceed ttl
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v;
    put_req.ttl = 1;
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());

    std::thread::sleep(Duration::from_secs(1));

    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k;
    let get_resp = client.raw_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(get_resp.error.is_empty());
    assert!(get_resp.value.is_empty());

    // Can't run transaction commands with TTL enabled.
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx);
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(!prewrite_resp.get_errors().is_empty());
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_basic() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;
    write_and_read_key(&client, &ctx, &mut ts, k.clone(), v.clone());

    // Get
    ts += 1;
    let get_version = ts;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    get_req.version = get_version;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(!get_resp.has_error());
    assert!(get_resp.get_exec_details_v2().has_time_detail());
    assert!(get_resp.get_exec_details_v2().has_time_detail_v2());
    let scan_detail_v2 = get_resp.get_exec_details_v2().get_scan_detail_v2();
    assert_eq!(scan_detail_v2.get_total_versions(), 1);
    assert_eq!(scan_detail_v2.get_processed_versions(), 1);
    assert!(scan_detail_v2.get_processed_versions_size() > 0);
    assert_eq!(get_resp.value, v);

    // Scan
    ts += 1;
    let scan_version = ts;
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.start_key = k.clone();
    scan_req.limit = 1;
    scan_req.version = scan_version;
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert_eq!(scan_resp.pairs.len(), 1);
    for kv in scan_resp.pairs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }

    // Batch get
    ts += 1;
    let batch_get_version = ts;
    let mut batch_get_req = BatchGetRequest::default();
    batch_get_req.set_context(ctx);
    batch_get_req.set_keys(vec![k.clone()].into_iter().collect());
    batch_get_req.version = batch_get_version;
    let batch_get_resp = client.kv_batch_get(&batch_get_req).unwrap();
    assert!(batch_get_resp.get_exec_details_v2().has_time_detail());
    assert!(batch_get_resp.get_exec_details_v2().has_time_detail_v2());
    let scan_detail_v2 = batch_get_resp.get_exec_details_v2().get_scan_detail_v2();
    assert_eq!(scan_detail_v2.get_total_versions(), 1);
    assert_eq!(scan_detail_v2.get_processed_versions(), 1);
    assert!(scan_detail_v2.get_processed_versions_size() > 0);
    assert_eq!(batch_get_resp.pairs.len(), 1);
    for kv in batch_get_resp.pairs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_rollback_and_cleanup() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;
    write_and_read_key(&client, &ctx, &mut ts, k.clone(), v);

    // Prewrite puts some locks.
    ts += 1;
    let prewrite_start_version2 = ts;
    let (k2, v2) = (b"key2".to_vec(), b"value2".to_vec());
    let mut mut_pri = Mutation::default();
    mut_pri.set_op(Op::Put);
    mut_pri.set_key(k2.clone());
    mut_pri.set_value(v2);
    let mut mut_sec = Mutation::default();
    mut_sec.set_op(Op::Put);
    mut_sec.set_key(k.clone());
    mut_sec.set_value(b"foo".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mut_pri, mut_sec],
        k2.clone(),
        prewrite_start_version2,
    );

    // Scan lock, expects locks
    ts += 1;
    let scan_lock_max_version = ts;
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx.clone());
    scan_lock_req.max_version = scan_lock_max_version;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_region_error());
    assert_eq!(scan_lock_resp.locks.len(), 2);
    for (lock, key) in scan_lock_resp
        .locks
        .into_iter()
        .zip(vec![k.clone(), k2.clone()])
    {
        assert_eq!(lock.primary_lock, k2);
        assert_eq!(lock.key, key);
        assert_eq!(lock.lock_version, prewrite_start_version2);
    }

    // Rollback
    let rollback_start_version = prewrite_start_version2;
    let mut rollback_req = BatchRollbackRequest::default();
    rollback_req.set_context(ctx.clone());
    rollback_req.start_version = rollback_start_version;
    rollback_req.set_keys(vec![k2.clone()].into_iter().collect());
    let rollback_resp = client.kv_batch_rollback(&rollback_req).unwrap();
    assert!(!rollback_resp.has_region_error());
    assert!(!rollback_resp.has_error());
    rollback_req.set_keys(vec![k].into_iter().collect());
    let rollback_resp2 = client.kv_batch_rollback(&rollback_req).unwrap();
    assert!(!rollback_resp2.has_region_error());
    assert!(!rollback_resp2.has_error());

    // Cleanup
    let cleanup_start_version = prewrite_start_version2;
    let mut cleanup_req = CleanupRequest::default();
    cleanup_req.set_context(ctx.clone());
    cleanup_req.start_version = cleanup_start_version;
    cleanup_req.set_key(k2);
    let cleanup_resp = client.kv_cleanup(&cleanup_req).unwrap();
    assert!(!cleanup_resp.has_region_error());
    assert!(!cleanup_resp.has_error());

    // There should be no locks
    ts += 1;
    let scan_lock_max_version2 = ts;
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx);
    scan_lock_req.max_version = scan_lock_max_version2;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_region_error());
    assert_eq!(scan_lock_resp.locks.len(), 0);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_resolve_lock_gc_and_delete() {
    use kvproto::kvrpcpb::*;

    let (cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;

    // Prewrite
    ts += 1;
    let prewrite_start_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_start_version,
    );

    // Commit
    ts += 1;
    let commit_version = ts;
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![k.clone()],
        prewrite_start_version,
        commit_version,
        commit_version,
    );

    // Prewrite puts some locks.
    ts += 1;
    let prewrite_start_version2 = ts;
    let (k2, v2) = (b"key2".to_vec(), b"value2".to_vec());
    let new_v = b"new value".to_vec();
    let mut mut_pri = Mutation::default();
    mut_pri.set_op(Op::Put);
    mut_pri.set_key(k.clone());
    mut_pri.set_value(new_v.clone());
    let mut mut_sec = Mutation::default();
    mut_sec.set_op(Op::Put);
    mut_sec.set_key(k2);
    mut_sec.set_value(v2);
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mut_pri, mut_sec],
        k.clone(),
        prewrite_start_version2,
    );

    // Resolve lock
    ts += 1;
    let resolve_lock_commit_version = ts;
    let mut resolve_lock_req = ResolveLockRequest::default();
    let mut temp_txninfo = TxnInfo::default();
    temp_txninfo.txn = prewrite_start_version2;
    temp_txninfo.status = resolve_lock_commit_version;
    let vec_txninfo = vec![temp_txninfo];
    resolve_lock_req.set_context(ctx.clone());
    resolve_lock_req.set_txn_infos(vec_txninfo.into());
    let resolve_lock_resp = client.kv_resolve_lock(&resolve_lock_req).unwrap();
    assert!(!resolve_lock_resp.has_region_error());
    assert!(!resolve_lock_resp.has_error());

    // Get `k` at the latest ts.
    ts += 1;
    let get_version1 = ts;
    let mut get_req1 = GetRequest::default();
    get_req1.set_context(ctx.clone());
    get_req1.key = k.clone();
    get_req1.version = get_version1;
    let get_resp1 = client.kv_get(&get_req1).unwrap();
    assert!(!get_resp1.has_region_error());
    assert!(!get_resp1.has_error());
    assert_eq!(get_resp1.value, new_v);

    // GC `k` at the latest ts.
    ts += 1;
    let gc_safe_ponit = TimeStamp::from(ts);
    let gc_scheduler = cluster.sim.rl().get_gc_worker(1).scheduler();
    let region = cluster.get_region(&k);
    sync_gc(&gc_scheduler, region, gc_safe_ponit).unwrap();

    // the `k` at the old ts should be none.
    let get_version2 = commit_version + 1;
    let mut get_req2 = GetRequest::default();
    get_req2.set_context(ctx.clone());
    get_req2.key = k.clone();
    get_req2.version = get_version2;
    let get_resp2 = client.kv_get(&get_req2).unwrap();
    assert!(!get_resp2.has_region_error());
    assert!(!get_resp2.has_error());
    assert_eq!(get_resp2.value, b"".to_vec());

    // Transaction debugger commands
    // MvccGetByKey
    let mut mvcc_get_by_key_req = MvccGetByKeyRequest::default();
    mvcc_get_by_key_req.set_context(ctx.clone());
    mvcc_get_by_key_req.key = k.clone();
    let mvcc_get_by_key_resp = client.mvcc_get_by_key(&mvcc_get_by_key_req).unwrap();
    assert!(!mvcc_get_by_key_resp.has_region_error());
    assert!(mvcc_get_by_key_resp.error.is_empty());
    assert!(mvcc_get_by_key_resp.has_info());
    // MvccGetByStartTs
    let mut mvcc_get_by_start_ts_req = MvccGetByStartTsRequest::default();
    mvcc_get_by_start_ts_req.set_context(ctx.clone());
    mvcc_get_by_start_ts_req.start_ts = prewrite_start_version2;
    let mvcc_get_by_start_ts_resp = client
        .mvcc_get_by_start_ts(&mvcc_get_by_start_ts_req)
        .unwrap();
    assert!(!mvcc_get_by_start_ts_resp.has_region_error());
    assert!(mvcc_get_by_start_ts_resp.error.is_empty());
    assert!(mvcc_get_by_start_ts_resp.has_info());
    assert_eq!(mvcc_get_by_start_ts_resp.key, k);

    // Delete range
    let mut del_req = DeleteRangeRequest::default();
    del_req.set_context(ctx);
    del_req.start_key = b"a".to_vec();
    del_req.end_key = b"z".to_vec();
    let del_resp = client.kv_delete_range(&del_req).unwrap();
    assert!(!del_resp.has_region_error());
    assert!(del_resp.error.is_empty());
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
#[cfg(feature = "failpoints")]
fn test_mvcc_flashback_failed_after_first_batch() {
    let (_cluster, client, ctx) = new_cluster();
    let mut ts = 0;
    for i in 0..FLASHBACK_BATCH_SIZE * 2 {
        // Meet the constraints of the alphabetical order for test
        let k = format!("key@{}", from_u32(i as u32).unwrap()).into_bytes();
        write_and_read_key(&client, &ctx, &mut ts, k.clone(), b"value@0".to_vec());
        ts -= 3;
    }
    ts += 3;
    let check_ts = ts;
    for i in 0..FLASHBACK_BATCH_SIZE * 2 {
        let k = format!("key@{}", from_u32(i as u32).unwrap()).into_bytes();
        write_and_read_key(&client, &ctx, &mut ts, k.clone(), b"value@1".to_vec());
        ts -= 3;
    }
    ts += 3;
    // Flashback
    fail::cfg("flashback_failed_after_first_batch", "return").unwrap();
    fail::cfg("flashback_skip_1_key_in_write", "1*return").unwrap();
    must_flashback_to_version(&client, ctx.clone(), check_ts, ts + 1, ts + 2);
    fail::remove("flashback_skip_1_key_in_write");
    fail::remove("flashback_failed_after_first_batch");
    // skip for key@1
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!("key@{}", from_u32(1_u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@1".to_vec(),
        ts + 2,
    );
    // The first batch of writes are flashbacked.
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!("key@{}", from_u32(2_u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@0".to_vec(),
        ts + 2,
    );
    // Subsequent batches of writes are not flashbacked.
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!("key@{}", from_u32(FLASHBACK_BATCH_SIZE as u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@1".to_vec(),
        ts + 2,
    );
    // Flashback batch 2.
    fail::cfg("flashback_failed_after_first_batch", "return").unwrap();
    must_flashback_to_version(&client, ctx.clone(), check_ts, ts + 1, ts + 2);
    fail::remove("flashback_failed_after_first_batch");
    // key@1 must be flashbacked in the second batch firstly.
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!("key@{}", from_u32(1_u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@0".to_vec(),
        ts + 2,
    );
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!("key@{}", from_u32(FLASHBACK_BATCH_SIZE as u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@0".to_vec(),
        ts + 2,
    );
    // 2 * (FLASHBACK_BATCH_SIZE - 1) keys are flashbacked.
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!(
            "key@{}",
            from_u32(2 * FLASHBACK_BATCH_SIZE as u32 - 2).unwrap()
        )
        .as_bytes()
        .to_vec(),
        b"value@1".to_vec(),
        ts + 2,
    );
    // Flashback needs to be continued.
    must_flashback_to_version(&client, ctx.clone(), check_ts, ts + 1, ts + 2);
    // Flashback again to check if any error occurs :)
    must_flashback_to_version(&client, ctx.clone(), check_ts, ts + 1, ts + 2);
    ts += 2;
    // Subsequent batches of writes are flashbacked.
    must_kv_read_equal(
        &client,
        ctx.clone(),
        format!(
            "key@{}",
            from_u32(2 * FLASHBACK_BATCH_SIZE as u32 - 2).unwrap()
        )
        .as_bytes()
        .to_vec(),
        b"value@0".to_vec(),
        ts,
    );
    // key@0 which used as prewrite lock also need to be flahsbacked.
    must_kv_read_equal(
        &client,
        ctx,
        format!("key@{}", from_u32(0_u32).unwrap())
            .as_bytes()
            .to_vec(),
        b"value@0".to_vec(),
        ts,
    );
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_flashback() {
    let (_cluster, client, ctx) = new_cluster();
    let mut ts = 0;
    // Need to write many batches.
    for i in 0..2000 {
        let v = format!("value@{}", i).into_bytes();
        let k = format!("key@{}", i % 1000).into_bytes();
        write_and_read_key(&client, &ctx, &mut ts, k.clone(), v.clone());
    }
    // Prewrite to leave a lock.
    let k = b"key@1".to_vec();
    ts += 1;
    let prewrite_start_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(b"value@latest".to_vec());
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        k.clone(),
        prewrite_start_version,
    );
    ts += 1;
    let get_version = ts;
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k;
    get_req.version = get_version;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(get_resp.get_error().has_locked());
    assert!(get_resp.value.is_empty());
    // Flashback
    must_flashback_to_version(&client, ctx.clone(), 5, ts + 1, ts + 2);
    ts += 2;
    // Should not meet the lock and can not get the latest data any more.
    must_kv_read_equal(&client, ctx, b"key@1".to_vec(), b"value@1".to_vec(), ts);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_flashback_block_rw() {
    let (_cluster, client, ctx) = new_cluster();
    // Prepare the flashback.
    must_prepare_flashback(&client, ctx.clone(), 1, 2);

    // Try to read version 3 (after flashback, FORBIDDEN).
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    // Get
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    get_req.version = 3;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(
        get_resp.get_region_error().has_flashback_in_progress(),
        "{:?}",
        get_resp
    );
    assert!(!get_resp.has_error());
    assert!(get_resp.value.is_empty());
    // Scan
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.start_key = k.clone();
    scan_req.limit = 1;
    scan_req.version = 3;
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(scan_resp.get_region_error().has_flashback_in_progress());
    assert!(!scan_resp.has_error());
    assert!(scan_resp.pairs.is_empty());
    // Try to read version 1 (before flashback, ALLOWED).
    // Get
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
    get_req.version = 1;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(!get_resp.has_error());
    assert!(get_resp.value.is_empty());
    // Scan
    let mut scan_req = ScanRequest::default();
    scan_req.set_context(ctx.clone());
    scan_req.start_key = k.clone();
    scan_req.limit = 1;
    scan_req.version = 1;
    let scan_resp = client.kv_scan(&scan_req).unwrap();
    assert!(!scan_resp.has_region_error());
    assert!(!scan_resp.has_error());
    assert!(scan_resp.pairs.is_empty());
    // Try to write (FORBIDDEN).
    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    let prewrite_resp = try_kv_prewrite(&client, ctx.clone(), vec![mutation], k, 1);
    assert!(prewrite_resp.get_region_error().has_flashback_in_progress());
    // Finish the flashback.
    must_finish_flashback(&client, ctx, 1, 2, 3);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_flashback_block_scheduling() {
    let (mut cluster, client, ctx) = new_cluster();
    // Prepare the flashback.
    must_prepare_flashback(&client, ctx.clone(), 0, 1);
    // Try to transfer leader.
    let transfer_leader_resp = cluster.try_transfer_leader(1, new_peer(2, 2));
    assert!(
        transfer_leader_resp
            .get_header()
            .get_error()
            .has_flashback_in_progress(),
        "{:?}",
        transfer_leader_resp
    );
    // Finish the flashback.
    must_finish_flashback(&client, ctx, 0, 1, 2);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_flashback_unprepared() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut ts = 0;
    write_and_read_key(&client, &ctx, &mut ts, k.clone(), v.clone());
    // Try to flashback without preparing first.
    let mut req = FlashbackToVersionRequest::default();
    req.set_context(ctx.clone());
    req.set_start_ts(4);
    req.set_commit_ts(5);
    req.set_version(0);
    req.set_start_key(b"a".to_vec());
    req.set_end_key(b"z".to_vec());
    let resp = client.kv_flashback_to_version(&req).unwrap();
    assert!(resp.get_region_error().has_flashback_not_prepared());
    assert!(resp.get_error().is_empty());
    must_kv_read_equal(&client, ctx.clone(), k.clone(), v, 6);
    // Flashback with preparing.
    must_flashback_to_version(&client, ctx.clone(), 0, 6, 7);
    must_kv_read_not_found(&client, ctx.clone(), k.clone(), 7);
    // Mock the flashback retry.
    must_finish_flashback(&client, ctx.clone(), 0, 6, 7);
    must_kv_read_not_found(&client, ctx, k, 7);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_flashback_with_unlimited_range() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut ts = 0;
    write_and_read_key(&client, &ctx, &mut ts, k.clone(), v.clone());
    must_kv_read_equal(&client, ctx.clone(), k.clone(), v, 6);

    let mut prepare_req = PrepareFlashbackToVersionRequest::default();
    prepare_req.set_context(ctx.clone());
    prepare_req.set_start_ts(6);
    prepare_req.set_version(0);
    prepare_req.set_start_key(b"".to_vec());
    prepare_req.set_end_key(b"".to_vec());
    client
        .kv_prepare_flashback_to_version(&prepare_req)
        .unwrap();
    let mut req = FlashbackToVersionRequest::default();
    req.set_context(ctx.clone());
    req.set_start_ts(6);
    req.set_commit_ts(7);
    req.set_version(0);
    req.set_start_key(b"".to_vec());
    req.set_end_key(b"".to_vec());
    let resp = client.kv_flashback_to_version(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_error().is_empty());

    must_kv_read_not_found(&client, ctx, k, 7);
}

// raft related RPC is tested as parts of test_snapshot.rs, so skip here.

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_coprocessor() {
    let (_cluster, client, _) = new_cluster();
    // SQL push down commands
    let mut req = Request::default();
    req.set_tp(REQ_TYPE_DAG);
    client.coprocessor(&req).unwrap();
}

#[test]
fn test_split_region() {
    test_split_region_impl::<ApiV1>(false);
    test_split_region_impl::<ApiV2>(false);
    test_split_region_impl::<ApiV1>(true);
    test_split_region_impl::<ApiV1Ttl>(true); // APIV1TTL for RawKV only.
    test_split_region_impl::<ApiV2>(true);
}

fn test_split_region_impl<F: KvFormat>(is_raw_kv: bool) {
    let encode_key = |k: &[u8]| -> Vec<u8> {
        if !is_raw_kv || F::TAG == ApiVersion::V2 {
            Key::from_raw(k).into_encoded()
        } else {
            k.to_vec()
        }
    };

    let (mut cluster, leader, mut ctx) =
        must_new_and_configure_cluster(|cluster| cluster.cfg.storage.set_api_version(F::TAG));
    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);
    ctx.set_api_version(F::CLIENT_TAG);

    // Split region commands
    let key = b"b";
    let mut req = SplitRegionRequest::default();
    req.set_context(ctx);
    req.set_is_raw_kv(is_raw_kv);
    req.set_split_key(key.to_vec());
    let resp = client.split_region(&req).unwrap();
    assert_eq!(resp.get_left().get_end_key().to_vec(), encode_key(key));
    assert_eq!(
        resp.get_left().get_end_key(),
        resp.get_right().get_start_key()
    );

    // Batch split region
    let region_id = resp.get_right().get_id();
    let leader = cluster.leader_of_region(region_id).unwrap();
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader);
    ctx.set_region_epoch(resp.get_right().get_region_epoch().to_owned());
    let mut req = SplitRegionRequest::default();
    req.set_context(ctx);
    req.set_is_raw_kv(is_raw_kv);
    let split_keys = vec![b"e".to_vec(), b"c".to_vec(), b"d".to_vec()];
    req.set_split_keys(split_keys.into());
    let resp = client.split_region(&req).unwrap();
    let result_split_keys: Vec<_> = resp
        .get_regions()
        .iter()
        .map(|x| x.get_start_key().to_vec())
        .collect();
    assert_eq!(
        result_split_keys,
        vec![b"b", b"c", b"d", b"e"]
            .into_iter()
            .map(|k| encode_key(&k[..]))
            .collect::<Vec<_>>()
    );
}

#[test_case(test_raftstore::must_new_cluster_and_debug_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_debug_client)]
fn test_debug_store() {
    let (mut cluster, debug_client, store_id) = new_cluster();
    let cluster_id = cluster.id();
    let req = debugpb::GetClusterInfoRequest::default();
    let resp = debug_client.get_cluster_info(&req).unwrap();
    assert_eq!(resp.get_cluster_id(), cluster_id);

    let req = debugpb::GetStoreInfoRequest::default();
    let resp = debug_client.get_store_info(&req).unwrap();
    assert_eq!(store_id, resp.get_store_id());

    cluster.must_put(b"a", b"val");
    cluster.must_put(b"c", b"val");
    cluster.flush_data();
    thread::sleep(Duration::from_millis(25));
    assert_eq!(b"val".to_vec(), cluster.must_get(b"a").unwrap());
    assert_eq!(b"val".to_vec(), cluster.must_get(b"c").unwrap());

    let mut req = debugpb::GetMetricsRequest::default();
    req.set_all(true);
    let resp = debug_client.get_metrics(&req).unwrap();
    assert_eq!(store_id, resp.get_store_id());
    assert!(!resp.get_rocksdb_kv().is_empty());
    assert!(resp.get_rocksdb_raft().is_empty());

    let mut req = debugpb::GetRegionPropertiesRequest::default();
    req.set_region_id(1);
    let resp = debug_client.get_region_properties(&req).unwrap();
    resp.get_props()
        .iter()
        .find(|p| {
            p.get_name() == "defaultcf.num_entries" && p.get_value().parse::<i32>().unwrap() >= 2
        })
        .unwrap();

    let req = debugpb::GetRangePropertiesRequest::default();
    let resp = debug_client.get_range_properties(&req).unwrap();
    resp.get_properties()
        .iter()
        .find(|p| {
            p.get_key() == "defaultcf.num_entries" && p.get_value().parse::<i32>().unwrap() >= 2
        })
        .unwrap();

    let mut req = debugpb::GetRangePropertiesRequest::default();
    req.set_start_key(b"d".to_vec());
    let resp = debug_client.get_range_properties(&req).unwrap();
    resp.get_properties()
        .iter()
        .find(|p| {
            p.get_key() == "defaultcf.num_entries" && p.get_value().parse::<i32>().unwrap() < 2
        })
        .unwrap();
}

#[test_case(test_raftstore::must_new_cluster_and_debug_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_debug_client)]
fn test_debug_get() {
    let (cluster, debug_client, store_id) = new_cluster();
    let (k, v) = (b"key", b"value");

    // Put some data.
    let engine = cluster.get_engine(store_id);
    let key = keys::data_key(k);
    engine.put(&key, v).unwrap();
    assert_eq!(engine.get_value(&key).unwrap().unwrap(), v);

    // Debug get
    let mut req = debugpb::GetRequest::default();
    req.set_cf(CF_DEFAULT.to_owned());
    req.set_db(debugpb::Db::Kv);
    req.set_key(key);
    let mut resp = debug_client.get(&req).unwrap();
    assert_eq!(resp.take_value(), v);

    req.set_key(b"foo".to_vec());
    match debug_client.get(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test_case(test_raftstore::must_new_cluster_and_debug_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_debug_client)]
fn test_debug_raft_log() {
    let (cluster, debug_client, store_id) = new_cluster();

    // Put some data.
    let engine = cluster.get_raft_engine(store_id);
    let (region_id, log_index) = (200, 200);
    let mut entry = eraftpb::Entry::default();
    entry.set_term(1);
    entry.set_index(log_index);
    entry.set_entry_type(eraftpb::EntryType::EntryNormal);
    entry.set_data(vec![42].into());
    let mut lb = engine.log_batch(0);
    lb.append(region_id, None, vec![entry.clone()]).unwrap();
    engine.consume(&mut lb, false).unwrap();
    assert_eq!(
        engine.get_entry(region_id, log_index).unwrap().unwrap(),
        entry
    );

    // Debug raft_log
    let mut req = debugpb::RaftLogRequest::default();
    req.set_region_id(region_id);
    req.set_log_index(log_index);
    let resp = debug_client.raft_log(&req).unwrap();
    assert_ne!(resp.get_entry(), &eraftpb::Entry::default());

    let mut req = debugpb::RaftLogRequest::default();
    req.set_region_id(region_id + 1);
    req.set_log_index(region_id + 1);
    match debug_client.raft_log(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

// Note: if modified in the future, should be sync with
// `test_debug_region_info_v2`
#[test]
fn test_debug_region_info() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();

    let raft_engine = cluster.get_raft_engine(store_id);
    let kv_engine = cluster.get_engine(store_id);

    let region_id = 100;
    let mut raft_state = raft_serverpb::RaftLocalState::default();
    raft_state.set_last_index(42);
    let mut lb = raft_engine.log_batch(0);
    lb.put_raft_state(region_id, &raft_state).unwrap();
    raft_engine.consume(&mut lb, false).unwrap();
    assert_eq!(
        raft_engine.get_raft_state(region_id).unwrap().unwrap(),
        raft_state
    );

    let apply_state_key = keys::apply_state_key(region_id);
    let mut apply_state = raft_serverpb::RaftApplyState::default();
    apply_state.set_applied_index(42);
    kv_engine
        .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .get_msg_cf::<raft_serverpb::RaftApplyState>(CF_RAFT, &apply_state_key)
            .unwrap()
            .unwrap(),
        apply_state
    );

    let region_state_key = keys::region_state_key(region_id);
    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(raft_serverpb::PeerState::Tombstone);
    kv_engine
        .put_msg_cf(CF_RAFT, &region_state_key, &region_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .get_msg_cf::<raft_serverpb::RegionLocalState>(CF_RAFT, &region_state_key)
            .unwrap()
            .unwrap(),
        region_state
    );

    // Debug region_info
    let mut req = debugpb::RegionInfoRequest::default();
    req.set_region_id(region_id);
    let mut resp = debug_client.region_info(&req).unwrap();
    assert_eq!(resp.take_raft_local_state(), raft_state);
    assert_eq!(resp.take_raft_apply_state(), apply_state);
    assert_eq!(resp.take_region_local_state(), region_state);

    req.set_region_id(region_id + 1);
    match debug_client.region_info(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

// Note: if modified in the future, should be sync with `test_debug_region_info`
#[test]
fn test_debug_region_info_v2() {
    let (cluster, debug_client, store_id) = test_raftstore_v2::must_new_cluster_and_debug_client();

    let raft_engine = cluster.get_raft_engine(store_id);
    let region_id = 100;
    let mut raft_state = raft_serverpb::RaftLocalState::default();
    raft_state.set_last_index(42);
    let mut lb = raft_engine.log_batch(10);
    lb.put_raft_state(region_id, &raft_state).unwrap();

    let mut apply_state = raft_serverpb::RaftApplyState::default();
    apply_state.set_applied_index(42);
    lb.put_apply_state(region_id, 42, &apply_state).unwrap();

    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(raft_serverpb::PeerState::Tombstone);
    lb.put_region_state(region_id, 42, &region_state).unwrap();

    lb.put_flushed_index(region_id, CF_RAFT, 5, 42).unwrap();
    raft_engine.consume(&mut lb, false).unwrap();
    assert_eq!(
        raft_engine.get_raft_state(region_id).unwrap().unwrap(),
        raft_state
    );

    assert_eq!(
        raft_engine
            .get_apply_state(region_id, u64::MAX)
            .unwrap()
            .unwrap(),
        apply_state
    );

    assert_eq!(
        raft_engine
            .get_region_state(region_id, u64::MAX)
            .unwrap()
            .unwrap(),
        region_state
    );

    // Debug region_info
    let mut req = debugpb::RegionInfoRequest::default();
    req.set_region_id(region_id);
    let mut resp = debug_client.region_info(&req).unwrap();
    assert_eq!(resp.take_raft_local_state(), raft_state);
    assert_eq!(resp.take_raft_apply_state(), apply_state);
    assert_eq!(resp.take_region_local_state(), region_state);

    req.set_region_id(region_id + 1);
    match debug_client.region_info(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

// Note: if modified in the future, should be sync with
// `test_debug_region_size_v2`
#[test]
fn test_debug_region_size() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
    let engine = cluster.get_engine(store_id);

    // Put some data.
    let region_id = 100;
    let region_state_key = keys::region_state_key(region_id);
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    region.set_start_key(b"a".to_vec());
    region.set_end_key(b"z".to_vec());
    let mut state = RegionLocalState::default();
    state.set_region(region);
    engine
        .put_msg_cf(CF_RAFT, &region_state_key, &state)
        .unwrap();

    let cfs = vec![CF_DEFAULT, CF_LOCK, CF_WRITE];
    // At lease 8 bytes for the WRITE cf.
    let (k, v) = (keys::data_key(b"kkkk_kkkk"), b"v");
    for cf in &cfs {
        engine.put_cf(cf, k.as_slice(), v).unwrap();
    }

    let mut req = debugpb::RegionSizeRequest::default();
    req.set_region_id(region_id);
    req.set_cfs(cfs.iter().map(|s| s.to_string()).collect());
    let entries: Vec<_> = debug_client
        .region_size(&req)
        .unwrap()
        .take_entries()
        .into();
    assert_eq!(entries.len(), 3);
    for e in entries {
        cfs.iter().find(|&&c| c == e.cf).unwrap();
        assert!(e.size > 0);
    }

    req.set_region_id(region_id + 1);
    match debug_client.region_size(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

// Note: if modified in the future, should be sync with `test_debug_region_size`
#[test]
fn test_debug_region_size_v2() {
    let (cluster, debug_client, store_id) = test_raftstore_v2::must_new_cluster_and_debug_client();
    let raft_engine = cluster.get_raft_engine(store_id);
    let engine = cluster.get_engine(store_id);

    let mut lb = raft_engine.log_batch(10);
    // Put some data.
    let region_id = 1;
    let mut region = metapb::Region::default();
    region.set_id(region_id);
    region.set_start_key(b"a".to_vec());
    region.set_end_key(b"z".to_vec());
    let mut state = RegionLocalState::default();
    state.set_region(region);
    state.set_tablet_index(5);
    lb.put_region_state(region_id, 5, &state).unwrap();
    raft_engine.consume(&mut lb, false).unwrap();

    let cfs = vec![CF_DEFAULT, CF_LOCK, CF_WRITE];
    // At lease 8 bytes for the WRITE cf.
    let (k, v) = (keys::data_key(b"kkkk_kkkk"), b"v");
    for cf in &cfs {
        engine.put_cf(cf, k.as_slice(), v).unwrap();
    }

    let mut req = debugpb::RegionSizeRequest::default();
    req.set_region_id(region_id);
    req.set_cfs(cfs.iter().map(|s| s.to_string()).collect());
    let entries: Vec<_> = debug_client
        .region_size(&req)
        .unwrap()
        .take_entries()
        .into();
    assert_eq!(entries.len(), 3);
    for e in entries {
        cfs.iter().find(|&&c| c == e.cf).unwrap();
        assert!(e.size > 0);
    }

    req.set_region_id(region_id + 1);
    match debug_client.region_size(&req).unwrap_err() {
        Error::RpcFailure(status) => {
            assert_eq!(status.code(), RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
#[cfg(feature = "failpoints")]
fn test_debug_fail_point() {
    let (_cluster, debug_client, _) = must_new_cluster_and_debug_client();

    let (fp, act) = ("raft_between_save", "off");

    let mut inject_req = debugpb::InjectFailPointRequest::default();
    inject_req.set_name(fp.to_owned());
    inject_req.set_actions(act.to_owned());
    debug_client.inject_fail_point(&inject_req).unwrap();

    let resp = debug_client
        .list_fail_points(&debugpb::ListFailPointsRequest::default())
        .unwrap();
    let entries = resp.get_entries();
    assert!(
        entries
            .iter()
            .any(|e| e.get_name() == fp && e.get_actions() == act)
    );

    let mut recover_req = debugpb::RecoverFailPointRequest::default();
    recover_req.set_name(fp.to_owned());
    debug_client.recover_fail_point(&recover_req).unwrap();

    let resp = debug_client
        .list_fail_points(&debugpb::ListFailPointsRequest::default())
        .unwrap();
    let entries = resp.get_entries();
    assert!(
        entries
            .iter()
            .all(|e| !(e.get_name() == fp && e.get_actions() == act))
    );
}

#[test_case(test_raftstore::must_new_cluster_and_debug_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_debug_client)]
fn test_debug_scan_mvcc() {
    let (cluster, debug_client, store_id) = new_cluster();
    let engine = cluster.get_engine(store_id);

    // Put some data.
    let keys = [
        keys::data_key(b"meta_lock_1"),
        keys::data_key(b"meta_lock_2"),
    ];
    for k in &keys {
        let v = Lock::new(
            LockType::Put,
            b"pk".to_vec(),
            1.into(),
            10,
            None,
            TimeStamp::zero(),
            0,
            TimeStamp::zero(),
            false,
        )
        .to_bytes();
        engine.put_cf(CF_LOCK, k.as_slice(), &v).unwrap();
    }

    let mut req = debugpb::ScanMvccRequest::default();
    req.set_from_key(keys::data_key(b"m"));
    req.set_to_key(keys::data_key(b"n"));
    req.set_limit(1);

    let receiver = debug_client.scan_mvcc(&req).unwrap();
    let future = receiver.try_fold(Vec::new(), |mut keys, mut resp| {
        let key = resp.take_key();
        keys.push(key);
        future::ok::<_, Error>(keys)
    });
    let keys = block_on(future).unwrap();
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0], keys::data_key(b"meta_lock_1"));
}

#[test]
fn test_double_run_node() {
    let count = 1;
    let mut cluster = new_node_cluster(0, count);
    cluster.run();
    let id = *cluster.engines.keys().next().unwrap();
    let engines = cluster.engines.values().next().unwrap().clone();
    let router = cluster.sim.rl().get_router(id).unwrap();
    let mut sim = cluster.sim.wl();
    let node = sim.get_node(id).unwrap();
    let pd_worker = LazyWorker::new("test-pd-worker");
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let snap_mgr = SnapManager::new(tmp.path().to_str().unwrap());
    let coprocessor_host = CoprocessorHost::new(router, raftstore::coprocessor::Config::default());
    let importer = {
        let dir = Path::new(MiscExt::path(&engines.kv)).join("import-sst");
        Arc::new(
            SstImporter::new(&ImportConfig::default(), dir, None, ApiVersion::V1, false).unwrap(),
        )
    };
    let (split_check_scheduler, _) = dummy_scheduler();

    let store_meta = Arc::new(Mutex::new(StoreMeta::new(20)));
    let e = node
        .start(
            engines,
            simulate_trans,
            snap_mgr,
            pd_worker,
            store_meta,
            coprocessor_host,
            importer,
            split_check_scheduler,
            AutoSplitController::default(),
            ConcurrencyManager::new(1.into()),
            CollectorRegHandle::new_for_test(),
            None,
            GrpcServiceManager::dummy(),
            Arc::new(AtomicU64::new(0)),
        )
        .unwrap_err();
    assert!(format!("{:?}", e).contains("already started"), "{:?}", e);
    drop(sim);
    cluster.shutdown();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pessimistic_lock() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 10);

    // KeyIsLocked
    for &return_values in &[false, true] {
        let resp =
            kv_pessimistic_lock(&client, ctx.clone(), vec![k.clone()], 20, 20, return_values);
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert_eq!(resp.errors.len(), 1);
        assert!(resp.errors[0].has_locked());
        assert!(resp.values.is_empty());
        assert!(resp.not_founds.is_empty());
    }

    must_kv_commit(&client, ctx.clone(), vec![k.clone()], 10, 30, 30);

    // WriteConflict
    for &return_values in &[false, true] {
        let resp =
            kv_pessimistic_lock(&client, ctx.clone(), vec![k.clone()], 20, 20, return_values);
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert_eq!(resp.errors.len(), 1);
        assert!(resp.errors[0].has_conflict());
        assert!(resp.values.is_empty());
        assert!(resp.not_founds.is_empty());
    }

    // Return multiple values
    for &return_values in &[false, true] {
        let resp = kv_pessimistic_lock(
            &client,
            ctx.clone(),
            vec![k.clone(), b"nonexsit".to_vec()],
            40,
            40,
            true,
        );
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert!(resp.errors.is_empty());
        if return_values {
            assert_eq!(resp.get_values().to_vec(), vec![v.clone(), vec![]]);
            assert_eq!(resp.get_not_founds().to_vec(), vec![false, true]);
        }
        must_kv_pessimistic_rollback(&client, ctx.clone(), k.clone(), 40, 40);
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pessimistic_lock_resumable() {
    let (_cluster, client, ctx) = new_cluster();

    // Resumable pessimistic lock request with multi-key is not supported yet.
    let resp = kv_pessimistic_lock_resumable(
        &client,
        ctx.clone(),
        vec![b"k1".to_vec(), b"k2".to_vec()],
        1,
        1,
        None,
        false,
        false,
    );
    assert_eq!(resp.get_results(), &[]);
    assert_ne!(resp.get_errors().len(), 0);

    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v.clone());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation.clone()], k.clone(), 5);

    // No wait
    let start_time = Instant::now();
    let resp = kv_pessimistic_lock_resumable(
        &client,
        ctx.clone(),
        vec![k.clone()],
        8,
        8,
        None,
        false,
        false,
    );
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert!(start_time.elapsed() < Duration::from_millis(200));
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].has_locked());
    assert_eq!(resp.get_results().len(), 1);
    assert_eq!(
        resp.get_results()[0].get_type(),
        PessimisticLockKeyResultType::LockResultFailed
    );

    // Wait Timeout
    let resp = kv_pessimistic_lock_resumable(
        &client,
        ctx.clone(),
        vec![k.clone()],
        8,
        8,
        Some(1),
        false,
        false,
    );
    assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
    assert_eq!(resp.errors.len(), 1);
    assert!(resp.errors[0].has_locked());
    assert_eq!(resp.get_results().len(), 1);
    assert_eq!(
        resp.get_results()[0].get_type(),
        PessimisticLockKeyResultType::LockResultFailed
    );

    must_kv_commit(&client, ctx.clone(), vec![k.clone()], 5, 9, 9);

    let mut curr_ts = 10;

    for &(return_values, check_existence) in
        &[(false, false), (false, true), (true, false), (true, true)]
    {
        let prewrite_start_ts = curr_ts;
        let commit_ts = curr_ts + 5;
        let test_lock_ts = curr_ts + 10;
        curr_ts += 20;

        // Prewrite
        must_kv_prewrite(
            &client,
            ctx.clone(),
            vec![mutation.clone()],
            k.clone(),
            prewrite_start_ts,
        );

        let (tx, rx) = std::sync::mpsc::channel();
        let handle = {
            let client = client.clone();
            let k = k.clone();
            let ctx = ctx.clone();
            thread::spawn(move || {
                let res = kv_pessimistic_lock_resumable(
                    &client,
                    ctx,
                    vec![k],
                    test_lock_ts,
                    test_lock_ts,
                    Some(1000),
                    return_values,
                    check_existence,
                );
                tx.send(()).unwrap();
                res
            })
        };
        // Blocked for lock waiting.
        rx.recv_timeout(Duration::from_millis(100)).unwrap_err();

        must_kv_commit(
            &client,
            ctx.clone(),
            vec![k.clone()],
            prewrite_start_ts,
            commit_ts,
            commit_ts,
        );
        rx.recv_timeout(Duration::from_millis(1000)).unwrap();
        let resp = handle.join().unwrap();
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert_eq!(resp.errors.len(), 0);
        assert_eq!(resp.get_results().len(), 1);
        let res = &resp.get_results()[0];
        if return_values {
            assert_eq!(
                res.get_type(),
                PessimisticLockKeyResultType::LockResultNormal
            );
            assert_eq!(res.get_value(), b"value");
            assert_eq!(res.get_existence(), true);
            assert_eq!(res.get_locked_with_conflict_ts(), 0);
        } else if check_existence {
            assert_eq!(
                res.get_type(),
                PessimisticLockKeyResultType::LockResultNormal
            );
            assert_eq!(res.get_value(), b"");
            assert_eq!(res.get_existence(), true);
            assert_eq!(res.get_locked_with_conflict_ts(), 0);
        } else {
            assert_eq!(
                res.get_type(),
                PessimisticLockKeyResultType::LockResultNormal
            );
            assert_eq!(res.get_value(), b"");
            assert_eq!(res.get_existence(), false);
            assert_eq!(res.get_locked_with_conflict_ts(), 0);
        }

        must_kv_pessimistic_rollback(&client, ctx.clone(), k.clone(), test_lock_ts, test_lock_ts);
    }

    for &(return_values, check_existence) in
        &[(false, false), (false, true), (true, false), (true, true)]
    {
        let test_lock_ts = curr_ts;
        let prewrite_start_ts = curr_ts + 10;
        let commit_ts = curr_ts + 11;
        curr_ts += 20;
        // Prewrite
        must_kv_prewrite(
            &client,
            ctx.clone(),
            vec![mutation.clone()],
            k.clone(),
            prewrite_start_ts,
        );

        let (tx, rx) = std::sync::mpsc::channel();
        let handle = {
            let client = client.clone();
            let k = k.clone();
            let ctx = ctx.clone();
            thread::spawn(move || {
                let res = kv_pessimistic_lock_resumable(
                    &client,
                    ctx,
                    vec![k],
                    test_lock_ts,
                    test_lock_ts,
                    Some(1000),
                    return_values,
                    check_existence,
                );
                tx.send(()).unwrap();
                res
            })
        };
        // Blocked for lock waiting.
        rx.recv_timeout(Duration::from_millis(100)).unwrap_err();
        must_kv_commit(
            &client,
            ctx.clone(),
            vec![k.clone()],
            prewrite_start_ts,
            commit_ts,
            commit_ts,
        );
        rx.recv_timeout(Duration::from_millis(1000)).unwrap();
        let resp = handle.join().unwrap();
        assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
        assert_eq!(resp.errors.len(), 0);
        assert_eq!(resp.get_results().len(), 1);
        assert_eq!(
            resp.get_results()[0].get_type(),
            PessimisticLockKeyResultType::LockResultLockedWithConflict
        );
        assert_eq!(resp.get_results()[0].get_value(), v);
        assert_eq!(resp.get_results()[0].get_existence(), true);
        assert_eq!(
            resp.get_results()[0].get_locked_with_conflict_ts(),
            commit_ts
        );

        must_kv_pessimistic_rollback(&client, ctx.clone(), k.clone(), test_lock_ts, commit_ts);
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_check_txn_status_with_max_ts() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let lock_ts = 10;

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), lock_ts);

    // Should return MinCommitTsPushed even if caller_start_ts is max.
    let status = must_check_txn_status(&client, ctx.clone(), &k, lock_ts, u64::MAX, lock_ts + 1);
    assert_eq!(status.lock_ttl, 3000);
    assert_eq!(status.action, Action::MinCommitTsPushed);

    // The min_commit_ts of k shouldn't be pushed.
    must_kv_commit(&client, ctx, vec![k], lock_ts, lock_ts + 1, lock_ts + 1);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_batch_commands() {
    let (_cluster, client, _ctx) = new_cluster();
    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            batch_req.mut_requests().push(Default::default());
            batch_req.mut_request_ids().push(i);
        }
        block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    }
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in block_on(
            receiver
                .map(move |b| b.unwrap().get_responses().len())
                .collect::<Vec<usize>>(),
        ) {
            count += x;
            if count == 10000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_health_feedback() {
    let (cluster, client, _ctx) = new_cluster();
    let store_id = *cluster.store_metas.iter().next().unwrap().0;
    assert_ne!(store_id, 0);

    let (mut sender, mut receiver) = client.batch_commands().unwrap();

    let mut batch_req = BatchCommandsRequest::default();
    batch_req.mut_requests().push(Default::default());
    batch_req.mut_request_ids().push(1);

    block_on(sender.send((batch_req.clone(), WriteFlags::default()))).unwrap();
    let resp = block_on(receiver.next()).unwrap().unwrap();
    assert!(resp.has_health_feedback());
    assert_eq!(resp.get_health_feedback().get_store_id(), store_id);

    block_on(sender.send((batch_req.clone(), WriteFlags::default()))).unwrap();
    let resp = block_on(receiver.next()).unwrap().unwrap();
    assert!(!resp.has_health_feedback());

    thread::sleep(Duration::from_millis(1100));
    block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    let resp = block_on(receiver.next()).unwrap().unwrap();
    assert!(resp.has_health_feedback());
    assert_eq!(resp.get_health_feedback().get_store_id(), store_id);

    block_on(sender.close()).unwrap();
    block_on(receiver.for_each(|_| future::ready(())));
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_explicit_get_health_feedback() {
    let (cluster, client, _ctx) = new_cluster();
    let store_id = *cluster.store_metas.iter().next().unwrap().0;
    assert_ne!(store_id, 0);

    let (mut sender, mut receiver) = client.batch_commands().unwrap();

    let mut batch_req = BatchCommandsRequest::default();
    batch_req.mut_request_ids().push(1);
    {
        let mut req = BatchCommandsRequestRequest::default();
        req.cmd = Some(BatchCommandsRequest_Request_oneof_cmd::GetHealthFeedback(
            Default::default(),
        ));
        batch_req.mut_requests().push(req);
    }

    block_on(sender.send((batch_req.clone(), WriteFlags::default()))).unwrap();
    let resp = block_on(receiver.next()).unwrap().unwrap();
    assert!(resp.has_health_feedback());
    assert_eq!(resp.get_health_feedback().get_store_id(), store_id);
    assert_eq!(resp.request_ids[0], 1);
    assert!(
        resp.get_responses()[0]
            .get_get_health_feedback()
            .has_health_feedback()
    );
    assert_eq!(
        resp.get_responses()[0]
            .get_get_health_feedback()
            .get_health_feedback(),
        resp.get_health_feedback()
    );
    let first_feedback_seq = resp.get_health_feedback().get_feedback_seq_no();

    batch_req.mut_request_ids()[0] = 2;
    block_on(sender.send((batch_req.clone(), WriteFlags::default()))).unwrap();
    let resp = block_on(receiver.next()).unwrap().unwrap();
    assert!(resp.has_health_feedback());
    assert_eq!(resp.get_health_feedback().get_store_id(), store_id);
    assert_eq!(resp.request_ids[0], 2);
    assert!(
        resp.get_responses()[0]
            .get_get_health_feedback()
            .has_health_feedback()
    );
    assert_eq!(
        resp.get_responses()[0]
            .get_get_health_feedback()
            .get_health_feedback(),
        resp.get_health_feedback()
    );
    assert_eq!(
        resp.get_health_feedback().get_feedback_seq_no(),
        first_feedback_seq + 1
    );

    block_on(sender.close()).unwrap();
    block_on(receiver.for_each(|_| future::ready(())));

    // Non-batched API
    let resp = client.get_health_feedback(&Default::default()).unwrap();
    assert!(resp.has_health_feedback());
    assert_eq!(resp.get_health_feedback().get_store_id(), store_id);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_empty_commands() {
    let (_cluster, client, _ctx) = new_cluster();
    let (mut sender, receiver) = client.batch_commands().unwrap();
    for _ in 0..1000 {
        let mut batch_req = BatchCommandsRequest::default();
        for i in 0..10 {
            let mut req = batch_commands_request::Request::default();
            req.cmd = Some(batch_commands_request::request::Cmd::Empty(
                Default::default(),
            ));
            batch_req.mut_requests().push(req);
            batch_req.mut_request_ids().push(i);
        }
        block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    }
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        // We have send 10k requests to the server, so we should get 10k responses.
        let mut count = 0;
        for x in block_on(
            receiver
                .map(move |b| b.unwrap().get_responses().len())
                .collect::<Vec<usize>>(),
        ) {
            count += x;
            if count == 10000 {
                tx.send(1).unwrap();
                return;
            }
        }
    });
    rx.recv_timeout(Duration::from_secs(5)).unwrap();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_async_commit_check_txn_status() {
    let (cluster, client, ctx) = new_cluster();

    let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"key".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"key".to_vec());
    mutation.set_value(b"value".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(start_ts.into_inner());
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    client.kv_prewrite(&req).unwrap();

    let mut req = CheckTxnStatusRequest::default();
    req.set_context(ctx);
    req.set_primary_key(b"key".to_vec());
    req.set_lock_ts(start_ts.into_inner());
    req.set_rollback_if_not_exist(true);
    let resp = client.kv_check_txn_status(&req).unwrap();
    assert_ne!(resp.get_action(), Action::MinCommitTsPushed);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_prewrite_check_max_commit_ts() {
    let (cluster, client, ctx) = new_cluster();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    cm.update_max_ts(100.into());

    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"k1".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k1".to_vec());
    mutation.set_value(b"v1".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(10);
    req.set_max_commit_ts(200);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    let resp = client.kv_prewrite(&req).unwrap();
    assert_eq!(resp.get_min_commit_ts(), 101);

    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(b"k2".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k2".to_vec());
    mutation.set_value(b"v2".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_min_commit_ts(21);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    // Test the idempotency of prewrite when falling back to 2PC.
    for _ in 0..2 {
        let resp = client.kv_prewrite(&req).unwrap();
        assert_eq!(resp.get_min_commit_ts(), 0);
        assert_eq!(resp.get_one_pc_commit_ts(), 0);
    }

    // 1PC
    let mut req = PrewriteRequest::default();
    req.set_context(ctx);
    req.set_primary_lock(b"k3".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k3".to_vec());
    mutation.set_value(b"v3".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_min_commit_ts(21);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    req.set_try_one_pc(true);
    // Test the idempotency of prewrite when falling back to 2PC.
    for _ in 0..2 {
        let resp = client.kv_prewrite(&req).unwrap();
        assert_eq!(resp.get_min_commit_ts(), 0);
        assert_eq!(resp.get_one_pc_commit_ts(), 0);
    }

    // There shouldn't be locks remaining in the lock table.
    cm.read_range_check(None, None, |_, _| Err(())).unwrap();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_txn_heart_beat() {
    let (_cluster, client, ctx) = new_cluster();
    let mut req = TxnHeartBeatRequest::default();
    let k = b"k".to_vec();
    let start_ts = 10;
    req.set_context(ctx);
    req.set_primary_lock(k.clone());
    req.set_start_version(start_ts);
    req.set_advise_lock_ttl(1000);
    let resp = client.kv_txn_heart_beat(&req).unwrap();
    assert!(!resp.has_region_error());
    assert_eq!(
        resp.get_error().get_txn_not_found(),
        &TxnNotFound {
            start_ts,
            primary_key: k,
            ..Default::default()
        }
    );
}

fn test_with_memory_lock_cluster(
    cm: ConcurrencyManager,
    client: TikvClient,
    f: impl FnOnce(TikvClient, /* raw_key */ Vec<u8>, Lock),
) {
    let raw_key = b"key".to_vec();
    let key = Key::from_raw(&raw_key);
    let guard = block_on(cm.lock_key(&key));
    let lock = Lock::new(
        LockType::Put,
        b"key".to_vec(),
        10.into(),
        20000,
        None,
        10.into(),
        1,
        20.into(),
        false,
    )
    .use_async_commit(vec![]);
    guard.with_lock(|l| {
        *l = Some(lock.clone());
    });
    f(client, raw_key, lock);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_batch_get_memory_lock() {
    let (cluster, client, ctx) = new_cluster();
    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    test_with_memory_lock_cluster(cm, client, |client, raw_key, lock| {
        let mut req = BatchGetRequest::default();
        req.set_context(ctx);
        req.set_keys(vec![b"unlocked".to_vec(), raw_key.clone()].into());
        req.version = 50;
        let resp = client.kv_batch_get(&req).unwrap();
        let lock_info = lock.into_lock_info(raw_key);
        assert_eq!(resp.pairs[0].get_error().get_locked(), &lock_info);
        assert_eq!(resp.get_error().get_locked(), &lock_info);
    });
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_kv_scan_memory_lock() {
    let (cluster, client, ctx) = new_cluster();
    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    test_with_memory_lock_cluster(cm, client, |client, raw_key, lock| {
        let mut req = ScanRequest::default();
        req.set_context(ctx);
        req.set_start_key(b"a".to_vec());
        req.version = 50;
        let resp = client.kv_scan(&req).unwrap();
        let lock_info = lock.into_lock_info(raw_key);
        assert_eq!(resp.pairs[0].get_error().get_locked(), &lock_info);
        assert_eq!(resp.get_error().get_locked(), &lock_info);
    });
}

macro_rules! test_func {
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $init:expr) => {{
        let mut req = $init;
        req.set_context($ctx.clone());

        // Not setting forwarding should lead to store not match.
        let resp = paste::paste! {
            $client.[<$func _opt>](&req, CallOption::default().timeout(Duration::from_secs(3))).unwrap()
        };
        let err = resp.get_region_error();
        assert!(err.has_store_not_match() || err.has_not_leader(), "{:?}", resp);

        // Proxy should redirect the request to the correct store.
        let resp = paste::paste! {
            $client.[<$func _opt>](&req, $call_opt.clone()).unwrap()
        };
        let err = resp.get_region_error();
        assert!(!err.has_store_not_match() && !err.has_not_leader(), "{:?}", resp);
    }};
}

macro_rules! test_func_init {
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident) => {{ test_func!($client, $ctx, $call_opt, $func, $req::default()) }};
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident,batch) => {{
        test_func!($client, $ctx, $call_opt, $func, {
            let mut req = $req::default();
            req.set_keys(vec![b"key".to_vec()].into());
            req
        })
    }};
    ($client:ident, $ctx:ident, $call_opt:ident, $func:ident, $req:ident, $op:expr) => {{
        test_func!($client, $ctx, $call_opt, $func, {
            let mut req = $req::default();
            let mut m = Mutation::default();
            m.set_op($op);
            m.key = b"key".to_vec();
            req.mut_mutations().push(m);
            req
        })
    }};
}

/// Check all supported requests can go through proxy correctly.
#[test_case(test_raftstore::setup_cluster)]
#[test_case(test_raftstore_v2::setup_cluster)]
fn test_tikv_forwarding() {
    let (_cluster, client, leader_addr, ctx) = new_cluster();
    let call_opt = server::build_forward_option(&leader_addr).timeout(Duration::from_secs(3));
    // Verify not setting forwarding header will result in store not match.
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(
        put_resp.get_region_error().has_store_not_match(),
        "{:?}",
        put_resp
    );
    assert!(put_resp.error.is_empty(), "{:?}", put_resp);

    test_func_init!(client, ctx, call_opt, kv_get, GetRequest);
    test_func_init!(client, ctx, call_opt, kv_scan, ScanRequest);
    test_func_init!(client, ctx, call_opt, kv_prewrite, PrewriteRequest, Op::Put);
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_pessimistic_lock,
        PessimisticLockRequest,
        Op::PessimisticLock
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_pessimistic_rollback,
        PessimisticRollbackRequest,
        batch
    );
    test_func_init!(client, ctx, call_opt, kv_commit, CommitRequest, batch);
    test_func_init!(client, ctx, call_opt, kv_cleanup, CleanupRequest);
    test_func_init!(client, ctx, call_opt, kv_batch_get, BatchGetRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_batch_rollback,
        BatchRollbackRequest,
        batch
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_txn_heart_beat,
        TxnHeartBeatRequest
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_check_txn_status,
        CheckTxnStatusRequest
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        kv_check_secondary_locks,
        CheckSecondaryLocksRequest,
        batch
    );
    test_func_init!(client, ctx, call_opt, kv_scan_lock, ScanLockRequest);
    test_func_init!(client, ctx, call_opt, kv_resolve_lock, ResolveLockRequest);
    test_func_init!(client, ctx, call_opt, kv_delete_range, DeleteRangeRequest);
    test_func_init!(client, ctx, call_opt, mvcc_get_by_key, MvccGetByKeyRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        mvcc_get_by_start_ts,
        MvccGetByStartTsRequest
    );
    test_func_init!(client, ctx, call_opt, raw_get, RawGetRequest);
    test_func_init!(client, ctx, call_opt, raw_batch_get, RawBatchGetRequest);
    test_func_init!(client, ctx, call_opt, raw_scan, RawScanRequest);
    test_func_init!(client, ctx, call_opt, raw_batch_scan, RawBatchScanRequest);
    test_func_init!(client, ctx, call_opt, raw_put, RawPutRequest);
    test_func!(client, ctx, call_opt, raw_batch_put, {
        let mut req = RawBatchPutRequest::default();
        req.set_pairs(vec![KvPair::default()].into());
        req
    });
    test_func_init!(client, ctx, call_opt, raw_delete, RawDeleteRequest);
    test_func_init!(
        client,
        ctx,
        call_opt,
        raw_batch_delete,
        RawBatchDeleteRequest,
        batch
    );
    test_func_init!(
        client,
        ctx,
        call_opt,
        raw_delete_range,
        RawDeleteRangeRequest
    );
    test_func!(client, ctx, call_opt, coprocessor, {
        let mut req = Request::default();
        req.set_tp(REQ_TYPE_DAG);
        req
    });
    test_func!(client, ctx, call_opt, split_region, {
        let mut req = SplitRegionRequest::default();
        req.set_split_key(b"k1".to_vec());
        req
    });

    // Test if duplex can be redirect correctly.
    let cases = vec![
        (CallOption::default().timeout(Duration::from_secs(3)), false),
        (call_opt, true),
    ];
    for (opt, success) in cases {
        let (mut sender, receiver) = client.batch_commands_opt(opt).unwrap();
        for _ in 0..100 {
            let mut batch_req = BatchCommandsRequest::default();
            for i in 0..10 {
                let mut get = GetRequest::default();
                get.set_context(ctx.clone());
                let mut req = batch_commands_request::Request::default();
                req.cmd = Some(batch_commands_request::request::Cmd::Get(get));
                batch_req.mut_requests().push(req);
                batch_req.mut_request_ids().push(i);
            }
            block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
        }
        block_on(sender.close()).unwrap();

        // We have send 1k requests to the server, so we should get 1k responses.
        let resps = block_on(
            receiver
                .map(move |b| futures::stream::iter(b.unwrap().take_responses().into_vec()))
                .flatten()
                .collect::<Vec<_>>(),
        );
        assert_eq!(resps.len(), 1000);
        for resp in resps {
            let resp = match resp.cmd {
                Some(batch_commands_response::response::Cmd::Get(g)) => g,
                _ => panic!("unexpected response {:?}", resp),
            };
            let error = resp.get_region_error();
            if success {
                assert!(!error.has_store_not_match(), "{:?}", resp);
            } else {
                assert!(error.has_store_not_match(), "{:?}", resp);
            }
        }
    }
}

/// Test if forwarding works correctly if the target node is shutdown and
/// restarted.
#[test_case(test_raftstore::setup_cluster)]
#[test_case(test_raftstore_v2::setup_cluster)]
fn test_forwarding_reconnect() {
    let (mut cluster, client, leader_addr, ctx) = new_cluster();
    let call_opt = server::build_forward_option(&leader_addr).timeout(Duration::from_secs(3));
    let leader = cluster.leader_of_region(1).unwrap();
    cluster.stop_node(leader.get_store_id());

    let mut req = RawGetRequest::default();
    req.set_context(ctx);
    // Large timeout value to ensure the error is from proxy instead of client.
    let timer = tikv_util::time::Instant::now();
    let timeout = Duration::from_secs(5);
    let res = client.raw_get_opt(&req, call_opt.clone().timeout(timeout));
    let elapsed = timer.saturating_elapsed();
    assert!(elapsed < timeout, "{:?}", elapsed);
    // Because leader server is shutdown, reconnecting has to be timeout.
    match res {
        Err(grpcio::Error::RpcFailure(s)) => assert_eq!(s.code(), RpcStatusCode::CANCELLED),
        _ => panic!("unexpected result {:?}", res),
    }

    cluster.run_node(leader.get_store_id()).unwrap();
    let resp = client.raw_get_opt(&req, call_opt).unwrap();
    assert!(!resp.get_region_error().has_store_not_match(), "{:?}", resp);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_health_check() {
    let (mut cluster, _client, _ctx) = new_cluster();
    let addr = cluster.sim.rl().get_addr(1);

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = HealthClient::new(channel);
    let req = HealthCheckRequest {
        service: "".to_string(),
        ..Default::default()
    };
    let resp = client.check(&req).unwrap();
    assert_eq!(ServingStatus::Serving, resp.status);

    cluster.shutdown();
    client.check(&req).unwrap_err();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_get_lock_wait_info_api() {
    let (_cluster, client, ctx) = new_cluster();
    let client2 = client.clone();

    let mut ctx1 = ctx.clone();
    ctx1.set_resource_group_tag(b"resource_group_tag1".to_vec());
    kv_pessimistic_lock_with_ttl(&client, ctx1, vec![b"a".to_vec()], 20, 20, false, 5000);
    let mut ctx2 = ctx.clone();
    let handle = thread::spawn(move || {
        ctx2.set_resource_group_tag(b"resource_group_tag2".to_vec());
        kv_pessimistic_lock_with_ttl(&client2, ctx2, vec![b"a".to_vec()], 30, 30, false, 5000);
    });

    let mut entries = None;
    for _retry in 0..200 {
        thread::sleep(Duration::from_millis(25));
        // The lock should be in waiting state here.
        let req = GetLockWaitInfoRequest::default();
        let resp = client.get_lock_wait_info(&req).unwrap();
        if resp.entries.len() != 0 {
            entries = Some(resp.entries.to_vec());
            break;
        }
    }

    let entries = entries.unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].txn, 30);
    assert_eq!(entries[0].wait_for_txn, 20);
    assert_eq!(entries[0].key, b"a".to_vec());
    assert_eq!(
        entries[0].resource_group_tag,
        b"resource_group_tag2".to_vec()
    );
    must_kv_pessimistic_rollback(&client, ctx, b"a".to_vec(), 20, 20);
    handle.join().unwrap();
}

// Test API version verification for transaction requests.
// See the following for detail:
//   * rfc: https://github.com/tikv/rfcs/blob/master/text/0069-api-v2.md.
//   * proto: https://github.com/pingcap/kvproto/blob/master/proto/kvrpcpb.proto,
//     enum APIVersion.
#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_txn_api_version() {
    const TIDB_KEY_CASE: &[u8] = b"t_a";
    const TXN_KEY_CASE: &[u8] = b"x\0a";
    const RAW_KEY_CASE: &[u8] = b"r\0a";

    let test_data = vec![
        // config api_version = V1|V1ttl, for backward compatible.
        (ApiVersion::V1, ApiVersion::V1, TIDB_KEY_CASE, None),
        (ApiVersion::V1, ApiVersion::V1, TXN_KEY_CASE, None),
        (ApiVersion::V1, ApiVersion::V1, RAW_KEY_CASE, None),
        // storage api_version = V1ttl, allow RawKV request only. Any key cases will be rejected.
        (
            ApiVersion::V1ttl,
            ApiVersion::V1,
            TXN_KEY_CASE,
            Some("ApiVersionNotMatched"),
        ),
        // config api_version = V1, reject all V2 requests.
        (
            ApiVersion::V1,
            ApiVersion::V2,
            TIDB_KEY_CASE,
            Some("ApiVersionNotMatched"),
        ),
        // config api_version = V2.
        // backward compatible for TiDB request, and TiDB request only.
        (ApiVersion::V2, ApiVersion::V1, TIDB_KEY_CASE, None),
        (
            ApiVersion::V2,
            ApiVersion::V1,
            TXN_KEY_CASE,
            Some("InvalidKeyMode"),
        ),
        (
            ApiVersion::V2,
            ApiVersion::V1,
            RAW_KEY_CASE,
            Some("InvalidKeyMode"),
        ),
        // V2 api validation.
        (ApiVersion::V2, ApiVersion::V2, TXN_KEY_CASE, None),
        (
            ApiVersion::V2,
            ApiVersion::V2,
            RAW_KEY_CASE,
            Some("InvalidKeyMode"),
        ),
        (
            ApiVersion::V2,
            ApiVersion::V2,
            TIDB_KEY_CASE,
            Some("InvalidKeyMode"),
        ),
    ];

    for (i, (storage_api_version, req_api_version, key, errcode)) in
        test_data.into_iter().enumerate()
    {
        let (cluster, leader, mut ctx) =
            new_cluster(|cluster| cluster.cfg.storage.set_api_version(storage_api_version));
        let env = Arc::new(Environment::new(1));
        let channel =
            ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
        let client = TikvClient::new(channel);

        ctx.set_api_version(req_api_version);

        let (k, v) = (key.to_vec(), b"value".to_vec());
        let mut ts = 0;

        if let Some(errcode) = errcode {
            let expect_err = |errs: &[KeyError]| {
                let expect_prefix = format!("Error({}", errcode);
                assert!(!errs.is_empty(), "case {}", i);
                assert!(
                    errs[0].get_abort().starts_with(&expect_prefix), /* e.g. Error(ApiVersionNotMatched { storage_api_version: V1, req_api_version: V2 }) */
                    "case {}: errs[0]: {:?}, expected: {}",
                    i,
                    errs[0],
                    expect_prefix,
                );
            };

            // Prewrite
            ts += 1;
            let prewrite_start_version = ts;
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(k.clone());
            mutation.set_value(v.clone());
            let res = try_kv_prewrite(
                &client,
                ctx.clone(),
                vec![mutation],
                k.clone(),
                prewrite_start_version,
            );
            expect_err(res.get_errors());

            // Prewrite Pessimistic
            ts += 1;
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(k.clone());
            mutation.set_value(v.clone());
            let res =
                try_kv_prewrite_pessimistic(&client, ctx.clone(), vec![mutation], k.clone(), ts);
            expect_err(res.get_errors());

            // Pessimistic Lock
            ts += 1;
            let resp = kv_pessimistic_lock(&client, ctx.clone(), vec![k.clone()], ts, ts, false);
            assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
            assert_eq!(resp.errors.len(), 1);
            assert!(!resp.errors[0].has_locked(), "{:?}", resp.get_errors());
            expect_err(resp.get_errors());
        } else {
            {
                // Prewrite
                ts += 1;
                let prewrite_start_version = ts;
                let mut mutation = Mutation::default();
                mutation.set_op(Op::Put);
                mutation.set_key(k.clone());
                mutation.set_value(v.clone());
                must_kv_prewrite(
                    &client,
                    ctx.clone(),
                    vec![mutation],
                    k.clone(),
                    prewrite_start_version,
                );

                // Pessimistic Lock
                ts += 1;
                let lock_ts = ts;
                let resp = kv_pessimistic_lock(
                    &client,
                    ctx.clone(),
                    vec![k.clone()],
                    lock_ts,
                    lock_ts,
                    false,
                );
                assert!(!resp.has_region_error(), "{:?}", resp.get_region_error());
                assert_eq!(resp.errors.len(), 1);
                assert!(resp.errors[0].has_locked());
                assert!(resp.values.is_empty());
                assert!(resp.not_founds.is_empty());

                // Commit
                ts += 1;
                let commit_version = ts;
                must_kv_commit(
                    &client,
                    ctx.clone(),
                    vec![k.clone()],
                    prewrite_start_version,
                    commit_version,
                    commit_version,
                );

                // Get
                ts += 1;
                let get_version = ts;
                let mut get_req = GetRequest::default();
                get_req.set_context(ctx.clone());
                get_req.key = k.clone();
                get_req.version = get_version;
                let get_resp = client.kv_get(&get_req).unwrap();
                assert!(!get_resp.has_region_error());
                assert!(!get_resp.has_error());
                assert!(get_resp.get_exec_details_v2().has_time_detail());
                assert!(get_resp.get_exec_details_v2().has_time_detail_v2());
            }
            {
                // Pessimistic Lock
                ts += 1;
                let lock_ts = ts;
                must_kv_pessimistic_lock(&client, ctx.clone(), k.clone(), lock_ts);

                // Prewrite Pessimistic
                let mut mutation = Mutation::default();
                mutation.set_op(Op::Put);
                mutation.set_key(k.clone());
                mutation.set_value(v.clone());
                must_kv_prewrite_pessimistic(
                    &client,
                    ctx.clone(),
                    vec![mutation],
                    k.clone(),
                    lock_ts,
                );
            }
        }
    }
}

#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_storage_with_quota_limiter_enable() {
    let (cluster, leader, ctx) = new_cluster(|cluster| {
        // write_bandwidth is limited to 1, which means that every write request will
        // trigger the limit.
        let quota_config = QuotaConfig {
            foreground_cpu_time: 2000,
            foreground_write_bandwidth: ReadableSize(10),
            ..Default::default()
        };
        cluster.cfg.quota = quota_config;
        cluster.cfg.storage.scheduler_worker_pool_size = 1;
    });

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut ts = 0;
    let begin = Instant::now();

    // Prewrite
    ts += 1;
    let prewrite_start_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(&client, ctx, vec![mutation], k, prewrite_start_version);

    // 500 only represents quota enabled, no specific significance
    assert!(begin.elapsed() > Duration::from_millis(500));
}

#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_storage_with_quota_limiter_disable() {
    let (cluster, leader, ctx) = new_cluster(|cluster| {
        // all limit set to 0, which means quota limiter not work.
        let quota_config = QuotaConfig::default();
        cluster.cfg.quota = quota_config;
        cluster.cfg.storage.scheduler_worker_pool_size = 1;
    });

    let env = Arc::new(Environment::new(1));
    let leader_store = leader.get_store_id();
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store));
    let client = TikvClient::new(channel);

    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut ts = 0;
    let begin = Instant::now();

    // Prewrite
    ts += 1;
    let prewrite_start_version = ts;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(&client, ctx, vec![mutation], k, prewrite_start_version);

    assert!(begin.elapsed() < Duration::from_millis(500));
}

#[test_case(test_raftstore::must_new_and_configure_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster_and_kv_client)]
fn test_commands_write_detail() {
    let (cluster, client, ctx) = new_cluster(|cluster| {
        cluster.cfg.pessimistic_txn.pipelined = false;
        cluster.cfg.pessimistic_txn.in_memory = false;
    });
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let check_scan_detail = |sc: &ScanDetailV2| {
        assert!(sc.get_get_snapshot_nanos() > 0);
    };
    let check_write_detail = |wd: &WriteDetail| {
        assert!(wd.get_persist_log_nanos() > 0);
        assert!(wd.get_raft_db_write_leader_wait_nanos() > 0);
        assert!(wd.get_raft_db_sync_log_nanos() > 0);
        assert!(wd.get_raft_db_write_memtable_nanos() > 0);
        assert!(wd.get_commit_log_nanos() > 0);
        assert!(wd.get_apply_batch_wait_nanos() > 0);
        assert!(wd.get_apply_log_nanos() > 0);
        // Mutex has been removed from write path.
        // Ref https://github.com/facebook/rocksdb/pull/7516
        // assert!(wd.get_apply_mutex_lock_nanos() > 0);

        // MultiRocksDB does not have wal
        if cluster.cfg.storage.engine == EngineType::RaftKv {
            assert!(wd.get_apply_write_wal_nanos() > 0);
        }
        assert!(wd.get_apply_write_memtable_nanos() > 0);
        assert!(wd.get_process_nanos() > 0);
    };

    let mut mutation = Mutation::default();
    mutation.set_op(Op::PessimisticLock);
    mutation.set_key(k.clone());

    let mut pessimistic_lock_req = PessimisticLockRequest::default();
    pessimistic_lock_req.set_context(ctx.clone());
    pessimistic_lock_req.set_mutations(vec![mutation.clone()].into());
    pessimistic_lock_req.set_start_version(20);
    pessimistic_lock_req.set_for_update_ts(20);
    pessimistic_lock_req.set_primary_lock(k.clone());
    pessimistic_lock_req.set_lock_ttl(3000);
    let pessimistic_lock_resp = client.kv_pessimistic_lock(&pessimistic_lock_req).unwrap();
    check_scan_detail(
        pessimistic_lock_resp
            .get_exec_details_v2()
            .get_scan_detail_v2(),
    );
    check_write_detail(
        pessimistic_lock_resp
            .get_exec_details_v2()
            .get_write_detail(),
    );

    let mut prewrite_req = PrewriteRequest::default();
    mutation.set_op(Op::Put);
    mutation.set_value(v);
    prewrite_req.set_mutations(vec![mutation].into());
    prewrite_req.set_pessimistic_actions(vec![DoPessimisticCheck]);
    prewrite_req.set_context(ctx.clone());
    prewrite_req.set_primary_lock(k.clone());
    prewrite_req.set_start_version(20);
    prewrite_req.set_for_update_ts(20);
    prewrite_req.set_lock_ttl(3000);
    let prewrite_resp = client.kv_prewrite(&prewrite_req).unwrap();
    check_scan_detail(prewrite_resp.get_exec_details_v2().get_scan_detail_v2());
    check_write_detail(prewrite_resp.get_exec_details_v2().get_write_detail());

    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx.clone());
    commit_req.set_keys(vec![k.clone()].into());
    commit_req.set_start_version(20);
    commit_req.set_commit_version(30);
    let commit_resp = client.kv_commit(&commit_req).unwrap();
    check_scan_detail(commit_resp.get_exec_details_v2().get_scan_detail_v2());
    check_write_detail(commit_resp.get_exec_details_v2().get_write_detail());

    let mut txn_heartbeat_req = TxnHeartBeatRequest::default();
    txn_heartbeat_req.set_context(ctx.clone());
    txn_heartbeat_req.set_primary_lock(k.clone());
    txn_heartbeat_req.set_start_version(20);
    txn_heartbeat_req.set_advise_lock_ttl(1000);
    let txn_heartbeat_resp = client.kv_txn_heart_beat(&txn_heartbeat_req).unwrap();
    check_scan_detail(
        txn_heartbeat_resp
            .get_exec_details_v2()
            .get_scan_detail_v2(),
    );
    assert!(
        txn_heartbeat_resp
            .get_exec_details_v2()
            .get_write_detail()
            .get_process_nanos()
            > 0
    );

    let mut check_txn_status_req = CheckTxnStatusRequest::default();
    check_txn_status_req.set_context(ctx);
    check_txn_status_req.set_primary_key(k);
    check_txn_status_req.set_lock_ts(20);
    check_txn_status_req.set_rollback_if_not_exist(true);
    let check_txn_status_resp = client.kv_check_txn_status(&check_txn_status_req).unwrap();
    check_scan_detail(
        check_txn_status_resp
            .get_exec_details_v2()
            .get_scan_detail_v2(),
    );
    assert!(
        check_txn_status_resp
            .get_exec_details_v2()
            .get_write_detail()
            .get_process_nanos()
            > 0
    );
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_rpc_wall_time() {
    let (_cluster, client, ctx) = new_cluster();
    let k = b"key".to_vec();
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.key = k;
    get_req.version = 10;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(
        get_resp
            .get_exec_details_v2()
            .get_time_detail_v2()
            .get_total_rpc_wall_time_ns()
            > 0
    );
    assert_eq!(
        get_resp
            .get_exec_details_v2()
            .get_time_detail_v2()
            .get_total_rpc_wall_time_ns(),
        get_resp
            .get_exec_details_v2()
            .get_time_detail()
            .get_total_rpc_wall_time_ns()
    );

    let (mut sender, receiver) = client.batch_commands().unwrap();
    let mut batch_req = BatchCommandsRequest::default();
    for i in 0..3 {
        let mut req = batch_commands_request::Request::default();
        req.cmd = Some(batch_commands_request::request::Cmd::Get(get_req.clone()));
        batch_req.mut_requests().push(req);
        batch_req.mut_request_ids().push(i);
    }
    block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
    block_on(sender.close()).unwrap();

    let (tx, rx) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut responses = Vec::new();
        for r in block_on(
            receiver
                .map(move |b| b.unwrap().take_responses())
                .collect::<Vec<_>>(),
        ) {
            responses.extend(r.into_vec());
        }
        tx.send(responses).unwrap();
    });
    let responses = rx.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(responses.len(), 3);
    for resp in responses {
        assert!(
            resp.get_get()
                .get_exec_details_v2()
                .get_time_detail_v2()
                .get_total_rpc_wall_time_ns()
                > 0
        );
        assert!(
            resp.get_get()
                .get_exec_details_v2()
                .get_time_detail_v2()
                .get_kv_grpc_process_time_ns()
                > 0
        );
        assert!(
            resp.get_get()
                .get_exec_details_v2()
                .get_time_detail_v2()
                .get_kv_grpc_wait_time_ns()
                > 0
        );
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pessimistic_lock_execution_tracking() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"k1".to_vec(), b"k2".to_vec());

    // Add a prewrite lock.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), 10);

    let block_duration = Duration::from_millis(300);
    let client_clone = client.clone();
    let ctx_clone = ctx.clone();
    let k_clone = k.clone();
    let handle = thread::spawn(move || {
        thread::sleep(block_duration);
        must_kv_commit(&client_clone, ctx_clone, vec![k_clone], 10, 30, 30);
    });

    let resp = kv_pessimistic_lock(&client, ctx, vec![k], 20, 20, false);
    assert!(
        resp.get_exec_details_v2()
            .get_write_detail()
            .get_pessimistic_lock_wait_nanos()
            > 0,
        "resp lock wait time={:?}, block_duration={:?}",
        resp.get_exec_details_v2()
            .get_write_detail()
            .get_pessimistic_lock_wait_nanos(),
        block_duration
    );

    handle.join().unwrap();
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_mvcc_scan_memory_and_cf_locks() {
    let (cluster, client, ctx) = new_cluster();

    // Create both pessimistic and prewrite locks.
    // The peer in memory limit is 512KiB, generate 1KiB key for pessimistic lock.
    // So Writing 512 pessimistic locks may exceed the memory limit and later
    // pessimistic locks would be written to the lock cf.
    let byte_slice: &[u8] = &[b'k'; 512];
    let start_ts = 11;
    let prewrite_start_ts = start_ts - 1;
    let num_keys = 1040;
    let prewrite_primary_key = b"prewrite_primary";
    let val = b"value";
    let format_key = |i| format!("{:?}{:04}", byte_slice, i).as_bytes().to_vec();
    for i in 0..num_keys {
        let key = format_key(i);
        if i % 2 == 0 {
            must_kv_pessimistic_lock(&client, ctx.clone(), key, start_ts);
        } else {
            let mut mutation = Mutation::default();
            mutation.set_op(Op::Put);
            mutation.set_key(key);
            mutation.set_value(val.to_vec());
            must_kv_prewrite_with(
                &client,
                ctx.clone(),
                vec![mutation],
                vec![],
                prewrite_primary_key.to_vec(),
                start_ts - 1,
                0,
                false,
                false,
            );
        }
    }
    // Ensure the pessimistic locks are written to the memory. The first key should
    // be written into the memory and the last key should be put to lock cf as
    // memory limit is exceeded.
    let engine = cluster.get_engine(1);
    let cf_res = engine
        .get_value_cf(
            CF_LOCK,
            keys::data_key(Key::from_raw(format_key(0).as_slice()).as_encoded()).as_slice(),
        )
        .unwrap();
    assert!(cf_res.is_none());
    let cf_res = engine
        .get_value_cf(
            CF_LOCK,
            keys::data_key(Key::from_raw(format_key(num_keys - 2).as_slice()).as_encoded())
                .as_slice(),
        )
        .unwrap();
    assert!(cf_res.is_some());

    // Scan lock, the pessimistic and prewrite results are returned.
    // When limit is 0 or it's larger than num_keys, all keys should be returned.
    // When limit is less than 512, in-memory pessimistic locks and prewrite locks
    // should be returned.
    // When limit is larger than 512, in-memory and lock cf pessimistic locks and
    // prewrite locks should be returned.
    for scan_limit in [0, 128, 256, 512, num_keys, num_keys * 2] {
        let scan_ts = 20;
        let scan_lock_max_version = scan_ts;
        let mut scan_lock_req = ScanLockRequest::default();
        scan_lock_req.set_context(ctx.clone());
        scan_lock_req.max_version = scan_lock_max_version;
        scan_lock_req.limit = scan_limit as u32;
        let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
        assert!(!scan_lock_resp.has_region_error());
        let expected_key_num = if scan_limit == 0 || scan_limit >= num_keys {
            num_keys
        } else {
            scan_limit
        };
        assert_eq!(scan_lock_resp.locks.len(), expected_key_num);

        for (i, lock_info) in (0..expected_key_num).zip(scan_lock_resp.locks.iter()) {
            let key = format_key(i);
            if i % 2 == 0 {
                assert_eq!(lock_info.lock_type, Op::PessimisticLock);
                assert_eq!(lock_info.lock_version, start_ts);
                assert_eq!(lock_info.key, key);
            } else {
                assert_eq!(
                    lock_info.lock_type,
                    Op::Put,
                    "i={:?} lock_info={:?} expected_key_num={:?}, scan_limit={:?}",
                    i,
                    lock_info,
                    expected_key_num,
                    scan_limit
                );
                assert_eq!(lock_info.primary_lock, prewrite_primary_key);
                assert_eq!(lock_info.lock_version, prewrite_start_ts);
                assert_eq!(lock_info.key, key);
            }
        }
    }

    // Scan with smaller ts returns empty result.
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx.clone());
    scan_lock_req.max_version = prewrite_start_ts - 1;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_region_error());
    assert_eq!(scan_lock_resp.locks.len(), 0);

    // Roll back the prewrite locks.
    let rollback_start_version = prewrite_start_ts;
    let mut rollback_req = BatchRollbackRequest::default();
    rollback_req.set_context(ctx.clone());
    rollback_req.start_version = rollback_start_version;
    let keys = (0..num_keys)
        .filter(|i| i % 2 != 0)
        .map(|i| format_key(i))
        .collect();
    rollback_req.set_keys(keys);
    let rollback_resp = client.kv_batch_rollback(&rollback_req).unwrap();
    assert!(!rollback_resp.has_region_error());
    assert!(!rollback_resp.has_error());

    // Scan lock again after removing prewrite locks.
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx.clone());
    scan_lock_req.max_version = start_ts + 1;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_region_error());
    assert_eq!(scan_lock_resp.locks.len(), num_keys / 2);
    for (i, lock_info) in (0..num_keys / 2).zip(scan_lock_resp.locks.iter()) {
        let key = format_key(i * 2);
        assert_eq!(lock_info.lock_version, start_ts);
        assert_eq!(lock_info.key, key);
        assert_eq!(lock_info.lock_type, Op::PessimisticLock);
    }

    // Pessimistic rollabck all the locks. Scan lock should return empty result.
    let mut pessimsitic_rollback_req = PessimisticRollbackRequest::default();
    pessimsitic_rollback_req.start_version = start_ts;
    pessimsitic_rollback_req.for_update_ts = start_ts;
    pessimsitic_rollback_req.set_context(ctx.clone());
    let keys = (0..num_keys)
        .filter(|i| i % 2 == 0)
        .map(|i| format_key(i))
        .collect();
    pessimsitic_rollback_req.set_keys(keys);
    let pessimistic_rollback_resp = client
        .kv_pessimistic_rollback(&pessimsitic_rollback_req)
        .unwrap();
    assert!(!pessimistic_rollback_resp.has_region_error());

    // Scan lock again after all the cleanup.
    let mut scan_lock_req = ScanLockRequest::default();
    scan_lock_req.set_context(ctx);
    scan_lock_req.max_version = start_ts + 1;
    let scan_lock_resp = client.kv_scan_lock(&scan_lock_req).unwrap();
    assert!(!scan_lock_resp.has_region_error());
    assert_eq!(scan_lock_resp.locks.len(), 0);
}

#[test_case(test_raftstore::must_new_and_configure_cluster)]
#[test_case(test_raftstore_v2::must_new_and_configure_cluster)]
fn test_pessimistic_rollback_with_read_first() {
    for enable_in_memory_lock in [true, false] {
        let (cluster, leader, ctx) = new_cluster(|cluster| {
            cluster.cfg.pessimistic_txn.pipelined = enable_in_memory_lock;
            cluster.cfg.pessimistic_txn.in_memory = enable_in_memory_lock;

            // Disable region split.
            const MAX_REGION_SIZE: u64 = 1024;
            const MAX_SPLIT_KEY: u64 = 1 << 31;
            cluster.cfg.coprocessor.region_max_size = Some(ReadableSize::gb(MAX_REGION_SIZE));
            cluster.cfg.coprocessor.region_split_size = Some(ReadableSize::gb(MAX_REGION_SIZE));
            cluster.cfg.coprocessor.region_max_keys = Some(MAX_SPLIT_KEY);
            cluster.cfg.coprocessor.region_split_keys = Some(MAX_SPLIT_KEY);
        });
        let env = Arc::new(Environment::new(1));
        let leader_store_id = leader.get_store_id();
        let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader_store_id));
        let client = TikvClient::new(channel);

        let format_key = |prefix: char, i: usize| format!("{}{:04}", prefix, i).as_bytes().to_vec();
        let (k1, k2, k3) = (format_key('k', 1), format_key('k', 2), format_key('k', 3));

        // Basic case, two keys could be rolled back within one pessimistic rollback
        // request.
        let start_ts = 10;
        must_kv_pessimistic_lock(&client, ctx.clone(), k1.clone(), start_ts);
        must_kv_pessimistic_lock(&client, ctx.clone(), k2, start_ts);
        must_lock_cnt(
            &client,
            ctx.clone(),
            start_ts + 10,
            k1.as_slice(),
            k3.as_slice(),
            Op::PessimisticLock,
            2,
            100,
        );
        must_kv_pessimistic_rollback_with_scan_first(&client, ctx.clone(), start_ts, start_ts);
        must_lock_cnt(
            &client,
            ctx.clone(),
            start_ts + 10,
            k1.as_slice(),
            k3.as_slice(),
            Op::PessimisticLock,
            0,
            100,
        );

        // Acquire pessimistic locks for more than 256(RESOLVE_LOCK_BATCH_SIZE) keys.
        let start_ts = 11;
        let num_keys = 1000;
        let prewrite_primary_key = format_key('k', 1);
        let val = b"value";
        for i in 0..num_keys {
            let key = format_key('k', i);
            if i % 2 == 0 {
                must_kv_pessimistic_lock(&client, ctx.clone(), key, start_ts);
            } else {
                let mut mutation = Mutation::default();
                mutation.set_op(Op::Put);
                mutation.set_key(key);
                mutation.set_value(val.to_vec());
                must_kv_prewrite(
                    &client,
                    ctx.clone(),
                    vec![mutation],
                    prewrite_primary_key.clone(),
                    start_ts,
                );
            }
        }

        // Pessimistic roll back one key.
        must_kv_pessimistic_rollback(&client, ctx.clone(), format_key('k', 0), start_ts, start_ts);
        must_lock_cnt(
            &client,
            ctx.clone(),
            start_ts + 10,
            format_key('k', 0).as_slice(),
            format_key('k', num_keys + 1).as_slice(),
            Op::PessimisticLock,
            num_keys / 2 - 1,
            0,
        );

        // All the pessimistic locks belonging to the same transaction are pessimistic
        // rolled back within one request.
        must_kv_pessimistic_rollback_with_scan_first(&client, ctx.clone(), start_ts, start_ts);
        must_lock_cnt(
            &client,
            ctx.clone(),
            start_ts + 10,
            format_key('k', 0).as_slice(),
            format_key('k', num_keys + 1).as_slice(),
            Op::PessimisticLock,
            0,
            0,
        );
        must_lock_cnt(
            &client,
            ctx,
            start_ts + 10,
            format_key('k', 0).as_slice(),
            format_key('k', num_keys + 1).as_slice(),
            Op::Put,
            num_keys / 2,
            0,
        );
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pipelined_dml_flush() {
    let (_cluster, client, ctx) = new_cluster();
    // k1 is put
    let (k1, v) = (b"key".to_vec(), b"value".to_vec());
    // k2 is deletion
    let k2 = b"key2".to_vec();
    // k3 is not touched
    let k3 = b"key3".to_vec();
    let pk = b"primary".to_vec();
    let mut flush_req = FlushRequest::default();
    flush_req.set_mutations(
        vec![
            Mutation {
                op: Op::Put,
                key: pk.clone(),
                value: v.clone(),
                ..Default::default()
            },
            Mutation {
                op: Op::Put,
                key: k1.clone(),
                value: v.clone(),
                ..Default::default()
            },
            Mutation {
                op: Op::Del,
                key: k2.clone(),
                value: vec![],
                ..Default::default()
            },
        ]
        .into(),
    );
    flush_req.set_context(ctx.clone());
    flush_req.set_start_ts(1);
    flush_req.set_primary_key(pk.clone());
    flush_req.set_generation(1);
    let flush_resp = client.kv_flush(&flush_req).unwrap();
    assert!(!flush_resp.has_region_error());
    assert!(flush_resp.get_errors().is_empty());

    let mut batch_get_req = BufferBatchGetRequest::default();
    batch_get_req.set_context(ctx.clone());
    batch_get_req.set_keys(vec![k1.clone(), k2.clone(), k3.clone()].into());
    batch_get_req.set_version(1);
    let batch_get_resp = client.kv_buffer_batch_get(&batch_get_req).unwrap();
    assert!(!batch_get_resp.has_region_error());
    let pairs = batch_get_resp.get_pairs();
    assert_eq!(pairs.len(), 2);
    assert!(!pairs[0].has_error());
    assert_eq!(pairs[0].get_key(), k1.as_slice());
    assert_eq!(pairs[0].get_value(), v.as_slice());
    assert_eq!(pairs[1].get_key(), k2.as_slice());
    assert!(pairs[1].get_value().is_empty());

    let mut commit_req = CommitRequest::default();
    commit_req.set_context(ctx.clone());
    commit_req.set_start_version(1);
    commit_req.set_commit_version(2);
    commit_req.set_keys(vec![pk.clone(), k1.clone()].into());
    let commit_resp = client.kv_commit(&commit_req).unwrap();
    assert!(!commit_resp.has_region_error());
    assert!(!commit_resp.has_error(), "{:?}", commit_resp.get_error());

    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(k1);
    get_req.set_version(10);
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(
        !get_resp.has_error(),
        "get error {:?}",
        get_resp.get_error()
    );
    assert_eq!(get_resp.get_value(), v);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pipelined_dml_write_conflict() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // flush x flush
    let mut req = FlushRequest::default();
    req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    req.set_generation(1);
    req.set_context(ctx.clone());
    req.set_start_ts(1);
    req.set_primary_key(k.clone());
    let flush_resp = client.kv_flush(&req).unwrap();
    assert!(!flush_resp.has_region_error());
    assert!(flush_resp.get_errors().is_empty());

    // another conflicting flush should return error
    let mut req = req.clone();
    req.set_start_ts(2);
    let resp = client.kv_flush(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().first().unwrap().has_locked());

    // flush x prerwite
    let mut req = PrewriteRequest::default();
    req.set_context(ctx.clone());
    req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    req.set_start_version(2);
    req.set_primary_lock(k.clone());
    let resp = client.kv_prewrite(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.errors.first().unwrap().has_locked());

    // flush x pessimistic lock
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(k.clone());
    req.set_start_version(2);
    req.set_for_update_ts(2);
    req.set_mutations(
        vec![Mutation {
            op: Op::PessimisticLock,
            key: k.clone(),
            value: [].into(),
            ..Default::default()
        }]
        .into(),
    );
    let resp = client.kv_pessimistic_lock(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().first().unwrap().has_locked());

    // prewrite x flush
    let k = b"key2".to_vec();
    let mut prewrite_req = PrewriteRequest::default();
    prewrite_req.set_context(ctx.clone());
    prewrite_req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    prewrite_req.set_start_version(1);
    prewrite_req.set_primary_lock(k.clone());
    let resp = client.kv_prewrite(&prewrite_req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.errors.is_empty());

    let mut req = FlushRequest::default();
    req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    req.set_generation(2);
    req.set_context(ctx.clone());
    req.set_start_ts(2);
    req.set_primary_key(k.clone());
    let resp = client.kv_flush(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().first().unwrap().has_locked());

    // pessimistic lock x flush
    let k = b"key3".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(k.clone());
    req.set_start_version(1);
    req.set_for_update_ts(1);
    req.set_mutations(
        vec![Mutation {
            op: Op::PessimisticLock,
            key: k.clone(),
            value: [].into(),
            ..Default::default()
        }]
        .into(),
    );
    let resp = client.kv_pessimistic_lock(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().is_empty());

    let mut req = FlushRequest::default();
    req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    req.set_generation(3);
    req.set_context(ctx.clone());
    req.set_start_ts(2);
    req.set_primary_key(k.clone());
    let resp = client.kv_flush(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().first().unwrap().has_locked());
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pipelined_dml_read_write_conflict() {
    let (_cluster, client, ctx) = new_cluster();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // flushed lock can be observed by another read, and its min_commit_ts can be
    // pushed
    let mut req = FlushRequest::default();
    req.set_mutations(
        vec![Mutation {
            op: Op::Put,
            key: k.clone(),
            value: v.clone(),
            ..Default::default()
        }]
        .into(),
    );
    req.set_generation(1);
    req.set_context(ctx.clone());
    req.set_start_ts(1);
    req.set_primary_key(k.clone());
    let resp = client.kv_flush(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().is_empty());

    let mut req = GetRequest::default();
    req.set_context(ctx.clone());
    req.set_version(2);
    req.set_key(k.clone());
    let resp = client.kv_get(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_error().has_locked());

    // reader pushing the lock's min_commit_ts
    let mut req = CheckTxnStatusRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_key(k.clone());
    req.set_lock_ts(1);
    req.set_caller_start_ts(2);
    req.set_current_ts(2);
    let resp = client.kv_check_txn_status(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(!resp.has_error());
    assert_eq!(resp.get_action(), MinCommitTsPushed);
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pipelined_dml_buffer_get_other_key() {
    let (_cluster, client, ctx) = new_cluster();
    let k = b"key".to_vec();
    let mut req = PessimisticLockRequest::default();
    req.set_context(ctx.clone());
    req.set_primary_lock(k.clone());
    req.set_start_version(1);
    req.set_for_update_ts(1);
    req.set_mutations(
        vec![Mutation {
            op: Op::PessimisticLock,
            key: k.clone(),
            value: [].into(),
            ..Default::default()
        }]
        .into(),
    );
    let resp = client.kv_pessimistic_lock(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().is_empty());

    let mut req = BufferBatchGetRequest::default();
    req.set_context(ctx.clone());
    req.set_keys(vec![k.clone()].into());
    req.set_version(2);
    let resp = client.kv_buffer_batch_get(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_pairs().is_empty());
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_pipelined_dml_buffer_get_unordered_keys() {
    let (_cluster, client, ctx) = new_cluster();
    let keys = vec![
        b"key1".to_vec(),
        b"key2".to_vec(),
        b"key3".to_vec(),
        b"key4".to_vec(),
    ];

    // flushed lock can be observed by another read
    let mut req = FlushRequest::default();
    req.set_mutations(
        keys.iter()
            .map(|key| Mutation {
                op: Op::Put,
                key: key.clone(),
                value: key.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>()
            .into(),
    );
    req.set_generation(1);
    req.set_context(ctx.clone());
    req.set_start_ts(1);
    req.set_primary_key(keys[0].clone());
    let resp = client.kv_flush(&req).unwrap();
    assert!(!resp.has_region_error());
    assert!(resp.get_errors().is_empty());

    let mut reversed_keys = keys.clone();
    reversed_keys.reverse();
    let duplicated_keys = keys
        .clone()
        .iter()
        .flat_map(|key| vec![key.clone(), key.clone()])
        .collect();
    let cases = vec![keys.clone(), reversed_keys, duplicated_keys];
    for case in cases {
        let mut req = BufferBatchGetRequest::default();
        req.set_keys(case.into());
        req.set_context(ctx.clone());
        req.set_version(1);
        let resp = client.kv_buffer_batch_get(&req).unwrap();
        let pairs = resp.get_pairs();
        assert_eq!(pairs.len(), 4);
        let pairs_map = pairs
            .iter()
            .map(|pair| (pair.get_key().to_vec(), pair.get_value().to_vec()))
            .collect::<HashMap<_, _>>();
        for key in &keys {
            assert_eq!(pairs_map.get(key).unwrap(), key.as_slice());
        }
    }
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_check_cluster_id() {
    let (cluster, client, ctx) = new_cluster();
    let k1 = b"k1";
    let v1 = b"v1";
    let ts = 1;
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k1.to_vec());
    mutation.set_value(v1.to_vec());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k1.to_vec(), ts);
    must_kv_commit(&client, ctx.clone(), vec![k1.to_vec()], ts, ts + 1, ts + 1);

    // Test unary requests, cluster id is not set.
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k1.to_vec();
    get_req.version = 10;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(
        !get_resp.has_error(),
        "get error {:?}",
        get_resp.get_error()
    );
    assert_eq!(get_resp.get_value(), v1);

    // Test unary request, cluster id is set correctly.
    get_req.mut_context().cluster_id = ctx.cluster_id;
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(!get_resp.has_region_error());
    assert!(
        !get_resp.has_error(),
        "get error {:?}",
        get_resp.get_error()
    );
    assert_eq!(get_resp.get_value(), v1);

    // Test unary request, cluster id is set incorrectly.
    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    let max_ts_before_req = cm.max_ts();
    get_req.mut_context().cluster_id = ctx.cluster_id + 1;
    get_req.version = max_ts_before_req.next().next().into_inner();
    let get_resp = client.kv_get(&get_req);
    let mut error_match = false;
    if let Error::RpcFailure(status) = get_resp.unwrap_err() {
        if status.code() == RpcStatusCode::INVALID_ARGUMENT {
            error_match = true;
        }
    }
    assert!(error_match);
    assert_eq!(max_ts_before_req, cm.max_ts());
}

#[test_case(test_raftstore::must_new_cluster_and_kv_client)]
#[test_case(test_raftstore_v2::must_new_cluster_and_kv_client)]
fn test_check_cluster_id_for_batch_cmds() {
    let (_cluster, client, ctx) = new_cluster();
    let k1 = b"k1";
    let v1 = b"v1";
    let ts = 1;
    // Prewrite and commit.
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k1.to_vec());
    mutation.set_value(v1.to_vec());
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k1.to_vec(), ts);
    must_kv_commit(&client, ctx.clone(), vec![k1.to_vec()], ts, ts + 1, ts + 1);

    // Test batch command requests.
    for set_cluster_id in [false, true] {
        let batch_req_num = 10usize;
        for invalid_req_index in [0, 5, 9, 20] {
            let (mut sender, receiver) = client.batch_commands().unwrap();
            let mut batch_req = BatchCommandsRequest::default();
            for i in 0..batch_req_num {
                let mut get = GetRequest::default();
                get.version = ts + 10;
                get.key = k1.to_vec();
                get.set_context(ctx.clone());
                if set_cluster_id {
                    get.mut_context().cluster_id = ctx.cluster_id;
                    if i == invalid_req_index {
                        get.mut_context().cluster_id = ctx.cluster_id + 100;
                    }
                }
                let mut req = batch_commands_request::Request::default();
                req.cmd = Some(batch_commands_request::request::Cmd::Get(get));
                batch_req.mut_requests().push(req);
                batch_req.mut_request_ids().push(i as u64);
            }
            block_on(sender.send((batch_req, WriteFlags::default()))).unwrap();
            block_on(sender.close()).unwrap();
            let (tx, rx) = mpsc::sync_channel(1);

            thread::spawn(move || {
                let mut count = 0;
                for x in block_on(
                    receiver
                        .map(move |b| match b {
                            Ok(batch) => batch.get_responses().len(),
                            Err(..) => 0,
                        })
                        .collect::<Vec<usize>>(),
                ) {
                    count += x;
                }
                tx.send(count).unwrap();
            });
            let received_cnt = rx.recv_timeout(Duration::from_secs(2)).unwrap();
            if !set_cluster_id || invalid_req_index >= batch_req_num {
                assert_eq!(received_cnt, batch_req_num);
            } else {
                assert!(received_cnt < batch_req_num);
            }
        }
    }
}
