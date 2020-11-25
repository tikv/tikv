// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;
use std::sync::*;
use std::thread;
use std::time::Duration;

use futures::{executor::block_on, future, SinkExt, StreamExt, TryStreamExt};
use grpcio::*;
use tempfile::Builder;

use kvproto::{
    coprocessor::*,
    debugpb,
    kvrpcpb::{self, *},
    metapb, raft_serverpb,
    raft_serverpb::*,
    tikvpb::*,
};
use raft::eraftpb;

use concurrency_manager::ConcurrencyManager;
use engine_rocks::{raw::Writable, Compat};
use engine_traits::{MiscExt, Peekable, SyncMutable, CF_DEFAULT, CF_LOCK, CF_RAFT, CF_WRITE};
use pd_client::PdClient;
use raftstore::coprocessor::CoprocessorHost;
use raftstore::store::{fsm::store::StoreMeta, AutoSplitController, SnapManager};
use test_raftstore::*;
use tikv::coprocessor::REQ_TYPE_DAG;
use tikv::import::SSTImporter;
use tikv::server::gc_worker::sync_gc;
use tikv::server::service::batch_commands_request;
use tikv_util::worker::{dummy_scheduler, FutureWorker};
use tikv_util::HandyRwLock;
use txn_types::{Key, Lock, LockType, TimeStamp};

#[test]
fn test_rawkv() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    // Raw put
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx.clone());
    put_req.key = k.clone();
    put_req.value = v.clone();
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());

    // Raw get
    let mut get_req = RawGetRequest::default();
    get_req.set_context(ctx.clone());
    get_req.key = k.clone();
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

#[test]
fn test_mvcc_basic() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());

    let mut ts = 0;

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
    assert_eq!(batch_get_resp.pairs.len(), 1);
    for kv in batch_get_resp.pairs.into_iter() {
        assert!(!kv.has_error());
        assert_eq!(kv.key, k);
        assert_eq!(kv.value, v);
    }
}

#[test]
fn test_mvcc_rollback_and_cleanup() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
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

#[test]
fn test_mvcc_resolve_lock_gc_and_delete() {
    use kvproto::kvrpcpb::*;

    let (cluster, client, ctx) = must_new_cluster_and_kv_client();
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
    sync_gc(&gc_scheduler, 0, vec![], vec![], gc_safe_ponit).unwrap();

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

// raft related RPC is tested as parts of test_snapshot.rs, so skip here.

#[test]
fn test_coprocessor() {
    let (_cluster, client, _) = must_new_cluster_and_kv_client();
    // SQL push down commands
    let mut req = Request::default();
    req.set_tp(REQ_TYPE_DAG);
    client.coprocessor(&req).unwrap();
}

#[test]
fn test_split_region() {
    let (mut cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Split region commands
    let key = b"b";
    let mut req = SplitRegionRequest::default();
    req.set_context(ctx);
    req.set_split_key(key.to_vec());
    let resp = client.split_region(&req).unwrap();
    assert_eq!(
        Key::from_encoded(resp.get_left().get_end_key().to_vec())
            .into_raw()
            .unwrap()
            .as_slice(),
        key
    );
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
    let split_keys = vec![b"e".to_vec(), b"c".to_vec(), b"d".to_vec()];
    req.set_split_keys(split_keys.into());
    let resp = client.split_region(&req).unwrap();
    let result_split_keys: Vec<_> = resp
        .get_regions()
        .iter()
        .map(|x| {
            Key::from_encoded(x.get_start_key().to_vec())
                .into_raw()
                .unwrap()
        })
        .collect();
    assert_eq!(
        result_split_keys,
        vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec(), b"e".to_vec()]
    );
}

#[test]
fn test_read_index() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    // Read index
    let mut req = ReadIndexRequest::default();
    req.set_context(ctx.clone());
    let mut resp = client.read_index(&req).unwrap();
    let last_index = resp.get_read_index();
    assert_eq!(last_index > 0, true);

    // Raw put
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let mut put_req = RawPutRequest::default();
    put_req.set_context(ctx);
    put_req.key = k;
    put_req.value = v;
    let put_resp = client.raw_put(&put_req).unwrap();
    assert!(!put_resp.has_region_error());
    assert!(put_resp.error.is_empty());

    // Read index again
    resp = client.read_index(&req).unwrap();
    assert_eq!(last_index + 1, resp.get_read_index());
}

#[test]
fn test_debug_get() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
    let (k, v) = (b"key", b"value");

    // Put some data.
    let engine = cluster.get_engine(store_id);
    let key = keys::data_key(k);
    engine.put(&key, v).unwrap();
    assert_eq!(engine.get(&key).unwrap().unwrap(), v);

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
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
fn test_debug_raft_log() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();

    // Put some data.
    let engine = cluster.get_raft_engine(store_id);
    let (region_id, log_index) = (200, 200);
    let key = keys::raft_log_key(region_id, log_index);
    let mut entry = eraftpb::Entry::default();
    entry.set_term(1);
    entry.set_index(1);
    entry.set_entry_type(eraftpb::EntryType::EntryNormal);
    entry.set_data(vec![42]);
    engine.c().put_msg(&key, &entry).unwrap();
    assert_eq!(
        engine.c().get_msg::<eraftpb::Entry>(&key).unwrap().unwrap(),
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
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

#[test]
fn test_debug_region_info() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();

    let raft_engine = cluster.get_raft_engine(store_id);
    let kv_engine = cluster.get_engine(store_id);

    let region_id = 100;
    let raft_state_key = keys::raft_state_key(region_id);
    let mut raft_state = raft_serverpb::RaftLocalState::default();
    raft_state.set_last_index(42);
    raft_engine
        .c()
        .put_msg(&raft_state_key, &raft_state)
        .unwrap();
    assert_eq!(
        raft_engine
            .c()
            .get_msg::<raft_serverpb::RaftLocalState>(&raft_state_key)
            .unwrap()
            .unwrap(),
        raft_state
    );

    let apply_state_key = keys::apply_state_key(region_id);
    let mut apply_state = raft_serverpb::RaftApplyState::default();
    apply_state.set_applied_index(42);
    kv_engine
        .c()
        .put_msg_cf(CF_RAFT, &apply_state_key, &apply_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .c()
            .get_msg_cf::<raft_serverpb::RaftApplyState>(CF_RAFT, &apply_state_key)
            .unwrap()
            .unwrap(),
        apply_state
    );

    let region_state_key = keys::region_state_key(region_id);
    let mut region_state = raft_serverpb::RegionLocalState::default();
    region_state.set_state(raft_serverpb::PeerState::Tombstone);
    kv_engine
        .c()
        .put_msg_cf(CF_RAFT, &region_state_key, &region_state)
        .unwrap();
    assert_eq!(
        kv_engine
            .c()
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
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
        }
        _ => panic!("expect NotFound"),
    }
}

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
        .c()
        .put_msg_cf(CF_RAFT, &region_state_key, &state)
        .unwrap();

    let cfs = vec![CF_DEFAULT, CF_LOCK, CF_WRITE];
    // At lease 8 bytes for the WRITE cf.
    let (k, v) = (keys::data_key(b"kkkk_kkkk"), b"v");
    for cf in &cfs {
        let cf_handle = engine.cf_handle(cf).unwrap();
        engine.put_cf(cf_handle, k.as_slice(), v).unwrap();
    }

    let mut req = debugpb::RegionSizeRequest::default();
    req.set_region_id(region_id);
    req.set_cfs(cfs.iter().map(|s| (*s).to_string()).collect());
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
            assert_eq!(status.status, RpcStatusCode::NOT_FOUND);
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
    assert!(entries
        .iter()
        .any(|e| e.get_name() == fp && e.get_actions() == act));

    let mut recover_req = debugpb::RecoverFailPointRequest::default();
    recover_req.set_name(fp.to_owned());
    debug_client.recover_fail_point(&recover_req).unwrap();

    let resp = debug_client
        .list_fail_points(&debugpb::ListFailPointsRequest::default())
        .unwrap();
    let entries = resp.get_entries();
    assert!(entries
        .iter()
        .all(|e| !(e.get_name() == fp && e.get_actions() == act)));
}

#[test]
fn test_debug_scan_mvcc() {
    let (cluster, debug_client, store_id) = must_new_cluster_and_debug_client();
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
        )
        .to_bytes();
        let cf_handle = engine.cf_handle(CF_LOCK).unwrap();
        engine.put_cf(cf_handle, k.as_slice(), &v).unwrap();
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
    let pd_worker = FutureWorker::new("test-pd-worker");
    let simulate_trans = SimulateTransport::new(ChannelTransport::new());
    let tmp = Builder::new().prefix("test_cluster").tempdir().unwrap();
    let snap_mgr = SnapManager::new(tmp.path().to_str().unwrap());
    let coprocessor_host = CoprocessorHost::new(router);
    let importer = {
        let dir = Path::new(engines.kv.path()).join("import-sst");
        Arc::new(SSTImporter::new(dir, None).unwrap())
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
        )
        .unwrap_err();
    assert!(format!("{:?}", e).contains("already started"), "{:?}", e);
    drop(sim);
    cluster.shutdown();
}

#[test]
fn test_pessimistic_lock() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
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
        }
        must_kv_pessimistic_rollback(&client, ctx.clone(), k.clone(), 40);
    }
}

#[test]
fn test_check_txn_status_with_max_ts() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
    let (k, v) = (b"key".to_vec(), b"value".to_vec());
    let lock_ts = 10;

    // Prewrite
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(k.clone());
    mutation.set_value(v);
    must_kv_prewrite(&client, ctx.clone(), vec![mutation], k.clone(), lock_ts);

    // Should return MinCommitTsPushed even if caller_start_ts is max.
    let status = must_check_txn_status(
        &client,
        ctx.clone(),
        &k,
        lock_ts,
        std::u64::MAX,
        lock_ts + 1,
    );
    assert_eq!(status.lock_ttl, 3000);
    assert_eq!(status.action, Action::MinCommitTsPushed);

    // The min_commit_ts of k shouldn't be pushed.
    must_kv_commit(&client, ctx, vec![k], lock_ts, lock_ts + 1, lock_ts + 1);
}

fn build_client(cluster: &Cluster<ServerCluster>) -> (TikvClient, Context) {
    let region = cluster.get_region(b"");
    let leader = region.get_peers()[0].clone();
    let addr = cluster.sim.rl().get_addr(leader.get_store_id());

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(leader.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(leader);

    (client, ctx)
}

#[test]
fn test_batch_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let (client, _) = build_client(&cluster);
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

#[test]
fn test_empty_commands() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let (client, _) = build_client(&cluster);
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
    rx.recv_timeout(Duration::from_secs(1)).unwrap();
}

#[test]
fn test_async_commit_check_txn_status() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let (client, ctx) = build_client(&cluster);

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

#[test]
fn test_read_index_check_memory_locks() {
    let mut cluster = new_server_cluster(0, 3);
    cluster.cfg.raft_store.hibernate_regions = false;
    cluster.run();

    // make sure leader has been elected.
    assert_eq!(cluster.must_get(b"k"), None);

    let region = cluster.get_region(b"");
    let leader = cluster.leader_of_region(region.get_id()).unwrap();
    let leader_cm = cluster.sim.rl().get_concurrency_manager(leader.get_id());

    let keys: Vec<_> = vec![b"k", b"l"]
        .into_iter()
        .map(|k| Key::from_raw(k))
        .collect();
    let guards = block_on(leader_cm.lock_keys(keys.iter()));
    let lock = Lock::new(
        LockType::Put,
        b"k".to_vec(),
        1.into(),
        20000,
        None,
        1.into(),
        1,
        2.into(),
    );
    guards[0].with_lock(|l| *l = Some(lock.clone()));

    // read on follower
    let mut follower_peer = None;
    let peers = region.get_peers();
    for p in peers {
        if p.get_id() != leader.get_id() {
            follower_peer = Some(p.clone());
            break;
        }
    }
    let follower_peer = follower_peer.unwrap();
    let addr = cluster.sim.rl().get_addr(follower_peer.get_store_id());

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(region.get_id());
    ctx.set_region_epoch(region.get_region_epoch().clone());
    ctx.set_peer(follower_peer);

    let read_index = |ranges: &[(&[u8], &[u8])]| {
        let mut req = ReadIndexRequest::default();
        let start_ts = block_on(cluster.pd_client.get_tso()).unwrap();
        req.set_context(ctx.clone());
        req.set_start_ts(start_ts.into_inner());
        for &(start_key, end_key) in ranges {
            let mut range = kvrpcpb::KeyRange::default();
            range.set_start_key(start_key.to_vec());
            range.set_end_key(end_key.to_vec());
            req.mut_ranges().push(range);
        }
        let resp = client.read_index(&req).unwrap();
        (resp, start_ts)
    };

    // wait a while until the node updates its own max ts
    thread::sleep(Duration::from_millis(300));

    let (resp, start_ts) = read_index(&[(b"l", b"yz")]);
    assert!(!resp.has_locked());
    assert_eq!(leader_cm.max_ts(), start_ts);

    let (resp, start_ts) = read_index(&[(b"a", b"b"), (b"j", b"k0")]);
    assert_eq!(resp.get_locked(), &lock.into_lock_info(b"k".to_vec()));
    assert_eq!(leader_cm.max_ts(), start_ts);

    drop(guards);

    let (resp, start_ts) = read_index(&[(b"a", b"z")]);
    assert!(!resp.has_locked());
    assert_eq!(leader_cm.max_ts(), start_ts);
}

#[test]
fn test_prewrite_check_max_commit_ts() {
    let mut cluster = new_server_cluster(0, 1);
    cluster.run();

    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
    cm.update_max_ts(100.into());

    let (client, ctx) = build_client(&cluster);

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
    req.set_context(ctx);
    req.set_primary_lock(b"k2".to_vec());
    let mut mutation = Mutation::default();
    mutation.set_op(Op::Put);
    mutation.set_key(b"k2".to_vec());
    mutation.set_value(b"v2".to_vec());
    req.mut_mutations().push(mutation);
    req.set_start_version(20);
    req.set_max_commit_ts(50);
    req.set_lock_ttl(20000);
    req.set_use_async_commit(true);
    let resp = client.kv_prewrite(&req).unwrap();
    assert_eq!(
        resp.get_errors()[0]
            .get_commit_ts_too_large()
            .get_commit_ts(),
        101
    );
    // There shouldn't be locks remaining in the lock table.
    assert!(cm.read_range_check(None, None, |_, _| Err(())).is_ok());
}

#[test]
fn test_txn_heart_beat() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();
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

#[test]
fn test_batch_get_memory_lock() {
    let (cluster, client, ctx) = must_new_cluster_and_kv_client();
    let cm = cluster.sim.read().unwrap().get_concurrency_manager(1);
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
    )
    .use_async_commit(vec![]);
    guard.with_lock(|l| {
        *l = Some(lock.clone());
    });

    let mut req = BatchGetRequest::default();
    req.set_context(ctx);
    req.set_keys(vec![b"unlocked".to_vec(), raw_key.clone()].into());
    req.version = 50;
    let resp = client.kv_batch_get(&req).unwrap();
    assert_eq!(resp.get_error().get_locked(), &lock.into_lock_info(raw_key));
}
