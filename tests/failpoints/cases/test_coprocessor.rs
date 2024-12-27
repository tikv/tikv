// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, thread, time::Duration};

use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    coprocessor::{KeyRange, Request},
    kvrpcpb::{Context, GetRequest, IsolationLevel, Mutation, Op},
    tikvpb::TikvClient,
};
use more_asserts::{assert_ge, assert_le};
use pd_client::PdClient;
use protobuf::Message;
use raftstore::store::Bucket;
use test_coprocessor::*;
use test_raftstore::{must_kv_commit, must_kv_prewrite, must_new_cluster_and_kv_client};
use test_raftstore_macro::test_case;
use test_storage::*;
use tidb_query_datatype::{
    codec::{datum, table::encode_row_key, Datum},
    expr::EvalContext,
};
use tikv_util::HandyRwLock;
use tipb::SelectResponse;
use txn_types::{Key, Lock, LockType};

#[test]
fn test_deadline() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&endpoint, req);
    let region_err = resp.get_region_error();
    assert_eq!(
        region_err.get_server_is_busy().reason,
        "deadline is exceeded".to_string()
    );
    assert_eq!(
        region_err.get_message(),
        "Coprocessor task terminated due to exceeding the deadline"
    );
}

#[test]
fn test_deadline_2() {
    // It should not even take any snapshots when request is outdated from the
    // beginning.
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&endpoint, req);
    let region_err = resp.get_region_error();
    assert_eq!(
        region_err.get_server_is_busy().reason,
        "deadline is exceeded".to_string()
    );
    assert_eq!(
        region_err.get_message(),
        "Coprocessor task terminated due to exceeding the deadline"
    );
}

/// Test deadline exceeded when request is handling
/// Note: only
#[test]
fn test_deadline_3() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = {
        let engine = tikv::storage::TestEngineBuilder::new().build().unwrap();
        let cfg = tikv::server::Config {
            end_point_request_max_handle_duration: Some(tikv_util::config::ReadableDuration::secs(
                1,
            )),
            ..Default::default()
        };
        init_data_with_details(Context::default(), engine, &product, &data, true, &cfg)
    };
    let req = DagSelect::from(&product).build();

    fail::cfg("kv_cursor_seek", "sleep(2000)").unwrap();
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    let cop_resp = handle_request(&endpoint, req);
    let mut resp = SelectResponse::default();
    resp.merge_from_bytes(cop_resp.get_data()).unwrap();

    let region_err = cop_resp.get_region_error();
    assert_eq!(
        region_err.get_server_is_busy().reason,
        "deadline is exceeded".to_string()
    );
    assert_eq!(
        region_err.get_message(),
        "Coprocessor task terminated due to exceeding the deadline"
    );
}

#[test]
fn test_parse_request_failed() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_parse_request_failed_2() {
    // It should not even take any snapshots when parse failed.
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_readpool_full() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("future_pool_spawn_full", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_server_is_busy());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("snapshot failed"));
}

#[test]
fn test_snapshot_failed_2() {
    let product = ProductTable::new();
    let (store, endpoint) = init_with_data(&product, &[]);
    let req = DagSelect::from(&product).build();

    store.get_engine().trigger_not_leader();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_not_leader());
}

#[test]
fn test_storage_error() {
    let data = vec![(1, Some("name:0"), 2), (2, Some("name:4"), 3)];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let req = DagSelect::from(&product).build();

    fail::cfg("kv_cursor_seek", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("kv cursor seek error"));
}

#[test]
fn test_region_error_in_scan() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_cluster, raft_engine, mut ctx) = new_raft_engine(1, "");
    ctx.set_isolation_level(IsolationLevel::Si);

    let (_, endpoint, _) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    fail::cfg("region_snapshot_seek", "return()").unwrap();
    let req = DagSelect::from(&product).build_with(ctx, &[0]);
    let resp = handle_request(&endpoint, req);

    assert!(
        resp.get_region_error()
            .get_message()
            .contains("region seek error")
    );
}

#[test]
fn test_paging_scan() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // set batch size and grow size to 1, so that only 1 row will be scanned in each
    // batch.
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    fail::cfg("copr_batch_grow_size", "return(1)").unwrap();
    for desc in [false, true] {
        for paging_size in 1..=4 {
            let mut exp = data.clone();
            if desc {
                exp.reverse();
            }

            let req = DagSelect::from(&product)
                .paging_size(paging_size as u64)
                .desc(desc)
                .build();
            let resp = handle_request(&endpoint, req);
            let mut select_resp = SelectResponse::default();
            select_resp.merge_from_bytes(resp.get_data()).unwrap();

            let mut row_count = 0;
            let spliter = DagChunkSpliter::new(select_resp.take_chunks().into(), 3);
            for (row, (id, name, cnt)) in spliter.zip(exp) {
                let name_datum = name.unwrap().as_bytes().into();
                let expected_encoded = datum::encode_value(
                    &mut EvalContext::default(),
                    &[Datum::I64(id), name_datum, Datum::I64(cnt)],
                )
                .unwrap();
                let result_encoded =
                    datum::encode_value(&mut EvalContext::default(), &row).unwrap();
                assert_eq!(result_encoded, &*expected_encoded);
                row_count += 1;
            }
            assert_eq!(row_count, paging_size);

            let res_range = resp.get_range();
            let (res_start_key, res_end_key) = match desc {
                true => (res_range.get_end(), res_range.get_start()),
                false => (res_range.get_start(), res_range.get_end()),
            };
            let start_key = match desc {
                true => product.get_record_range_one(i64::MAX),
                false => product.get_record_range_one(i64::MIN),
            };
            let end_key = match desc {
                true => product.get_record_range_one(data[data.len() - paging_size].0),
                false => product.get_record_range_one(data[paging_size - 1].0),
            };
            assert_eq!(res_start_key, start_key.get_start());
            assert_ge!(res_end_key, end_key.get_start());
            assert_le!(res_end_key, end_key.get_end());
        }

        // test limit with early return
        let req = DagSelect::from(&product)
            .paging_size(2)
            .limit(1)
            .desc(desc)
            .build();
        let resp = handle_request(&endpoint, req);
        assert!(resp.range.is_none());
        assert!(resp.range.is_none());

        let agg_req = DagSelect::from(&product)
            .count(&product["count"])
            .group_by(&[&product["name"]])
            .output_offsets(Some(vec![0, 1]))
            .desc(desc)
            .paging_size(2)
            .build();
        let resp = handle_request(&endpoint, agg_req);
        assert!(resp.range.is_some());
    }
}

#[test]
fn test_paging_scan_multi_ranges() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (3, Some("name:5"), 5),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // set batch size and grow size to 1, so that only 1 row will be scanned in each
    // batch.
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    fail::cfg("copr_batch_grow_size", "return(1)").unwrap();

    // test multi ranges with gap
    for desc in [true, false] {
        for paging_size in [3, 5] {
            let mut exp = [data[0], data[1], data[3], data[4]];
            if desc {
                exp.reverse();
            }

            let builder = DagSelect::from(&product)
                .paging_size(paging_size)
                .desc(desc);
            let mut range1 = builder.key_ranges[0].clone();
            range1.set_end(product.get_record_range_one(data[1].0).get_end().into());
            let mut range2 = builder.key_ranges[0].clone();
            range2.set_start(product.get_record_range_one(data[3].0).get_start().into());
            let key_ranges = vec![range1.clone(), range2.clone()];

            let req = builder.key_ranges(key_ranges).build();
            let resp = handle_request(&endpoint, req);
            let mut select_resp = SelectResponse::default();
            select_resp.merge_from_bytes(resp.get_data()).unwrap();

            let mut row_count = 0;
            let spliter = DagChunkSpliter::new(select_resp.take_chunks().into(), 3);
            for (row, (id, name, cnt)) in spliter.zip(exp) {
                let name_datum = name.unwrap().as_bytes().into();
                let expected_encoded = datum::encode_value(
                    &mut EvalContext::default(),
                    &[Datum::I64(id), name_datum, Datum::I64(cnt)],
                )
                .unwrap();
                let result_encoded =
                    datum::encode_value(&mut EvalContext::default(), &row).unwrap();
                assert_eq!(result_encoded, &*expected_encoded);
                row_count += 1;
            }
            let exp_len = if paging_size <= 4 {
                paging_size
            } else {
                exp.len() as u64
            };
            assert_eq!(row_count, exp_len);

            let res_range = resp.get_range();

            let (res_start_key, res_end_key) = match desc {
                true => (res_range.get_end(), res_range.get_start()),
                false => (res_range.get_start(), res_range.get_end()),
            };
            if paging_size != 5 {
                let start_key = match desc {
                    true => range2.get_end(),
                    false => range1.get_start(),
                };
                let end_id = match desc {
                    true => data[1].0,
                    false => data[3].0,
                };
                let end_key = product.get_record_range_one(end_id);
                assert_eq!(res_start_key, start_key);
                assert_ge!(res_end_key, end_key.get_start());
                assert_le!(res_end_key, end_key.get_end());
            } else {
                // drained.
                assert!(res_start_key.is_empty());
                assert!(res_end_key.is_empty());
            }
        }
    }
}

// TODO: #[test_case(test_raftstore_v2::must_new_cluster_and_kv_client_mul)]
#[test_case(test_raftstore::must_new_cluster_and_kv_client_mul)]
fn test_read_index_lock_checking_on_follower() {
    let (mut cluster, _client, _ctx) = new_cluster(2);
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = 1;
    cluster.must_put(b"k1", b"v1");
    pd_client.must_add_peer(rid, new_peer(2, 2));
    must_get_equal(&cluster.get_engine(2), b"k1", b"v1");

    // Transfer leader to store 1
    let r1 = cluster.get_region(b"k1");
    cluster.must_transfer_leader(1, new_peer(1, 1));

    // Connect to store 2, the follower.
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(2));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(r1.get_id());
    ctx.set_region_epoch(r1.get_region_epoch().clone());
    ctx.set_peer(new_peer(2, 2));
    ctx.set_replica_read(true);

    let product = ProductTable::new();
    let mut req = DagSelect::from(&product).build();
    req.set_context(ctx);
    req.set_start_ts(100);

    let leader_cm = cluster.sim.rl().get_concurrency_manager(1);
    let lock = Lock::new(
        LockType::Put,
        b"k1".to_vec(),
        10.into(),
        20000,
        None,
        10.into(),
        1,
        20.into(),
        false,
    )
    .use_async_commit(vec![]);
    // Set a memory lock which is in the coprocessor query range on the leader
    let locked_key = req.get_ranges()[0].get_start();
    let guard = block_on(leader_cm.lock_key(&Key::from_raw(locked_key)));
    guard.with_lock(|l| *l = Some(lock.clone()));

    let resp = client.coprocessor(&req).unwrap();
    assert_eq!(
        &lock.into_lock_info(locked_key.to_vec()),
        resp.get_locked(),
        "{:?}",
        resp
    );
}

#[test_case(test_raftstore::new_server_cluster)]
#[test_case(test_raftstore_v2::new_server_cluster)]
fn test_follower_buckets() {
    let mut cluster = new_cluster(0, 3);
    cluster.run();
    fail::cfg("skip_check_stale_read_safe", "return()").unwrap();
    let product = ProductTable::new();
    let (raft_engine, ctx) = leader_raft_engine!(cluster, "");
    let (_, endpoint, _) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &[], true);

    let mut req = DagSelect::from(&product).build_with(ctx, &[0]);
    let resp = handle_request(&endpoint, req.clone());
    assert_eq!(resp.get_latest_buckets_version(), 0);

    let mut bucket_key = product.get_record_range_all().get_start().to_owned();
    bucket_key.push(0);
    let region = cluster.get_region(&bucket_key);
    let bucket = Bucket {
        keys: vec![bucket_key],
        size: 1024,
    };

    cluster.refresh_region_bucket_keys(&region, vec![bucket], None, None);
    thread::sleep(Duration::from_millis(100));
    let wait_refresh_buckets = |endpoint, req: &mut Request| {
        for _ in 0..10 {
            req.mut_context().set_buckets_version(0);
            let resp = handle_request(&endpoint, req.clone());
            if resp.get_latest_buckets_version() == 0 {
                thread::sleep(Duration::from_millis(100));
                continue;
            }

            req.mut_context().set_buckets_version(1);
            let resp = handle_request(&endpoint, req.clone());
            if !resp.has_region_error() {
                thread::sleep(Duration::from_millis(100));
                continue;
            }
            assert_ge!(
                resp.get_region_error()
                    .get_bucket_version_not_match()
                    .version,
                1
            );
            return;
        }
        panic!("test_follower_buckets test case failed, can not get bucket version in time");
    };
    wait_refresh_buckets(endpoint, &mut req.clone());
    for (engine, ctx) in follower_raft_engine!(cluster, "") {
        req.set_context(ctx.clone());
        let (_, endpoint, _) =
            init_data_with_engine_and_commit(ctx.clone(), engine, &product, &[], true);
        wait_refresh_buckets(endpoint, &mut req.clone());
    }
    fail::remove("skip_check_stale_read_safe");
}

#[test]
fn test_default_not_found_log_info() {
    let (mut cluster, _client, _ctx) = must_new_cluster_and_kv_client();
    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let product = ProductTable::new();
    let row_key = encode_row_key(product.table_id(), 2);

    let r1 = cluster.get_region(row_key.as_slice());
    let region_id = r1.get_id();
    let leader = cluster.leader_of_region(region_id).unwrap();
    let epoch = cluster.get_region_epoch(region_id);
    let mut ctx = Context::default();
    ctx.set_region_id(region_id);
    ctx.set_peer(leader.clone());
    ctx.set_region_epoch(epoch);
    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    // Write record.
    fail::cfg("is_short_value_always_false", "return()").unwrap();
    let mut mutation = Mutation::default();
    let value = b"v2".to_vec();
    mutation.set_op(Op::Put);
    mutation.set_key(row_key.clone());
    mutation.set_value(value.clone());
    let prewrite_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    must_kv_prewrite(
        &client,
        ctx.clone(),
        vec![mutation],
        row_key.clone(),
        prewrite_ts,
    );
    let commit_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    must_kv_commit(
        &client,
        ctx.clone(),
        vec![row_key.clone()],
        prewrite_ts,
        commit_ts,
        commit_ts,
    );

    let env = Arc::new(Environment::new(1));
    let channel =
        ChannelBuilder::new(env).connect(&cluster.sim.rl().get_addr(leader.get_store_id()));
    let client = TikvClient::new(channel);

    let mut ctx = Context::default();
    ctx.set_region_id(r1.get_id());
    ctx.set_region_epoch(r1.get_region_epoch().clone());
    ctx.set_peer(test_raftstore::new_peer(1, 1));

    // Read with coprocessor request.
    let read_ts = block_on(pd_client.get_tso()).unwrap().into_inner();
    let mut cop_req = DagSelect::from(&product).build();
    cop_req.set_context(ctx.clone());
    cop_req.set_start_ts(read_ts);
    let mut key_range = KeyRange::new();
    let start_key = encode_row_key(product.table_id(), 1);
    let end_key = encode_row_key(product.table_id(), 3);
    key_range.set_start(start_key);
    key_range.set_end(end_key);
    cop_req.mut_ranges().clear();
    cop_req.mut_ranges().push(key_range);
    fail::cfg("near_load_data_by_write_default_not_found", "return()").unwrap();
    let cop_resp = client.coprocessor(&cop_req).unwrap();
    assert!(cop_resp.get_other_error().contains("default not found"));

    // Read with get request.
    let mut get_req = GetRequest::default();
    get_req.set_context(ctx);
    get_req.set_key(row_key.clone());
    get_req.set_version(read_ts);
    fail::cfg("load_data_from_default_cf_default_not_found", "return()").unwrap();
    let get_resp = client.kv_get(&get_req).unwrap();
    assert!(get_resp.get_error().get_abort().contains("DefaultNotFound"));
    fail::remove("is_short_value_always_false");
    fail::remove("near_load_data_by_write_default_not_found");
    fail::remove("load_data_from_default_cf_default_not_found");
}
