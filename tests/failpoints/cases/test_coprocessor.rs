// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use futures::executor::block_on;
use grpcio::{ChannelBuilder, Environment};
use kvproto::{
    kvrpcpb::{Context, IsolationLevel},
    tikvpb::TikvClient,
};
use more_asserts::{assert_ge, assert_le};
use protobuf::Message;
use test_coprocessor::*;
use test_raftstore::{must_get_equal, new_peer, new_server_cluster};
use test_storage::*;
use tidb_query_datatype::{
    codec::{datum, Datum},
    expr::EvalContext,
};
use tikv_util::HandyRwLock;
use tipb::SelectResponse;
use txn_types::{Key, Lock, LockType};

#[test]
fn test_deadline() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("exceeding the deadline"));
}

#[test]
fn test_deadline_2() {
    // It should not even take any snapshots when request is outdated from the beginning.
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("deadline_check_fail", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("exceeding the deadline"));
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
    let (_, endpoint) = {
        let engine = tikv::storage::TestEngineBuilder::new().build().unwrap();
        let cfg = tikv::server::Config {
            end_point_request_max_handle_duration: tikv_util::config::ReadableDuration::secs(1),
            ..Default::default()
        };
        init_data_with_details(Context::default(), engine, &product, &data, true, &cfg)
    };
    let req = DAGSelect::from(&product).build();

    fail::cfg("kv_cursor_seek", "sleep(2000)").unwrap();
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    let cop_resp = handle_request(&endpoint, req);
    let mut resp = SelectResponse::default();
    resp.merge_from_bytes(cop_resp.get_data()).unwrap();

    assert!(
        cop_resp.other_error.contains("exceeding the deadline")
            || resp
                .get_error()
                .get_msg()
                .contains("exceeding the deadline")
    );
}

#[test]
fn test_parse_request_failed() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_parse_request_failed_2() {
    // It should not even take any snapshots when parse failed.
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "panic").unwrap();
    fail::cfg("coprocessor_parse_request", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("unsupported tp"));
}

#[test]
fn test_readpool_full() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("future_pool_spawn_full", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_server_is_busy());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_other_error().contains("snapshot failed"));
}

#[test]
fn test_snapshot_failed_2() {
    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &[]);
    let req = DAGSelect::from(&product).build();

    fail::cfg("rockskv_async_snapshot_not_leader", "return()").unwrap();
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_not_leader());
}

#[test]
fn test_storage_error() {
    let data = vec![(1, Some("name:0"), 2), (2, Some("name:4"), 3)];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let req = DAGSelect::from(&product).build();

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

    let (_, endpoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    fail::cfg("region_snapshot_seek", "return()").unwrap();
    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
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
    // set batch size and grow size to 1, so that only 1 row will be scanned in each batch.
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    fail::cfg("copr_batch_grow_size", "return(1)").unwrap();
    for desc in [false, true] {
        for paging_size in 1..=4 {
            let mut exp = data.clone();
            if desc {
                exp.reverse();
            }

            let req = DAGSelect::from(&product)
                .paging_size(paging_size as u64)
                .desc(desc)
                .build();
            let resp = handle_request(&endpoint, req);
            let mut select_resp = SelectResponse::default();
            select_resp.merge_from_bytes(resp.get_data()).unwrap();

            let mut row_count = 0;
            let spliter = DAGChunkSpliter::new(select_resp.take_chunks().into(), 3);
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
    // set batch size and grow size to 1, so that only 1 row will be scanned in each batch.
    fail::cfg("copr_batch_initial_size", "return(1)").unwrap();
    fail::cfg("copr_batch_grow_size", "return(1)").unwrap();

    // test multi ranges with gap
    for desc in [true] {
        let paging_size = 3;
        let mut exp = [data[0], data[1], data[3], data[4]];
        if desc {
            exp.reverse();
        }

        let builder = DAGSelect::from(&product)
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
        let spliter = DAGChunkSpliter::new(select_resp.take_chunks().into(), 3);
        for (row, (id, name, cnt)) in spliter.zip(exp) {
            let name_datum = name.unwrap().as_bytes().into();
            let expected_encoded = datum::encode_value(
                &mut EvalContext::default(),
                &[Datum::I64(id), name_datum, Datum::I64(cnt)],
            )
            .unwrap();
            let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
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
    }

    // test drained
    for desc in [false, true] {
        let paging_size = 5;
        let mut exp = [data[0], data[1], data[3], data[4]];
        if desc {
            exp.reverse();
        }

        let builder = DAGSelect::from(&product)
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
        let spliter = DAGChunkSpliter::new(select_resp.take_chunks().into(), 3);
        for (row, (id, name, cnt)) in spliter.zip(exp) {
            let name_datum = name.unwrap().as_bytes().into();
            let expected_encoded = datum::encode_value(
                &mut EvalContext::default(),
                &[Datum::I64(id), name_datum, Datum::I64(cnt)],
            )
            .unwrap();
            let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, exp.len());

        let res_range = resp.get_range();
        let (res_start_key, res_end_key) = match desc {
            true => (res_range.get_end(), res_range.get_start()),
            false => (res_range.get_start(), res_range.get_end()),
        };
        let start_key = match desc {
            true => range2.get_end(),
            false => range1.get_start(),
        };
        let end_key = match desc {
            true => product.get_record_range_one(i64::MIN),
            false => product.get_record_range_one(i64::MAX),
        };
        assert_eq!(res_start_key, start_key);
        assert_eq!(res_end_key, end_key.get_start(), "{}", desc);
    }
}

#[test]
fn test_read_index_lock_checking_on_follower() {
    let mut cluster = new_server_cluster(0, 2);

    let pd_client = Arc::clone(&cluster.pd_client);
    pd_client.disable_default_operator();

    let rid = cluster.run_conf_change();
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
    let mut req = DAGSelect::from(&product).build();
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
