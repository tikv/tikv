// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::kvrpcpb::{Context, IsolationLevel};
use protobuf::Message;
use tipb::SelectResponse;

use test_coprocessor::*;
use test_storage::*;

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
