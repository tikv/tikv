// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    coprocessor::{KeyRange, Request, StoreBatchTask},
    kvrpcpb::{Context, IsolationLevel},
};
use protobuf::Message;
use test_coprocessor::*;
use test_storage::*;
use tipb::{
    AnalyzeColumnGroup, AnalyzeColumnsReq, AnalyzeColumnsResp, AnalyzeIndexReq, AnalyzeIndexResp,
    AnalyzeReq, AnalyzeType,
};
use txn_types::Key;

pub const REQ_TYPE_ANALYZE: i64 = 104;

fn new_analyze_req(data: Vec<u8>, range: KeyRange, start_ts: u64) -> Request {
    let mut req = Request::default();
    req.set_data(data);
    req.set_ranges(vec![range].into());
    req.set_start_ts(start_ts);
    req.set_tp(REQ_TYPE_ANALYZE);
    req
}

fn new_analyze_column_req(
    table: &Table,
    columns_info_len: usize,
    bucket_size: i64,
    fm_sketch_size: i64,
    sample_size: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
) -> Request {
    let mut col_req = AnalyzeColumnsReq::default();
    col_req.set_columns_info(table.columns_info()[..columns_info_len].into());
    col_req.set_bucket_size(bucket_size);
    col_req.set_sketch_size(fm_sketch_size);
    col_req.set_sample_size(sample_size);
    col_req.set_cmsketch_depth(cm_sketch_depth);
    col_req.set_cmsketch_width(cm_sketch_width);
    let mut analy_req = AnalyzeReq::default();
    analy_req.set_tp(AnalyzeType::TypeColumn);
    analy_req.set_col_req(col_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        table.get_record_range_all(),
        next_id() as u64,
    )
}

fn new_analyze_index_req(
    table: &Table,
    bucket_size: i64,
    idx: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
    top_n_size: i32,
    stats_ver: i32,
) -> Request {
    let mut idx_req = AnalyzeIndexReq::default();
    idx_req.set_num_columns(2);
    idx_req.set_bucket_size(bucket_size);
    idx_req.set_cmsketch_depth(cm_sketch_depth);
    idx_req.set_cmsketch_width(cm_sketch_width);
    idx_req.set_top_n_size(top_n_size);
    idx_req.set_version(stats_ver);
    let mut analy_req = AnalyzeReq::default();
    analy_req.set_tp(AnalyzeType::TypeIndex);
    analy_req.set_idx_req(idx_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        table.get_index_range_all(idx),
        next_id() as u64,
    )
}

fn new_analyze_sampling_req(
    table: &Table,
    idx: i64,
    sample_size: i64,
    sample_rate: f64,
) -> Request {
    let mut col_req = AnalyzeColumnsReq::default();
    let mut col_groups: Vec<AnalyzeColumnGroup> = Vec::new();
    let mut col_group = AnalyzeColumnGroup::default();
    let offsets = vec![idx];
    let lengths = vec![-1_i64];
    col_group.set_column_offsets(offsets);
    col_group.set_prefix_lengths(lengths);
    col_groups.push(col_group);
    col_req.set_column_groups(col_groups.into());
    col_req.set_columns_info(table.columns_info().into());
    col_req.set_sample_size(sample_size);
    col_req.set_sample_rate(sample_rate);
    let mut analy_req = AnalyzeReq::default();
    analy_req.set_tp(AnalyzeType::TypeColumn);
    analy_req.set_tp(AnalyzeType::TypeFullSampling);
    analy_req.set_col_req(col_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        table.get_record_range_all(),
        next_id() as u64,
    )
}

#[test]
fn test_analyze_column_with_lock() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    for &iso_level in &[IsolationLevel::Si, IsolationLevel::Rc] {
        let (_, endpoint, _) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_column_req(&product, 3, 3, 3, 3, 4, 32);
        let mut ctx = Context::default();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&endpoint, req);
        match iso_level {
            IsolationLevel::Si => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::Rc => {
                let mut analyze_resp = AnalyzeColumnsResp::default();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_pk_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
            }
            IsolationLevel::RcCheckTs => unimplemented!(),
        }
    }
}

#[test]
fn test_analyze_column() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_column_req(&product, 3, 3, 3, 3, 4, 32);
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeColumnsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_pk_hist();
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_ndv(), 4);
    let collectors = analyze_resp.get_collectors().to_vec();
    assert_eq!(collectors.len(), product.columns_info().len() - 1);
    assert_eq!(collectors[0].get_null_count(), 1);
    assert_eq!(collectors[0].get_count(), 3);
    let rows = collectors[0].get_cm_sketch().get_rows();
    assert_eq!(rows.len(), 4);
    let sum: u32 = rows.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 3);
    assert_eq!(collectors[0].get_total_size(), 21);
    assert_eq!(collectors[1].get_total_size(), 4);
}

#[test]
fn test_analyze_single_primary_column() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_column_req(&product, 1, 3, 3, 3, 4, 32);
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeColumnsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_pk_hist();
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_ndv(), 4);
    let collectors = analyze_resp.get_collectors().to_vec();
    assert_eq!(collectors.len(), 0);
}

#[test]
fn test_analyze_index_with_lock() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    for &iso_level in &[IsolationLevel::Si, IsolationLevel::Rc] {
        let (_, endpoint, _) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32, 0, 1);
        let mut ctx = Context::default();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&endpoint, req);
        match iso_level {
            IsolationLevel::Si => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::Rc => {
                let mut analyze_resp = AnalyzeIndexResp::default();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
            }
            IsolationLevel::RcCheckTs => unimplemented!(),
        }
    }
}

#[test]
fn test_analyze_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
        (6, Some("name:1"), 1),
        (7, Some("name:1"), 1),
        (8, Some("name:1"), 1),
        (9, Some("name:2"), 1),
        (10, Some("name:2"), 1),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32, 2, 2);
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeIndexResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_hist();
    assert_eq!(hist.get_ndv(), 6);
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_buckets()[0].get_count(), 5);
    assert_eq!(hist.get_buckets()[0].get_ndv(), 3);
    assert_eq!(hist.get_buckets()[1].get_count(), 9);
    assert_eq!(hist.get_buckets()[1].get_ndv(), 3);
    let rows = analyze_resp.get_cms().get_rows();
    assert_eq!(rows.len(), 4);
    let sum: u32 = rows.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 13);
    let top_n = analyze_resp.get_cms().get_top_n();
    let mut top_n_count = top_n
        .iter()
        .map(|data| data.get_count())
        .collect::<Vec<_>>();
    top_n_count.sort_unstable();
    assert_eq!(top_n_count, vec![2, 3]);
}

#[test]
fn test_analyze_sampling_reservoir() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
        (6, Some("name:1"), 1),
        (7, Some("name:1"), 1),
        (8, Some("name:1"), 1),
        (9, Some("name:2"), 1),
        (10, Some("name:2"), 1),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);

    // Pass the 2nd column as a column group.
    let req = new_analyze_sampling_req(&product, 1, 5, 0.0);
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeColumnsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let collector = analyze_resp.get_row_collector();
    assert_eq!(collector.get_samples().len(), 5);
    // The column group is at 4th place and the data should be equal to the 2nd.
    assert_eq!(collector.get_null_counts(), vec![0, 1, 0, 1]);
    assert_eq!(collector.get_count(), 9);
    assert_eq!(collector.get_fm_sketch().len(), 4);
    assert_eq!(collector.get_total_size(), vec![72, 56, 9, 56]);
}

#[test]
fn test_analyze_sampling_bernoulli() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, None, 4),
        (6, Some("name:1"), 1),
        (7, Some("name:1"), 1),
        (8, Some("name:1"), 1),
        (9, Some("name:2"), 1),
        (10, Some("name:2"), 1),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);

    // Pass the 2nd column as a column group.
    let req = new_analyze_sampling_req(&product, 1, 0, 0.5);
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeColumnsResp::default();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let collector = analyze_resp.get_row_collector();
    // The column group is at 4th place and the data should be equal to the 2nd.
    assert_eq!(collector.get_null_counts(), vec![0, 1, 0, 1]);
    assert_eq!(collector.get_count(), 9);
    assert_eq!(collector.get_fm_sketch().len(), 4);
    assert_eq!(collector.get_total_size(), vec![72, 56, 9, 56]);
}

#[test]
fn test_invalid_range() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint, _) = init_data_with_commit(&product, &data, true);
    let mut req = new_analyze_index_req(&product, 3, product["name"].index, 4, 32, 0, 1);
    let mut key_range = KeyRange::default();
    key_range.set_start(b"xxx".to_vec());
    key_range.set_end(b"zzz".to_vec());
    req.set_ranges(vec![key_range].into());
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_other_error().is_empty());
}

#[test]
fn test_batched_full_sampling_responses() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
        (9, Some("name:8"), 7),
        (10, Some("name:6"), 8),
    ];
    let product = ProductTable::new();
    let (mut cluster, raft_engine, ctx) = new_raft_engine(1, "");
    let (_, endpoint, _) =
        init_data_with_engine_and_commit(ctx, raft_engine, &product, &data, true);

    // Split the region into [1, 2], [4, 5], [9, 10].
    let region =
        cluster.get_region(Key::from_raw(&product.get_record_range(1, 1).start).as_encoded());
    let split_key = Key::from_raw(&product.get_record_range(3, 3).start);
    cluster.must_split(&region, split_key.as_encoded());
    let second_region =
        cluster.get_region(Key::from_raw(&product.get_record_range(4, 4).start).as_encoded());
    let second_split_key = Key::from_raw(&product.get_record_range(8, 8).start);
    cluster.must_split(&second_region, second_split_key.as_encoded());

    let mut build_req = |allow_merge: bool| -> Request {
        let mut col_req = AnalyzeColumnsReq::default();
        col_req.set_columns_info(product.columns_info().into());
        // A sample rate of one keeps every row, so the merged sample set is
        // deterministic.
        col_req.set_sample_rate(1.0);
        col_req.set_sketch_size(1000);
        let mut analyze_req = AnalyzeReq::default();
        analyze_req.set_tp(AnalyzeType::TypeFullSampling);
        analyze_req.set_col_req(col_req);

        let top_range = product.get_record_range(1, 2);
        let top_region = cluster.get_region(Key::from_raw(&top_range.start).as_encoded());
        let mut top_ctx = Context::default();
        top_ctx.set_region_id(top_region.get_id());
        top_ctx.set_region_epoch(top_region.get_region_epoch().clone());
        top_ctx.set_peer(cluster.leader_of_region(top_region.get_id()).unwrap());

        let mut req = Request::default();
        req.set_tp(REQ_TYPE_ANALYZE);
        req.set_data(analyze_req.write_to_bytes().unwrap());
        req.set_ranges(vec![top_range].into());
        req.set_start_ts(100);
        req.set_context(top_ctx);
        req.set_allow_batch_task_data_merge(allow_merge);
        for (task_id, (start, end)) in [(1, (4, 5)), (2, (9, 10))] {
            let range = product.get_record_range(start, end);
            let batch_region = cluster.get_region(Key::from_raw(&range.start).as_encoded());
            let mut task = StoreBatchTask::new();
            task.set_region_id(batch_region.get_id());
            task.set_region_epoch(batch_region.get_region_epoch().clone());
            task.set_peer(cluster.leader_of_region(batch_region.get_id()).unwrap());
            task.set_ranges(vec![range].into());
            task.set_task_id(task_id);
            req.tasks.push(task);
        }
        req
    };

    let parse_collector = |data: &[u8]| -> tipb::RowSampleCollector {
        let mut resp = AnalyzeColumnsResp::default();
        resp.merge_from_bytes(data).unwrap();
        resp.take_row_collector()
    };

    // Negotiation alone does not activate deferred finalization. With no
    // child tasks the ordinary full-sampling path must still serialize the
    // top result inside its handler.
    let mut no_child_req = build_req(true);
    no_child_req.clear_tasks();
    let resp = handle_request(&endpoint, no_child_req);
    assert!(!resp.has_region_error(), "{:?}", resp);
    assert!(resp.get_other_error().is_empty(), "{:?}", resp);
    assert_eq!(parse_collector(resp.get_data()).get_count(), 2);
    assert!(resp.get_batch_responses().is_empty());

    // A negotiated batch wider than the supported four children must be
    // rejected as one retryable top error before any collector is consumed.
    // Reusing regions and task IDs is intentional: the whole-batch response
    // must remain unambiguous even when task IDs cannot identify a subset.
    let mut over_width_req = build_req(true);
    let duplicate_task = over_width_req.tasks[0].clone();
    over_width_req.tasks.push(duplicate_task.clone());
    over_width_req.tasks.push(duplicate_task.clone());
    over_width_req.tasks.push(duplicate_task);
    let resp = handle_request(&endpoint, over_width_req);
    assert!(resp.has_region_error(), "{:?}", resp);
    assert!(resp.get_region_error().has_server_is_busy(), "{:?}", resp);
    assert!(resp.get_data().is_empty());
    assert!(resp.get_batch_responses().is_empty());

    // Establish the exact accounting baseline from the ordinary per-task
    // response shape. The merged shape below must expose the same total once
    // in its outer execution details.
    let unmerged_resp = handle_request(&endpoint, build_req(false));
    assert!(!unmerged_resp.has_region_error(), "{:?}", unmerged_resp);
    assert!(
        unmerged_resp.get_other_error().is_empty(),
        "{:?}",
        unmerged_resp
    );
    let all_unmerged_details = std::iter::once(unmerged_resp.get_exec_details_v2()).chain(
        unmerged_resp
            .get_batch_responses()
            .iter()
            .map(|child| child.get_exec_details_v2()),
    );
    let expected_processed_versions = all_unmerged_details
        .clone()
        .map(|details| details.get_scan_detail_v2().get_processed_versions())
        .sum::<u64>();
    let expected_processed_versions_size = all_unmerged_details
        .clone()
        .map(|details| details.get_scan_detail_v2().get_processed_versions_size())
        .sum::<u64>();
    let expected_total_versions = all_unmerged_details
        .clone()
        .map(|details| details.get_scan_detail_v2().get_total_versions())
        .sum::<u64>();
    let expected_rocksdb_key_skipped = all_unmerged_details
        .clone()
        .map(|details| details.get_scan_detail_v2().get_rocksdb_key_skipped_count())
        .sum::<u64>();
    let expected_table_scan_iterations = all_unmerged_details
        .map(|details| {
            details
                .get_ru_v2()
                .get_executor_inputs()
                .get_tikv_coprocessor_executor_work_total_batch_table_scan()
        })
        .sum::<u64>();

    // When the client allows merging, the response carries the merged result
    // of all three regions and every batched task is acknowledged without
    // data of its own.
    let mut resp = handle_request(&endpoint, build_req(true));
    assert!(!resp.has_region_error(), "{:?}", resp);
    assert!(resp.get_other_error().is_empty(), "{:?}", resp);
    let collector = parse_collector(resp.get_data());
    assert_eq!(collector.get_count(), 6);
    assert_eq!(collector.get_samples().len(), 6);
    // The outer response is the single accounting owner for the top task and
    // every successfully merged child. Per-task ACK details remain available
    // for diagnostics, but clients must not add them again.
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_processed_versions(),
        expected_processed_versions
    );
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_processed_versions_size(),
        expected_processed_versions_size
    );
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_total_versions(),
        expected_total_versions
    );
    // Unlike processed/total versions, this counter is written from the
    // global tracker. It guards against the outer wrapper overwriting the
    // already-aggregated child RocksDB details with top-only values.
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_rocksdb_key_skipped_count(),
        expected_rocksdb_key_skipped
    );
    assert_eq!(
        resp.get_exec_details_v2()
            .get_ru_v2()
            .get_executor_inputs()
            .get_tikv_coprocessor_executor_work_total_batch_table_scan(),
        expected_table_scan_iterations
    );
    assert!(
        resp.get_exec_details_v2()
            .get_time_detail_v2()
            .get_process_wall_time_ns()
            >= resp
                .get_batch_responses()
                .iter()
                .map(|child| {
                    child
                        .get_exec_details_v2()
                        .get_time_detail_v2()
                        .get_process_wall_time_ns()
                })
                .sum::<u64>()
    );
    let mut batch_resps = resp.take_batch_responses().into_vec();
    batch_resps.sort_unstable_by_key(|resp| resp.get_task_id());
    assert_eq!(batch_resps.len(), 2);
    for (batch_resp, task_id) in batch_resps.iter().zip([1, 2]) {
        assert_eq!(batch_resp.get_task_id(), task_id);
        assert!(batch_resp.get_data_merged_into_response());
        assert!(batch_resp.get_data().is_empty());
        assert_eq!(
            batch_resp
                .get_exec_details_v2()
                .get_scan_detail_v2()
                .get_processed_versions(),
            2
        );
    }

    // A failed task remains separate for retry while successful tasks are
    // still merged and acknowledged. Duplicate/default task IDs deliberately
    // make IDs unusable for ordering; acknowledgements must follow input
    // ordinal (successful first, failed second).
    let mut req = build_req(true);
    req.tasks[0].set_task_id(0);
    req.tasks[1].set_task_id(0);
    req.tasks[1].mut_region_epoch().set_version(0);
    let mut resp = handle_request(&endpoint, req);
    assert!(!resp.has_region_error(), "{:?}", resp);
    assert!(resp.get_other_error().is_empty(), "{:?}", resp);
    let collector = parse_collector(resp.get_data());
    assert_eq!(collector.get_count(), 4);
    assert_eq!(collector.get_samples().len(), 4);
    // Only the successfully merged child belongs to the outer aggregate. The
    // failed child remains a separate response for retry and accounting.
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_processed_versions(),
        4
    );
    let batch_resps = resp.take_batch_responses().into_vec();
    assert_eq!(batch_resps.len(), 2);
    assert_eq!(batch_resps[0].get_task_id(), 0);
    assert!(batch_resps[0].get_data_merged_into_response());
    assert!(batch_resps[0].get_data().is_empty());
    assert_eq!(batch_resps[1].get_task_id(), 0);
    assert!(!batch_resps[1].get_data_merged_into_response());
    assert!(batch_resps[1].has_region_error());

    // Without the client's consent every task keeps its own serialized
    // result.
    let mut resp = unmerged_resp;
    assert!(!resp.has_region_error(), "{:?}", resp);
    assert!(resp.get_other_error().is_empty(), "{:?}", resp);
    assert_eq!(parse_collector(resp.get_data()).get_count(), 2);
    assert_eq!(
        resp.get_exec_details_v2()
            .get_scan_detail_v2()
            .get_processed_versions(),
        2
    );
    let mut batch_resps = resp.take_batch_responses().into_vec();
    batch_resps.sort_unstable_by_key(|resp| resp.get_task_id());
    assert_eq!(batch_resps.len(), 2);
    for (batch_resp, task_id) in batch_resps.iter().zip([1, 2]) {
        assert_eq!(batch_resp.get_task_id(), task_id);
        assert!(!batch_resp.get_data_merged_into_response());
        assert_eq!(parse_collector(batch_resp.get_data()).get_count(), 2);
    }
}
