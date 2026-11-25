// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::{
    coprocessor::{KeyRange, Request},
    kvrpcpb::{Context, IsolationLevel},
};
use protobuf::Message;
use test_coprocessor::*;
use tipb::{
    AnalyzeColumnGroup, AnalyzeColumnsReq, AnalyzeColumnsResp, AnalyzeIndexReq, AnalyzeIndexResp,
    AnalyzeReq, AnalyzeType,
};

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

// A background batched request is charged to `bg-egress-limit` once, for the
// data its response returns: tasks of the same request must not wait at
// admission for each other's buffered output, and a request that times out
// returns no data and must not be charged, using the existing parallel batch
// executor.
#[test]
fn test_background_batch_egress_charged_once_returned() {
    use std::{sync::Arc, time::Duration};

    use concurrency_manager::ConcurrencyManager;
    use kvproto::{
        coprocessor::StoreBatchTask,
        resource_manager::{GroupMode, GroupRequestUnitSettings, ResourceGroup},
    };
    use raftstore::store::{ReadStats, WriteStats};
    use resource_control::ResourceGroupManager;
    use resource_metering::ResourceTagFactory;
    use test_storage::new_raft_engine;
    use tikv::{
        config::UnifiedReadPoolConfig,
        coprocessor::Endpoint,
        read_pool::{ReadPool, build_yatp_read_pool},
        server::Config,
        storage::{Engine, kv::FlowStatsReporter},
    };
    use tikv_util::{quota_limiter::QuotaLimiter, time::Instant, yatp_pool::CleanupMethod};
    use txn_types::Key;

    #[derive(Clone)]
    struct NoopReporter;
    impl FlowStatsReporter for NoopReporter {
        fn report_read_stats(&self, _: ReadStats) {}
        fn report_write_stats(&self, _: WriteStats) {}
    }

    fn new_endpoint<E: Engine>(
        _: &E,
        read_pool: &ReadPool,
        manager: Arc<ResourceGroupManager>,
    ) -> Endpoint<E> {
        Endpoint::new(
            &Config::default(),
            read_pool.handle(),
            ConcurrencyManager::new(1.into()),
            ResourceTagFactory::new_for_test(),
            Arc::new(QuotaLimiter::default()),
            Some(manager),
        )
    }

    fn returned_bytes(resp: &kvproto::coprocessor::Response) -> u64 {
        resp.get_data().len() as u64
            + resp
                .get_batch_responses()
                .iter()
                .map(|r| r.get_data().len() as u64)
                .sum::<u64>()
    }

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
    let (store, ..) = init_data_with_engine_and_commit(ctx, raft_engine, &product, &data, true);

    // The region is split into [1, 2], [4, 5], [9, 10].
    let region =
        cluster.get_region(Key::from_raw(&product.get_record_range(1, 1).start).as_encoded());
    let split_key = Key::from_raw(&product.get_record_range(3, 3).start);
    cluster.must_split(&region, split_key.as_encoded());
    let second_region =
        cluster.get_region(Key::from_raw(&product.get_record_range(4, 4).start).as_encoded());
    let second_split_key = Key::from_raw(&product.get_record_range(8, 8).start);
    cluster.must_split(&second_region, second_split_key.as_encoded());

    let mut build_req = |has_batch_tasks: bool, timeout_ms: u64| -> Request {
        let mut col_req = AnalyzeColumnsReq::default();
        col_req.set_columns_info(product.columns_info().into());
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
        // `ddl` is a background task type of the default group below.
        top_ctx.set_request_source("internal_ddl".to_owned());
        top_ctx.set_max_execution_duration_ms(timeout_ms);

        let mut req = Request::default();
        req.set_tp(REQ_TYPE_ANALYZE);
        req.set_data(analyze_req.write_to_bytes().unwrap());
        req.set_ranges(vec![top_range].into());
        req.set_start_ts(100);
        req.set_context(top_ctx);
        for (task_id, (start, end)) in [(1, (4, 5)), (2, (9, 10))]
            .into_iter()
            .take(if has_batch_tasks { 2 } else { 0 })
        {
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

    for has_batch_tasks in [false, true] {
        let case = format!("has_batch_tasks={has_batch_tasks}");
        // A fresh background limiter per case, so cases do not share debt. At
        // 64 B/s one task's response is several seconds of debt, much longer
        // than the request deadline.
        let manager = Arc::new(ResourceGroupManager::default());
        let mut default_group = ResourceGroup::new();
        default_group.set_name("default".to_owned());
        default_group.set_mode(GroupMode::RuMode);
        let mut ru_setting = GroupRequestUnitSettings::new();
        ru_setting
            .mut_r_u()
            .mut_settings()
            .set_fill_rate(i32::MAX as u64);
        default_group.set_r_u_settings(ru_setting);
        default_group
            .mut_background_settings()
            .set_job_types(vec!["ddl".to_owned()].into());
        manager.add_resource_group(default_group);
        let bg_limiter = manager.get_background_limiter();
        bg_limiter.set_egress_limit_for_test(64.0);

        let read_pool = build_yatp_read_pool(
            &UnifiedReadPoolConfig::default(),
            NoopReporter,
            store.get_engine(),
            None,
            Some(manager.clone()),
            CleanupMethod::InPlace,
            false,
        );
        let engine = store.get_engine();
        let endpoint = new_endpoint(&engine, &read_pool, manager);

        // With no debt yet, the whole request is returned and charged exactly
        // the data it returns.
        let before = bg_limiter.egress_bytes_charged_for_test();
        let resp = handle_request(&endpoint, build_req(has_batch_tasks, 1000));
        assert!(!resp.has_region_error(), "{case}: {resp:?}");
        assert!(resp.get_other_error().is_empty(), "{case}: {resp:?}");
        assert!(!resp.get_data().is_empty(), "{case}");
        let batch_resps = resp.get_batch_responses();
        assert_eq!(
            batch_resps.len(),
            if has_batch_tasks { 2 } else { 0 },
            "{case}"
        );
        for batch_resp in batch_resps {
            assert!(!batch_resp.has_region_error(), "{case}: {batch_resp:?}");
            assert!(batch_resp.get_other_error().is_empty(), "{case}");
            assert!(!batch_resp.get_data().is_empty(), "{case}: {batch_resp:?}");
        }
        let charged = bg_limiter.egress_bytes_charged_for_test() - before;
        assert_eq!(charged, returned_bytes(&resp), "{case}");
        assert!(
            bg_limiter.admission_delay(true) > Duration::from_secs(1),
            "{case}"
        );

        // A retry waits behind that debt, gives up at its deadline without
        // returning data, and is not charged.
        for _ in 0..2 {
            let before = bg_limiter.egress_bytes_charged_for_test();
            let started_at = Instant::now();
            let resp = handle_request(&endpoint, build_req(has_batch_tasks, 200));
            assert!(
                resp.get_region_error().has_server_is_busy(),
                "{case}: {resp:?}"
            );
            assert_eq!(
                resp.get_region_error().get_server_is_busy().get_reason(),
                "deadline is exceeded",
                "{case}"
            );
            assert!(
                started_at.saturating_elapsed() < Duration::from_secs(10),
                "{case}"
            );
            assert_eq!(bg_limiter.egress_bytes_charged_for_test(), before, "{case}");
        }
    }
}
