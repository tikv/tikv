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

use super::test_select::*;
use kvproto::coprocessor::{KeyRange, Request};
use kvproto::kvrpcpb::{Context, IsolationLevel};
use protobuf::{Message, RepeatedField};
use tipb::analyze::{
    AnalyzeColumnsReq, AnalyzeColumnsResp, AnalyzeIndexReq, AnalyzeIndexResp, AnalyzeReq,
    AnalyzeType,
};

pub const REQ_TYPE_ANALYZE: i64 = 104;

fn new_analyze_req(data: Vec<u8>, range: KeyRange) -> Request {
    let mut req = Request::new();
    req.set_data(data);
    req.set_ranges(RepeatedField::from_vec(vec![range]));
    req.set_tp(REQ_TYPE_ANALYZE);
    req
}

fn new_analyze_column_req(
    table: &Table,
    bucket_size: i64,
    fm_sketch_size: i64,
    sample_size: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
) -> Request {
    let mut col_req = AnalyzeColumnsReq::new();
    col_req.set_columns_info(RepeatedField::from_vec(table.get_table_columns()));
    col_req.set_bucket_size(bucket_size);
    col_req.set_sketch_size(fm_sketch_size);
    col_req.set_sample_size(sample_size);
    col_req.set_cmsketch_depth(cm_sketch_depth);
    col_req.set_cmsketch_width(cm_sketch_width);
    let mut analy_req = AnalyzeReq::new();
    analy_req.set_tp(AnalyzeType::TypeColumn);
    analy_req.set_start_ts(next_id() as u64);
    analy_req.set_col_req(col_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        table.get_select_range(),
    )
}

fn new_analyze_index_req(
    table: &Table,
    bucket_size: i64,
    idx: i64,
    cm_sketch_depth: i32,
    cm_sketch_width: i32,
) -> Request {
    let mut idx_req = AnalyzeIndexReq::new();
    idx_req.set_num_columns(2);
    idx_req.set_bucket_size(bucket_size);
    idx_req.set_cmsketch_depth(cm_sketch_depth);
    idx_req.set_cmsketch_width(cm_sketch_width);
    let mut analy_req = AnalyzeReq::new();
    analy_req.set_tp(AnalyzeType::TypeIndex);
    analy_req.set_start_ts(next_id() as u64);
    analy_req.set_idx_req(idx_req);
    new_analyze_req(
        analy_req.write_to_bytes().unwrap(),
        table.get_index_range(idx),
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
    for &iso_level in &[IsolationLevel::SI, IsolationLevel::RC] {
        let (_, mut end_point) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_column_req(&product.table, 3, 3, 3, 4, 32);
        let mut ctx = Context::new();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&end_point, req);
        match iso_level {
            IsolationLevel::SI => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::RC => {
                let mut analyze_resp = AnalyzeColumnsResp::new();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_pk_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
                end_point.stop().unwrap().join().unwrap();
            }
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
    let (_, mut end_point) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_column_req(&product.table, 3, 3, 3, 4, 32);
    let resp = handle_request(&end_point, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeColumnsResp::new();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_pk_hist();
    assert_eq!(hist.get_buckets().len(), 2);
    assert_eq!(hist.get_ndv(), 4);
    let collectors = analyze_resp.get_collectors().to_vec();
    assert_eq!(
        collectors.len(),
        product.table.get_table_columns().len() - 1
    );
    assert_eq!(collectors[0].get_null_count(), 1);
    assert_eq!(collectors[0].get_count(), 3);
    let rows = collectors[0].get_cm_sketch().get_rows();
    assert_eq!(rows.len(), 4);
    let sum: u32 = rows.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 3);
    end_point.stop().unwrap().join().unwrap();
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
    for &iso_level in &[IsolationLevel::SI, IsolationLevel::RC] {
        let (_, end_point) = init_data_with_commit(&product, &data, false);

        let mut req = new_analyze_index_req(&product.table, 3, product.name.index, 4, 32);
        let mut ctx = Context::new();
        ctx.set_isolation_level(iso_level);
        req.set_context(ctx);

        let resp = handle_request(&end_point, req);
        match iso_level {
            IsolationLevel::SI => {
                assert!(resp.get_data().is_empty(), "{:?}", resp);
                assert!(resp.has_locked(), "{:?}", resp);
            }
            IsolationLevel::RC => {
                let mut analyze_resp = AnalyzeIndexResp::new();
                analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
                let hist = analyze_resp.get_hist();
                assert!(hist.get_buckets().is_empty());
                assert_eq!(hist.get_ndv(), 0);
            }
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
    ];

    let product = ProductTable::new();
    let (_, mut end_point) = init_data_with_commit(&product, &data, true);

    let req = new_analyze_index_req(&product.table, 3, product.name.index, 4, 32);
    let resp = handle_request(&end_point, req);
    assert!(!resp.get_data().is_empty());
    let mut analyze_resp = AnalyzeIndexResp::new();
    analyze_resp.merge_from_bytes(resp.get_data()).unwrap();
    let hist = analyze_resp.get_hist();
    assert_eq!(hist.get_ndv(), 4);
    assert_eq!(hist.get_buckets().len(), 2);
    let rows = analyze_resp.get_cms().get_rows();
    assert_eq!(rows.len(), 4);
    let sum: u32 = rows.first().unwrap().get_counters().iter().sum();
    assert_eq!(sum, 8);
    end_point.stop().unwrap().join().unwrap();
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
    let (_, mut end_point) = init_data_with_commit(&product, &data, true);
    let mut req = new_analyze_index_req(&product.table, 3, product.name.index, 4, 32);
    let mut key_range = KeyRange::new();
    key_range.set_start(b"xxx".to_vec());
    key_range.set_end(b"zzz".to_vec());
    req.set_ranges(RepeatedField::from_vec(vec![key_range]));
    let resp = handle_request(&end_point, req);
    assert!(!resp.get_other_error().is_empty());
    end_point.stop().unwrap().join().unwrap();
}
