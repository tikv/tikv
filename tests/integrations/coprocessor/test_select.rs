// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cmp, thread, time::Duration};

use kvproto::{
    coprocessor::{Request, Response},
    kvrpcpb::{Context, IsolationLevel},
};
use protobuf::Message;
use raftstore::store::Bucket;
use test_coprocessor::*;
use test_storage::*;
use tidb_query_datatype::{
    codec::{datum, Datum},
    expr::EvalContext,
};
use tikv::{
    coprocessor::{REQ_TYPE_ANALYZE, REQ_TYPE_CHECKSUM},
    server::Config,
    storage::TestEngineBuilder,
};
use tikv_util::codec::number::*;
use tipb::{
    AnalyzeColumnsReq, AnalyzeReq, AnalyzeType, ChecksumRequest, Chunk, Expr, ExprType,
    ScalarFuncSig, SelectResponse,
};
use txn_types::TimeStamp;

const FLAG_IGNORE_TRUNCATE: u64 = 1;
const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

fn check_chunk_datum_count(chunks: &[Chunk], datum_limit: usize) {
    let mut iter = chunks.iter();
    let res = iter.any(|x| datum::decode(&mut x.get_rows_data()).unwrap().len() != datum_limit);
    if res {
        assert!(iter.next().is_none());
    }
}

/// sort_by sorts the `$v`(a vector of `Vec<Datum>`) by the $index elements in `Vec<Datum>`
macro_rules! sort_by {
    ($v:ident,  $index:expr, $t:ident) => {
        $v.sort_by(|a, b| match (&a[$index], &b[$index]) {
            (Datum::Null, Datum::Null) => std::cmp::Ordering::Equal,
            (Datum::$t(a), Datum::$t(b)) => a.cmp(&b),
            (Datum::Null, _) => std::cmp::Ordering::Less,
            (_, Datum::Null) => std::cmp::Ordering::Greater,
            _ => unreachable!(),
        });
    };
}

#[test]
fn test_select() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag selection
    let req = DAGSelect::from(&product).build();
    let mut resp = handle_select(&endpoint, req);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[Datum::I64(id), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
    }
}

#[test]
fn test_batch_row_limit() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];
    let batch_row_limit = 3;
    let chunk_datum_limit = batch_row_limit * 3; // we have 3 fields.
    let product = ProductTable::new();
    let (_, endpoint) = {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut cfg = Config::default();
        cfg.end_point_batch_row_limit = batch_row_limit;
        init_data_with_details(Context::default(), engine, &product, &data, true, &cfg)
    };

    // for dag selection
    let req = DAGSelect::from(&product).build();
    let mut resp = handle_select(&endpoint, req);
    check_chunk_datum_count(resp.get_chunks(), chunk_datum_limit);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[Datum::I64(id), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
    }
}

#[test]
fn test_stream_batch_row_limit() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
        (8, Some("name:2"), 4),
    ];

    let product = ProductTable::new();
    let stream_row_limit = 2;
    let (_, endpoint) = {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut cfg = Config::default();
        cfg.end_point_stream_batch_row_limit = stream_row_limit;
        init_data_with_details(Context::default(), engine, &product, &data, true, &cfg)
    };

    let req = DAGSelect::from(&product).build();
    assert_eq!(req.get_ranges().len(), 1);

    // only ignore first 7 bytes of the row id
    let ignored_suffix_len = tidb_query_datatype::codec::table::RECORD_ROW_KEY_LEN - 1;

    // `expected_ranges_last_bytes` checks those assertions:
    // 1. We always fetch no more than stream_row_limit rows.
    // 2. The responses' key ranges are disjoint.
    // 3. Each returned key range should cover the returned rows.
    let mut expected_ranges_last_bytes: Vec<(&[u8], &[u8])> = vec![
        (b"\x00", b"\x02\x00"),
        (b"\x02\x00", b"\x05\x00"),
        (b"\x05\x00", b"\xFF"),
    ];
    let check_range = move |resp: &Response| {
        let (start_last_bytes, end_last_bytes) = expected_ranges_last_bytes.remove(0);
        let start = resp.get_range().get_start();
        let end = resp.get_range().get_end();
        assert_eq!(&start[ignored_suffix_len..], start_last_bytes);

        assert_eq!(&end[ignored_suffix_len..], end_last_bytes);
    };

    let resps = handle_streaming_select(&endpoint, req, check_range);
    assert_eq!(resps.len(), 3);
    let expected_output_counts = vec![vec![2_i64], vec![2_i64], vec![1_i64]];
    for (i, resp) in resps.into_iter().enumerate() {
        let mut chunk = Chunk::default();
        chunk.merge_from_bytes(resp.get_data()).unwrap();
        assert_eq!(
            resp.get_output_counts(),
            expected_output_counts[i].as_slice(),
        );

        let chunks = vec![chunk];
        let chunk_data_limit = stream_row_limit * 3; // we have 3 fields.
        check_chunk_datum_count(&chunks, chunk_data_limit);

        let spliter = DAGChunkSpliter::new(chunks, 3);
        let j = cmp::min((i + 1) * stream_row_limit, data.len());
        let cur_data = &data[i * stream_row_limit..j];
        for (row, &(id, name, cnt)) in spliter.zip(cur_data) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded = datum::encode_value(
                &mut EvalContext::default(),
                &[Datum::I64(id), name_datum, cnt.into()],
            )
            .unwrap();
            let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
        }
    }
}

#[test]
fn test_select_after_lease() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (cluster, raft_engine, ctx) = new_raft_engine(1, "");
    let (_, endpoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    // Sleep until the leader lease is expired.
    thread::sleep(cluster.cfg.raft_store.raft_store_max_leader_lease.0);
    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
    let mut resp = handle_select(&endpoint, req);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[Datum::I64(id), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
    }
}

#[test]
fn test_scan_detail() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = {
        let engine = TestEngineBuilder::new().build().unwrap();
        let mut cfg = Config::default();
        cfg.end_point_batch_row_limit = 50;
        init_data_with_details(Context::default(), engine, &product, &data, true, &cfg)
    };

    let reqs = vec![
        DAGSelect::from(&product).build(),
        DAGSelect::from_index(&product, &product["name"]).build(),
    ];

    for mut req in reqs {
        req.mut_context().set_record_scan_stat(true);
        req.mut_context().set_record_time_stat(true);

        let resp = handle_request(&endpoint, req);
        assert!(resp.get_exec_details().has_time_detail());
        let scan_detail = resp.get_exec_details().get_scan_detail();
        // Values would occur in data cf are inlined in write cf.
        assert_eq!(scan_detail.get_write().get_total(), 5);
        assert_eq!(scan_detail.get_write().get_processed(), 4);
        assert_eq!(scan_detail.get_lock().get_total(), 1);

        assert!(resp.get_exec_details_v2().has_time_detail());
        let scan_detail_v2 = resp.get_exec_details_v2().get_scan_detail_v2();
        assert_eq!(scan_detail_v2.get_total_versions(), 5);
        assert_eq!(scan_detail_v2.get_processed_versions(), 4);
        assert!(scan_detail_v2.get_processed_versions_size() > 0);
    }
}

#[test]
fn test_group_by() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:2"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from(&product)
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    // should only have name:0, name:2 and name:1
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 0, Bytes);
    for (row, name) in results.iter().zip(&[b"name:0", b"name:1", b"name:2"]) {
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[Datum::Bytes(name.to_vec())])
                .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 3);
}

#[test]
fn test_aggr_count() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let exp = vec![
        (Datum::Null, 1),
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .count(&product["count"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, cnt)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);

    let exp = vec![
        (vec![Datum::Null, Datum::I64(4)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .count(&product["id"])
        .group_by(&[&product["name"], &product["count"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (gk_data, cnt)) in results.iter().zip(exp) {
        let mut expected_datum = vec![Datum::U64(cnt)];
        expected_datum.extend_from_slice(gk_data.as_slice());
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_aggr_first() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (3, Some("name:5"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
        (8, None, 5),
        (9, Some("name:5"), 5),
        (10, None, 6),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 7),
        (Datum::Bytes(b"name:0".to_vec()), 1),
        (Datum::Bytes(b"name:3".to_vec()), 2),
        (Datum::Bytes(b"name:5".to_vec()), 3),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .first(&product["id"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, id)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::I64(id), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);

    let exp = vec![
        (5, Datum::Null),
        (6, Datum::Null),
        (2, Datum::Bytes(b"name:0".to_vec())),
        (1, Datum::Bytes(b"name:0".to_vec())),
        (3, Datum::Bytes(b"name:3".to_vec())),
        (4, Datum::Bytes(b"name:5".to_vec())),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .first(&product["name"])
        .group_by(&[&product["count"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 0, Bytes);
    for (row, (count, name)) in results.iter().zip(exp) {
        let expected_datum = vec![name, Datum::I64(count)];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_aggr_avg() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    store
        .insert_into(&product)
        .set(&product["id"], Datum::I64(8))
        .set(&product["name"], Datum::Bytes(b"name:4".to_vec()))
        .set(&product["count"], Datum::Null)
        .execute();
    store.commit();

    let exp = vec![
        (Datum::Null, (Datum::Dec(4.into()), 1)),
        (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
        (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
        (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
        (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
    ];
    // for dag
    let req = DAGSelect::from(&product)
        .avg(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 2, Bytes);
    for (row, (name, (sum, cnt))) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), sum, name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_aggr_sum() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 4),
        (Datum::Bytes(b"name:0".to_vec()), 3),
        (Datum::Bytes(b"name:3".to_vec()), 3),
        (Datum::Bytes(b"name:5".to_vec()), 8),
    ];
    // for dag
    let req = DAGSelect::from(&product)
        .sum(&product["count"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, cnt)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::Dec(cnt.into()), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_aggr_extre() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product)
            .set(&product["id"], Datum::I64(id))
            .set(&product["name"], Datum::Bytes(name.to_vec()))
            .set(&product["count"], Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::I64(2),
            Datum::I64(1),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::I64(5),
            Datum::I64(4),
        ),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .max(&product["count"])
        .min(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 2, Bytes);
    for (row, (name, max, min)) in results.iter().zip(exp) {
        let expected_datum = vec![max, min, name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_aggr_bit_ops() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product)
            .set(&product["id"], Datum::I64(id))
            .set(&product["name"], Datum::Bytes(name.to_vec()))
            .set(&product["count"], Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Null, Datum::I64(4), Datum::I64(4), Datum::I64(4)),
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::I64(0),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::I64(3),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::I64(4),
            Datum::I64(5),
            Datum::I64(1),
        ),
        (
            Datum::Bytes(b"name:6".to_vec()),
            Datum::I64(-1),
            Datum::I64(0),
            Datum::I64(0),
        ),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .bit_and(&product["count"])
        .bit_or(&product["count"])
        .bit_xor(&product["count"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1, 2, 3]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 4);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 3, Bytes);
    for (row, (name, bitand, bitor, bitxor)) in results.iter().zip(exp) {
        let expected_datum = vec![bitand, bitor, bitxor, name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_order_by_column() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:6"), 4),
        (6, Some("name:5"), 4),
        (7, Some("name:4"), 4),
        (8, None, 4),
    ];

    let exp = vec![
        (8, None, 4),
        (7, Some("name:4"), 4),
        (6, Some("name:5"), 4),
        (5, Some("name:6"), 4),
        (2, Some("name:3"), 3),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from(&product)
        .order_by(&product["count"], true)
        .order_by(&product["name"], false)
        .limit(5)
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(exp) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[i64::from(id).into(), name_datum, i64::from(cnt).into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_order_by_pk_with_select_from_index() {
    let mut data = vec![
        (8, Some("name:0"), 2),
        (7, Some("name:3"), 3),
        (6, Some("name:0"), 1),
        (5, Some("name:6"), 4),
        (4, Some("name:5"), 4),
        (3, Some("name:4"), 4),
        (2, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .order_by(&product["id"], true)
        .limit(5)
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(expect) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[name_datum, (cnt as i64).into(), (id as i64).into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_limit() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();
    // for dag
    let req = DAGSelect::from(&product).limit(5).build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(expect) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[id.into(), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_reverse() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    data.reverse();
    let expect: Vec<_> = data.drain(..5).collect();
    // for dag
    let req = DAGSelect::from(&product)
        .limit(5)
        .order_by(&product["id"], true)
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(expect) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[id.into(), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from_index(&product, &product["id"]).build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, (id, ..)) in spliter.zip(data) {
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 6);
}

#[test]
fn test_index_reverse_limit() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    data.reverse();
    let expect: Vec<_> = data.drain(..5).collect();
    // for dag
    let req = DAGSelect::from_index(&product, &product["id"])
        .limit(5)
        .order_by(&product["id"], true)
        .build();

    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, (id, ..)) in spliter.zip(expect) {
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_limit_oom() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from_index(&product, &product["id"])
        .limit(100000000)
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, (id, ..)) in spliter.zip(data) {
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 6);
}

#[test]
fn test_del_select() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    let (id, name, cnt) = data.remove(3);
    let name_datum = name.map(|s| s.as_bytes()).into();
    store
        .delete_from(&product)
        .execute(id, vec![id.into(), name_datum, cnt.into()]);
    store.commit();

    // for dag
    let mut req = DAGSelect::from_index(&product, &product["id"]).build();
    req.mut_context().set_record_scan_stat(true);

    let resp = handle_request(&endpoint, req);
    let mut sel_resp = SelectResponse::default();
    sel_resp.merge_from_bytes(resp.get_data()).unwrap();
    let spliter = DAGChunkSpliter::new(sel_resp.take_chunks().into(), 1);
    let mut row_count = 0;
    for _ in spliter {
        row_count += 1;
    }
    assert_eq!(row_count, 5);

    assert!(resp.get_exec_details_v2().has_time_detail());
    let scan_detail_v2 = resp.get_exec_details_v2().get_scan_detail_v2();
    assert_eq!(scan_detail_v2.get_total_versions(), 8);
    assert_eq!(scan_detail_v2.get_processed_versions(), 5);
    assert!(scan_detail_v2.get_processed_versions_size() > 0);
}

#[test]
fn test_index_group_by() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:2"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    // should only have name:0, name:2 and name:1
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 0, Bytes);
    for (row, name) in results.iter().zip(&[b"name:0", b"name:1", b"name:2"]) {
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[Datum::Bytes(name.to_vec())])
                .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 3);
}

#[test]
fn test_index_aggr_count() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .count(&product["id"])
        .output_offsets(Some(vec![0]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    let expected_encoded = datum::encode_value(
        &mut EvalContext::default(),
        &[Datum::U64(data.len() as u64)],
    )
    .unwrap();
    let ret_data = spliter.next();
    assert_eq!(ret_data.is_some(), true);
    let result_encoded =
        datum::encode_value(&mut EvalContext::default(), &ret_data.unwrap()).unwrap();
    assert_eq!(&*result_encoded, &*expected_encoded);
    assert_eq!(spliter.next().is_none(), true);

    let exp = vec![
        (Datum::Null, 1),
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
    ];
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .count(&product["id"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, cnt)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);

    let exp = vec![
        (vec![Datum::Null, Datum::I64(4)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
    ];
    let req = DAGSelect::from_index(&product, &product["name"])
        .count(&product["id"])
        .group_by(&[&product["name"], &product["count"]])
        .build();
    resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (gk_data, cnt)) in results.iter().zip(exp) {
        let mut expected_datum = vec![Datum::U64(cnt)];
        expected_datum.extend_from_slice(gk_data.as_slice());
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_index_aggr_first() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 7),
        (Datum::Bytes(b"name:0".to_vec()), 4),
        (Datum::Bytes(b"name:3".to_vec()), 2),
        (Datum::Bytes(b"name:5".to_vec()), 5),
    ];
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .first(&product["id"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, id)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::I64(id), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();

        assert_eq!(
            &*result_encoded, &*expected_encoded,
            "exp: {:?}, got: {:?}",
            expected_datum, row
        );
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_index_aggr_avg() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    store
        .insert_into(&product)
        .set(&product["id"], Datum::I64(8))
        .set(&product["name"], Datum::Bytes(b"name:4".to_vec()))
        .set(&product["count"], Datum::Null)
        .execute();
    store.commit();

    let exp = vec![
        (Datum::Null, (Datum::Dec(4.into()), 1)),
        (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
        (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
        (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
        (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
    ];
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .avg(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 2, Bytes);
    for (row, (name, (sum, cnt))) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), sum, name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_index_aggr_sum() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let exp = vec![
        (Datum::Null, 4),
        (Datum::Bytes(b"name:0".to_vec()), 3),
        (Datum::Bytes(b"name:3".to_vec()), 3),
        (Datum::Bytes(b"name:5".to_vec()), 8),
    ];
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .sum(&product["count"])
        .group_by(&[&product["name"]])
        .output_offsets(Some(vec![0, 1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 1, Bytes);
    for (row, (name, cnt)) in results.iter().zip(exp) {
        let expected_datum = vec![Datum::Dec(cnt.into()), name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_index_aggr_extre() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 5),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let (mut store, endpoint) = init_with_data(&product, &data);

    store.begin();
    for &(id, name) in &[(8, b"name:5"), (9, b"name:6")] {
        store
            .insert_into(&product)
            .set(&product["id"], Datum::I64(id))
            .set(&product["name"], Datum::Bytes(name.to_vec()))
            .set(&product["count"], Datum::Null)
            .execute();
    }
    store.commit();

    let exp = vec![
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::I64(2),
            Datum::I64(1),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::I64(3),
            Datum::I64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::I64(5),
            Datum::I64(4),
        ),
        (Datum::Bytes(b"name:6".to_vec()), Datum::Null, Datum::Null),
    ];
    // for dag
    let req = DAGSelect::from_index(&product, &product["name"])
        .max(&product["count"])
        .min(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let mut results = spliter.collect::<Vec<Vec<Datum>>>();
    sort_by!(results, 2, Bytes);
    for (row, (name, max, min)) in results.iter().zip(exp) {
        let expected_datum = vec![max, min, name];
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &expected_datum).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);
}

#[test]
fn test_where() {
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};

    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let cols = product.columns_info();
    let cond = {
        let mut col = Expr::default();
        col.set_tp(ExprType::ColumnRef);
        let count_offset = offset_for_column(&cols, product["count"].id);
        col.mut_val().encode_i64(count_offset).unwrap();
        col.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);

        let mut value = Expr::default();
        value.set_tp(ExprType::String);
        value.set_val(String::from("2").into_bytes());
        value
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::VarString);

        let mut right = Expr::default();
        right.set_tp(ExprType::ScalarFunc);
        right.set_sig(ScalarFuncSig::CastStringAsInt);
        right
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        right.mut_children().push(value);

        let mut cond = Expr::default();
        cond.set_tp(ExprType::ScalarFunc);
        cond.set_sig(ScalarFuncSig::LtInt);
        cond.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        cond.mut_children().push(col);
        cond.mut_children().push(right);
        cond
    };

    let req = DAGSelect::from(&product).where_expr(cond).build();
    let mut resp = handle_select(&endpoint, req);
    let mut spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    let row = spliter.next().unwrap();
    let (id, name, cnt) = data[2];
    let name_datum = name.map(|s| s.as_bytes()).into();
    let expected_encoded = datum::encode_value(
        &mut EvalContext::default(),
        &[Datum::I64(id), name_datum, cnt.into()],
    )
    .unwrap();
    let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
    assert_eq!(&*result_encoded, &*expected_encoded);
    assert_eq!(spliter.next().is_none(), true);
}

#[test]
fn test_handle_truncate() {
    use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);
    let cols = product.columns_info();
    let cases = vec![
        {
            // count > "2x"
            let mut col = Expr::default();
            col.set_tp(ExprType::ColumnRef);
            col.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            let count_offset = offset_for_column(&cols, product["count"].id);
            col.mut_val().encode_i64(count_offset).unwrap();

            // "2x" will be truncated.
            let mut value = Expr::default();
            value
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::String);
            value.set_tp(ExprType::String);
            value.set_val(String::from("2x").into_bytes());

            let mut right = Expr::default();
            right
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            right.set_tp(ExprType::ScalarFunc);
            right.set_sig(ScalarFuncSig::CastStringAsInt);
            right.mut_children().push(value);

            let mut cond = Expr::default();
            cond.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            cond.set_tp(ExprType::ScalarFunc);
            cond.set_sig(ScalarFuncSig::LtInt);
            cond.mut_children().push(col);
            cond.mut_children().push(right);
            cond
        },
        {
            // id
            let mut col_id = Expr::default();
            col_id
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            col_id.set_tp(ExprType::ColumnRef);
            let id_offset = offset_for_column(&cols, product["id"].id);
            col_id.mut_val().encode_i64(id_offset).unwrap();

            // "3x" will be truncated.
            let mut value = Expr::default();
            value
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::String);
            value.set_tp(ExprType::String);
            value.set_val(String::from("3x").into_bytes());

            let mut int_3 = Expr::default();
            int_3
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            int_3.set_tp(ExprType::ScalarFunc);
            int_3.set_sig(ScalarFuncSig::CastStringAsInt);
            int_3.mut_children().push(value);

            // count
            let mut col_count = Expr::default();
            col_count
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            col_count.set_tp(ExprType::ColumnRef);
            let count_offset = offset_for_column(&cols, product["count"].id);
            col_count.mut_val().encode_i64(count_offset).unwrap();

            // "3x" + count
            let mut plus = Expr::default();
            plus.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            plus.set_tp(ExprType::ScalarFunc);
            plus.set_sig(ScalarFuncSig::PlusInt);
            plus.mut_children().push(int_3);
            plus.mut_children().push(col_count);

            // id = "3x" + count
            let mut cond = Expr::default();
            cond.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            cond.set_tp(ExprType::ScalarFunc);
            cond.set_sig(ScalarFuncSig::EqInt);
            cond.mut_children().push(col_id);
            cond.mut_children().push(plus);
            cond
        },
    ];

    for cond in cases {
        // Ignore truncate error.
        let req = DAGSelect::from(&product)
            .where_expr(cond.clone())
            .build_with(Context::default(), &[FLAG_IGNORE_TRUNCATE]);
        let resp = handle_select(&endpoint, req);
        assert!(!resp.has_error());
        assert!(resp.get_warnings().is_empty());

        // truncate as warning
        let req = DAGSelect::from(&product)
            .where_expr(cond.clone())
            .build_with(Context::default(), &[FLAG_TRUNCATE_AS_WARNING]);
        let mut resp = handle_select(&endpoint, req);
        assert!(!resp.has_error());
        assert!(!resp.get_warnings().is_empty());
        // check data
        let mut spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
        let row = spliter.next().unwrap();
        let (id, name, cnt) = data[2];
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[Datum::I64(id), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        assert_eq!(spliter.next().is_none(), true);

        // Do NOT ignore truncate error.
        let req = DAGSelect::from(&product).where_expr(cond.clone()).build();
        let resp = handle_select(&endpoint, req);
        assert!(resp.has_error());
        assert!(resp.get_warnings().is_empty());
    }
}

#[test]
fn test_default_val() {
    let mut data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:3"), 3),
        (4, Some("name:0"), 1),
        (5, Some("name:5"), 4),
        (6, Some("name:5"), 4),
        (7, None, 4),
    ];

    let product = ProductTable::new();
    let added = ColumnBuilder::new()
        .col_type(TYPE_LONG)
        .default(Datum::I64(3))
        .build();
    let mut tbl = TableBuilder::new()
        .add_col("id", product["id"].clone())
        .add_col("name", product["name"].clone())
        .add_col("count", product["count"].clone())
        .add_col("added", added)
        .build();
    tbl.id = product.id;

    let (_, endpoint) = init_with_data(&product, &data);
    let expect: Vec<_> = data.drain(..5).collect();
    let req = DAGSelect::from(&tbl).limit(5).build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 4);
    for (row, (id, name, cnt)) in spliter.zip(expect) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[id.into(), name_datum, cnt.into(), Datum::I64(3)],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, 5);
}

#[test]
fn test_output_offsets() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let req = DAGSelect::from(&product)
        .output_offsets(Some(vec![1]))
        .build();
    let mut resp = handle_select(&endpoint, req);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, (_, name, _)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded =
            datum::encode_value(&mut EvalContext::default(), &[name_datum]).unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
    }
}

#[test]
fn test_key_is_locked_for_primary() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_data_with_commit(&product, &data, false);

    let req = DAGSelect::from(&product).build();
    let resp = handle_request(&endpoint, req);
    assert!(resp.get_data().is_empty(), "{:?}", resp);
    assert!(resp.has_locked(), "{:?}", resp);
}

#[test]
fn test_key_is_locked_for_index() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_data_with_commit(&product, &data, false);

    let req = DAGSelect::from_index(&product, &product["name"]).build();
    let resp = handle_request(&endpoint, req);
    assert!(resp.get_data().is_empty(), "{:?}", resp);
    assert!(resp.has_locked(), "{:?}", resp);
}

#[test]
fn test_output_counts() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let req = DAGSelect::from(&product).build();
    let resp = handle_select(&endpoint, req);
    assert_eq!(resp.get_output_counts(), &[data.len() as i64]);
}

#[test]
fn test_exec_details() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_, endpoint) = init_with_data(&product, &data);

    let flags = &[0];

    let ctx = Context::default();
    let req = DAGSelect::from(&product).build_with(ctx, flags);
    let resp = handle_request(&endpoint, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(exec_details.has_time_detail());
    assert!(exec_details.has_scan_detail());
    assert!(resp.has_exec_details_v2());
    let exec_details = resp.get_exec_details_v2();
    assert!(exec_details.has_time_detail());
    assert!(exec_details.has_scan_detail_v2());
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
    let (_, endpoint) = init_with_data(&product, &data);

    let mut select = DAGSelect::from(&product);
    select.key_ranges[0].set_start(b"xxx".to_vec());
    select.key_ranges[0].set_end(b"zzz".to_vec());
    let req = select.build();
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_other_error().is_empty());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductTable::new();
    let (_cluster, raft_engine, ctx) = new_raft_engine(1, "");

    let (_, endpoint) = init_data_with_engine_and_commit(ctx, raft_engine, &product, &[], true);

    // Use an invalid context to make errors.
    let req = DAGSelect::from(&product).build_with(Context::default(), &[0]);
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_store_not_match());
}

#[test]
fn test_cache() {
    let data = vec![
        (1, Some("name:0"), 2),
        (2, Some("name:4"), 3),
        (4, Some("name:3"), 1),
        (5, Some("name:1"), 4),
    ];

    let product = ProductTable::new();
    let (_cluster, raft_engine, ctx) = new_raft_engine(1, "");

    let (_, endpoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &data, true);

    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
    let resp = handle_request(&endpoint, req.clone());

    assert!(!resp.get_is_cache_hit());
    let cache_version = resp.get_cache_last_version();

    // Cache version must be >= 5 because Raft apply index must be >= 5.
    assert!(cache_version >= 5);

    // Send the request again using is_cache_enabled == false (default) and a matching version.
    // The request should be processed as usual.

    let mut req2 = req.clone();
    req2.set_cache_if_match_version(cache_version);
    let resp2 = handle_request(&endpoint, req2);

    assert!(!resp2.get_is_cache_hit());
    assert_eq!(
        resp.get_cache_last_version(),
        resp2.get_cache_last_version()
    );
    assert_eq!(resp.get_data(), resp2.get_data());

    // Send the request again using is_cached_enabled == true and a matching version.
    // The request should be skipped.

    let mut req3 = req.clone();
    req3.set_is_cache_enabled(true);
    req3.set_cache_if_match_version(cache_version);
    let resp3 = handle_request(&endpoint, req3);

    assert!(resp3.get_is_cache_hit());
    assert!(resp3.get_data().is_empty());

    // Send the request using a non-matching version. The request should be processed.

    let mut req4 = req;
    req4.set_is_cache_enabled(true);
    req4.set_cache_if_match_version(cache_version + 1);
    let resp4 = handle_request(&endpoint, req4);

    assert!(!resp4.get_is_cache_hit());
    assert_eq!(
        resp.get_cache_last_version(),
        resp4.get_cache_last_version()
    );
    assert_eq!(resp.get_data(), resp4.get_data());
}

#[test]
fn test_copr_bypass_or_access_locks() {
    let data = vec![
        (1, Some("name:1"), 1), /* no lock */
        (2, Some("name:2"), 2), /* bypass lock */
        (3, Some("name:3"), 3), /* access lock(range) */
        (4, Some("name:4"), 4), /* access lock(range) */
        (6, Some("name:6"), 6), /* access lock(point) */
        (8, Some("name:8"), 8), /* not conflict lock */
    ];

    let product = ProductTable::new();
    let (store, _) = init_with_data(&product, &data);
    let expected_data = vec![
        (1, Some("name:1"), 1),
        (2, Some("name:2"), 2),
        (3, Some("name:33"), 33),
        (4, Some("name:44"), 44),
        (6, Some("name:66"), 66),
        (8, Some("name:8"), 8),
    ];
    // lock row 3, 4, 6
    let (mut store, endpoint) = init_data_with_engine_and_commit(
        Default::default(),
        store.get_engine(),
        &product,
        &expected_data[2..5],
        false,
    );
    let access_lock = store.current_ts();
    // lock row 2
    store.begin();
    store.delete_from(&product).execute(
        data[1].0,
        vec![
            data[1].0.into(),
            data[1].1.map(|s| s.as_bytes()).into(),
            data[1].2.into(),
        ],
    );
    let bypass_lock = store.current_ts();
    let read_ts = TimeStamp::new(next_id() as u64);
    // lock row 8 with larger ts
    store.begin();
    store.delete_from(&product).execute(
        data[5].0,
        vec![
            data[5].0.into(),
            data[5].1.map(|s| s.as_bytes()).into(),
            data[5].2.into(),
        ],
    );

    let mut ctx = Context::default();
    ctx.set_isolation_level(IsolationLevel::Si);
    ctx.set_resolved_locks(vec![bypass_lock.into_inner()]);
    ctx.set_committed_locks(vec![access_lock.into_inner()]);
    let ranges = vec![
        product.get_record_range(1, 4),
        product.get_record_range_one(6),
        product.get_record_range_one(8),
    ];

    // DAG
    {
        let mut req = DAGSelect::from(&product).build_with(ctx.clone(), &[0]);
        req.set_start_ts(read_ts.into_inner());
        req.set_ranges(ranges.clone().into());

        let mut resp = handle_select(&endpoint, req);
        let mut row_count = 0;
        let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
        for (row, (id, name, cnt)) in spliter.zip(expected_data) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded = datum::encode_value(
                &mut EvalContext::default(),
                &[Datum::I64(id), name_datum, cnt.into()],
            )
            .unwrap();
            let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
            assert_eq!(result_encoded, &*expected_encoded);
            row_count += 1;
        }
        assert_eq!(row_count, data.len());
    }

    // analyze
    {
        let mut col_req = AnalyzeColumnsReq::default();
        col_req.set_columns_info(product.columns_info().into());
        let mut analy_req = AnalyzeReq::default();
        analy_req.set_tp(AnalyzeType::TypeColumn);
        analy_req.set_col_req(col_req);
        let mut req = Request::default();
        req.set_context(ctx.clone());
        req.set_start_ts(read_ts.into_inner());
        req.set_ranges(ranges.clone().into());
        req.set_tp(REQ_TYPE_ANALYZE);
        req.set_data(analy_req.write_to_bytes().unwrap());
        let resp = handle_request(&endpoint, req);
        assert!(!resp.get_data().is_empty());
        assert!(!resp.has_locked(), "{:?}", resp);
    }

    // checksum
    {
        let checksum = ChecksumRequest::default();
        let mut req = Request::default();
        req.set_context(ctx);
        req.set_start_ts(read_ts.into_inner());
        req.set_ranges(ranges.into());
        req.set_tp(REQ_TYPE_CHECKSUM);
        req.set_data(checksum.write_to_bytes().unwrap());
        let resp = handle_request(&endpoint, req);
        assert!(!resp.get_data().is_empty());
        assert!(!resp.has_locked(), "{:?}", resp);
    }
}

#[test]
fn test_rc_read() {
    let data = vec![
        (1, Some("name:1"), 1), /* no lock */
        (2, Some("name:2"), 2), /* no lock */
        (3, Some("name:3"), 3), /* update lock */
        (4, Some("name:4"), 4), /* delete lock */
    ];

    let product = ProductTable::new();
    let (store, _) = init_with_data(&product, &data);
    let expected_data = vec![
        (1, Some("name:1"), 1),
        (2, Some("name:22"), 2),
        (3, Some("name:3"), 3),
        (4, Some("name:4"), 4),
    ];

    // uncommitted lock to be ignored
    let (store, _) = init_data_with_engine_and_commit(
        Default::default(),
        store.get_engine(),
        &product,
        &[(3, Some("name:33"), 3)],
        false,
    );

    // committed lock to be read
    let (mut store, endpoint) = init_data_with_engine_and_commit(
        Default::default(),
        store.get_engine(),
        &product,
        &expected_data[1..2],
        true,
    );

    // delete and lock row 3
    store.begin();
    store.delete_from(&product).execute(
        data[3].0,
        vec![
            data[3].0.into(),
            data[3].1.map(|s| s.as_bytes()).into(),
            data[3].2.into(),
        ],
    );

    let mut ctx = Context::default();
    ctx.set_isolation_level(IsolationLevel::Rc);
    let ranges = vec![product.get_record_range(1, 4)];

    let mut req = DAGSelect::from(&product).build_with(ctx.clone(), &[0]);
    req.set_start_ts(u64::MAX - 1);
    req.set_ranges(ranges.into());

    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(expected_data.clone()) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded = datum::encode_value(
            &mut EvalContext::default(),
            &[Datum::I64(id), name_datum, cnt.into()],
        )
        .unwrap();
        let result_encoded = datum::encode_value(&mut EvalContext::default(), &row).unwrap();
        assert_eq!(result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, expected_data.len());
}

#[test]
fn test_buckets() {
    let product = ProductTable::new();
    let (mut cluster, raft_engine, ctx) = new_raft_engine(1, "");

    let (_, endpoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &[], true);

    let req = DAGSelect::from(&product).build_with(ctx, &[0]);
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

    let wait_refresh_buckets = |old_buckets_ver| {
        let mut resp = Default::default();
        for _ in 0..10 {
            resp = handle_request(&endpoint, req.clone());
            if resp.get_latest_buckets_version() != old_buckets_ver {
                break;
            }
            thread::sleep(Duration::from_millis(100));
        }
        assert_ne!(resp.get_latest_buckets_version(), old_buckets_ver);
    };

    wait_refresh_buckets(0);
}
