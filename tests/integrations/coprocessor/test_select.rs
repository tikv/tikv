// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp;
use std::i64;
use std::thread;

use kvproto::coprocessor::Response;
use kvproto::kvrpcpb::Context;
use protobuf::Message;
use tipb::{Chunk, Expr, ExprType, ScalarFuncSig};

use test_coprocessor::*;
use test_storage::*;
use tidb_query::codec::{datum, Datum};
use tikv::server::Config;
use tikv::storage::TestEngineBuilder;
use tikv_util::codec::number::*;

const FLAG_IGNORE_TRUNCATE: u64 = 1;
const FLAG_TRUNCATE_AS_WARNING: u64 = 1 << 1;

fn check_chunk_datum_count(chunks: &[Chunk], datum_limit: usize) {
    let mut iter = chunks.iter();
    let res = iter.any(|x| datum::decode(&mut x.get_rows_data()).unwrap().len() != datum_limit);
    if res {
        assert!(iter.next().is_none());
    }
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
        let expected_encoded =
            datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded =
            datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    let ignored_suffix_len = tidb_query::codec::table::RECORD_ROW_KEY_LEN - 1;
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
    let expected_output_counts = vec![vec![2 as i64], vec![2 as i64], vec![1 as i64]];
    for (i, resp) in resps.into_iter().enumerate() {
        let mut chunk = Chunk::default();
        chunk.merge_from_bytes(resp.get_data()).unwrap();
        assert_eq!(
            resp.get_output_counts(),
            expected_output_counts[i].as_slice()
        );

        let chunks = vec![chunk];
        let chunk_data_limit = stream_row_limit * 3; // we have 3 fields.
        check_chunk_datum_count(&chunks, chunk_data_limit);

        let spliter = DAGChunkSpliter::new(chunks, 3);
        let j = cmp::min((i + 1) * stream_row_limit, data.len());
        let cur_data = &data[i * stream_row_limit..j];
        for (row, &(id, name, cnt)) in spliter.zip(cur_data) {
            let name_datum = name.map(|s| s.as_bytes()).into();
            let expected_encoded =
                datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
            let result_encoded = datum::encode_value(&row).unwrap();
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
    let req = DAGSelect::from(&product).build_with(ctx.clone(), &[0]);
    let mut resp = handle_select(&endpoint, req);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (id, name, cnt)) in spliter.zip(data) {
        let name_datum = name.map(|s| s.as_bytes()).into();
        let expected_encoded =
            datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        req.mut_context().set_scan_detail(true);
        req.mut_context().set_handle_time(true);

        let resp = handle_request(&endpoint, req);
        assert!(resp.get_exec_details().has_handle_time());

        let scan_detail = resp.get_exec_details().get_scan_detail();
        // Values would occur in data cf are inlined in write cf.
        assert_eq!(scan_detail.get_write().get_total(), 5);
        assert_eq!(scan_detail.get_write().get_processed(), 4);
        assert_eq!(scan_detail.get_lock().get_total(), 1);
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
        .build();
    let mut resp = handle_select(&endpoint, req);
    // should only have name:0, name:2 and name:1
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, name) in spliter.zip(&[b"name:0", b"name:2", b"name:1"]) {
        let expected_encoded = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (Datum::Bytes(b"name:0".to_vec()), 2),
        (Datum::Bytes(b"name:3".to_vec()), 1),
        (Datum::Bytes(b"name:5".to_vec()), 2),
        (Datum::Null, 1),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .count()
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, cnt)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);

    let exp = vec![
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(2)], 1),
        (vec![Datum::Bytes(b"name:3".to_vec()), Datum::I64(3)], 1),
        (vec![Datum::Bytes(b"name:0".to_vec()), Datum::I64(1)], 1),
        (vec![Datum::Bytes(b"name:5".to_vec()), Datum::I64(4)], 2),
        (vec![Datum::Null, Datum::I64(4)], 1),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .count()
        .group_by(&[&product["name"], &product["count"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (gk_data, cnt)) in spliter.zip(exp) {
        let mut expected_datum = vec![Datum::U64(cnt)];
        expected_datum.extend_from_slice(gk_data.as_slice());
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (Datum::Bytes(b"name:0".to_vec()), 1),
        (Datum::Bytes(b"name:3".to_vec()), 2),
        (Datum::Bytes(b"name:5".to_vec()), 3),
        (Datum::Null, 7),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .first(&product["id"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, id)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::I64(id), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
        assert_eq!(&*result_encoded, &*expected_encoded);
        row_count += 1;
    }
    assert_eq!(row_count, exp_len);

    let exp = vec![
        (2, Datum::Bytes(b"name:0".to_vec())),
        (3, Datum::Bytes(b"name:3".to_vec())),
        (1, Datum::Bytes(b"name:0".to_vec())),
        (4, Datum::Bytes(b"name:5".to_vec())),
        (5, Datum::Null),
        (6, Datum::Null),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .first(&product["name"])
        .group_by(&[&product["count"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (count, name)) in spliter.zip(exp) {
        let expected_datum = vec![name, Datum::I64(count)];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (Datum::Bytes(b"name:0".to_vec()), (Datum::Dec(3.into()), 2)),
        (Datum::Bytes(b"name:3".to_vec()), (Datum::Dec(3.into()), 1)),
        (Datum::Bytes(b"name:5".to_vec()), (Datum::Dec(8.into()), 2)),
        (Datum::Null, (Datum::Dec(4.into()), 1)),
        (Datum::Bytes(b"name:4".to_vec()), (Datum::Null, 0)),
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
    for (row, (name, (sum, cnt))) in spliter.zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), sum, name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (Datum::Bytes(b"name:0".to_vec()), 3),
        (Datum::Bytes(b"name:3".to_vec()), 3),
        (Datum::Bytes(b"name:5".to_vec()), 8),
        (Datum::Null, 4),
    ];
    // for dag
    let req = DAGSelect::from(&product)
        .sum(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, cnt)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::Dec(cnt.into()), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (Datum::Null, Datum::I64(4), Datum::I64(4)),
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
    for (row, (name, max, min)) in spliter.zip(exp) {
        let expected_datum = vec![max, min, name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        (
            Datum::Bytes(b"name:0".to_vec()),
            Datum::U64(0),
            Datum::U64(3),
            Datum::U64(3),
        ),
        (
            Datum::Bytes(b"name:3".to_vec()),
            Datum::U64(3),
            Datum::U64(3),
            Datum::U64(3),
        ),
        (
            Datum::Bytes(b"name:5".to_vec()),
            Datum::U64(4),
            Datum::U64(5),
            Datum::U64(1),
        ),
        (Datum::Null, Datum::U64(4), Datum::U64(4), Datum::U64(4)),
        (
            Datum::Bytes(b"name:6".to_vec()),
            Datum::U64(18446744073709551615),
            Datum::U64(0),
            Datum::U64(0),
        ),
    ];

    // for dag
    let req = DAGSelect::from(&product)
        .bit_and(&product["count"])
        .bit_or(&product["count"])
        .bit_xor(&product["count"])
        .group_by(&[&product["name"]])
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 4);
    for (row, (name, bitand, bitor, bitxor)) in spliter.zip(exp) {
        let expected_datum = vec![bitand, bitor, bitxor, name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded =
            datum::encode_value(&[i64::from(id).into(), name_datum, i64::from(cnt).into()])
                .unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded =
            datum::encode_value(&[name_datum, (cnt as i64).into(), (id as i64).into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded = datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded = datum::encode_value(&[id.into(), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    for (row, (id, _, _)) in spliter.zip(data) {
        let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    for (row, (id, _, _)) in spliter.zip(expect) {
        let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    for (row, (id, _, _)) in spliter.zip(data) {
        let expected_encoded = datum::encode_value(&[id.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    let req = DAGSelect::from_index(&product, &product["id"]).build();
    let mut resp = handle_select(&endpoint, req);
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    let mut row_count = 0;
    for _ in spliter {
        row_count += 1;
    }
    assert_eq!(row_count, 5);
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
        .build();
    let mut resp = handle_select(&endpoint, req);
    // should only have name:0, name:2 and name:1
    let mut row_count = 0;
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    for (row, name) in spliter.zip(&[b"name:0", b"name:1", b"name:2"]) {
        let expected_encoded = datum::encode_value(&[Datum::Bytes(name.to_vec())]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        .count()
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 1);
    let expected_encoded = datum::encode_value(&[Datum::U64(data.len() as u64)]).unwrap();
    let ret_data = spliter.next();
    assert_eq!(ret_data.is_some(), true);
    let result_encoded = datum::encode_value(&ret_data.unwrap()).unwrap();
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
        .count()
        .group_by(&[&product["name"]])
        .build();
    resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, cnt)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        .count()
        .group_by(&[&product["name"], &product["count"]])
        .build();
    resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 3);
    for (row, (gk_data, cnt)) in spliter.zip(exp) {
        let mut expected_datum = vec![Datum::U64(cnt)];
        expected_datum.extend_from_slice(gk_data.as_slice());
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, id)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::I64(id), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    for (row, (name, (sum, cnt))) in spliter.zip(exp) {
        let expected_datum = vec![Datum::U64(cnt), sum, name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        .build();
    let mut resp = handle_select(&endpoint, req);
    let mut row_count = 0;
    let exp_len = exp.len();
    let spliter = DAGChunkSpliter::new(resp.take_chunks().into(), 2);
    for (row, (name, cnt)) in spliter.zip(exp) {
        let expected_datum = vec![Datum::Dec(cnt.into()), name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    for (row, (name, max, min)) in spliter.zip(exp) {
        let expected_datum = vec![max, min, name];
        let expected_encoded = datum::encode_value(&expected_datum).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    let expected_encoded = datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
    let result_encoded = datum::encode_value(&row).unwrap();
    assert_eq!(&*result_encoded, &*expected_encoded);
    assert_eq!(spliter.next().is_none(), true);
}

#[test]
fn test_handle_truncate() {
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
            let count_offset = offset_for_column(&cols, product["count"].id);
            col.mut_val().encode_i64(count_offset).unwrap();

            // "2x" will be truncated.
            let mut value = Expr::default();
            value.set_tp(ExprType::String);
            value.set_val(String::from("2x").into_bytes());

            let mut right = Expr::default();
            right.set_tp(ExprType::ScalarFunc);
            right.set_sig(ScalarFuncSig::CastStringAsInt);
            right.mut_children().push(value);

            let mut cond = Expr::default();
            cond.set_tp(ExprType::ScalarFunc);
            cond.set_sig(ScalarFuncSig::LtInt);
            cond.mut_children().push(col);
            cond.mut_children().push(right);
            cond
        },
        {
            // id
            let mut col_id = Expr::default();
            col_id.set_tp(ExprType::ColumnRef);
            let id_offset = offset_for_column(&cols, product["id"].id);
            col_id.mut_val().encode_i64(id_offset).unwrap();

            // "3x" will be truncated.
            let mut value = Expr::default();
            value.set_tp(ExprType::String);
            value.set_val(String::from("3x").into_bytes());

            let mut int_3 = Expr::default();
            int_3.set_tp(ExprType::ScalarFunc);
            int_3.set_sig(ScalarFuncSig::CastStringAsInt);
            int_3.mut_children().push(value);

            // count
            let mut col_count = Expr::default();
            col_count.set_tp(ExprType::ColumnRef);
            let count_offset = offset_for_column(&cols, product["count"].id);
            col_count.mut_val().encode_i64(count_offset).unwrap();

            // "3x" + count
            let mut plus = Expr::default();
            plus.set_tp(ExprType::ScalarFunc);
            plus.set_sig(ScalarFuncSig::PlusInt);
            plus.mut_children().push(int_3);
            plus.mut_children().push(col_count);

            // id = "3x" + count
            let mut cond = Expr::default();
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
        let expected_encoded =
            datum::encode_value(&[Datum::I64(id), name_datum, cnt.into()]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded =
            datum::encode_value(&[id.into(), name_datum, cnt.into(), Datum::I64(3)]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
        let expected_encoded = datum::encode_value(&[name_datum]).unwrap();
        let result_encoded = datum::encode_value(&row).unwrap();
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
    assert_eq!(resp.get_output_counts(), [data.len() as i64]);
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

    // get none
    let req = DAGSelect::from(&product).build();
    let resp = handle_request(&endpoint, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(!exec_details.has_handle_time());
    assert!(!exec_details.has_scan_detail());

    let flags = &[0];

    // get handle_time
    let mut ctx = Context::default();
    ctx.set_handle_time(true);
    let req = DAGSelect::from(&product).build_with(ctx, flags);
    let resp = handle_request(&endpoint, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(exec_details.has_handle_time());
    assert!(!exec_details.has_scan_detail());

    // get scan detail
    let mut ctx = Context::default();
    ctx.set_scan_detail(true);
    let req = DAGSelect::from(&product).build_with(ctx, flags);
    let resp = handle_request(&endpoint, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(!exec_details.has_handle_time());
    assert!(exec_details.has_scan_detail());

    // get both
    let mut ctx = Context::default();
    ctx.set_scan_detail(true);
    ctx.set_handle_time(true);
    let req = DAGSelect::from(&product).build_with(ctx, flags);
    let resp = handle_request(&endpoint, req);
    assert!(resp.has_exec_details());
    let exec_details = resp.get_exec_details();
    assert!(exec_details.has_handle_time());
    assert!(exec_details.has_scan_detail());
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
    select.key_range.set_start(b"xxx".to_vec());
    select.key_range.set_end(b"zzz".to_vec());
    let req = select.build();
    let resp = handle_request(&endpoint, req);
    assert!(!resp.get_other_error().is_empty());
}

#[test]
fn test_snapshot_failed() {
    let product = ProductTable::new();
    let (_cluster, raft_engine, ctx) = new_raft_engine(1, "");

    let (_, endpoint) =
        init_data_with_engine_and_commit(ctx.clone(), raft_engine, &product, &[], true);

    // Use an invalid context to make errors.
    let req = DAGSelect::from(&product).build_with(Context::default(), &[0]);
    let resp = handle_request(&endpoint, req);

    assert!(resp.get_region_error().has_store_not_match());
}
