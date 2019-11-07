// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

mod chunk;

use test::Bencher;

use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};
use tipb::FieldType;

use tidb_query::codec::chunk::{Chunk, ChunkEncoder};
use tidb_query::codec::datum::Datum;
use tidb_query::codec::mysql::*;
use tidb_query::expr::EvalContext;

fn field_type(tp: FieldTypeTp) -> FieldType {
    let mut fp = FieldType::default();
    fp.as_mut_accessor().set_tp(tp);
    fp
}

#[bench]
fn bench_encode_chunk(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::VarChar),
        field_type(FieldTypeTp::VarChar),
        field_type(FieldTypeTp::NewDecimal),
        field_type(FieldTypeTp::JSON),
    ];
    let mut chunk = Chunk::new(&fields, rows);
    for row_id in 0..rows {
        let s = format!("{}.123435", row_id);
        let bs = Datum::Bytes(s.as_bytes().to_vec());
        let dec = Datum::Dec(s.parse().unwrap());
        let json = Datum::Json(Json::String(s));
        chunk.append_datum(0, &Datum::Null).unwrap();
        chunk.append_datum(1, &Datum::I64(row_id as i64)).unwrap();
        chunk.append_datum(2, &bs).unwrap();
        chunk.append_datum(3, &bs).unwrap();
        chunk.append_datum(4, &dec).unwrap();
        chunk.append_datum(5, &json).unwrap();
    }

    b.iter(|| {
        let mut buf = vec![];
        buf.encode_chunk(&chunk).unwrap();
    });
}

#[bench]
fn bench_chunk_build_tidb(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::LongLong),
    ];

    b.iter(|| {
        let mut chunk = Chunk::new(&fields, rows);
        for row_id in 0..rows {
            chunk.append_datum(0, &Datum::Null).unwrap();
            chunk.append_datum(1, &Datum::I64(row_id as i64)).unwrap();
        }
    });
}

#[bench]
fn bench_chunk_build_official(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::LongLong),
    ];

    b.iter(|| {
        let mut chunk = chunk::ChunkBuilder::new(fields.len(), rows);
        for row_id in 0..rows {
            chunk.append_datum(0, Datum::Null);
            chunk.append_datum(1, Datum::I64(row_id as i64));
        }
        chunk.build(&fields);
    });
}

#[bench]
fn bench_chunk_iter_tidb(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::Double),
    ];
    let mut chunk = Chunk::new(&fields, rows);
    let mut ctx = EvalContext::default();
    for row_id in 0..rows {
        if row_id & 1 == 0 {
            chunk.append_datum(0, &Datum::Null).unwrap();
        } else {
            chunk.append_datum(0, &Datum::I64(row_id as i64)).unwrap();
        }
        chunk.append_datum(1, &Datum::F64(row_id as f64)).unwrap();
    }

    b.iter(|| {
        let mut col1 = 0;
        let mut col2 = 0.0;
        for row in chunk.iter() {
            col1 += match row.get_datum(0, &fields[0], &mut ctx).unwrap() {
                Datum::I64(v) => v,
                Datum::Null => 0,
                _ => unreachable!(),
            };
            col2 += match row.get_datum(1, &fields[1], &mut ctx).unwrap() {
                Datum::F64(v) => v,
                _ => unreachable!(),
            };
        }
        assert_eq!(col1, 262_144);
        assert!(!(523_776.0 - col2).is_normal());
    });
}

#[bench]
fn bench_chunk_iter_official(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(FieldTypeTp::LongLong),
        field_type(FieldTypeTp::Double),
    ];
    let mut chunk = chunk::ChunkBuilder::new(fields.len(), rows);
    for row_id in 0..rows {
        if row_id & 1 == 0 {
            chunk.append_datum(0, Datum::Null);
        } else {
            chunk.append_datum(0, Datum::I64(row_id as i64));
        }

        chunk.append_datum(1, Datum::F64(row_id as f64));
    }
    let chunk = chunk.build(&fields);
    b.iter(|| {
        let (mut col1, mut col2) = (0, 0.0);
        for row_id in 0..chunk.data.num_rows() {
            col1 += match chunk.get_datum(0, row_id, &fields[0]) {
                Datum::I64(v) => v,
                Datum::Null => 0,
                _ => unreachable!(),
            };
            col2 += match chunk.get_datum(1, row_id, &fields[1]) {
                Datum::F64(v) => v,
                _ => unreachable!(),
            };
        }
        assert_eq!(col1, 262_144);
        assert!(!(523_776.0 - col2).is_normal());
    });
}
