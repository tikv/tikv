// Copyright 2018 PingCAP, Inc.
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

use test::Bencher;

use tipb::expression::FieldType;

use tikv::coprocessor::codec::chunk::{Chunk, ChunkEncoder};
use tikv::coprocessor::codec::datum::Datum;
use tikv::coprocessor::codec::mysql::*;

fn field_type(tp: u8) -> FieldType {
    let mut fp = FieldType::new();
    fp.set_tp(i32::from(tp));
    fp
}

#[bench]
fn bench_encode_chunk(b: &mut Bencher) {
    let rows = 1024;
    let fields = vec![
        field_type(types::LONG_LONG),
        field_type(types::LONG_LONG),
        field_type(types::VARCHAR),
        field_type(types::VARCHAR),
        field_type(types::NEW_DECIMAL),
        field_type(types::JSON),
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
