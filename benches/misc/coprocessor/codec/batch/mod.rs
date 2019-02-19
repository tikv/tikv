// Copyright 2019 PingCAP, Inc.
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
use tikv::coprocessor::codec::batch::*;
use tikv::coprocessor::codec::mysql::*;

#[bench]
fn bench_column_agg_long(b: &mut Bencher) {
    let rows = 1024;
    let mut long_col = VectorValue::with_capacity(rows, cop_datatype::EvalType::Int);
    for row_id in 0..rows {
        if row_id & 1 == 0 {
            long_col.push_int(None);
            continue;
        }
        long_col.push_int(Some(row_id as i64));
    }
    b.iter(|| {
        let slice = long_col.as_int_slice();
        let mut long_sum = 0;
        for v in slice {
            if let Some(t) = v {
                long_sum += t;
            }
        }
        assert_eq!(262144, long_sum);
    });
}

#[bench]
fn bench_column_agg_str(b: &mut Bencher) {
    let rows = 1024;
    let mut str_col = VectorValue::with_capacity(rows, cop_datatype::EvalType::Bytes);
    let mut total_size = 0;
    for row_id in 0..rows {
        if row_id & 1 == 0 {
            str_col.push_bytes(None);
            continue;
        }
        let dec = Decimal::from(row_id);
        let s = dec.to_string().into_bytes();
        total_size += s.len();
        str_col.push_bytes(Some(s));
    }

    b.iter(|| {
        let mut get_size = 0;
        let slice = str_col.as_bytes_slice();
        for v in slice {
            if let Some(t) = v {
                get_size += t.len();
            }
        }
        assert_eq!(get_size, total_size);
    });
}

#[bench]
fn bench_column_agg_dec(b: &mut Bencher) {
    let rows = 1024;
    let mut dec_col = VectorValue::with_capacity(rows, cop_datatype::EvalType::Decimal);

    for row_id in 0..rows {
        if row_id & 1 == 0 {
            dec_col.push_decimal(None);
            continue;
        }
        dec_col.push_decimal(Some(Decimal::from(row_id)));
    }
    b.iter(|| {
        let mut dec_sum = Decimal::from(0);
        let slice = dec_col.as_decimal_slice();
        for v in slice {
            if let Some(t) = v {
                dec_sum = (&dec_sum + t).unwrap();
            }
        }
        assert_eq!(Decimal::from(262144), dec_sum);
    });
}

#[bench]
fn bench_column_agg_dec_null(b: &mut Bencher) {
    let rows = 1024;
    let mut dec_col = VectorValue::with_capacity(rows, cop_datatype::EvalType::Decimal);

    for row_id in 0..rows {
        if row_id & 1 == 0 {
            dec_col.push_decimal(None);
            continue;
        }
        dec_col.push_decimal(Some(Decimal::from(row_id)));
    }
    b.iter(|| {
        let mut null_cnt = 0;
        let slice = dec_col.as_decimal_slice();
        for v in slice {
            if v.is_none() {
                null_cnt += 1;
            }
        }
        assert_eq!(null_cnt, 512);
    });
}
