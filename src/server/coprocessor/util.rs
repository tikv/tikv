// Copyright 2016 PingCAP, Inc.
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


use std::collections::HashMap;
use std::collections::hash_map::Entry;

use tipb::select::Chunk;
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};

use kvproto::coprocessor::KeyRange;
use util::codec::{Datum, mysql};
use util::xeval::Evaluator;
use util::codec::table::TableDecoder;
use util::codec::number::NumberDecoder;

use super::Result;

pub const BATCH_ROW_COUNT: usize = 64;

pub fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
pub fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[inline]
pub fn inflate_with_col<'a, T>(eval: &mut Evaluator,
                               values: &HashMap<i64, &[u8]>,
                               cols: T,
                               h: i64)
                               -> Result<()>
    where T: IntoIterator<Item = &'a ColumnInfo>
{
    for col in cols {
        let col_id = col.get_column_id();
        if let Entry::Vacant(e) = eval.row.entry(col_id) {
            if col.get_pk_handle() {
                let v = get_pk(col, h);
                e.insert(v);
            } else {
                let value = match values.get(&col_id) {
                    None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                        return Err(box_err!("column {} of {} is missing", col_id, h));
                    }
                    None => Datum::Null,
                    Some(bs) => box_try!(bs.clone().decode_col_value(&eval.ctx, col)),
                };
                e.insert(value);
            }
        }
    }
    Ok(())
}

#[inline]
pub fn get_chunk(chunks: &mut Vec<Chunk>) -> &mut Chunk {
    if chunks.last().map_or(true, |chunk| chunk.get_rows_meta().len() >= BATCH_ROW_COUNT) {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}

pub fn collect_col_in_expr(cols: &mut HashMap<i64, ColumnInfo>,
                           col_meta: &[ColumnInfo],
                           expr: &Expr)
                           -> Result<()> {
    if expr.get_tp() == ExprType::ColumnRef {
        let i = box_try!(expr.get_val().decode_i64());
        if let Entry::Vacant(e) = cols.entry(i) {
            for c in col_meta {
                if c.get_column_id() == i {
                    e.insert(c.clone());
                    return Ok(());
                }
            }
            return Err(box_err!("column meta of {} is missing", i));
        }
    }
    for c in expr.get_children() {
        try!(collect_col_in_expr(cols, col_meta, c));
    }
    Ok(())
}
