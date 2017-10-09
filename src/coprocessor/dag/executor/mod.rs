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

use std::rc::Rc;

use util::codec::number::NumberDecoder;
use tipb::expression::{Expr, ExprType};
use tipb::schema::ColumnInfo;
use util::collections::HashSet;

use coprocessor::codec::mysql;
use coprocessor::codec::datum::{self, Datum};
use coprocessor::codec::table::{RowColsDict, TableDecoder};
use coprocessor::endpoint::get_pk;
use coprocessor::select::xeval::EvalContext;
use coprocessor::{Error, Result};

mod scanner;
mod table_scan;
mod index_scan;
mod selection;
mod topn;
mod limit;
mod aggregation;

pub use self::table_scan::TableScanExecutor;
pub use self::index_scan::IndexScanExecutor;
pub use self::selection::SelectionExecutor;
pub use self::topn::TopNExecutor;
pub use self::limit::LimitExecutor;
pub use self::aggregation::AggregationExecutor;

pub struct ExprColumnRefVisitor {
    cols_offset: HashSet<usize>,
    cols_len: usize,
}

impl ExprColumnRefVisitor {
    pub fn new(cols_len: usize) -> ExprColumnRefVisitor {
        ExprColumnRefVisitor {
            cols_offset: HashSet::default(),
            cols_len: cols_len,
        }
    }

    pub fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            let offset = box_try!(expr.get_val().decode_i64()) as usize;
            if offset >= self.cols_len {
                return Err(Error::Other(box_err!(
                    "offset {} overflow, should be less than {}",
                    offset,
                    self.cols_len
                )));
            }
            self.cols_offset.insert(offset);
        } else {
            for sub_expr in expr.get_children() {
                self.visit(sub_expr)?;
            }
        }
        Ok(())
    }

    pub fn batch_visit(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    pub fn column_offsets(self) -> Vec<usize> {
        self.cols_offset.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct Row {
    pub handle: i64,
    pub data: RowColsDict,
}

impl Row {
    pub fn new(handle: i64, data: RowColsDict) -> Row {
        Row {
            handle: handle,
            data: data,
        }
    }

    // get binary of each column in order of columns
    pub fn get_binary_cols(&self, columns: &[ColumnInfo]) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(columns.len());
        for col in columns {
            if col.get_pk_handle() {
                let v = get_pk(col, self.handle);
                let bt = box_try!(datum::encode_value(&[v]));
                res.push(bt);
                continue;
            }
            let col_id = col.get_column_id();
            let value = match self.data.get(col_id) {
                None if col.has_default_val() => col.get_default_val().to_vec(),
                None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                    return Err(box_err!("column {} of {} is missing", col_id, self.handle));
                }
                None => box_try!(datum::encode_value(&[Datum::Null])),
                Some(bs) => bs.to_vec(),
            };
            res.push(value);
        }
        Ok(res)
    }
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
}

pub fn inflate_with_col_for_dag(
    ctx: &EvalContext,
    values: &RowColsDict,
    columns: Rc<Vec<ColumnInfo>>,
    offsets: &[usize],
    h: i64,
) -> Result<Vec<Datum>> {
    let mut res = vec![Datum::Null; columns.len()];
    for offset in offsets {
        let col = columns.get(*offset).unwrap();
        if col.get_pk_handle() {
            let v = get_pk(col, h);
            res[*offset] = v;
        } else {
            let col_id = col.get_column_id();
            let value = match values.get(col_id) {
                None if col.has_default_val() => {
                    // TODO: optimize it to decode default value only once.
                    box_try!(col.get_default_val().decode_col_value(ctx, col))
                }
                None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                    return Err(box_err!("column {} of {} is missing", col_id, h));
                }
                None => Datum::Null,
                Some(mut bs) => box_try!(bs.decode_col_value(ctx, col)),
            };
            res[*offset] = value;
        }
    }
    Ok(res)
}
