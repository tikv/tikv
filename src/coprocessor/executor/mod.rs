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

// TODO: remove it
#![allow(dead_code)]

use std::collections::HashSet;
use util::codec::number::NumberDecoder;
use tipb::expression::{Expr, ExprType};
use super::codec::table::RowColsDict;
use super::Result;

mod scanner;
pub mod table_scan;
pub mod index_scan;
pub mod selection;
pub mod topn;
pub mod limit;
pub mod aggregation;

#[allow(dead_code)]
pub struct ExprColumnRefVisitor {
    pub col_ids: HashSet<i64>,
}

#[allow(dead_code)]
impl ExprColumnRefVisitor {
    pub fn new() -> ExprColumnRefVisitor {
        ExprColumnRefVisitor { col_ids: HashSet::new() }
    }

    pub fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            self.col_ids.insert(box_try!(expr.get_val().decode_i64()));
        } else {
            for sub_expr in expr.get_children() {
                try!(self.visit(sub_expr));
            }
        }
        Ok(())
    }

    pub fn batch_visit(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            try!(self.visit(expr));
        }
        Ok(())
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
}

pub trait Executor {
    fn next(&mut self) -> Result<Option<Row>>;
}
