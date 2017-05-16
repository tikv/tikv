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
use tipb::schema::ColumnInfo;
use tipb::expression::Expr;
use util::collections::HashMap;
use util::xeval::{Evaluator, EvalContext};
use super::{Row, Executor};
use super::super::Result;
use super::super::endpoint::{collect_col_in_expr, inflate_with_col};


fn collect_col_in_exprs(columns: &mut HashMap<i64, ColumnInfo>,
                        col_meta: &[ColumnInfo],
                        exprs: &[Expr])
                        -> Result<()> {
    for e in exprs {
        try!(collect_col_in_expr(columns, col_meta, e));
    }
    Ok(())
}


struct SelectionExecutor {
    conditions: Vec<Expr>,
    columns: Vec<ColumnInfo>,
    ctx: Rc<EvalContext>,
    evaluator: Evaluator,

    src: Box<Executor>,
}

impl SelectionExecutor {
    pub fn new(ctx: Rc<EvalContext>,
               conditions: Vec<Expr>,
               column_infos: &[ColumnInfo],
               src: Box<Executor>)
               -> Result<SelectionExecutor> {
        let mut columns = HashMap::default();
        try!(collect_col_in_exprs(&mut columns, column_infos, &conditions));
        Ok(SelectionExecutor {
            conditions: conditions,
            columns: columns.into_iter().map(|(_, v)| v).collect(),
            ctx: ctx,
            evaluator: Default::default(),
            src: src,
        })
    }

    // `should_include` evaluates condition expressions on the prepared row to check whether
    // this row should be included in result set.
    fn should_include_row(&mut self) -> Result<bool> {
        for e in &self.conditions {
            let res = box_try!(self.evaluator.eval(&self.ctx, e));
            let b = box_try!(res.into_bool(&self.ctx));
            if !b.map_or(false, |v| v) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl Executor for SelectionExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        loop {
            let next = try!(self.src.next());
            match next {
                None => {
                    return Ok(None);
                }
                Some(row) => {
                    try!(inflate_with_col(&mut self.evaluator,
                                          &self.ctx,
                                          &row.data,
                                          &self.columns,
                                          row.handle));
                    if try!(self.should_include_row()) {
                        return Ok(Some(row));
                    }
                    continue;
                }
            }
        }
    }
}
