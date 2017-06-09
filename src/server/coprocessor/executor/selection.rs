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

// FIXME(andelf): remove following later
#![allow(dead_code)]

use std::rc::Rc;
use std::collections::HashSet;

use tipb::executor::Selection;
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};
use util::xeval::{Evaluator, EvalContext};
use util::codec::number::NumberDecoder;

use super::{Row, Executor};
use super::super::Result;
use super::super::endpoint::inflate_with_col;

/// Collect all mentioned column ids in expr tree
pub struct ExprColumnRefVisitor {
    pub column_ids: HashSet<i64>,
}

impl ExprColumnRefVisitor {
    pub fn new() -> ExprColumnRefVisitor {
        ExprColumnRefVisitor { column_ids: HashSet::new() }
    }

    pub fn visit_expr<'e>(&mut self, expr: &'e Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            self.column_ids.insert(try!(expr.get_val().decode_i64()));
        } else {
            for sub_expr in expr.get_children() {
                try!(self.visit_expr(sub_expr));
            }
        }
        Ok(())
    }
}

pub struct SelectionExecutor<'a> {
    conditions: Vec<Expr>,
    columns: Vec<ColumnInfo>,
    ctx: Rc<EvalContext>,
    src: Box<Executor + 'a>,
}

impl<'a> SelectionExecutor<'a> {
    pub fn new(mut meta: Selection,
               ctx: Rc<EvalContext>,
               columns_info: &[ColumnInfo],
               src: Box<Executor + 'a>)
               -> Result<SelectionExecutor<'a>> {
        let conditions = meta.take_conditions().into_vec();

        let mut visitor = ExprColumnRefVisitor::new();
        for cond in &conditions {
            try!(visitor.visit_expr(cond));
        }

        // FIXME(andelf): assume all items in columns_info are unique.
        //   For now, `SelectionExecutor` is dangling.
        let columns = columns_info.iter()
            .filter(|col| visitor.column_ids.get(&col.get_column_id()).is_some())
            .cloned()
            .collect::<Vec<ColumnInfo>>();

        assert_eq!(columns.len(), visitor.column_ids.len());

        Ok(SelectionExecutor {
            conditions: conditions,
            columns: columns,
            ctx: ctx,
            src: src,
        })
    }
}


impl<'a> Executor for SelectionExecutor<'a> {
    fn next(&mut self) -> Result<Option<Row>> {
        'next: loop {
            if let Some(row) = try!(self.src.next()) {
                let mut evaluator = Evaluator::default();
                try!(inflate_with_col(&mut evaluator,
                                      &self.ctx,
                                      &row.data,
                                      &self.columns,
                                      row.handle));
                for expr in &self.conditions {
                    let is_selected =
                        evaluator.eval(&self.ctx, expr)?.into_bool(&self.ctx)?.unwrap_or(false);
                    if !is_selected {
                        continue 'next;
                    }
                }
                return Ok(Some(row));
            } else {
                return Ok(None);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::scanner::test::{TestStore, prepare_table_data, get_range};
    use tipb::executor::TableScan;
    use super::super::table_scan::TableScanExecutor;

    use std::i64;
    use kvproto::coprocessor::KeyRange;
    use storage::Statistics;
    use protobuf::RepeatedField;
    use server::coprocessor::endpoint::{is_point, prefix_next};

    use util::codec::number::NumberEncoder;

    // build expr
    use tipb::expression::{Expr, ExprType};


    #[test]
    fn test_selection_smoke() {
        let test_data = prepare_table_data(10, 1);
        let mut store = TestStore::new(&test_data.kv_data, test_data.pk.clone());
        let mut table_scan = TableScan::new();
        // prepare cols
        let cols = test_data.get_prev_2_cols();
        table_scan.set_columns(RepeatedField::from_vec(cols.clone()));
        // prepare range
        // whole key range for the table
        let range = get_range(1, i64::MIN, i64::MAX);
        let key_ranges = vec![range];


        // make a simple range with only 1 item
        let mut range = KeyRange::new();
        range.set_start(test_data.pk.clone());
        let end = prefix_next(&test_data.pk.clone());
        range.set_end(end);
        assert!(is_point(&range));

        let mut statistics = Statistics::default();

        let (snapshot, start_ts) = store.get_snapshot();
        let inner_table_scan =
            TableScanExecutor::new(table_scan, key_ranges, snapshot, &mut statistics, start_ts);

        // NULL IS NULL
        let mut expr = Expr::new();
        expr.set_tp(ExprType::NullEQ);
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::Null);
            lhs
        });
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::Null);
            lhs
        });

        let mut selection = Selection::new();
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(selection,
                                                            Rc::new(EvalContext::default()),
                                                            &cols,
                                                            Box::new(inner_table_scan));

        assert!(selection_executor.is_ok());
        let executor = selection_executor.as_mut().unwrap();
        let nxt = executor.next();
        assert!(nxt.is_ok());
        assert!(nxt.as_ref().unwrap().is_some());
        let a_row = nxt.unwrap().unwrap();
        assert_eq!(a_row.data.len(), cols.len(), "same column size");

        let encode_data = &test_data.encode_data[0];
        for col in &cols {
            let cid = col.get_column_id();
            let v = a_row.data.get(cid).unwrap();
            assert_eq!(encode_data[&cid], v.to_vec());
        }
    }

    #[test]
    fn test_selection_simple_condition() {
        let test_data = prepare_table_data(10, 1);
        let mut store = TestStore::new(&test_data.kv_data, test_data.pk.clone());
        let mut table_scan = TableScan::new();
        // prepare cols
        // let cols = test_data.get_prev_2_cols();
        let cols = test_data.cols.clone(); // all cols
        table_scan.set_columns(RepeatedField::from_vec(cols.clone()));
        // prepare range
        // whole key range for the table
        let range = get_range(1, i64::MIN, i64::MAX);
        let key_ranges = vec![range];

        // make a simple range with only 1 item
        let mut range = KeyRange::new();
        range.set_start(test_data.pk.clone());
        let end = prefix_next(&test_data.pk.clone());
        range.set_end(end);
        assert!(is_point(&range));

        let mut statistics = Statistics::default();

        let (snapshot, start_ts) = store.get_snapshot();
        let inner_table_scan =
            TableScanExecutor::new(table_scan, key_ranges, snapshot, &mut statistics, start_ts);
        // col3 > 3
        let mut expr = Expr::new();
        expr.set_tp(ExprType::GT);
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::ColumnRef);
            lhs.mut_val().encode_i64(3).unwrap();
            lhs
        });
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.set_tp(ExprType::Uint64);
            lhs.mut_val().encode_u64(5).unwrap();
            lhs
        });

        let mut selection = Selection::new();
        selection.mut_conditions().push(expr);

        let mut selection_executor = SelectionExecutor::new(selection,
                                                            Rc::new(EvalContext::default()),
                                                            &cols,
                                                            Box::new(inner_table_scan));

        assert!(selection_executor.is_ok());
        let executor = selection_executor.as_mut().unwrap();

        // 6 to 10 is > 5
        for idx in 6..10 {
            let nxt = executor.next();
            assert!(nxt.is_ok(), "error: {:?}", nxt.unwrap());
            assert!(nxt.as_ref().unwrap().is_some(), "must have value");
            let a_row = nxt.unwrap().unwrap();
            assert_eq!(a_row.data.len(), cols.len(), "must have same column size");
            let encode_data = &test_data.encode_data[idx];
            for col in &cols {
                let cid = col.get_column_id();
                let v = a_row.data.get(cid).unwrap();
                assert_eq!(encode_data[&cid], v.to_vec());
            }
        }
        assert!(executor.next().unwrap().is_none());
    }
}
