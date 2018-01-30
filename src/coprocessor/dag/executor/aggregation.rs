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

use std::sync::Arc;

use tipb::schema::ColumnInfo;
use tipb::executor::Aggregation;
use tipb::expression::{Expr, ExprType};

use util::collections::{OrderMap, OrderMapEntry};
use coprocessor::codec::table::RowColsDict;
use coprocessor::codec::datum::{self, approximate_size, Datum, DatumEncoder};
use coprocessor::endpoint::SINGLE_GROUP;
use coprocessor::dag::expr::{EvalContext, Expression};
use coprocessor::local_metrics::ExecutorMetrics;
use coprocessor::Result;

use super::aggregate::{self, AggrFunc};
use super::{inflate_with_col_for_dag, Executor, ExprColumnRefVisitor, Row};

struct AggFuncExpr {
    args: Vec<Expression>,
    tp: ExprType,
}

impl AggFuncExpr {
    fn batch_build(ctx: &EvalContext, expr: Vec<Expr>) -> Result<Vec<AggFuncExpr>> {
        expr.into_iter()
            .map(|v| AggFuncExpr::build(ctx, v))
            .collect()
    }

    fn build(ctx: &EvalContext, mut expr: Expr) -> Result<AggFuncExpr> {
        let args = box_try!(Expression::batch_build(
            ctx,
            expr.take_children().into_vec()
        ));
        let tp = expr.get_tp();
        Ok(AggFuncExpr { args: args, tp: tp })
    }

    fn eval_args(&self, ctx: &EvalContext, row: &[Datum]) -> Result<Vec<Datum>> {
        let res: Vec<Datum> = box_try!(self.args.iter().map(|v| v.eval(ctx, row)).collect());
        Ok(res)
    }
}

impl AggrFunc {
    fn update_with_expr(
        &mut self,
        ctx: &EvalContext,
        expr: &AggFuncExpr,
        row: &[Datum],
    ) -> Result<()> {
        let vals = expr.eval_args(ctx, row)?;
        self.update(ctx, vals)?;
        Ok(())
    }
}

pub struct HashAggExecutor {
    group_by: Vec<Expression>,
    aggr_func: Vec<AggFuncExpr>,
    group_key_aggrs: OrderMap<Vec<u8>, Vec<Box<AggrFunc>>>,
    cursor: usize,
    executed: bool,
    ctx: Arc<EvalContext>,
    cols: Arc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>, // offset of related columns
    src: Box<Executor>,
    count: i64,
    first_collect: bool,
}

impl HashAggExecutor {
    pub fn new(
        mut meta: Aggregation,
        ctx: Arc<EvalContext>,
        columns: Arc<Vec<ColumnInfo>>,
        src: Box<Executor>,
    ) -> Result<HashAggExecutor> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new(columns.len());
        let group_by = meta.take_group_by().into_vec();
        visitor.batch_visit(&group_by)?;
        let aggr_func = meta.take_agg_func().into_vec();
        visitor.batch_visit(&aggr_func)?;
        Ok(HashAggExecutor {
            group_by: box_try!(Expression::batch_build(&ctx, group_by)),
            aggr_func: AggFuncExpr::batch_build(&ctx, aggr_func)?,
            group_key_aggrs: OrderMap::new(),
            cursor: 0,
            executed: false,
            ctx: ctx,
            cols: columns,
            related_cols_offset: visitor.column_offsets(),
            src: src,
            count: 0,
            first_collect: true,
        })
    }

    fn get_group_key(&self, row: &[Datum]) -> Result<Vec<u8>> {
        if self.group_by.is_empty() {
            let single_group = Datum::Bytes(SINGLE_GROUP.to_vec());
            return Ok(box_try!(datum::encode_value(&[single_group])));
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = box_try!(expr.eval(&self.ctx, row));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(row) = self.src.next()? {
            let cols = inflate_with_col_for_dag(
                &self.ctx,
                &row.data,
                &self.cols,
                &self.related_cols_offset,
                row.handle,
            )?;
            let group_key = self.get_group_key(&cols)?;
            match self.group_key_aggrs.entry(group_key) {
                OrderMapEntry::Vacant(e) => {
                    let mut aggrs = Vec::with_capacity(self.aggr_func.len());
                    for expr in &self.aggr_func {
                        let mut aggr = aggregate::build_aggr_func(expr.tp)?;
                        aggr.update_with_expr(&self.ctx, expr, &cols)?;
                        aggrs.push(aggr);
                    }
                    e.insert(aggrs);
                }
                OrderMapEntry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.aggr_func.iter().zip(aggrs) {
                        aggr.update_with_expr(&self.ctx, expr, &cols)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl Executor for HashAggExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if !self.executed {
            self.aggregate()?;
            self.executed = true;
        }

        match self.group_key_aggrs.get_index_mut(self.cursor) {
            Some((group_key, aggrs)) => {
                self.cursor += 1;
                let mut aggr_cols = Vec::with_capacity(2 * self.aggr_func.len());

                // calc all aggr func
                for aggr in aggrs {
                    aggr.calc(&mut aggr_cols)?;
                }

                // construct row data
                let value_size = group_key.len() + approximate_size(&aggr_cols, false);
                let mut value = Vec::with_capacity(value_size);
                box_try!(value.encode(aggr_cols.as_slice(), false));
                if !self.group_by.is_empty() {
                    value.extend_from_slice(group_key);
                }
                self.count += 1;
                Ok(Some(Row {
                    handle: 0,
                    data: RowColsDict::new(map![], value),
                }))
            }
            None => Ok(None),
        }
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
        counts.push(self.count);
        self.count = 0;
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.increase("aggregation");
            self.first_collect = false;
        }
    }
}

#[cfg(test)]
mod test {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use coprocessor::codec::datum::{Datum, DatumDecoder};
    use coprocessor::codec::mysql::decimal::Decimal;
    use coprocessor::codec::mysql::types;
    use storage::SnapshotStore;
    use util::codec::number::NumberEncoder;

    use super::*;
    use super::super::table_scan::TableScanExecutor;
    use super::super::scanner::test::{get_range, new_col_info, TestStore};
    use super::super::topn::test::gen_table_data;

    #[inline]
    fn build_expr(tp: ExprType, id: Option<i64>, child: Option<Expr>) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(tp);
        if tp == ExprType::ColumnRef {
            expr.mut_val().encode_i64(id.unwrap()).unwrap();
        } else {
            expr.mut_children().push(child.unwrap());
        }
        expr
    }

    fn build_group_by(col_ids: &[i64]) -> Vec<Expr> {
        let mut group_by = Vec::with_capacity(col_ids.len());
        for id in col_ids {
            group_by.push(build_expr(ExprType::ColumnRef, Some(*id), None));
        }
        group_by
    }

    fn build_aggr_func(aggrs: &[(ExprType, i64)]) -> Vec<Expr> {
        let mut aggr_func = Vec::with_capacity(aggrs.len());
        for aggr in aggrs {
            let &(tp, id) = aggr;
            let col_ref = build_expr(ExprType::ColumnRef, Some(id), None);
            aggr_func.push(build_expr(tp, None, Some(col_ref)));
        }
        aggr_func
    }

    #[test]
    fn test_aggregation() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
            new_col_info(4, types::FLOAT),
            new_col_info(5, types::DOUBLE),
        ];
        let raw_data = vec![
            vec![
                Datum::I64(1),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(1.0),
                Datum::F64(1.0),
            ],
            vec![
                Datum::I64(2),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(2.0),
                Datum::F64(2.0),
            ],
            vec![
                Datum::I64(3),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
                Datum::F64(3.0),
                Datum::F64(3.0),
            ],
            vec![
                Datum::I64(4),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
                Datum::F64(4.0),
                Datum::F64(4.0),
            ],
            vec![
                Datum::I64(5),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(5.into()),
                Datum::F64(5.0),
                Datum::F64(5.0),
            ],
            vec![
                Datum::I64(6),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
                Datum::F64(6.0),
                Datum::F64(6.0),
            ],
            vec![
                Datum::I64(7),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(6.into()),
                Datum::F64(7.0),
                Datum::F64(7.0),
            ],
        ];
        let table_data = gen_table_data(tid, &cis, &raw_data);
        let mut test_store = TestStore::new(&table_data);
        // init table scan meta
        let mut table_scan = TableScan::new();
        table_scan.set_table_id(tid);
        table_scan.set_columns(RepeatedField::from_vec(cis.clone()));
        // init TableScan Exectutor
        let key_ranges = vec![get_range(tid, i64::MIN, i64::MAX)];
        let (snapshot, start_ts) = test_store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let ts_ect = TableScanExecutor::new(&table_scan, key_ranges, store).unwrap();

        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![1, 2];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(RepeatedField::from_vec(group_by));
        let aggr_funcs = vec![
            (ExprType::Avg, 0),
            (ExprType::Count, 2),
            (ExprType::Sum, 3),
            (ExprType::Avg, 4),
        ];
        let aggr_funcs = build_aggr_func(&aggr_funcs);
        aggregation.set_agg_func(RepeatedField::from_vec(aggr_funcs));
        // init Aggregation Executor
        let mut aggr_ect = HashAggExecutor::new(
            aggregation,
            Arc::new(EvalContext::default()),
            Arc::new(cis),
            Box::new(ts_ect),
        ).unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(row) = aggr_ect.next().unwrap() {
            row_data.push(row.data);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![
            (
                3 as u64,
                Decimal::from(7),
                3 as u64,
                7.0 as f64,
                3 as u64,
                7.0 as f64,
                b"a".as_ref(),
                Decimal::from(7),
            ),
            (
                2 as u64,
                Decimal::from(9),
                2 as u64,
                9.0 as f64,
                2 as u64,
                9.0 as f64,
                b"b".as_ref(),
                Decimal::from(8),
            ),
            (
                1 as u64,
                Decimal::from(5),
                1 as u64,
                5.0 as f64,
                1 as u64,
                5.0 as f64,
                b"f".as_ref(),
                Decimal::from(5),
            ),
            (
                1 as u64,
                Decimal::from(7),
                1 as u64,
                7.0 as f64,
                1 as u64,
                7.0 as f64,
                b"f".as_ref(),
                Decimal::from(6),
            ),
        ];
        let expect_col_cnt = 8;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = row.value.as_slice().decode().unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
            assert_eq!(ds[4], Datum::from(expect_cols.4));
        }
        let expected_counts = vec![raw_data.len() as i64, expect_row_cnt as i64];
        let mut counts = Vec::with_capacity(2);
        aggr_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }
}
