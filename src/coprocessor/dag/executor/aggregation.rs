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

use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

use tipb::executor::Aggregation;
use tipb::expression::{Expr, ExprType};
use tipb::schema::ColumnInfo;

use util::collections::{OrderMap, OrderMapEntry};

use coprocessor::codec::datum::{self, approximate_size, Datum, DatumEncoder};
use coprocessor::codec::table::RowColsDict;
use coprocessor::dag::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use coprocessor::*;

use super::aggregate::{self, AggrFunc};
use super::ExecutorMetrics;
use super::{inflate_with_col_for_dag, Executor, ExprColumnRefVisitor, Row};

struct AggFuncExpr {
    args: Vec<Expression>,
    tp: ExprType,
}

impl AggFuncExpr {
    fn batch_build(ctx: &mut EvalContext, expr: Vec<Expr>) -> Result<Vec<AggFuncExpr>> {
        expr.into_iter()
            .map(|v| AggFuncExpr::build(ctx, v))
            .collect()
    }

    fn build(ctx: &mut EvalContext, mut expr: Expr) -> Result<AggFuncExpr> {
        let args = Expression::batch_build(ctx, expr.take_children().into_vec())?;
        let tp = expr.get_tp();
        Ok(AggFuncExpr { args, tp })
    }

    fn eval_args(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Vec<Datum>> {
        let mut res = Vec::with_capacity(self.args.len());
        for arg in &self.args {
            res.push(arg.eval(ctx, row)?);
        }
        Ok(res)
    }
}

impl AggrFunc {
    fn update_with_expr(
        &mut self,
        ctx: &mut EvalContext,
        expr: &AggFuncExpr,
        row: &[Datum],
    ) -> Result<()> {
        let vals = expr.eval_args(ctx, row)?;
        self.update(ctx, vals)?;
        Ok(())
    }
}

// HashAggExecutor deals with the aggregate functions.
// When Next() is called, it reads all the data from src
// and updates all the values in group_key_aggrs, then returns a result.
pub struct HashAggExecutor {
    group_by: Vec<Expression>,
    aggr_func: Vec<AggFuncExpr>,
    group_key_aggrs: OrderMap<Vec<u8>, Vec<Box<AggrFunc>>>,
    cursor: usize,
    executed: bool,
    ctx: EvalContext,
    cols: Arc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>, // offset of related columns
    src: Box<Executor + Send>,
    first_collect: bool,
}

impl HashAggExecutor {
    pub fn new(
        mut meta: Aggregation,
        eval_config: Arc<EvalConfig>,
        cols: Arc<Vec<ColumnInfo>>,
        src: Box<Executor + Send>,
    ) -> Result<HashAggExecutor> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new(cols.len());
        let group_by = meta.take_group_by().into_vec();
        visitor.batch_visit(&group_by)?;
        let aggr_func = meta.take_agg_func().into_vec();
        visitor.batch_visit(&aggr_func)?;
        let mut ctx = EvalContext::new(eval_config);
        Ok(HashAggExecutor {
            group_by: Expression::batch_build(&mut ctx, group_by)?,
            aggr_func: AggFuncExpr::batch_build(&mut ctx, aggr_func)?,
            group_key_aggrs: OrderMap::new(),
            cursor: 0,
            executed: false,
            ctx,
            cols,
            related_cols_offset: visitor.column_offsets(),
            src,
            first_collect: true,
        })
    }

    fn get_group_key(&mut self, row: &[Datum]) -> Result<Vec<u8>> {
        if self.group_by.is_empty() {
            let single_group = Datum::Bytes(SINGLE_GROUP.to_vec());
            return Ok(box_try!(datum::encode_value(&[single_group])));
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = expr.eval(&mut self.ctx, row)?;
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(row) = self.src.next()? {
            let cols = inflate_with_col_for_dag(
                &mut self.ctx,
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
                        aggr.update_with_expr(&mut self.ctx, expr, &cols)?;
                        aggrs.push(aggr);
                    }
                    e.insert(aggrs);
                }
                OrderMapEntry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.aggr_func.iter().zip(aggrs) {
                        aggr.update_with_expr(&mut self.ctx, expr, &cols)?;
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
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.aggregation += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }
}

impl Executor for StreamAggExecutor {
    fn next(&mut self) -> Result<Option<Row>> {
        if self.executed {
            return Ok(None);
        }

        while let Some(row) = self.src.next()? {
            let cols = inflate_with_col_for_dag(
                &mut self.ctx,
                &row.data,
                &self.cols,
                &self.related_cols_offset,
                row.handle,
            )?;
            self.has_data = true;
            let new_group = self.meet_new_group(&cols)?;
            let mut ret = if new_group {
                Some(self.get_partial_result()?)
            } else {
                None
            };
            for (expr, func) in self.agg_exprs.iter().zip(&mut self.agg_funcs) {
                func.update_with_expr(&mut self.ctx, expr, &cols)?;
            }
            if new_group {
                return Ok(ret);
            }
        }
        self.executed = true;
        // If there is no data in the t, then whether there is 'group by' that can affect the result.
        // e.g. select count(*) from t. Result is 0.
        // e.g. select count(*) from t group by c. Result is empty.
        if !self.has_data && !self.group_by_exprs.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.get_partial_result()?))
    }

    fn collect_output_counts(&mut self, counts: &mut Vec<i64>) {
        self.src.collect_output_counts(counts);
    }

    fn collect_metrics_into(&mut self, metrics: &mut ExecutorMetrics) {
        self.src.collect_metrics_into(metrics);
        if self.first_collect {
            metrics.executor_count.aggregation += 1;
            self.first_collect = false;
        }
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }
}

// StreamAggExecutor deals with the aggregation functions.
// It assumes all the input data is sorted by group by key.
// When next() is called, it finds a group and returns a result for the same group.
pub struct StreamAggExecutor {
    ctx: EvalContext,
    src: Box<Executor + Send>,

    executed: bool,
    group_by_exprs: Vec<Expression>,
    agg_exprs: Vec<AggFuncExpr>,
    agg_funcs: Vec<Box<AggrFunc>>,
    cols: Arc<Vec<ColumnInfo>>,
    related_cols_offset: Vec<usize>,
    cur_group_row: Vec<Datum>,
    next_group_row: Vec<Datum>,
    is_first_group: bool,
    count: i64,
    first_collect: bool,
    has_data: bool,
}

impl StreamAggExecutor {
    pub fn new(
        eval_config: Arc<EvalConfig>,
        src: Box<Executor + Send>,
        mut meta: Aggregation,
        columns: Arc<Vec<ColumnInfo>>,
    ) -> Result<StreamAggExecutor> {
        let mut visitor = ExprColumnRefVisitor::new(columns.len());
        let group_bys = meta.take_group_by().into_vec();
        visitor.batch_visit(&group_bys)?;
        let aggs = meta.take_agg_func().into_vec();
        visitor.batch_visit(&aggs)?;
        let group_len = group_bys.len();
        let mut ctx = EvalContext::new(eval_config);
        let exprs = AggFuncExpr::batch_build(&mut ctx, aggs)?;
        // Get aggregation functions.
        let mut funcs = Vec::with_capacity(exprs.len());
        for expr in &exprs {
            let agg = aggregate::build_aggr_func(expr.tp)?;
            funcs.push(agg);
        }

        Ok(StreamAggExecutor {
            src,
            executed: false,
            agg_exprs: exprs,
            agg_funcs: funcs,
            group_by_exprs: Expression::batch_build(&mut ctx, group_bys)?,
            ctx,
            related_cols_offset: visitor.column_offsets(),
            cols: columns,
            cur_group_row: Vec::with_capacity(group_len),
            next_group_row: Vec::with_capacity(group_len),
            is_first_group: true,
            count: 0,
            first_collect: true,
            has_data: false,
        })
    }

    fn meet_new_group(&mut self, row: &[Datum]) -> Result<bool> {
        if self.group_by_exprs.is_empty() {
            return Ok(false);
        }

        let mut tmp_group_row = Vec::with_capacity(self.group_by_exprs.len());
        let mut matched = !self.is_first_group;
        for (i, expr) in self.group_by_exprs.iter().enumerate() {
            let v = expr.eval(&mut self.ctx, row)?;
            if matched && v.cmp(&mut self.ctx, &self.cur_group_row[i])? != Ordering::Equal {
                matched = false;
            }
            tmp_group_row.push(v);
        }
        if self.is_first_group {
            mem::swap(&mut self.cur_group_row, &mut tmp_group_row);
            self.is_first_group = false;
            return Ok(false);
        }
        if matched {
            return Ok(false);
        }
        mem::swap(&mut self.next_group_row, &mut tmp_group_row);
        Ok(true)
    }

    // get_partial_result gets a result for the same group.
    fn get_partial_result(&mut self) -> Result<Row> {
        let mut agg_cols = Vec::with_capacity(2 * self.agg_funcs.len());
        // Calculate all aggregation funcutions.
        for (i, agg_func) in self.agg_funcs.iter_mut().enumerate() {
            agg_func.calc(&mut agg_cols)?;
            let agg = aggregate::build_aggr_func(self.agg_exprs[i].tp)?;
            *agg_func = agg;
        }
        // Get and decode the values of 'group by'.
        let mut group_key = Vec::with_capacity(0);
        if !self.group_by_exprs.is_empty() {
            group_key = box_try!(datum::encode_value(self.cur_group_row.as_slice()));
            mem::swap(&mut self.cur_group_row, &mut self.next_group_row);
        }
        // Construct row data.
        let value_size = group_key.len() + approximate_size(&agg_cols, false);
        let mut value = Vec::with_capacity(value_size);
        box_try!(value.encode(agg_cols.as_slice(), false));
        if !self.group_by_exprs.is_empty() {
            value.extend_from_slice(&group_key);
        }

        self.count += 1;

        Ok(Row {
            handle: 0,
            data: RowColsDict::new(map![], value),
        })
    }
}

#[cfg(test)]
mod test {
    use std::i64;

    use kvproto::kvrpcpb::IsolationLevel;
    use protobuf::RepeatedField;
    use tipb::executor::TableScan;
    use tipb::expression::{Expr, ExprType};

    use coprocessor::codec::datum::{self, Datum};
    use coprocessor::codec::mysql::decimal::Decimal;
    use coprocessor::codec::mysql::types;
    use coprocessor::codec::table;
    use storage::SnapshotStore;
    use util::codec::number::NumberEncoder;
    use util::collections::HashMap;

    use super::super::index_scan::test::IndexTestWrapper;
    use super::super::index_scan::IndexScanExecutor;
    use super::super::scanner::test::{get_range, new_col_info, Data, TestStore};
    use super::super::table_scan::TableScanExecutor;
    use super::super::topn::test::gen_table_data;
    use super::*;

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

    pub fn generate_index_data(
        table_id: i64,
        index_id: i64,
        handle: i64,
        idx_vals: Vec<(i64, Datum)>,
    ) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
        let mut expect_row = HashMap::default();
        let mut v: Vec<_> = idx_vals
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        v.push(Datum::I64(handle));
        let encoded = datum::encode_key(&v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(
        table_id: i64,
        index_id: i64,
        cols: Vec<ColumnInfo>,
        idx_vals: Vec<Vec<(i64, Datum)>>,
    ) -> Data {
        let mut kv_data = Vec::new();
        let mut expect_rows = Vec::new();

        let mut handle = 1;
        for val in idx_vals {
            let (expect_row, idx_key) =
                generate_index_data(table_id, index_id, i64::from(handle), val);
            expect_rows.push(expect_row);
            let value = vec![1; 0];
            kv_data.push((idx_key, value));
            handle += 1;
        }
        Data {
            kv_data,
            expect_rows,
            cols,
        }
    }

    #[test]
    fn test_stream_agg() {
        // prepare data and store
        let tid = 1;
        let idx_id = 1;
        let col_infos = vec![
            new_col_info(1, types::LONG_LONG),
            new_col_info(2, types::VARCHAR),
            new_col_info(3, types::NEW_DECIMAL),
        ];
        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![1, 2];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(RepeatedField::from_vec(group_by));
        let funcs = vec![(ExprType::Count, 1), (ExprType::Sum, 2), (ExprType::Avg, 2)];
        let agg_funcs = build_aggr_func(&funcs);
        aggregation.set_agg_func(RepeatedField::from_vec(agg_funcs));

        // test no row
        let idx_vals = vec![];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let unique = false;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, unique, true).unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
            Arc::new(col_infos.clone()),
        ).unwrap();
        let expect_row_cnt = 0;
        let mut row_data = Vec::with_capacity(1);
        while let Some(row) = agg_ect.next().unwrap() {
            row_data.push(row.data);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);

        // test one row
        let idx_vals = vec![vec![
            (2, Datum::Bytes(b"a".to_vec())),
            (3, Datum::Dec(12.into())),
        ]];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let unique = false;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, unique, true).unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
            Arc::new(col_infos.clone()),
        ).unwrap();
        let expect_row_cnt = 1;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(row) = agg_ect.next().unwrap() {
            row_data.push(row.data);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![(
            1 as u64,
            Decimal::from(12),
            1 as u64,
            Decimal::from(12),
            b"a".as_ref(),
            Decimal::from(12),
        )];
        let expect_col_cnt = 6;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = datum::decode(&mut row.value.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);

        // test multiple rows
        let idx_vals = vec![
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"c".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"c".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"b".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
            vec![(2, Datum::Bytes(b"b".to_vec())), (3, Datum::Dec(2.into()))],
            vec![(2, Datum::Bytes(b"a".to_vec())), (3, Datum::Dec(12.into()))],
        ];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len() as i64;
        let mut wrapper = IndexTestWrapper::new(unique, idx_data);
        let (snapshot, start_ts) = wrapper.store.get_snapshot();
        let store = SnapshotStore::new(snapshot, start_ts, IsolationLevel::SI, true);
        let is_executor =
            IndexScanExecutor::new(wrapper.scan, wrapper.ranges, store, unique, true).unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation,
            Arc::new(col_infos),
        ).unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(row) = agg_ect.next().unwrap() {
            row_data.push(row.data);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expect_row_data = vec![
            (
                3 as u64,
                Decimal::from(36),
                3 as u64,
                Decimal::from(36),
                b"a".as_ref(),
                Decimal::from(12),
            ),
            (
                2 as u64,
                Decimal::from(4),
                2 as u64,
                Decimal::from(4),
                b"b".as_ref(),
                Decimal::from(2),
            ),
            (
                1 as u64,
                Decimal::from(2),
                1 as u64,
                Decimal::from(2),
                b"c".as_ref(),
                Decimal::from(2),
            ),
            (
                1 as u64,
                Decimal::from(12),
                1 as u64,
                Decimal::from(12),
                b"c".as_ref(),
                Decimal::from(12),
            ),
        ];
        let expect_col_cnt = 6;
        for (row, expect_cols) in row_data.into_iter().zip(expect_row_data) {
            let ds = datum::decode(&mut row.value.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut counts = Vec::with_capacity(1);
        agg_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }

    #[test]
    fn test_hash_agg() {
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
        let ts_ect = TableScanExecutor::new(&table_scan, key_ranges, store, true).unwrap();

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
        // init the hash aggregation executor
        let mut aggr_ect = HashAggExecutor::new(
            aggregation,
            Arc::new(EvalConfig::default()),
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
            let ds = datum::decode(&mut row.value.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
            assert_eq!(ds[4], Datum::from(expect_cols.4));
        }
        let expected_counts = vec![raw_data.len() as i64];
        let mut counts = Vec::with_capacity(1);
        aggr_ect.collect_output_counts(&mut counts);
        assert_eq!(expected_counts, counts);
    }
}
