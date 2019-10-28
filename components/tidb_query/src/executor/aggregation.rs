// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::Ordering;
use std::mem;
use std::sync::Arc;

use tipb::{Aggregation, Expr, ExprType};

use indexmap::map::Entry as OrderMapEntry;
use indexmap::IndexMap as OrderMap;

use super::aggregate::{self, AggrFunc};
use super::{Executor, ExprColumnRefVisitor, Row};
use crate::codec::datum::{self, Datum};
use crate::execute_stats::ExecuteStats;
use crate::expr::{EvalConfig, EvalContext, EvalWarnings, Expression};
use crate::storage::IntervalRange;
use crate::Result;

const SINGLE_GROUP: &[u8] = b"SingleGroup";

struct AggFuncExpr {
    args: Vec<Expression>,
    tp: ExprType,
    eval_buffer: Vec<Datum>,
}

impl AggFuncExpr {
    fn batch_build(ctx: &mut EvalContext, expr: Vec<Expr>) -> Result<Vec<AggFuncExpr>> {
        expr.into_iter()
            .map(|v| AggFuncExpr::build(ctx, v))
            .collect()
    }

    fn build(ctx: &mut EvalContext, mut expr: Expr) -> Result<AggFuncExpr> {
        let args = Expression::batch_build(ctx, expr.take_children().into())?;
        let tp = expr.get_tp();
        let eval_buffer = Vec::with_capacity(args.len());
        Ok(AggFuncExpr {
            args,
            tp,
            eval_buffer,
        })
    }

    fn eval_args(&mut self, ctx: &mut EvalContext, row: &[Datum]) -> Result<()> {
        self.eval_buffer.clear();
        for arg in &self.args {
            self.eval_buffer.push(arg.eval(ctx, row)?);
        }
        Ok(())
    }
}

impl dyn AggrFunc {
    fn update_with_expr(
        &mut self,
        ctx: &mut EvalContext,
        expr: &mut AggFuncExpr,
        row: &[Datum],
    ) -> Result<()> {
        expr.eval_args(ctx, row)?;
        self.update(ctx, &mut expr.eval_buffer)?;
        Ok(())
    }
}

struct AggExecutor<Src: Executor> {
    group_by: Vec<Expression>,
    aggr_func: Vec<AggFuncExpr>,
    executed: bool,
    ctx: EvalContext,
    related_cols_offset: Vec<usize>, // offset of related columns
    src: Src,
}

impl<Src: Executor> AggExecutor<Src> {
    fn new(
        group_by: Vec<Expr>,
        aggr_func: Vec<Expr>,
        eval_config: Arc<EvalConfig>,
        src: Src,
    ) -> Result<Self> {
        // collect all cols used in aggregation
        let mut visitor = ExprColumnRefVisitor::new(src.get_len_of_columns());
        visitor.batch_visit(&group_by)?;
        visitor.batch_visit(&aggr_func)?;
        let mut ctx = EvalContext::new(eval_config);
        Ok(AggExecutor {
            group_by: Expression::batch_build(&mut ctx, group_by)?,
            aggr_func: AggFuncExpr::batch_build(&mut ctx, aggr_func)?,
            executed: false,
            ctx,
            related_cols_offset: visitor.column_offsets(),
            src,
        })
    }

    fn next(&mut self) -> Result<Option<Vec<Datum>>> {
        if let Some(row) = self.src.next()? {
            let row = row.take_origin()?;
            row.inflate_cols_with_offsets(&mut self.ctx, &self.related_cols_offset)
                .map(Some)
        } else {
            Ok(None)
        }
    }

    fn get_group_by_cols(&mut self, row: &[Datum]) -> Result<Vec<Datum>> {
        if self.group_by.is_empty() {
            return Ok(Vec::default());
        }
        let mut vals = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let v = expr.eval(&mut self.ctx, row)?;
            vals.push(v);
        }
        Ok(vals)
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(&mut self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Src::StorageStats) {
        self.src.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.src.take_scanned_range()
    }
}
// HashAggExecutor deals with the aggregate functions.
// When Next() is called, it reads all the data from src
// and updates all the values in group_key_aggrs, then returns a result.
pub struct HashAggExecutor<Src: Executor> {
    inner: AggExecutor<Src>,
    group_key_aggrs: OrderMap<Vec<u8>, Vec<Box<dyn AggrFunc>>>,
    cursor: usize,
}

impl<Src: Executor> HashAggExecutor<Src> {
    pub fn new(mut meta: Aggregation, eval_config: Arc<EvalConfig>, src: Src) -> Result<Self> {
        let group_bys = meta.take_group_by().into();
        let aggs = meta.take_agg_func().into();
        let inner = AggExecutor::new(group_bys, aggs, eval_config, src)?;
        Ok(HashAggExecutor {
            inner,
            group_key_aggrs: OrderMap::new(),
            cursor: 0,
        })
    }

    fn get_group_key(&mut self, row: &[Datum]) -> Result<Vec<u8>> {
        let group_by_cols = self.inner.get_group_by_cols(row)?;
        if group_by_cols.is_empty() {
            let single_group = Datum::Bytes(SINGLE_GROUP.to_vec());
            return Ok(box_try!(datum::encode_value(
                &mut self.inner.ctx,
                &[single_group],
            )));
        }
        let res = box_try!(datum::encode_value(&mut self.inner.ctx, &group_by_cols,));
        Ok(res)
    }

    fn aggregate(&mut self) -> Result<()> {
        while let Some(cols) = self.inner.next()? {
            let group_key = self.get_group_key(&cols)?;
            match self.group_key_aggrs.entry(group_key) {
                OrderMapEntry::Vacant(e) => {
                    let mut aggrs = Vec::with_capacity(self.inner.aggr_func.len());
                    for expr in &mut self.inner.aggr_func {
                        let mut aggr = aggregate::build_aggr_func(expr.tp)?;
                        aggr.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
                        aggrs.push(aggr);
                    }
                    e.insert(aggrs);
                }
                OrderMapEntry::Occupied(e) => {
                    let aggrs = e.into_mut();
                    for (expr, aggr) in self.inner.aggr_func.iter_mut().zip(aggrs) {
                        aggr.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
                    }
                }
            }
        }
        Ok(())
    }
}

impl<Src: Executor> Executor for HashAggExecutor<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        if !self.inner.executed {
            self.aggregate()?;
            self.inner.executed = true;
        }

        match self.group_key_aggrs.get_index_mut(self.cursor) {
            Some((mut group_key, aggrs)) => {
                self.cursor += 1;
                let mut aggr_cols = Vec::with_capacity(2 * self.inner.aggr_func.len());

                // calc all aggr func
                for aggr in aggrs {
                    aggr.calc(&mut aggr_cols)?;
                }

                if !self.inner.group_by.is_empty() {
                    Ok(Some(Row::agg(
                        aggr_cols,
                        mem::replace(&mut group_key, Vec::new()),
                    )))
                } else {
                    Ok(Some(Row::agg(aggr_cols, Vec::default())))
                }
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.inner.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.inner.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.inner.get_len_of_columns()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.inner.take_eval_warnings()
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.inner.take_scanned_range()
    }
}

impl<Src: Executor> Executor for StreamAggExecutor<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        if self.inner.executed {
            return Ok(None);
        }

        while let Some(cols) = self.inner.next()? {
            self.has_data = true;
            let new_group = self.meet_new_group(&cols)?;
            let ret = if new_group {
                Some(self.get_partial_result()?)
            } else {
                None
            };
            for (expr, func) in self.inner.aggr_func.iter_mut().zip(&mut self.agg_funcs) {
                func.update_with_expr(&mut self.inner.ctx, expr, &cols)?;
            }
            if new_group {
                return Ok(ret);
            }
        }
        self.inner.executed = true;
        // If there is no data in the t, then whether there is 'group by' that can affect the result.
        // e.g. select count(*) from t. Result is 0.
        // e.g. select count(*) from t group by c. Result is empty.
        if !self.has_data && !self.inner.group_by.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.get_partial_result()?))
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.inner.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.inner.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.inner.get_len_of_columns()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.inner.take_eval_warnings()
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.inner.take_scanned_range()
    }
}

// StreamAggExecutor deals with the aggregation functions.
// It assumes all the input data is sorted by group by key.
// When next() is called, it finds a group and returns a result for the same group.
pub struct StreamAggExecutor<Src: Executor> {
    inner: AggExecutor<Src>,
    // save partial agg result
    agg_funcs: Vec<Box<dyn AggrFunc>>,
    cur_group_row: Vec<Datum>,
    next_group_row: Vec<Datum>,
    count: i64,
    has_data: bool,
}

impl<Src: Executor> StreamAggExecutor<Src> {
    pub fn new(eval_config: Arc<EvalConfig>, src: Src, mut meta: Aggregation) -> Result<Self> {
        let group_bys: Vec<_> = meta.take_group_by().into();
        let aggs = meta.take_agg_func().into();
        let group_len = group_bys.len();
        let inner = AggExecutor::new(group_bys, aggs, eval_config, src)?;
        // Get aggregation functions.
        let mut funcs = Vec::with_capacity(inner.aggr_func.len());
        for expr in &inner.aggr_func {
            let agg = aggregate::build_aggr_func(expr.tp)?;
            funcs.push(agg);
        }

        Ok(StreamAggExecutor {
            inner,
            agg_funcs: funcs,
            cur_group_row: Vec::with_capacity(group_len),
            next_group_row: Vec::with_capacity(group_len),
            count: 0,
            has_data: false,
        })
    }

    fn meet_new_group(&mut self, row: &[Datum]) -> Result<bool> {
        let mut cur_group_by_cols = self.inner.get_group_by_cols(row)?;
        if cur_group_by_cols.is_empty() {
            return Ok(false);
        }

        // first group
        if self.cur_group_row.is_empty() {
            mem::swap(&mut self.cur_group_row, &mut cur_group_by_cols);
            return Ok(false);
        }
        let mut meet_new_group = false;
        for (prev, cur) in self.cur_group_row.iter().zip(cur_group_by_cols.iter()) {
            if prev.cmp(&mut self.inner.ctx, cur)? != Ordering::Equal {
                meet_new_group = true;
                break;
            }
        }
        if meet_new_group {
            mem::swap(&mut self.next_group_row, &mut cur_group_by_cols);
        }
        Ok(meet_new_group)
    }

    // get_partial_result gets a result for the same group.
    fn get_partial_result(&mut self) -> Result<Row> {
        let mut cols = Vec::with_capacity(2 * self.agg_funcs.len() + self.cur_group_row.len());
        // Calculate all aggregation funcutions.
        for (i, agg_func) in self.agg_funcs.iter_mut().enumerate() {
            agg_func.calc(&mut cols)?;
            let agg = aggregate::build_aggr_func(self.inner.aggr_func[i].tp)?;
            *agg_func = agg;
        }

        // Get the values of 'group by'.
        if !self.inner.group_by.is_empty() {
            cols.extend_from_slice(self.cur_group_row.as_slice());
            mem::swap(&mut self.cur_group_row, &mut self.next_group_row);
        }

        self.count += 1;
        Ok(Row::agg(cols, Vec::default()))
    }
}

#[cfg(test)]
mod tests {
    use std::i64;

    use tidb_query_datatype::FieldTypeTp;
    use tikv_util::collections::HashMap;
    use tipb::ColumnInfo;
    use tipb::{Expr, ExprType};

    use super::super::index_scan::tests::IndexTestWrapper;
    use super::super::index_scan::IndexScanExecutor;
    use super::super::tests::*;
    use super::*;
    use crate::codec::datum::{self, Datum};
    use crate::codec::mysql::decimal::Decimal;
    use crate::codec::table;

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
        let mut ctx = EvalContext::default();
        let mut v: Vec<_> = idx_vals
            .iter()
            .map(|&(ref cid, ref value)| {
                expect_row.insert(*cid, datum::encode_key(&mut ctx, &[value.clone()]).unwrap());
                value.clone()
            })
            .collect();
        v.push(Datum::I64(handle));
        let encoded = datum::encode_key(&mut ctx, &v).unwrap();
        let idx_key = table::encode_index_seek_key(table_id, index_id, &encoded);
        (expect_row, idx_key)
    }

    pub fn prepare_index_data(
        table_id: i64,
        index_id: i64,
        cols: Vec<ColumnInfo>,
        idx_vals: Vec<Vec<(i64, Datum)>>,
    ) -> TableData {
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
        TableData {
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
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];
        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![0, 1];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(group_by.into());
        let funcs = vec![(ExprType::Count, 0), (ExprType::Sum, 1), (ExprType::Avg, 1)];
        let agg_funcs = build_aggr_func(&funcs);
        aggregation.set_agg_func(agg_funcs.into());

        // test no row
        let idx_vals = vec![];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len();
        let unique = false;
        let wrapper = IndexTestWrapper::new(unique, idx_data);
        let is_executor = IndexScanExecutor::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
        )
        .unwrap();
        let expect_row_cnt = 0;
        let mut row_data = Vec::with_capacity(1);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.value);
        }
        assert_eq!(row_data.len(), expect_row_cnt);
        let expected_counts = vec![idx_row_cnt];
        let mut exec_stats = ExecuteStats::new(0);
        agg_ect.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);

        // test one row
        let idx_vals = vec![vec![
            (2, Datum::Bytes(b"a".to_vec())),
            (3, Datum::Dec(12.into())),
        ]];
        let idx_data = prepare_index_data(tid, idx_id, col_infos.clone(), idx_vals);
        let idx_row_cnt = idx_data.kv_data.len();
        let unique = false;
        let wrapper = IndexTestWrapper::new(unique, idx_data);
        let is_executor = IndexScanExecutor::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation.clone(),
        )
        .unwrap();
        let expect_row_cnt = 1;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.get_binary(&mut EvalContext::default()).unwrap());
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
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut exec_stats = ExecuteStats::new(0);
        agg_ect.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);

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
        let idx_row_cnt = idx_data.kv_data.len();
        let wrapper = IndexTestWrapper::new(unique, idx_data);
        let is_executor = IndexScanExecutor::index_scan(
            wrapper.scan,
            EvalContext::default(),
            wrapper.ranges,
            wrapper.store,
            unique,
            false,
        )
        .unwrap();
        // init the stream aggregation executor
        let mut agg_ect = StreamAggExecutor::new(
            Arc::new(EvalConfig::default()),
            Box::new(is_executor),
            aggregation,
        )
        .unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = agg_ect.next().unwrap() {
            row_data.push(row.get_binary(&mut EvalContext::default()).unwrap());
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
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
        }
        let expected_counts = vec![idx_row_cnt];
        let mut exec_stats = ExecuteStats::new(0);
        agg_ect.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);
    }

    #[test]
    fn test_hash_agg() {
        // prepare data and store
        let tid = 1;
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
            new_col_info(4, FieldTypeTp::Float),
            new_col_info(5, FieldTypeTp::Double),
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

        let key_ranges = vec![get_range(tid, i64::MIN, i64::MAX)];
        let ts_ect = gen_table_scan_executor(tid, cis, &raw_data, Some(key_ranges));

        // init aggregation meta
        let mut aggregation = Aggregation::default();
        let group_by_cols = vec![1, 2];
        let group_by = build_group_by(&group_by_cols);
        aggregation.set_group_by(group_by.into());
        let aggr_funcs = vec![
            (ExprType::Avg, 0),
            (ExprType::Count, 2),
            (ExprType::Sum, 3),
            (ExprType::Avg, 4),
        ];
        let aggr_funcs = build_aggr_func(&aggr_funcs);
        aggregation.set_agg_func(aggr_funcs.into());
        // init the hash aggregation executor
        let mut aggr_ect =
            HashAggExecutor::new(aggregation, Arc::new(EvalConfig::default()), ts_ect).unwrap();
        let expect_row_cnt = 4;
        let mut row_data = Vec::with_capacity(expect_row_cnt);
        while let Some(Row::Agg(row)) = aggr_ect.next().unwrap() {
            row_data.push(row.get_binary(&mut EvalContext::default()).unwrap());
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
            let ds = datum::decode(&mut row.as_slice()).unwrap();
            assert_eq!(ds.len(), expect_col_cnt);
            assert_eq!(ds[0], Datum::from(expect_cols.0));
            assert_eq!(ds[1], Datum::from(expect_cols.1));
            assert_eq!(ds[2], Datum::from(expect_cols.2));
            assert_eq!(ds[3], Datum::from(expect_cols.3));
            assert_eq!(ds[4], Datum::from(expect_cols.4));
        }
        let expected_counts = vec![raw_data.len()];
        let mut exec_stats = ExecuteStats::new(0);
        aggr_ect.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.scanned_rows_per_range);
    }
}
