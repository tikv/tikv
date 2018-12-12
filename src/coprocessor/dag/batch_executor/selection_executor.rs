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

use std::convert::TryFrom;
use std::sync::Arc;

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::Expr;

use super::interface::*;
use coprocessor::codec::batch::{LazyBatchColumn, LazyBatchColumnVec};
use coprocessor::dag::executor::ExprColumnRefVisitor;
use coprocessor::dag::expr::{EvalConfig, EvalContext, Expression};
use coprocessor::*;

pub struct BatchSelectionExecutor<Src: BatchExecutor> {
    context: ExecutorContext,
    src: Src,
    eval_context: EvalContext,
    conditions: Vec<Expression>,

    /// The index (in `context.columns_info`) of referred columns in expression.
    referred_columns: Vec<usize>,

    /// A `LazyBatchColumnVec` to hold unused data produced in `next_batch`.
    pending_data: LazyBatchColumnVec,
    /// Unused errors during `next_batch` if any.
    pending_error: Option<Error>,
    has_thrown_error: bool,
    /// Whether underlying executor has drained or there are error during filter and no more buffer
    /// is needed to be fetched.
    has_drained: bool,
}

impl<Src: BatchExecutor> BatchSelectionExecutor<Src> {
    pub fn new(
        context: ExecutorContext,
        src: Src,
        conditions: Vec<Expr>,
        eval_config: Arc<EvalConfig>,
    ) -> Result<Self> {
        let referred_columns = {
            let mut ref_visitor = ExprColumnRefVisitor::new(context.columns_info.len());
            ref_visitor.batch_visit(&conditions)?;
            ref_visitor.column_offsets()
        };
        // println!("referred_columns = {:?}", referred_columns);
        let pending_data = {
            let mut is_column_referred = vec![false; context.columns_info.len()];
            for idx in &referred_columns {
                is_column_referred[*idx] = true;
            }
            let mut columns = Vec::with_capacity(context.columns_info.len());
            for (idx, column_info) in context.columns_info.iter().enumerate() {
                if is_column_referred[idx] || column_info.get_pk_handle() {
                    let eval_type = EvalType::try_from(column_info.tp())
                        .map_err(|e| Error::Other(box_err!(e)))?;
                    columns.push(LazyBatchColumn::decoded_with_capacity_and_tp(
                        2048, eval_type,
                    ));
                } else {
                    columns.push(LazyBatchColumn::raw_with_capacity(2048));
                }
            }
            LazyBatchColumnVec::from(columns)
        };

        let eval_context = EvalContext::new(eval_config);
        let conditions = Expression::batch_build(&eval_context, conditions)?;

        Ok(Self {
            context,
            src,
            eval_context,
            conditions,
            referred_columns,

            pending_data,
            pending_error: None,
            has_thrown_error: false,
            has_drained: false,
        })
    }

    fn filter_rows(&mut self, data: &LazyBatchColumnVec, retain_map: &mut [bool]) -> Result<()> {
        use coprocessor::codec::datum;

        // For each row, calculate a remain map.
        // FIXME: We are still evaluating row by row here.
        let cols_len = self.context.columns_info.len();
        let rows_len = data.rows_len();

        // `cols` only contains referred columns.
        let mut cols = vec![datum::Datum::Null; cols_len];
        for ri in 0..rows_len {
            // Convert data into datum
            for ci in &self.referred_columns {
                cols[*ci] = data[*ci].decoded().to_datum(ri);
            }

            let mut retain = true;
            for filter in &self.conditions {
                // If there are any errors during filter rows, we just return.
                // Elements in the retain map remains `false` for uncovered rows.
                let val = filter.eval(&mut self.eval_context, &cols)?;
                if !val.into_bool(&mut self.eval_context)?.unwrap_or(false) {
                    retain = false;
                    break;
                }
            }

            retain_map[ri] = retain;
            // Clear columns, for next row evaluation
            for i in 0..cols_len {
                cols[i] = datum::Datum::Null;
            }
        }
        Ok(())
    }

    /// Fetches and filter next batch rows from the underlying executor,
    /// fill into the pending buffer.
    #[inline]
    fn fill_buffer(&mut self) {
        assert!(!self.has_drained);

        // Fetch some rows
        let mut result = self.src.next_batch(1024); // TODO: Remove magic number

        for idx in &self.referred_columns {
            result.data.ensure_column_decoded(
                *idx,
                self.eval_context.cfg.tz,
                &self.context.columns_info[*idx],
            );
            // TODO: what if ensure_column_decoded failed?
        }
        // Filter fetched rows. If there are errors, less rows will be retained.
        let mut retain_map = vec![false; result.data.rows_len()];
        let filter_result = self.filter_rows(&result.data, &mut retain_map);

        // Append by retain map.
        self.pending_data
            .append_by_index(&mut result.data, |idx| retain_map[idx]);
        self.pending_error = self
            .pending_error
            .take()
            .or_else(|| filter_result.err())
            .or(result.error);

        self.has_drained = self.pending_error.is_some() || result.data.rows_len() == 0;
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchSelectionExecutor<Src> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        assert!(!self.has_thrown_error);

        // Ensure there are `expect_rows` in the pending buffer if not drained.
        while !self.has_drained && self.pending_data.rows_len() < expect_rows {
            self.fill_buffer();
        }

        // Retrive first `expect_rows` from the pending buffer.
        // If pending buffer is not sifficient, pending_error is also carried.
        let data = self.pending_data.take_and_collect(expect_rows);

        let error = if data.rows_len() < expect_rows {
            self.pending_error.take()
        } else {
            None
        };

        if error.is_some() {
            self.has_thrown_error = true;
        }

        BatchExecuteResult { data, error }
    }
}
