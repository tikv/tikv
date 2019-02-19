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

use std::sync::Arc;

use tipb::expression::Expr;

use super::interface::*;
use crate::coprocessor::dag::executor::ExprColumnRefVisitor;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::dag::rpn_expr::{RpnExpressionEvalContext, RpnExpressionNodeVec};
use crate::coprocessor::Result;

pub struct BatchSelectionExecutor<Src: BatchExecutor> {
    context: ExecutorContext,
    src: Src,
    eval_context: RpnExpressionEvalContext,
    conditions: Vec<RpnExpressionNodeVec>,

    /// The index (in `context.columns_info`) of referred columns in expression.
    referred_columns: Vec<usize>,

    is_ended: bool,
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

        let eval_context = RpnExpressionEvalContext::new(eval_config);
        let conditions = conditions
            .into_iter()
            .map(|def| RpnExpressionNodeVec::build_from_def(def, eval_context.config.tz))
            .collect();

        Ok(Self {
            context,
            src,
            eval_context,
            conditions,
            referred_columns,

            is_ended: false,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchSelectionExecutor<Src> {
    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let mut result = self.src.next_batch(expect_rows);
        for idx in &self.referred_columns {
            let _ = result.data.ensure_column_decoded(
                *idx,
                self.eval_context.config.tz,
                &self.context.columns_info[*idx],
            );
            // TODO: what if ensure_column_decoded failed?
            // TODO: We should trim length of all columns to 0 when there is a decoding error.
        }

        let rows_len = result.data.rows_len();
        let mut base_retain_map = vec![true; rows_len];
        let mut head_retain_map = vec![false; rows_len];

        for condition in &self.conditions {
            condition.eval_as_mysql_bools(
                &mut self.eval_context,
                rows_len,
                &result.data,
                head_retain_map.as_mut_slice(),
            );
            for i in 0..rows_len {
                base_retain_map[i] &= head_retain_map[i];
            }
        }

        result.data.retain_rows_by_index(|idx| base_retain_map[idx]);
        result
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
    }
}
