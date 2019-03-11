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
use tipb::expression::FieldType;

use super::interface::*;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::RpnExpressionNodeVec;
use crate::coprocessor::Result;

pub struct BatchSelectionExecutor<Src: BatchExecutor> {
    context: EvalContext,
    src: Src,
    conditions: Vec<RpnExpressionNodeVec>,

    is_ended: bool,
}

impl<Src: BatchExecutor> BatchSelectionExecutor<Src> {
    pub fn new(config: Arc<EvalConfig>, src: Src, conditions_def: Vec<Expr>) -> Result<Self> {
        let mut conditions = Vec::with_capacity(conditions_def.len());
        for def in conditions_def {
            conditions.push(RpnExpressionNodeVec::build_from_expr_tree(def, config.tz)?);
        }

        Ok(Self {
            context: EvalContext::new(config),
            src,
            conditions,
            is_ended: false,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchSelectionExecutor<Src> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        // The selection executor's schema comes from its child.
        self.src.schema()
    }

    #[inline]
    fn next_batch(&mut self, expect_rows: usize) -> BatchExecuteResult {
        assert!(!self.is_ended);

        let mut result = self.src.next_batch(expect_rows);

        let rows_len = result.data.rows_len();
        let mut base_retain_map = vec![true; rows_len];
        let mut head_retain_map = vec![false; rows_len];

        for condition in &self.conditions {
            let r = condition.eval_as_mysql_bools(
                &mut self.context,
                rows_len,
                self.src.schema(),
                &mut result.data,
                head_retain_map.as_mut_slice(),
            );
            if let Err(e) = r {
                // TODO: We should not return error when it comes from unused rows.
                result.is_drained = result.is_drained.and(Err(e));
                return result;
            }
            for i in 0..rows_len {
                base_retain_map[i] &= head_retain_map[i];
            }
        }

        result.data.retain_rows_by_index(|idx| base_retain_map[idx]);
        result.warnings.merge(&mut self.context.warnings);
        result
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
    }
}
