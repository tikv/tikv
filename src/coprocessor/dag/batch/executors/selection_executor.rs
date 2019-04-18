// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tipb::executor::Selection;
use tipb::expression::Expr;
use tipb::expression::FieldType;

use super::super::interface::*;
use crate::coprocessor::dag::batch::statistics::ExecSummaryCollectorDisabled;
use crate::coprocessor::dag::expr::{EvalConfig, EvalContext};
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::{Error, Result};

pub struct BatchSelectionExecutor<C: ExecSummaryCollector, Src: BatchExecutor> {
    summary_collector: C,
    context: EvalContext,
    src: Src,

    conditions: Vec<RpnExpression>,
}

impl BatchSelectionExecutor<ExecSummaryCollectorDisabled, Box<dyn BatchExecutor>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Selection) -> Result<()> {
        let conditions = descriptor.get_conditions();
        for c in conditions {
            RpnExpressionBuilder::check_expr_tree_supported(c).map_err(|e| {
                Error::Other(box_err!("Unable to use BatchTableScanExecutor: {}", e))
            })?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchSelectionExecutor<ExecSummaryCollectorDisabled, Src> {
    #[cfg(test)]
    pub fn new_for_test(src: Src, conditions: Vec<RpnExpression>) -> Self {
        Self {
            summary_collector: ExecSummaryCollectorDisabled,
            context: EvalContext::default(),
            src,
            conditions,
        }
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchSelectionExecutor<C, Src> {
    pub fn new(
        summary_collector: C,
        config: Arc<EvalConfig>,
        src: Src,
        conditions_def: Vec<Expr>,
    ) -> Result<Self> {
        let mut conditions = Vec::with_capacity(conditions_def.len());
        for def in conditions_def {
            conditions.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &config.tz,
                src.schema().len(),
            )?);
        }

        Ok(Self {
            summary_collector,
            context: EvalContext::new(config),
            src,
            conditions,
        })
    }

    #[inline]
    fn handle_next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut src_result = self.src.next_batch(scan_rows);

        // We don't care whether there are errors during the `next_batch()` in the src executor.

        let rows_len = src_result.data.rows_len();
        if rows_len > 0 {
            let mut base_retain_map = vec![true; rows_len];
            let mut head_retain_map = vec![false; rows_len];

            for condition in &self.conditions {
                let r = condition.eval_as_mysql_bools(
                    &mut self.context,
                    rows_len,
                    self.src.schema(),
                    &mut src_result.data,
                    head_retain_map.as_mut_slice(),
                );
                if let Err(e) = r {
                    // TODO: We should not return error when it comes from unused rows.
                    // When there are errors *during filtering*, let's reset data to empty because
                    // the data is not filtered at all in this case.
                    src_result.is_drained = src_result.is_drained.and(Err(e));
                    src_result.data.truncate(0);
                    return src_result;
                }
                for i in 0..rows_len {
                    base_retain_map[i] &= head_retain_map[i];
                }
            }

            // TODO: When there are many conditions, it would be better to filter column each time.

            src_result
                .data
                .retain_rows_by_index(|idx| base_retain_map[idx]);
        }

        // Only append warnings when there is no error during filtering because we clear the data
        // when there are errors.
        src_result.warnings.merge(&mut self.context.warnings);
        src_result
    }
}

impl<C: ExecSummaryCollector, Src: BatchExecutor> BatchExecutor for BatchSelectionExecutor<C, Src> {
    #[inline]
    fn schema(&self) -> &[FieldType] {
        // The selection executor's schema comes from its child.
        self.src.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let timer = self.summary_collector.on_start_batch();
        let result = self.handle_next_batch(scan_rows);
        self.summary_collector
            .on_finish_batch(timer, result.data.rows_len());
        result
    }

    #[inline]
    fn collect_statistics(&mut self, destination: &mut BatchExecuteStatistics) {
        self.src.collect_statistics(destination);
        self.summary_collector
            .collect_into(&mut destination.summary_per_executor);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::builder::FieldTypeBuilder;
    use cop_datatype::FieldTypeTp;

    use crate::coprocessor::codec::batch::LazyBatchColumnVec;
    use crate::coprocessor::codec::data_type::VectorValue;
    use crate::coprocessor::dag::batch::executors::util::mock_executor::MockExecutor;
    use crate::coprocessor::dag::expr::EvalWarnings;
    use crate::coprocessor::dag::rpn_expr::types::RpnFnCallPayload;
    use crate::coprocessor::dag::rpn_expr::RpnFunction;

    #[test]
    fn test_empty_rows() {
        #[derive(Debug, Clone, Copy)]
        struct FnFoo;

        impl RpnFunction for FnFoo {
            fn name(&self) -> &'static str {
                "FnFoo"
            }

            fn args_len(&self) -> usize {
                0
            }

            fn eval(
                &self,
                _rows: usize,
                _context: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
            ) -> Result<VectorValue> {
                // This function should never be called because we filter no rows
                unreachable!()
            }
        }

        let src_exec = MockExecutor::new(
            vec![],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchSelectionExecutor::new_for_test(
            src_exec,
            vec![RpnExpressionBuilder::new()
                .push_fn_call(FnFoo, FieldTypeBuilder::new().tp(FieldTypeTp::LongLong))
                .build()],
        );

        // When source executor returns empty rows, selection executor should process correctly.
        // No errors should be generated and the predicate function should not be called.

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }
}
