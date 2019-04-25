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

        // When there are errors during the `next_batch()` in the src executor, it means that the
        // first several rows do not have error, which should be filtered according to predicate
        // in this executor. So we actually don't care whether or not there are errors from src
        // executor.

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
                    // TODO: Rows before we meeting an evaluation error are innocent.
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

            fn box_clone(&self) -> Box<dyn RpnFunction> {
                Box::new(*self)
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
                .push_fn_call(FnFoo, FieldTypeTp::LongLong)
                .build()],
        );

        // When source executor returns empty rows, selection executor should process correctly.
        // No errors should be generated and the predicate function should not be called.

        let r = exec.next_batch(1);
        // The scan rows parameter has no effect for mock executor. We don't care.
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these data:
    ///
    /// == Schema ==
    /// Col0 (Int)      Col1(Real)
    /// == Call #1 ==
    /// 1               NULL
    /// NULL            7.0
    /// == Call #2 ==
    /// == Call #3 ==
    /// NULL            NULL
    /// (drained)
    fn make_src_executor_using_fixture_1() -> MockExecutor {
        MockExecutor::new(
            vec![FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(1), None]),
                        VectorValue::Real(vec![None, Some(7.0)]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None]),
                        VectorValue::Real(vec![None]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    /// Tests the scenario that there is no predicate or there is a predicate but always returns
    /// true (no data is filtered).
    #[test]
    fn test_no_predicate_or_predicate_always_true() {
        // Build a selection executor without predicate.
        let exec_no_predicate =
            |src_exec: MockExecutor| BatchSelectionExecutor::new_for_test(src_exec, vec![]);

        // Build a selection executor with a predicate that always returns true.
        let exec_predicate_true = |src_exec: MockExecutor| {
            let predicate = RpnExpressionBuilder::new()
                .push_constant(1i64, FieldTypeTp::LongLong)
                .build();
            BatchSelectionExecutor::new_for_test(src_exec, vec![predicate])
        };

        let executor_builders: Vec<Box<dyn std::boxed::FnBox(MockExecutor) -> _>> =
            vec![Box::new(exec_no_predicate), Box::new(exec_predicate_true)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_using_fixture_1();

            let mut exec = exec_builder(src_exec);

            // The selection executor should return data as it is.

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 2);
            assert_eq!(r.data.columns_len(), 2);
            assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(1), None]);
            assert_eq!(r.data[1].decoded().as_real_slice(), &[None, Some(7.0)]);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 1);
            assert_eq!(r.data.columns_len(), 2);
            assert_eq!(r.data[0].decoded().as_int_slice(), &[None]);
            assert_eq!(r.data[1].decoded().as_real_slice(), &[None]);
            assert!(r.is_drained.unwrap());
        }
    }

    /// Tests the scenario that the predicate always returns false.
    #[test]
    fn test_predicate_always_false() {
        let src_exec = make_src_executor_using_fixture_1();

        let predicate = RpnExpressionBuilder::new()
            .push_constant(0i64, FieldTypeTp::LongLong)
            .build();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        // The selection executor should always return empty rows.

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    /// This function returns 1 when the value is even, 0 otherwise.
    #[derive(Debug, Clone, Copy)]
    struct FnIsEven;

    impl FnIsEven {
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v: &Option<i64>,
        ) -> Result<Option<i64>> {
            let r = match v {
                None => Some(0),
                Some(v) => {
                    if v % 2 == 0 {
                        Some(1)
                    } else {
                        Some(0)
                    }
                }
            };
            Ok(r)
        }
    }

    impl RpnFunction for FnIsEven {
        fn name(&self) -> &'static str {
            "FnIsEven"
        }

        fn args_len(&self) -> usize {
            1
        }

        fn eval(
            &self,
            rows: usize,
            context: &mut EvalContext,
            payload: RpnFnCallPayload<'_>,
        ) -> Result<VectorValue> {
            // This function should never be called because we filter no rows
            crate::coprocessor::dag::rpn_expr::function::Helper::eval_1_arg(
                rows,
                Self::call,
                context,
                payload,
            )
        }

        fn box_clone(&self) -> Box<dyn RpnFunction> {
            Box::new(*self)
        }
    }

    /// Builds an executor that will return these data:
    ///
    /// == Schema ==
    /// Col0 (Int)      Col1(Int)       Col2(Int)
    /// == Call #1 ==
    /// 4               NULL            1
    /// NULL            NULL            2
    /// 2               4               3
    /// NULL            2               4
    /// == Call #2 ==
    /// == Call #3 ==
    /// NULL            NULL            2
    /// (drained)
    fn make_src_executor_using_fixture_2() -> MockExecutor {
        MockExecutor::new(
            vec![
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
                FieldTypeTp::LongLong.into(),
            ],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(4), None, Some(2), None]),
                        VectorValue::Int(vec![None, None, Some(4), Some(2)]),
                        VectorValue::Int(vec![Some(1), Some(2), Some(3), Some(4)]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::empty(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None]),
                        VectorValue::Int(vec![None]),
                        VectorValue::Int(vec![Some(2)]),
                    ]),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    /// Tests the scenario that the predicate returns both true and false. Rows that predicate
    /// returns false should be removed from the result.
    #[test]
    fn test_predicate_1() {
        let src_exec = make_src_executor_using_fixture_2();

        // Use FnIsEven(column[0]) as the predicate.

        let predicate = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(FnIsEven, FieldTypeTp::LongLong)
            .build();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 2);
        assert_eq!(r.data.columns_len(), 3);
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(4), Some(2)]);
        assert_eq!(r.data[1].decoded().as_int_slice(), &[None, Some(4)]);
        assert_eq!(r.data[2].decoded().as_int_slice(), &[Some(1), Some(3)]);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_predicate_2() {
        let src_exec = make_src_executor_using_fixture_2();

        // Use FnIsEven(column[1]) as the predicate.

        let predicate = RpnExpressionBuilder::new()
            .push_column_ref(1)
            .push_fn_call(FnIsEven, FieldTypeTp::LongLong)
            .build();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 2);
        assert_eq!(r.data.columns_len(), 3);
        assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(2), None]);
        assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(4), Some(2)]);
        assert_eq!(r.data[2].decoded().as_int_slice(), &[Some(3), Some(4)]);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.unwrap());
    }

    /// Tests the scenario that there are multiple predicates. Only the row that all predicates
    /// return true should be remained.
    #[test]
    fn test_multiple_predicate_1() {
        // Use [FnIsEven(column[0]), FnIsEven(column[1])] as the predicate.

        let predicate: Vec<_> = (0..=1)
            .map(|offset| {
                RpnExpressionBuilder::new()
                    .push_column_ref(offset)
                    .push_fn_call(FnIsEven, FieldTypeTp::LongLong)
                    .build()
            })
            .collect();

        for predicates in vec![
            // Swap predicates should produce same results.
            vec![predicate[0].clone(), predicate[1].clone()],
            vec![predicate[1].clone(), predicate[0].clone()],
        ] {
            let src_exec = make_src_executor_using_fixture_2();
            let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 1);
            assert_eq!(r.data.columns_len(), 3);
            assert_eq!(r.data[0].decoded().as_int_slice(), &[Some(2)]);
            assert_eq!(r.data[1].decoded().as_int_slice(), &[Some(4)]);
            assert_eq!(r.data[2].decoded().as_int_slice(), &[Some(3)]);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(r.is_drained.unwrap());
        }
    }

    #[test]
    fn test_multiple_predicate_2() {
        let predicate: Vec<_> = (0..=2)
            .map(|offset| {
                RpnExpressionBuilder::new()
                    .push_column_ref(offset)
                    .push_fn_call(FnIsEven, FieldTypeTp::LongLong)
                    .build()
            })
            .collect();

        for predicates in vec![
            // Swap predicates should produce same results.
            vec![
                predicate[0].clone(),
                predicate[1].clone(),
                predicate[2].clone(),
            ],
            vec![
                predicate[1].clone(),
                predicate[2].clone(),
                predicate[0].clone(),
            ],
        ] {
            let src_exec = make_src_executor_using_fixture_2();
            let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(r.data.rows_len(), 0);
            assert!(r.is_drained.unwrap());
        }
    }

    #[test]
    fn test_predicate_error() {
        /// This function returns error when value is None.
        #[derive(Debug, Clone, Copy)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<i64>,
            ) -> Result<Option<i64>> {
                match v {
                    None => Err(Error::Other(box_err!("foo"))),
                    Some(v) => Ok(Some(*v)),
                }
            }
        }

        impl RpnFunction for FnFoo {
            fn name(&self) -> &'static str {
                "FnFoo"
            }

            fn args_len(&self) -> usize {
                1
            }

            fn eval(
                &self,
                rows: usize,
                context: &mut EvalContext,
                payload: RpnFnCallPayload<'_>,
            ) -> Result<VectorValue> {
                // This function should never be called because we filter no rows
                crate::coprocessor::dag::rpn_expr::function::Helper::eval_1_arg(
                    rows,
                    Self::call,
                    context,
                    payload,
                )
            }

            fn box_clone(&self) -> Box<dyn RpnFunction> {
                Box::new(*self)
            }
        }

        // The built data is as follows:
        //
        // == Schema ==
        // Col0 (Int)       Col1(Int)
        // == Call #1 ==
        // 4                4
        // 1                2
        // 2                NULL
        // 1                NULL
        // == Call #2 ==
        // (drained)
        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()],
            vec![
                BatchExecuteResult {
                    data: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(4), Some(1), Some(2), Some(1)]),
                        VectorValue::Int(vec![Some(4), Some(2), None, None]),
                    ]),
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

        // When evaluating predicates[0], there will be no error. However we will meet errors for
        // predicates[1].

        let predicates = (0..=1)
            .map(|offset| {
                RpnExpressionBuilder::new()
                    .push_column_ref(offset)
                    .push_fn_call(FnFoo, FieldTypeTp::LongLong)
                    .build()
            })
            .collect();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

        // TODO: A more precise result is that the first two rows are returned and error starts from
        // the third row.

        let r = exec.next_batch(1);
        assert_eq!(r.data.rows_len(), 0);
        assert!(r.is_drained.is_err());
    }
}
