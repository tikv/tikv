// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tipb::Expr;
use tipb::FieldType;
use tipb::Selection;

use crate::interface::*;
use tidb_query_common::storage::IntervalRange;
use tidb_query_common::Result;
use tidb_query_datatype::codec::data_type::*;
use tidb_query_datatype::expr::{EvalConfig, EvalContext};
use tidb_query_vec_expr::RpnStackNode;
use tidb_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

pub struct BatchSelectionExecutor<Src: BatchExecutor> {
    context: EvalContext,
    src: Src,

    conditions: Vec<RpnExpression>,
}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchSelectionExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Selection) -> Result<()> {
        let conditions = descriptor.get_conditions();
        for c in conditions {
            RpnExpressionBuilder::check_expr_tree_supported(c)?;
        }
        Ok(())
    }
}

impl<Src: BatchExecutor> BatchSelectionExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(src: Src, conditions: Vec<RpnExpression>) -> Self {
        Self {
            context: EvalContext::default(),
            src,
            conditions,
        }
    }

    pub fn new(config: Arc<EvalConfig>, src: Src, conditions_def: Vec<Expr>) -> Result<Self> {
        let mut conditions = Vec::with_capacity(conditions_def.len());
        let mut ctx = EvalContext::new(config);
        for def in conditions_def {
            conditions.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &mut ctx,
                src.schema().len(),
            )?);
        }

        Ok(Self {
            context: ctx,
            src,
            conditions,
        })
    }

    /// Accepts source result and mutates its `logical_rows` according to predicates.
    ///
    /// When errors are returned, it means there are errors during the evaluation. Currently
    /// we treat this situation as "completely failed".
    fn handle_src_result(&mut self, src_result: &mut BatchExecuteResult) -> Result<()> {
        // When there are errors in `src_result`, it means that the first several rows do not
        // have error, which should be filtered according to predicate in this executor.
        // So we actually don't care whether or not there are errors from src executor.

        // TODO: Avoid allocation.
        let mut src_logical_rows_copy = Vec::with_capacity(src_result.logical_rows.len());
        let mut condition_index = 0;
        while condition_index < self.conditions.len() && !src_result.logical_rows.is_empty() {
            src_logical_rows_copy.clear();
            src_logical_rows_copy.extend_from_slice(&src_result.logical_rows);

            match self.conditions[condition_index].eval(
                &mut self.context,
                self.src.schema(),
                &mut src_result.physical_columns,
                &src_logical_rows_copy,
                src_logical_rows_copy.len(),
            )? {
                RpnStackNode::Scalar { value, .. } => {
                    update_logical_rows_by_scalar_value(
                        &mut src_result.logical_rows,
                        &mut self.context,
                        value,
                    )?;
                }
                RpnStackNode::Vector { value, .. } => {
                    let eval_result_logical_rows = value.logical_rows();
                    match_template_evaluable! {
                        TT, match value.as_ref() {
                            VectorValue::TT(eval_result) => {
                                update_logical_rows_by_vector_value(
                                    &mut src_result.logical_rows,
                                    &mut self.context,
                                    eval_result,
                                    eval_result_logical_rows,
                                )?;
                            },
                        }
                    }
                }
            }

            condition_index += 1;
        }

        Ok(())
    }
}

fn update_logical_rows_by_scalar_value(
    logical_rows: &mut Vec<usize>,
    ctx: &mut EvalContext,
    value: &ScalarValue,
) -> Result<()> {
    let b = value.as_mysql_bool(ctx)?;
    if !b {
        // No rows should be preserved
        logical_rows.clear();
    }
    Ok(())
}

fn update_logical_rows_by_vector_value<T: AsMySQLBool>(
    logical_rows: &mut Vec<usize>,
    ctx: &mut EvalContext,
    eval_result: &[Option<T>],
    eval_result_logical_rows: &[usize],
) -> tidb_query_common::error::Result<()> {
    let mut err_result = Ok(());
    let mut logical_index = 0;
    logical_rows.retain(|_| {
        // We don't care the physical index indicated by `logical_rows`, since what's in there
        // does not affect the filtering. Instead, the eval result in corresponding logical index
        // matters.

        let eval_result_physical_index = eval_result_logical_rows[logical_index];
        logical_index += 1;

        match eval_result[eval_result_physical_index].as_mysql_bool(ctx) {
            Err(e) => {
                if err_result.is_ok() {
                    err_result = Err(e);
                }
                false
            }
            Ok(b) => b,
        }
    });
    err_result
}

impl<Src: BatchExecutor> BatchExecutor for BatchSelectionExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        // The selection executor's schema comes from its child.
        self.src.schema()
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut src_result = self.src.next_batch(scan_rows);

        if let Err(e) = self.handle_src_result(&mut src_result) {
            // TODO: Rows before we meeting an evaluation error are innocent.
            src_result.is_drained = src_result.is_drained.and(Err(e));
            src_result.logical_rows.clear();
        } else {
            // Only append warnings when there is no error during filtering because
            // we clear the data when there are errors.
            src_result.warnings.merge(&mut self.context.warnings);
        }

        src_result
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.src.collect_storage_stats(dest);
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.src.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.src.can_be_cached()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_codegen::rpn_fn;
    use tidb_query_datatype::FieldTypeTp;

    use crate::util::mock_executor::MockExecutor;
    use tidb_query_datatype::codec::batch::LazyBatchColumnVec;
    use tidb_query_datatype::expr::EvalWarnings;

    #[test]
    fn test_empty_rows() {
        #[rpn_fn]
        fn foo() -> Result<Option<i64>> {
            // This function should never be called because we filter no rows
            unreachable!()
        }

        let src_exec = MockExecutor::new(
            vec![FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()],
            vec![
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None]),
                        VectorValue::Real(vec![None]),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        let mut exec = BatchSelectionExecutor::new_for_test(
            src_exec,
            vec![RpnExpressionBuilder::new_for_test()
                .push_fn_call_for_test(foo_fn_meta(), 0, FieldTypeTp::LongLong)
                .build_for_test()],
        );

        // When source executor returns empty rows, selection executor should process correctly.
        // No errors should be generated and the predicate function should not be called.

        let r = exec.next_batch(1);
        // The scan rows parameter has no effect for mock executor. We don't care.
        // FIXME: A compiler bug prevented us write:
        //    |         assert_eq!(r.logical_rows.as_slice(), &[]);
        //    |         ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^ cannot infer type
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.unwrap());
    }

    /// Builds an executor that will return these logical data:
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None, None, Some(1), None, Some(5)]),
                        VectorValue::Real(vec![
                            Real::new(7.0).ok(),
                            Real::new(-5.0).ok(),
                            None,
                            None,
                            None,
                        ]),
                    ]),
                    logical_rows: vec![2, 0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None]),
                        VectorValue::Real(vec![None]),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(1), None]),
                        VectorValue::Real(vec![None, None]),
                    ]),
                    logical_rows: vec![1],
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
            let predicate = RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(1i64)
                .build_for_test();
            BatchSelectionExecutor::new_for_test(src_exec, vec![predicate])
        };

        let executor_builders: Vec<Box<dyn FnOnce(MockExecutor) -> _>> =
            vec![Box::new(exec_no_predicate), Box::new(exec_predicate_true)];

        for exec_builder in executor_builders {
            let src_exec = make_src_executor_using_fixture_1();

            let mut exec = exec_builder(src_exec);

            // The selection executor should return data as it is.

            let r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[2, 0]);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[1]);
            assert!(r.is_drained.unwrap());
        }
    }

    /// Tests the scenario that the predicate always returns false.
    #[test]
    fn test_predicate_always_false() {
        let src_exec = make_src_executor_using_fixture_1();

        let predicate = RpnExpressionBuilder::new_for_test()
            .push_constant_for_test(0i64)
            .build_for_test();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        // The selection executor should always return empty rows.

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.unwrap());
    }

    /// This function returns 1 when the value is even, 0 otherwise.
    #[rpn_fn]
    fn is_even(v: &Option<i64>) -> Result<Option<i64>> {
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

    /// Builds an executor that will return these logical data:
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(2), Some(1), None, Some(4), None]),
                        VectorValue::Int(vec![Some(4), None, Some(2), None, None]),
                        VectorValue::Int(vec![Some(3), Some(-1), Some(4), Some(1), Some(2)]),
                    ]),
                    logical_rows: vec![3, 4, 0, 2],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::empty(),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None, Some(1)]),
                        VectorValue::Int(vec![None, Some(-1)]),
                        VectorValue::Int(vec![Some(2), Some(42)]),
                    ]),
                    logical_rows: vec![0],
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

        let predicate = RpnExpressionBuilder::new_for_test()
            .push_column_ref_for_test(0)
            .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
            .build_for_test();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[3, 0]);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_predicate_2() {
        let src_exec = make_src_executor_using_fixture_2();

        // Use is_even(column[1]) as the predicate.

        let predicate = RpnExpressionBuilder::new_for_test()
            .push_column_ref_for_test(1)
            .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
            .build_for_test();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, vec![predicate]);

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[0, 2]);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.unwrap());
    }

    /// Tests the scenario that there are multiple predicates. Only the row that all predicates
    /// return true should be remained.
    #[test]
    fn test_multiple_predicate_1() {
        // Use [is_even(column[0]), is_even(column[1])] as the predicate.

        let predicate: Vec<_> = (0..=1)
            .map(|offset| {
                move || {
                    RpnExpressionBuilder::new_for_test()
                        .push_column_ref_for_test(offset)
                        .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
                        .build_for_test()
                }
            })
            .collect();

        for predicates in vec![
            // Swap predicates should produce same results.
            vec![predicate[0](), predicate[1]()],
            vec![predicate[1](), predicate[0]()],
        ] {
            let src_exec = make_src_executor_using_fixture_2();
            let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

            let r = exec.next_batch(1);
            assert_eq!(&r.logical_rows, &[0]);
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(r.is_drained.unwrap());
        }
    }

    #[test]
    fn test_multiple_predicate_2() {
        let predicate: Vec<_> = (0..=2)
            .map(|offset| {
                move || {
                    RpnExpressionBuilder::new_for_test()
                        .push_column_ref_for_test(offset)
                        .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
                        .build_for_test()
                }
            })
            .collect();

        for predicates in vec![
            // Swap predicates should produce same results.
            vec![predicate[0](), predicate[1](), predicate[2]()],
            vec![predicate[1](), predicate[2](), predicate[0]()],
        ] {
            let src_exec = make_src_executor_using_fixture_2();
            let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(!r.is_drained.unwrap());

            let r = exec.next_batch(1);
            assert!(r.logical_rows.is_empty());
            assert!(r.is_drained.unwrap());
        }
    }

    #[test]
    fn test_predicate_error() {
        /// This function returns error when value is None.
        #[rpn_fn]
        fn foo(v: &Option<i64>) -> Result<Option<i64>> {
            match v {
                None => Err(other_err!("foo")),
                Some(v) => Ok(Some(*v)),
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
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(1), Some(4), None, Some(1), Some(2)]),
                        VectorValue::Int(vec![None, Some(4), None, Some(2), None]),
                    ]),
                    logical_rows: vec![1, 3, 4, 0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(-5)]),
                        VectorValue::Int(vec![Some(5)]),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        // When evaluating predicates[0], there will be no error. However we will meet errors for
        // predicates[1].

        let predicates = (0..=1)
            .map(|offset| {
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(offset)
                    .push_fn_call_for_test(foo_fn_meta(), 1, FieldTypeTp::LongLong)
                    .build_for_test()
            })
            .collect();
        let mut exec = BatchSelectionExecutor::new_for_test(src_exec, predicates);

        // TODO: A more precise result is that the first two rows are returned and error starts from
        // the third row.

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.is_err());
    }
}
