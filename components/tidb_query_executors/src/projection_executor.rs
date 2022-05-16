// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use tidb_query_common::{storage::IntervalRange, Result};
use tidb_query_datatype::{
    codec::{batch::LazyBatchColumnVec, data_type::*},
    expr::{EvalConfig, EvalContext},
};
use tidb_query_expr::{RpnExpression, RpnExpressionBuilder};
use tipb::{Expr, FieldType, Projection};

use crate::interface::*;

pub struct BatchProjectionExecutor<Src: BatchExecutor> {
    context: EvalContext,
    src: Src,
    schema: Vec<FieldType>,

    exprs: Vec<RpnExpression>,
}

// We assign a dummy type `Box<dyn BatchExecutor<StorageStats = ()>>` so that we can omit the type
// when calling `check_supported`.
impl BatchProjectionExecutor<Box<dyn BatchExecutor<StorageStats = ()>>> {
    /// Checks whether this executor can be used.
    #[inline]
    pub fn check_supported(descriptor: &Projection) -> Result<()> {
        let exprs = descriptor.get_exprs();
        for e in exprs {
            RpnExpressionBuilder::check_expr_tree_supported(e)?;
        }
        Ok(())
    }
}

fn get_schema_from_exprs(child_schema: &[FieldType], exprs: &[RpnExpression]) -> Vec<FieldType> {
    exprs
        .iter()
        .map(|expr: &RpnExpression| -> FieldType { expr.ret_field_type(child_schema).clone() })
        .collect::<Vec<FieldType>>()
}

impl<Src: BatchExecutor> BatchProjectionExecutor<Src> {
    #[cfg(test)]
    pub fn new_for_test(src: Src, exprs: Vec<RpnExpression>) -> Self {
        let schema = get_schema_from_exprs(src.schema(), &exprs);

        Self {
            context: EvalContext::default(),
            src,
            schema,
            exprs,
        }
    }

    pub fn new(config: Arc<EvalConfig>, src: Src, exprs_def: Vec<Expr>) -> Result<Self> {
        let mut exprs = Vec::with_capacity(exprs_def.len());
        let mut ctx = EvalContext::new(config);
        for def in exprs_def {
            exprs.push(RpnExpressionBuilder::build_from_expr_tree(
                def,
                &mut ctx,
                src.schema().len(),
            )?);
        }
        let schema = get_schema_from_exprs(src.schema(), &exprs);

        Ok(Self {
            context: ctx,
            src,
            schema,
            exprs,
        })
    }
}

impl<Src: BatchExecutor> BatchExecutor for BatchProjectionExecutor<Src> {
    type StorageStats = Src::StorageStats;

    #[inline]
    fn schema(&self) -> &[FieldType] {
        &self.schema
    }

    #[inline]
    fn next_batch(&mut self, scan_rows: usize) -> BatchExecuteResult {
        let mut src_result = self.src.next_batch(scan_rows);
        let child_schema = self.src.schema();
        let mut eval_result = Vec::with_capacity(self.schema().len());
        let BatchExecuteResult {
            mut is_drained,
            mut logical_rows,
            mut warnings,
            ..
        } = src_result;
        let logical_len = logical_rows.len();
        let physical_len = src_result.physical_columns.rows_len();

        if is_drained.is_ok() && logical_len != 0 {
            for expr in &self.exprs {
                match expr.eval(
                    &mut self.context,
                    child_schema,
                    &mut src_result.physical_columns,
                    &logical_rows,
                    logical_len,
                ) {
                    Err(e) => {
                        is_drained = is_drained.and(Err(e));
                        logical_rows.clear();
                        break;
                    }
                    Ok(col) => {
                        if col.is_scalar() {
                            eval_result.push(VectorValue::from_scalar(
                                col.scalar_value().unwrap(),
                                physical_len,
                            ));
                        } else {
                            // since column often refer to vector values, we can't easily
                            // transfer the ownership, so we use clone here.
                            eval_result.push(col.vector_value().unwrap().as_ref().clone());
                        }
                    }
                }
            }
        }

        warnings.merge(&mut self.context.warnings);
        BatchExecuteResult {
            physical_columns: LazyBatchColumnVec::from(eval_result),
            logical_rows,
            is_drained,
            warnings,
        }
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
    use tidb_query_codegen::rpn_fn;
    use tidb_query_datatype::{codec::batch::LazyBatchColumnVec, expr::EvalWarnings, FieldTypeTp};

    use super::*;
    use crate::util::mock_executor::MockExecutor;

    #[test]
    fn test_empty_rows() {
        #[rpn_fn]
        fn foo() -> Result<Option<i64>> {
            // This function should never be called because we evaluate no rows
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
                        VectorValue::Int(vec![None].into()),
                        VectorValue::Real(vec![None].into()),
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

        let mut exec = BatchProjectionExecutor::new_for_test(
            src_exec,
            vec![
                RpnExpressionBuilder::new_for_test()
                    .push_fn_call_for_test(foo_fn_meta(), 0, FieldTypeTp::LongLong)
                    .build_for_test(),
            ],
        );

        // When source executor returns empty rows, projection executor should process correctly.
        // No errors should be generated and the expression functions should not be called.

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
                        VectorValue::Int(vec![None, None, Some(1), None, Some(5)].into()),
                        VectorValue::Real(
                            vec![Real::new(7.0).ok(), Real::new(-5.0).ok(), None, None, None]
                                .into(),
                        ),
                    ]),
                    logical_rows: vec![2, 0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![None].into()),
                        VectorValue::Real(vec![None].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(1), None].into()),
                        VectorValue::Real(vec![None, None].into()),
                    ]),
                    logical_rows: vec![1],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_constant_projection() {
        let src_exec = make_src_executor_using_fixture_1();
        let exprs = vec![
            RpnExpressionBuilder::new_for_test()
                .push_constant_for_test(1i64)
                .build_for_test(),
        ];
        let mut exec = BatchProjectionExecutor::new_for_test(src_exec, exprs);
        assert_eq!(exec.schema().len(), 1);
        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[2, 0]);
        assert_eq!(r.physical_columns.columns_len(), 1);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            vec![Some(1), Some(1), Some(1), Some(1), Some(1)]
        );
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.columns_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1]);
        assert_eq!(r.physical_columns.columns_len(), 1);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            vec![Some(1), Some(1)]
        );
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_full_projection() {
        let src_exec = make_src_executor_using_fixture_1();
        let exprs = vec![
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(0)
                .build_for_test(),
            RpnExpressionBuilder::new_for_test()
                .push_column_ref_for_test(1)
                .build_for_test(),
        ];
        let mut exec = BatchProjectionExecutor::new_for_test(src_exec, exprs);
        assert_eq!(exec.schema().len(), 2);
        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[2, 0]);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            vec![None, None, Some(1), None, Some(5)]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_real_vec(),
            vec![Real::new(7.0).ok(), Real::new(-5.0).ok(), None, None, None]
        );
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert_eq!(r.physical_columns.columns_len(), 0);
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[1]);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            vec![Some(1), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_real_vec(),
            vec![None, None]
        );
        assert!(r.is_drained.unwrap());
    }

    /// This function returns 1 when the value is even, 0 otherwise.
    #[rpn_fn(nullable)]
    fn is_even(v: Option<&i64>) -> Result<Option<i64>> {
        let r = match v.cloned() {
            None => None,
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
                        VectorValue::Int(vec![Some(2), Some(1), None, Some(4), None].into()),
                        VectorValue::Int(vec![Some(4), None, Some(2), None, None].into()),
                        VectorValue::Int(vec![Some(3), Some(-1), Some(4), Some(1), Some(2)].into()),
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
                        VectorValue::Int(vec![None, Some(1)].into()),
                        VectorValue::Int(vec![None, Some(-1)].into()),
                        VectorValue::Int(vec![Some(2), Some(42)].into()),
                    ]),
                    logical_rows: vec![0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        )
    }

    #[test]
    fn test_simple_projection() {
        let src_exec = make_src_executor_using_fixture_2();
        let expr1 = RpnExpressionBuilder::new_for_test()
            .push_column_ref_for_test(0)
            .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
            .build_for_test();
        let expr2 = RpnExpressionBuilder::new_for_test()
            .push_column_ref_for_test(2)
            .push_fn_call_for_test(is_even_fn_meta(), 1, FieldTypeTp::LongLong)
            .build_for_test();

        let mut exec = BatchProjectionExecutor::new_for_test(src_exec, vec![expr1, expr2]);
        let r = exec.next_batch(1);
        assert_eq!(&r.logical_rows, &[3, 4, 0, 2]);
        assert_eq!(r.physical_columns.columns_len(), 2);
        assert_eq!(
            r.physical_columns[0].decoded().to_int_vec(),
            vec![Some(1), None, Some(1), None]
        );
        assert_eq!(
            r.physical_columns[1].decoded().to_int_vec(),
            vec![Some(0), Some(1), Some(0), Some(1)]
        );
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(!r.is_drained.unwrap());

        let r = exec.next_batch(1);
        assert_eq!(r.logical_rows, &[0]);
        assert_eq!(r.physical_columns[0].decoded().to_int_vec(), vec![None]);
        assert_eq!(r.physical_columns[1].decoded().to_int_vec(), vec![Some(1)]);
        assert!(r.is_drained.unwrap());
    }

    #[test]
    fn test_projection_error() {
        /// This function returns error when value is None.
        #[rpn_fn(nullable)]
        fn foo(v: Option<&i64>) -> Result<Option<i64>> {
            match v.cloned() {
                None => Err(other_err!("foo")),
                Some(v) => Ok(Some(v)),
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
                        VectorValue::Int(vec![Some(1), Some(4), None, Some(1), Some(2)].into()),
                        VectorValue::Int(vec![None, Some(4), None, Some(2), None].into()),
                    ]),
                    logical_rows: vec![1, 3, 4, 0],
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(false),
                },
                BatchExecuteResult {
                    physical_columns: LazyBatchColumnVec::from(vec![
                        VectorValue::Int(vec![Some(-5)].into()),
                        VectorValue::Int(vec![Some(5)].into()),
                    ]),
                    logical_rows: Vec::new(),
                    warnings: EvalWarnings::default(),
                    is_drained: Ok(true),
                },
            ],
        );

        // When evaluating expr[0], there will be no error. However we will meet errors for
        // expr[1].

        let exprs = (0..=1)
            .map(|offset| {
                RpnExpressionBuilder::new_for_test()
                    .push_column_ref_for_test(offset)
                    .push_fn_call_for_test(foo_fn_meta(), 1, FieldTypeTp::LongLong)
                    .build_for_test()
            })
            .collect();
        let mut exec = BatchProjectionExecutor::new_for_test(src_exec, exprs);

        let r = exec.next_batch(1);
        assert!(r.logical_rows.is_empty());
        assert!(r.is_drained.is_err());
    }
}
