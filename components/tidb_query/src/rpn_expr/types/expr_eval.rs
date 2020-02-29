// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::FieldType;

use super::expr::{RpnExpression, RpnExpressionNode};
use super::RpnFnCallExtra;
use crate::batch::runner::BATCH_MAX_SIZE;
use crate::codec::batch::LazyBatchColumnVec;
use crate::codec::data_type::{ScalarValue, ScalarValueRef, VectorValue};
use crate::expr::EvalContext;
use crate::Result;

/// Identical logical row is a special case in expression evaluation that
/// the rows in physical_value are continuous and in order.
static IDENTICAL_LOGICAL_ROWS: [usize; BATCH_MAX_SIZE] = {
    let mut logical_rows = [0; BATCH_MAX_SIZE];
    let mut row = 0;
    while row < logical_rows.len() {
        logical_rows[row] = row;
        row += 1;
    }
    logical_rows
};

/// Represents a vector value node in the RPN stack.
///
/// It can be either an owned node or a reference node.
///
/// When node comes from a column reference, it is a reference node (both value and field_type
/// are references).
///
/// When nodes comes from an evaluated result, it is an owned node.
#[derive(Debug)]
pub enum RpnStackNodeVectorValue<'a> {
    Generated {
        // TODO: Maybe box it can be faster.
        physical_value: VectorValue,
    },
    Ref {
        physical_value: &'a VectorValue,
        logical_rows: &'a [usize],
    },
}

impl<'a> RpnStackNodeVectorValue<'a> {
    /// Gets a reference to the inner physical vector value.
    pub fn as_ref(&self) -> &VectorValue {
        match self {
            RpnStackNodeVectorValue::Generated { physical_value, .. } => &physical_value,
            RpnStackNodeVectorValue::Ref { physical_value, .. } => *physical_value,
        }
    }

    /// Gets a reference to the logical rows.
    pub fn logical_rows(&self) -> &[usize] {
        match self {
            RpnStackNodeVectorValue::Generated { physical_value } => {
                &IDENTICAL_LOGICAL_ROWS[0..physical_value.len()]
            }
            RpnStackNodeVectorValue::Ref { logical_rows, .. } => logical_rows,
        }
    }
}

/// A type for each node in the RPN evaluation stack. It can be one of a scalar value node or a
/// vector value node. The vector value node can be either an owned vector value or a reference.
#[derive(Debug)]
pub enum RpnStackNode<'a> {
    /// Represents a scalar value. Comes from a constant node in expression list.
    Scalar {
        value: &'a ScalarValue,
        field_type: &'a FieldType,
    },

    /// Represents a vector value. Comes from a column reference or evaluated result.
    Vector {
        value: RpnStackNodeVectorValue<'a>,
        field_type: &'a FieldType,
    },
}

impl<'a> RpnStackNode<'a> {
    /// Gets the field type.
    #[inline]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnStackNode::Scalar { field_type, .. } => field_type,
            RpnStackNode::Vector { field_type, .. } => field_type,
        }
    }

    /// Borrows the inner scalar value for `Scalar` variant.
    #[inline]
    pub fn scalar_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnStackNode::Scalar { value, .. } => Some(*value),
            RpnStackNode::Vector { .. } => None,
        }
    }

    /// Borrows the inner vector value for `Vector` variant.
    #[inline]
    pub fn vector_value(&self) -> Option<&RpnStackNodeVectorValue<'_>> {
        match self {
            RpnStackNode::Scalar { .. } => None,
            RpnStackNode::Vector { value, .. } => Some(&value),
        }
    }

    /// Whether this is a `Scalar` variant.
    #[inline]
    pub fn is_scalar(&self) -> bool {
        match self {
            RpnStackNode::Scalar { .. } => true,
            _ => false,
        }
    }

    /// Whether this is a `Vector` variant.
    #[inline]
    pub fn is_vector(&self) -> bool {
        match self {
            RpnStackNode::Vector { .. } => true,
            _ => false,
        }
    }

    /// Gets a reference of the element by logical index.
    ///
    /// If this is a `Scalar` variant, the returned reference will be the same for any index.
    ///
    /// # Panics
    ///
    /// Panics if index is out of range and this is a `Vector` variant.
    #[inline]
    pub fn get_logical_scalar_ref(&self, logical_index: usize) -> ScalarValueRef<'_> {
        match self {
            RpnStackNode::Vector { value, .. } => {
                let physical_vector = value.as_ref();
                let logical_rows = value.logical_rows();
                physical_vector.get_scalar_ref(logical_rows[logical_index])
            }
            RpnStackNode::Scalar { value, .. } => value.as_scalar_value_ref(),
        }
    }
}

impl RpnExpression {
    /// Evaluates the expression into a vector.
    ///
    /// If referred columns are not decoded, they will be decoded according to the given schema.
    ///
    /// # Panics
    ///
    /// Panics if the expression is not valid.
    ///
    /// Panics when referenced column does not have equal length as specified in `rows`.
    pub fn eval<'a>(
        &'a self,
        ctx: &mut EvalContext,
        schema: &'a [FieldType],
        input_physical_columns: &'a mut LazyBatchColumnVec,
        input_logical_rows: &'a [usize],
        output_rows: usize,
    ) -> Result<RpnStackNode<'a>> {
        // We iterate two times. The first time we decode all referred columns. The second time
        // we evaluate. This is to make Rust's borrow checker happy because there will be
        // mutable reference during the first iteration and we can't keep these references.
        self.ensure_columns_decoded(ctx, schema, input_physical_columns, input_logical_rows)?;
        self.eval_decoded(
            ctx,
            schema,
            input_physical_columns,
            input_logical_rows,
            output_rows,
        )
    }

    /// Decodes all referred columns which are not decoded. Then we ensure
    /// all referred columns are decoded.
    pub fn ensure_columns_decoded<'a>(
        &'a self,
        ctx: &mut EvalContext,
        schema: &'a [FieldType],
        input_physical_columns: &'a mut LazyBatchColumnVec,
        input_logical_rows: &[usize],
    ) -> Result<()> {
        for node in self.as_ref() {
            if let RpnExpressionNode::ColumnRef { offset, .. } = node {
                input_physical_columns[*offset].ensure_decoded(
                    ctx,
                    &schema[*offset],
                    input_logical_rows,
                )?;
            }
        }
        Ok(())
    }

    /// Evaluates the expression into a stack node. The input columns must be already decoded.
    ///
    /// It differs from `eval` in that `eval_decoded` needn't receive a mutable reference
    /// to `LazyBatchColumnVec`. However, since `eval_decoded` doesn't decode columns,
    /// it will panic if referred columns are not decoded.
    ///
    /// # Panics
    ///
    /// Panics if the expression is not valid.
    ///
    /// Panics if referred columns are not decoded.
    ///
    /// Panics when referenced column does not have equal length as specified in `rows`.
    pub fn eval_decoded<'a>(
        &'a self,
        ctx: &mut EvalContext,
        schema: &'a [FieldType],
        input_physical_columns: &'a LazyBatchColumnVec,
        input_logical_rows: &'a [usize],
        output_rows: usize,
    ) -> Result<RpnStackNode<'a>> {
        assert!(output_rows > 0);
        assert!(output_rows <= BATCH_MAX_SIZE);
        let mut stack = Vec::with_capacity(self.len());

        for node in self.as_ref() {
            match node {
                RpnExpressionNode::Constant { value, field_type } => {
                    stack.push(RpnStackNode::Scalar { value, field_type });
                }
                RpnExpressionNode::ColumnRef { offset } => {
                    let field_type = &schema[*offset];
                    let decoded_physical_column = input_physical_columns[*offset].decoded();
                    assert_eq!(input_logical_rows.len(), output_rows);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Ref {
                            physical_value: &decoded_physical_column,
                            logical_rows: input_logical_rows,
                        },
                        field_type,
                    });
                }
                RpnExpressionNode::FnCall {
                    func_meta,
                    args_len,
                    field_type: ret_field_type,
                    metadata,
                } => {
                    // Suppose that we have function call `Foo(A, B, C)`, the RPN nodes looks like
                    // `[A, B, C, Foo]`.
                    // Now we receives a function call `Foo`, so there are `[A, B, C]` in the stack
                    // as the last several elements. We will directly use the last N (N = number of
                    // arguments) elements in the stack as function arguments.
                    assert!(stack.len() >= *args_len);
                    let stack_slice_begin = stack.len() - *args_len;
                    let stack_slice = &stack[stack_slice_begin..];
                    let mut call_extra = RpnFnCallExtra { ret_field_type };
                    let ret = (func_meta.fn_ptr)(
                        ctx,
                        output_rows,
                        stack_slice,
                        &mut call_extra,
                        &**metadata,
                    )?;
                    stack.truncate(stack_slice_begin);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Generated {
                            physical_value: ret,
                        },
                        field_type: ret_field_type,
                    });
                }
            }
        }

        assert_eq!(stack.len(), 1);
        Ok(stack.into_iter().next().unwrap())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)]

    use super::*;

    use tidb_query_codegen::rpn_fn;
    use tidb_query_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::FieldType;
    use tipb_helper::ExprDefBuilder;

    use crate::codec::batch::LazyBatchColumn;
    use crate::codec::data_type::{Int, Real};
    use crate::codec::datum::{Datum, DatumEncoder};
    use crate::expr::EvalContext;
    use crate::rpn_expr::impl_arithmetic::*;
    use crate::rpn_expr::impl_compare::*;
    use crate::rpn_expr::{RpnExpressionBuilder, RpnFnMeta};
    use crate::Result;
    use test::{black_box, Bencher};

    /// Single constant node
    #[test]
    fn test_eval_single_constant_node() {
        let exp = RpnExpressionBuilder::new().push_constant(1.5f64).build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, &[], &mut columns, &[], 10);
        let val = result.unwrap();
        assert!(val.is_scalar());
        assert_eq!(*val.scalar_value().unwrap().as_real(), Real::new(1.5).ok());
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Creates fixture to be used in `test_eval_single_column_node_xxx`.
    fn new_single_column_node_fixture() -> (LazyBatchColumnVec, Vec<usize>, [FieldType; 2]) {
        let physical_columns = LazyBatchColumnVec::from(vec![
            {
                // this column is not referenced
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Real);
                col.mut_decoded().push_real(Real::new(1.0).ok());
                col.mut_decoded().push_real(None);
                col.mut_decoded().push_real(Real::new(7.5).ok());
                col.mut_decoded().push_real(None);
                col.mut_decoded().push_real(None);
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(None);
                col.mut_decoded().push_int(None);
                col.mut_decoded().push_int(Some(42));
                col
            },
        ]);
        let schema = [FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];
        let logical_rows = (0..5).collect();
        (physical_columns, logical_rows, schema)
    }

    /// Single column node
    #[test]
    fn test_eval_single_column_node_normal() {
        let (columns, logical_rows, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, &schema, &mut c, &logical_rows, 5);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(1), Some(5), None, None, Some(42)]
        );
        assert_eq!(
            val.vector_value().unwrap().logical_rows(),
            logical_rows.as_slice()
        );
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, &schema, &mut c, &[2, 0, 1], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        // Physical column is unchanged
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(1), Some(5), None, None, Some(42)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[2, 0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);

        let mut c = columns;
        let exp = RpnExpressionBuilder::new().push_column_ref(0).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, &schema, &mut c, &logical_rows, 5);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [Real::new(1.0).ok(), None, Real::new(7.5).ok(), None, None]
        );
        assert_eq!(
            val.vector_value().unwrap().logical_rows(),
            logical_rows.as_slice()
        );
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Single column node but row numbers in `eval()` does not match column length, should panic.
    #[test]
    fn test_eval_single_column_node_mismatch_rows() {
        let (columns, logical_rows, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = panic_hook::recover_safe(|| {
            // smaller row number
            let _ = exp.eval(&mut ctx, &schema, &mut c, &logical_rows, 4);
        });
        assert!(hooked_eval.is_err());

        let mut c = columns;
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = panic_hook::recover_safe(|| {
            // larger row number
            let _ = exp.eval(&mut ctx, &schema, &mut c, &logical_rows, 6);
        });
        assert!(hooked_eval.is_err());
    }

    /// Single function call node (i.e. nullary function)
    #[test]
    fn test_eval_single_fn_call_node() {
        #[rpn_fn]
        fn foo() -> Result<Option<i64>> {
            Ok(Some(42))
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(foo_fn_meta(), 0, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, &[], &mut columns, &[], 4);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(42), Some(42), Some(42), Some(42)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2, 3]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is scalar)
    #[test]
    fn test_eval_unary_function_scalar() {
        /// foo(v) performs v * 2.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.0))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, &[], &mut columns, &[], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [
                Real::new(3.0).ok(),
                Real::new(3.0).ok(),
                Real::new(3.0).ok()
            ]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Unary function (argument is vector)
    #[test]
    fn test_eval_unary_function_vector() {
        /// foo(v) performs v + 5.
        #[rpn_fn]
        fn foo(v: &Option<i64>) -> Result<Option<i64>> {
            Ok(v.map(|v| v + 5))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(None);
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[2, 0], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [None, Some(6)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is raw column). The column should be decoded.
    #[test]
    fn test_eval_unary_function_raw_column() {
        /// foo(v) performs v + 5.
        #[rpn_fn]
        fn foo(v: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v.unwrap() + 5))
        }

        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(-5)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(-7)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(3)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[2, 0, 1], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(8), Some(0), Some(-2)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are scalar, scalar)
    #[test]
    fn test_eval_binary_function_scalar_scalar() {
        /// foo(v) performs v1 + float(v2) - 1.
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() + v2.unwrap() as f64 - 1.0))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_constant(3i64)
            .push_fn_call(foo_fn_meta(), 2, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, &[], &mut columns, &[], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [
                Real::new(3.5).ok(),
                Real::new(3.5).ok(),
                Real::new(3.5).ok()
            ]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, scalar)
    #[test]
    fn test_eval_binary_function_vector_scalar() {
        /// foo(v) performs v1 - v2.
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<Real>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() - v2.unwrap()))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
            col.mut_decoded().push_real(Real::new(1.0).ok());
            col.mut_decoded().push_real(Real::new(5.5).ok());
            col.mut_decoded().push_real(Real::new(-4.3).ok());
            col
        }]);
        let schema = &[FieldTypeTp::Double.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_constant(1.5f64)
            .push_fn_call(foo_fn_meta(), 2, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[2, 0], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [
                Real::new(-5.8).ok(), // original row 2
                Real::new(-0.5).ok(), // original row 0
            ]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are scalar, vector)
    #[test]
    fn test_eval_binary_function_scalar_vector() {
        /// foo(v) performs v1 - float(v2).
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() - v2.unwrap() as f64))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(Some(-4));
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 2, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[1, 2], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [
                Real::new(-3.5).ok(), // original row 1
                Real::new(5.5).ok(),  // original row 2
            ]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, vector)
    #[test]
    fn test_eval_binary_function_vector_vector() {
        /// foo(v) performs int(v1*2.5 - float(v2)*3.5).
        #[rpn_fn]
        fn foo(v1: &Option<Real>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(
                (v1.unwrap().into_inner() * 2.5 - (v2.unwrap() as f64) * 3.5) as i64,
            ))
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4));
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        // foo(col1, col0)
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(1)
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 2, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[0, 2, 1], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [
                Some(-2),  // original row 0
                Some(22),  // original row 2
                Some(-17), // original row 1
            ]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are both raw columns). The same column is referred multiple times
    /// and it should be Ok.
    #[test]
    fn test_eval_binary_function_raw_column() {
        /// foo(v1, v2) performs v1 * v2.
        #[rpn_fn]
        fn foo(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() * v2.unwrap()))
        }

        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(-5)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(-7)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            let mut datum_raw = Vec::new();
            datum_raw
                .write_datum(&mut ctx, &[Datum::I64(3)], false)
                .unwrap();
            col.mut_raw().push(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 2, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[1], 1);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(49)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    /// Ternary function (arguments are vector, scalar, vector)
    #[test]
    fn test_eval_ternary_function() {
        /// foo(v) performs v1 - v2 * v3.
        #[rpn_fn]
        fn foo(v1: &Option<i64>, v2: &Option<i64>, v3: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() - v2.unwrap() * v3.unwrap()))
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(5));
            col.mut_decoded().push_int(Some(-4));
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_constant(3i64)
            .push_column_ref(0)
            .push_fn_call(foo_fn_meta(), 3, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[1, 0, 2], 3);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(-10), Some(-2), Some(8)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1, 2]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    // Comprehensive expression:
    //      fn_a(
    //          Col0,
    //          fn_b(),
    //          fn_c(
    //              fn_d(Col1, Const0),
    //              Const1
    //          )
    //      )
    //
    // RPN: Col0, fn_b, Col1, Const0, fn_d, Const1, fn_c, fn_a
    #[test]
    fn test_eval_comprehensive() {
        /// fn_a(v1, v2, v3) performs v1 * v2 - v3.
        #[rpn_fn]
        fn fn_a(v1: &Option<Real>, v2: &Option<Real>, v3: &Option<Real>) -> Result<Option<Real>> {
            Ok(Some(v1.unwrap() * v2.unwrap() - v3.unwrap()))
        }

        /// fn_b() returns 42.0.
        #[rpn_fn]
        fn fn_b() -> Result<Option<Real>> {
            Ok(Real::new(42.0).ok())
        }

        /// fn_c(v1, v2) performs float(v2 - v1).
        #[rpn_fn]
        fn fn_c(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new((v2.unwrap() - v1.unwrap()) as f64).ok())
        }

        /// fn_d(v1, v2) performs v1 + v2 * 2.
        #[rpn_fn]
        fn fn_d(v1: &Option<i64>, v2: &Option<i64>) -> Result<Option<i64>> {
            Ok(Some(v1.unwrap() + v2.unwrap() * 2))
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1));
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4));
                col
            },
        ]);
        let schema = &[FieldTypeTp::Double.into(), FieldTypeTp::LongLong.into()];

        // Col0, fn_b, Col1, Const0, fn_d, Const1, fn_c, fn_a
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(fn_b_fn_meta(), 0, FieldTypeTp::Double)
            .push_column_ref(1)
            .push_constant(7i64)
            .push_fn_call(fn_d_fn_meta(), 2, FieldTypeTp::LongLong)
            .push_constant(11i64)
            .push_fn_call(fn_c_fn_meta(), 2, FieldTypeTp::Double)
            .push_fn_call(fn_a_fn_meta(), 3, FieldTypeTp::Double)
            .build();

        //      fn_a(
        //          [0.5, -0.1, 3.5],
        //          42.0,
        //          fn_c(
        //              fn_d([1, 5, -4], 7),
        //              11
        //          )
        //      )
        //      => [25.0, 3.8, 146.0]

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[2, 0], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_real_slice(),
            [Real::new(146.0).ok(), Real::new(25.0).ok(),]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::Double);
    }

    /// Unary function, but supplied zero arguments. Should panic.
    #[test]
    fn test_eval_fail_1() {
        #[rpn_fn]
        fn foo(_v: &Option<i64>) -> Result<Option<i64>> {
            unreachable!()
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, &[], &mut columns, &[], 3);
        });
        assert!(hooked_eval.is_err());
    }

    /// Irregular RPN expression (contains unused node). Should panic.
    #[test]
    fn test_eval_fail_2() {
        /// foo(v) performs v * 2.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.0))
        }

        // foo() only accepts 1 parameter but we will give 2.

        let exp = RpnExpressionBuilder::new()
            .push_constant(3.0f64)
            .push_constant(1.5f64)
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, &[], &mut columns, &[], 3);
        });
        assert!(hooked_eval.is_err());
    }

    /// Eval type does not match. Should panic.
    /// Note: When field type is not matching, it doesn't panic.
    #[test]
    fn test_eval_fail_3() {
        /// Expects real argument, receives int argument.
        #[rpn_fn]
        fn foo(v: &Option<Real>) -> Result<Option<Real>> {
            Ok(v.map(|v| v * 2.5))
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(7i64)
            .push_fn_call(foo_fn_meta(), 1, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, &[], &mut columns, &[], 3);
        });
        assert!(hooked_eval.is_err());
    }

    /// Parse from an expression tree then evaluate.
    #[test]
    fn test_parse_and_eval() {
        use tipb::{Expr, ScalarFuncSig};

        // We will build an expression tree from:
        //      fn_d(
        //          fn_a(
        //              Const1,
        //              fn_b(Col1, fn_c()),
        //              Col0
        //          )
        //      )

        /// fn_a(a: int, b: float, c: int) performs: float(a) - b * float(c)
        #[rpn_fn]
        fn fn_a(a: &Option<i64>, b: &Option<Real>, c: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new(a.unwrap() as f64 - b.unwrap().into_inner() * c.unwrap() as f64).ok())
        }

        /// fn_b(a: float, b: int) performs: a * (float(b) - 1.5)
        #[rpn_fn]
        fn fn_b(a: &Option<Real>, b: &Option<i64>) -> Result<Option<Real>> {
            Ok(Real::new(a.unwrap().into_inner() * (b.unwrap() as f64 - 1.5)).ok())
        }

        /// fn_c() returns: int(42)
        #[rpn_fn]
        fn fn_c() -> Result<Option<i64>> {
            Ok(Some(42))
        }

        /// fn_d(a: float) performs: int(a)
        #[rpn_fn]
        fn fn_d(a: &Option<Real>) -> Result<Option<i64>> {
            Ok(Some(a.unwrap().into_inner() as i64))
        }

        fn fn_mapper(expr: &Expr) -> Result<RpnFnMeta> {
            // fn_a: CastIntAsInt
            // fn_b: CastIntAsReal
            // fn_c: CastIntAsString
            // fn_d: CastIntAsDecimal
            Ok(match expr.get_sig() {
                ScalarFuncSig::CastIntAsInt => fn_a_fn_meta(),
                ScalarFuncSig::CastIntAsReal => fn_b_fn_meta(),
                ScalarFuncSig::CastIntAsString => fn_c_fn_meta(),
                ScalarFuncSig::CastIntAsDecimal => fn_d_fn_meta(),
                _ => unreachable!(),
            })
        }

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDecimal, FieldTypeTp::LongLong)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::constant_int(7))
                        .push_child(
                            ExprDefBuilder::scalar_func(
                                ScalarFuncSig::CastIntAsReal,
                                FieldTypeTp::Double,
                            )
                            .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::Double))
                            .push_child(ExprDefBuilder::scalar_func(
                                ScalarFuncSig::CastIntAsString,
                                FieldTypeTp::LongLong,
                            )),
                        )
                        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)),
                )
                .build();

        // Build RPN expression from this expression tree.
        let exp =
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 2).unwrap();

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Int);
                col.mut_decoded().push_int(Some(1)); // row 1
                col.mut_decoded().push_int(Some(5));
                col.mut_decoded().push_int(Some(-4)); // row 0
                col
            },
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Real::new(0.5).ok());
                col.mut_decoded().push_real(Real::new(-0.1).ok());
                col.mut_decoded().push_real(Real::new(3.5).ok());
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[2, 0], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(574), Some(-13)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    #[test]
    fn test_rpn_fn_data() {
        use crate::codec::data_type::Evaluable;
        use tipb::{Expr, ScalarFuncSig};

        #[allow(clippy::trivially_copy_pass_by_ref)]
        #[rpn_fn(capture = [metadata], metadata_mapper = prepare_a::<T>)]
        fn fn_a<T: Evaluable>(metadata: &i64, v: &Option<Int>) -> Result<Option<Int>> {
            assert_eq!(*metadata, 42);
            Ok(v.map(|v| v + *metadata))
        }

        fn prepare_a<T: Evaluable>(_expr: &mut Expr) -> Result<i64> {
            Ok(42)
        }

        #[allow(clippy::trivially_copy_pass_by_ref, clippy::ptr_arg)]
        #[rpn_fn(varg, capture = [metadata], metadata_mapper = prepare_b::<T>)]
        fn fn_b<T: Evaluable>(metadata: &String, v: &[&Option<T>]) -> Result<Option<T>> {
            assert_eq!(metadata, &format!("{}", std::mem::size_of::<T>()));
            Ok(v[0].clone())
        }

        fn prepare_b<T: Evaluable>(_expr: &mut Expr) -> Result<String> {
            Ok(format!("{}", std::mem::size_of::<T>()))
        }

        #[allow(clippy::trivially_copy_pass_by_ref)]
        #[rpn_fn(raw_varg, capture = [metadata], metadata_mapper = prepare_c::<T>)]
        fn fn_c<T: Evaluable>(
            _data: &std::marker::PhantomData<T>,
            args: &[ScalarValueRef<'_>],
        ) -> Result<Option<Int>> {
            Ok(Some(args.len() as i64))
        }

        fn prepare_c<T: Evaluable>(_expr: &mut Expr) -> Result<std::marker::PhantomData<T>> {
            Ok(std::marker::PhantomData)
        }

        fn fn_mapper(expr: &Expr) -> Result<RpnFnMeta> {
            // fn_a: CastIntAsInt
            // fn_b: CastIntAsReal
            // fn_c: CastIntAsString
            Ok(match expr.get_sig() {
                ScalarFuncSig::CastIntAsInt => fn_a_fn_meta::<Real>(),
                ScalarFuncSig::CastIntAsReal => fn_b_fn_meta::<Real>(),
                ScalarFuncSig::CastIntAsString => fn_c_fn_meta::<Int>(),
                _ => unreachable!(),
            })
        }

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsString, FieldTypeTp::LongLong)
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsReal, FieldTypeTp::Double)
                        .push_child(ExprDefBuilder::constant_real(0.5)),
                )
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::LongLong)
                        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong)),
                )
                .build();

        // Build RPN expression from this expression tree.
        let exp =
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 1).unwrap();

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(2, EvalType::Int);
            col.mut_decoded().push_int(Some(1)); // row 1
            col.mut_decoded().push_int(None); // row 0
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, schema, &mut columns, &[1, 0], 2);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_ref().as_int_slice(),
            [Some(2), Some(2)]
        );
        assert_eq!(val.vector_value().unwrap().logical_rows(), &[0, 1]);
        assert_eq!(val.field_type().as_accessor().tp(), FieldTypeTp::LongLong);
    }

    #[bench]
    fn bench_eval_plus_1024_rows(b: &mut Bencher) {
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1024, EvalType::Int);
            for i in 0..1024 {
                col.mut_decoded().push_int(Some(i));
            }
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(arithmetic_fn_meta::<IntIntPlus>(), 2, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let logical_rows: Vec<_> = (0..1024).collect();

        profiler::start("./bench_eval_plus_1024_rows.profile");
        b.iter(|| {
            let result = black_box(&exp).eval(
                black_box(&mut ctx),
                black_box(schema),
                black_box(&mut columns),
                black_box(&logical_rows),
                black_box(1024),
            );
            assert!(result.is_ok());
        });
        profiler::stop();
    }

    #[bench]
    fn bench_eval_compare_1024_rows(b: &mut Bencher) {
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(1024, EvalType::Int);
            for i in 0..1024 {
                col.mut_decoded().push_int(Some(i));
            }
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(
                compare_fn_meta::<BasicComparer<Int, CmpOpLE>>(),
                2,
                FieldTypeTp::LongLong,
            )
            .build();
        let mut ctx = EvalContext::default();
        let logical_rows: Vec<_> = (0..1024).collect();

        profiler::start("./eval_compare_1024_rows.profile");
        b.iter(|| {
            let result = black_box(&exp).eval(
                black_box(&mut ctx),
                black_box(schema),
                black_box(&mut columns),
                black_box(&logical_rows),
                black_box(1024),
            );
            assert!(result.is_ok());
        });
        profiler::stop();
    }

    #[bench]
    fn bench_eval_compare_5_rows(b: &mut Bencher) {
        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Int);
            for i in 0..5 {
                col.mut_decoded().push_int(Some(i));
            }
            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(
                compare_fn_meta::<BasicComparer<Int, CmpOpLE>>(),
                2,
                FieldTypeTp::LongLong,
            )
            .build();
        let mut ctx = EvalContext::default();
        let logical_rows: Vec<_> = (0..5).collect();

        profiler::start("./bench_eval_compare_5_rows.profile");
        b.iter(|| {
            let result = black_box(&exp).eval(
                black_box(&mut ctx),
                black_box(schema),
                black_box(&mut columns),
                black_box(&logical_rows),
                black_box(5),
            );
            assert!(result.is_ok());
        });
        profiler::stop();
    }
}
