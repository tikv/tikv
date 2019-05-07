// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::expression::FieldType;

use super::super::function::RpnFunction;
use super::expr::{RpnExpression, RpnExpressionNode};
use super::RpnFnCallPayload;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::VectorLikeValueRef;
use crate::coprocessor::codec::data_type::{ScalarValue, VectorValue};
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

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
    /// There can be frequent stack push & pops, so we wrap this field in a `Box` to reduce move
    /// cost.
    // TODO: Check whether it is more efficient to just remove the box.
    Owned(Box<VectorValue>),
    Ref(&'a VectorValue),
}

impl<'a> AsRef<VectorValue> for RpnStackNodeVectorValue<'a> {
    #[inline]
    fn as_ref(&self) -> &VectorValue {
        match self {
            RpnStackNodeVectorValue::Owned(ref value) => &value,
            RpnStackNodeVectorValue::Ref(ref value) => *value,
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
            RpnStackNode::Scalar { ref field_type, .. } => field_type,
            RpnStackNode::Vector { ref field_type, .. } => field_type,
        }
    }

    /// Borrows the inner scalar value for `Scalar` variant.
    #[inline]
    pub fn scalar_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnStackNode::Scalar { ref value, .. } => Some(*value),
            RpnStackNode::Vector { .. } => None,
        }
    }

    /// Borrows the inner vector value for `Vector` variant.
    #[inline]
    pub fn vector_value(&self) -> Option<&VectorValue> {
        match self {
            RpnStackNode::Scalar { .. } => None,
            RpnStackNode::Vector { ref value, .. } => Some(value.as_ref()),
        }
    }

    /// Borrows the inner scalar or vector value as a vector like value.
    #[inline]
    pub fn as_vector_like(&self) -> VectorLikeValueRef<'_> {
        match self {
            RpnStackNode::Scalar { ref value, .. } => value.as_vector_like(),
            RpnStackNode::Vector { ref value, .. } => value.as_ref().as_vector_like(),
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
        context: &mut EvalContext,
        rows: usize,
        schema: &'a [FieldType],
        columns: &'a mut LazyBatchColumnVec,
    ) -> Result<RpnStackNode<'a>> {
        assert!(rows > 0);

        let mut stack = Vec::with_capacity(self.len());

        // We iterate two times. The first time we decode all referred columns. The second time
        // we evaluate. This is to make Rust's borrow checker happy because there will be
        // mutable reference during the first iteration and we can't keep these references.

        for node in self.as_ref() {
            if let RpnExpressionNode::ColumnRef { ref offset, .. } = node {
                columns.ensure_column_decoded(*offset, &context.cfg.tz, &schema[*offset])?;
            }
        }

        for node in self.as_ref() {
            match node {
                RpnExpressionNode::Constant {
                    ref value,
                    ref field_type,
                } => {
                    stack.push(RpnStackNode::Scalar {
                        value: &value,
                        field_type,
                    });
                }
                RpnExpressionNode::ColumnRef { ref offset } => {
                    let field_type = &schema[*offset];
                    let decoded_column = columns[*offset].decoded();
                    assert_eq!(decoded_column.len(), rows);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Ref(&decoded_column),
                        field_type,
                    });
                }
                RpnExpressionNode::FnCall {
                    ref func,
                    ref field_type,
                } => {
                    // Suppose that we have function call `Foo(A, B, C)`, the RPN nodes looks like
                    // `[A, B, C, Foo]`.
                    // Now we receives a function call `Foo`, so there are `[A, B, C]` in the stack
                    // as the last several elements. We will directly use the last N (N = number of
                    // arguments) elements in the stack as function arguments.
                    assert!(stack.len() >= func.args_len());
                    let stack_slice_begin = stack.len() - func.args_len();
                    let stack_slice = &stack[stack_slice_begin..];
                    let call_info = RpnFnCallPayload {
                        raw_args: stack_slice,
                        ret_field_type: field_type,
                    };
                    let ret = func.eval(rows, context, call_info)?;
                    stack.truncate(stack_slice_begin);
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Owned(Box::new(ret)),
                        field_type,
                    });
                }
            }
        }

        assert_eq!(stack.len(), 1);
        Ok(stack.into_iter().next().unwrap())
    }

    /// Evaluates the expression into a boolean vector.
    ///
    /// # Panics
    ///
    /// Panics if referenced columns are not decoded.
    ///
    /// Panics if the boolean vector output buffer is not large enough to contain all values.
    pub fn eval_as_mysql_bools(
        &self,
        context: &mut EvalContext,
        rows: usize,
        schema: &[FieldType],
        columns: &mut LazyBatchColumnVec,
        outputs: &mut [bool], // modify an existing buffer to avoid repeated allocation
    ) -> Result<()> {
        use crate::coprocessor::codec::data_type::AsMySQLBool;

        assert!(outputs.len() >= rows);
        let values = self.eval(context, rows, schema, columns)?;
        match values {
            RpnStackNode::Scalar { value, .. } => {
                let b = value.as_mysql_bool(context)?;
                for i in 0..rows {
                    outputs[i] = b;
                }
            }
            RpnStackNode::Vector { value, .. } => {
                let vec_ref = value.as_ref();
                assert_eq!(vec_ref.len(), rows);
                vec_ref.eval_as_mysql_bools(context, outputs)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::float_cmp)]

    use super::*;

    use cop_codegen::RpnFunction;
    use cop_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
    use tipb::expression::FieldType;

    use super::super::RpnFnCallPayload;

    use crate::coprocessor::codec::batch::LazyBatchColumn;
    use crate::coprocessor::codec::datum::{Datum, DatumEncoder};
    use crate::coprocessor::dag::expr::EvalContext;
    use crate::coprocessor::dag::rpn_expr::RpnExpressionBuilder;
    use crate::coprocessor::Result;

    /// Single constant node
    #[test]
    fn test_eval_single_constant_node() {
        let exp = RpnExpressionBuilder::new().push_constant(1.5f64).build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 10, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_scalar());
        assert_eq!(*val.scalar_value().unwrap().as_real(), Some(1.5));
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Creates fixture to be used in `test_eval_single_column_node_xxx`.
    fn new_single_column_node_fixture() -> (LazyBatchColumnVec, [FieldType; 2]) {
        let columns = LazyBatchColumnVec::from(vec![
            {
                // this column is not referenced
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(5, EvalType::Real);
                col.mut_decoded().push_real(Some(1.0));
                col.mut_decoded().push_real(None);
                col.mut_decoded().push_real(Some(7.5));
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
        (columns, schema)
    }

    /// Single column node
    #[test]
    fn test_eval_single_column_node_normal() {
        let (columns, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 5, &schema, &mut c);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(1), Some(5), None, None, Some(42)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(0).build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 5, &schema, &mut c);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(1.0), None, Some(7.5), None, None]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Single column node but row numbers in `eval()` does not match column length, should panic.
    #[test]
    fn test_eval_single_column_node_mismatch_rows() {
        let (columns, schema) = new_single_column_node_fixture();

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            // smaller row number
            let _ = exp.eval(&mut ctx, 4, &schema, &mut c);
        });
        assert!(hooked_eval.is_err());

        let mut c = columns.clone();
        let exp = RpnExpressionBuilder::new().push_column_ref(1).build();
        let mut ctx = EvalContext::default();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            // larger row number
            let _ = exp.eval(&mut ctx, 6, &schema, &mut c);
        });
        assert!(hooked_eval.is_err());
    }

    /// Single function call node (i.e. nullary function)
    #[test]
    fn test_eval_single_fn_call_node() {
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 0)]
        struct FnFoo;

        impl FnFoo {
            fn call(_ctx: &mut EvalContext, _payload: RpnFnCallPayload<'_>) -> Result<Option<i64>> {
                Ok(Some(42))
            }
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 4, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(42), Some(42), Some(42), Some(42)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is scalar)
    #[test]
    fn test_eval_unary_function_scalar() {
        /// FnFoo(v) performs v * 2.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<f64>,
            ) -> Result<Option<f64>> {
                Ok(v.map(|v| v * 2.0))
            }
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 3, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(3.0), Some(3.0), Some(3.0)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Unary function (argument is vector)
    #[test]
    fn test_eval_unary_function_vector() {
        /// FnFoo(v) performs v + 5.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(v.map(|v| v + 5))
            }
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
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(6), Some(10), None]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Unary function (argument is raw column). The column should be decoded.
    #[test]
    fn test_eval_unary_function_raw_column() {
        /// FnFoo(v) performs v + 5.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(Some(v.unwrap() + 5))
            }
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-5)], false).unwrap();
            col.push_raw(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-7)], false).unwrap();
            col.push_raw(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(3)], false).unwrap();
            col.push_raw(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(0), Some(-2), Some(8)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are scalar, scalar)
    #[test]
    fn test_eval_binary_function_scalar_scalar() {
        /// FnFoo(v) performs v1 + float(v2) - 1.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<f64>,
                v2: &Option<i64>,
            ) -> Result<Option<f64>> {
                Ok(Some(v1.unwrap() + v2.unwrap() as f64 - 1.0))
            }
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(1.5f64)
            .push_constant(3i64)
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let result = exp.eval(&mut ctx, 3, &[], &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(3.5), Some(3.5), Some(3.5)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, scalar)
    #[test]
    fn test_eval_binary_function_vector_scalar() {
        /// FnFoo(v) performs v1 - v2.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<f64>,
                v2: &Option<f64>,
            ) -> Result<Option<f64>> {
                Ok(Some(v1.unwrap() - v2.unwrap()))
            }
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
            col.mut_decoded().push_real(Some(1.0));
            col.mut_decoded().push_real(Some(5.5));
            col.mut_decoded().push_real(Some(-4.3));
            col
        }]);
        let schema = &[FieldTypeTp::Double.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_constant(1.5f64)
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(-0.5), Some(4.0), Some(-5.8)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are scalar, vector)
    #[test]
    fn test_eval_binary_function_scalar_vector() {
        /// FnFoo(v) performs v1 - float(v2).
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<f64>,
                v2: &Option<i64>,
            ) -> Result<Option<f64>> {
                Ok(Some(v1.unwrap() - v2.unwrap() as f64))
            }
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
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(0.5), Some(-3.5), Some(5.5)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Binary function (arguments are vector, vector)
    #[test]
    fn test_eval_binary_function_vector_vector() {
        /// FnFoo(v) performs int(v1*2.5 - float(v2)*3.5).
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<f64>,
                v2: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(Some(
                    (v1.unwrap() * 2.5 - (v2.unwrap() as f64) * 3.5) as i64,
                ))
            }
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
                col.mut_decoded().push_real(Some(0.5));
                col.mut_decoded().push_real(Some(-0.1));
                col.mut_decoded().push_real(Some(3.5));
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        // FnFoo(col1, col0)
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(1)
            .push_column_ref(0)
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-2), Some(-17), Some(22)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Binary function (arguments are both raw columns). The same column is referred multiple times
    /// and it should be Ok.
    #[test]
    fn test_eval_binary_function_raw_column() {
        /// FnFoo(v1, v2) performs v1 * v2.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<i64>,
                v2: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(Some(v1.unwrap() * v2.unwrap()))
            }
        }

        let mut columns = LazyBatchColumnVec::from(vec![{
            let mut col = LazyBatchColumn::raw_with_capacity(3);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-5)], false).unwrap();
            col.push_raw(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(-7)], false).unwrap();
            col.push_raw(&datum_raw);

            let mut datum_raw = Vec::new();
            DatumEncoder::encode(&mut datum_raw, &[Datum::I64(3)], false).unwrap();
            col.push_raw(&datum_raw);

            col
        }]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::LongLong.into()];

        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_column_ref(0)
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(25), Some(49), Some(9)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    /// Ternary function (arguments are vector, scalar, vector)
    #[test]
    fn test_eval_ternary_function() {
        /// FnFoo(v) performs v1 - v2 * v3.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 3)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<i64>,
                v2: &Option<i64>,
                v3: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(Some(v1.unwrap() - v2.unwrap() * v3.unwrap()))
            }
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
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-2), Some(-10), Some(8)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }

    // Comprehensive expression:
    //      FnA(
    //          Col0,
    //          FnB(),
    //          FnC(
    //              FnD(Col1, Const0),
    //              Const1
    //          )
    //      )
    //
    // RPN: Col0, FnB, Col1, Const0, FnD, Const1, FnC, FnA
    #[test]
    fn test_eval_comprehensive() {
        /// FnA(v1, v2, v3) performs v1 * v2 - v3.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 3)]
        struct FnA;

        impl FnA {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<f64>,
                v2: &Option<f64>,
                v3: &Option<f64>,
            ) -> Result<Option<f64>> {
                Ok(Some(v1.unwrap() * v2.unwrap() - v3.unwrap()))
            }
        }

        /// FnB() returns 42.0.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 0)]
        struct FnB;

        impl FnB {
            fn call(_ctx: &mut EvalContext, _payload: RpnFnCallPayload<'_>) -> Result<Option<f64>> {
                Ok(Some(42.0))
            }
        }

        /// FnC(v1, v2) performs float(v2 - v1).
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnC;

        impl FnC {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<i64>,
                v2: &Option<i64>,
            ) -> Result<Option<f64>> {
                Ok(Some((v2.unwrap() - v1.unwrap()) as f64))
            }
        }

        /// FnD(v1, v2) performs v1 + v2 * 2.
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnD;

        impl FnD {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v1: &Option<i64>,
                v2: &Option<i64>,
            ) -> Result<Option<i64>> {
                Ok(Some(v1.unwrap() + v2.unwrap() * 2))
            }
        }

        let mut columns = LazyBatchColumnVec::from(vec![
            {
                let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(3, EvalType::Real);
                col.mut_decoded().push_real(Some(0.5));
                col.mut_decoded().push_real(Some(-0.1));
                col.mut_decoded().push_real(Some(3.5));
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

        // Col0, FnB, Col1, Const0, FnD, Const1, FnC, FnA
        let exp = RpnExpressionBuilder::new()
            .push_column_ref(0)
            .push_fn_call(FnB, FieldTypeTp::Double)
            .push_column_ref(1)
            .push_constant(7i64)
            .push_fn_call(FnD, FieldTypeTp::LongLong)
            .push_constant(11i64)
            .push_fn_call(FnC, FieldTypeTp::Double)
            .push_fn_call(FnA, FieldTypeTp::Double)
            .build();

        //      FnA(
        //          [0.5, -0.1, 3.5],
        //          42.0,
        //          FnC(
        //              FnD([1, 5, -4], 7),
        //              11
        //          )
        //      )
        //      => [25.0, 3.8, 146.0]

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_real_slice(),
            [Some(25.0), Some(3.8), Some(146.0)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::Double);
    }

    /// Unary function, but supplied zero arguments. Should panic.
    #[test]
    fn test_eval_fail_1() {
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                _v: &Option<i64>,
            ) -> Result<Option<i64>> {
                unreachable!()
            }
        }

        let exp = RpnExpressionBuilder::new()
            .push_fn_call(FnFoo, FieldTypeTp::LongLong)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Irregular RPN expression (contains unused node). Should panic.
    #[test]
    fn test_eval_fail_2() {
        /// FnFoo(v) performs v * 2.

        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<f64>,
            ) -> Result<Option<f64>> {
                Ok(v.map(|v| v * 2.0))
            }
        }

        // FnFoo only accepts 1 parameter but we will give 2.

        let exp = RpnExpressionBuilder::new()
            .push_constant(3.0f64)
            .push_constant(1.5f64)
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Eval type does not match. Should panic.
    /// Note: When field type is not matching, it doesn't panic.
    #[test]
    fn test_eval_fail_3() {
        // Expects real argument, receives int argument.

        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnFoo;

        impl FnFoo {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                v: &Option<f64>,
            ) -> Result<Option<f64>> {
                Ok(v.map(|v| v * 2.5))
            }
        }

        let exp = RpnExpressionBuilder::new()
            .push_constant(7i64)
            .push_fn_call(FnFoo, FieldTypeTp::Double)
            .build();
        let mut ctx = EvalContext::default();
        let mut columns = LazyBatchColumnVec::empty();
        let hooked_eval = ::panic_hook::recover_safe(|| {
            let _ = exp.eval(&mut ctx, 3, &[], &mut columns);
        });
        assert!(hooked_eval.is_err());
    }

    /// Parse from an expression tree then evaluate.
    #[test]
    fn test_parse_and_eval() {
        use tipb::expression::ScalarFuncSig;
        use tipb::expression::{Expr, ExprType};

        use tikv_util::codec::number::NumberEncoder;

        // We will build an expression tree from:
        //      FnD(
        //          FnA(
        //              Const1,
        //              FnB(Col1, FnC()),
        //              Col0
        //          )
        //      )

        /// FnA(a: int, b: float, c: int) performs: float(a) - b * float(c)
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 3)]
        struct FnA;

        impl FnA {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                a: &Option<i64>,
                b: &Option<f64>,
                c: &Option<i64>,
            ) -> Result<Option<f64>> {
                Ok(Some(a.unwrap() as f64 - b.unwrap() * c.unwrap() as f64))
            }
        }

        /// FnB(a: float, b: int) performs: a * (float(b) - 1.5)
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 2)]
        struct FnB;

        impl FnB {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                a: &Option<f64>,
                b: &Option<i64>,
            ) -> Result<Option<f64>> {
                Ok(Some(a.unwrap() * (b.unwrap() as f64 - 1.5)))
            }
        }

        /// FnC() returns: int(42)
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 0)]
        struct FnC;

        impl FnC {
            fn call(_ctx: &mut EvalContext, _payload: RpnFnCallPayload<'_>) -> Result<Option<i64>> {
                Ok(Some(42))
            }
        }

        /// FnD(a: float) performs: int(a)
        #[derive(Debug, Clone, Copy, RpnFunction)]
        #[rpn_function(args = 1)]
        struct FnD;

        impl FnD {
            fn call(
                _ctx: &mut EvalContext,
                _payload: RpnFnCallPayload<'_>,
                a: &Option<f64>,
            ) -> Result<Option<i64>> {
                Ok(Some(a.unwrap() as i64))
            }
        }

        fn fn_mapper(value: ScalarFuncSig) -> Result<Box<dyn RpnFunction>> {
            // FnA: CastIntAsInt
            // FnB: CastIntAsReal
            // FnC: CastIntAsString
            // FnD: CastIntAsDecimal
            match value {
                ScalarFuncSig::CastIntAsInt => Ok(Box::new(FnA)),
                ScalarFuncSig::CastIntAsReal => Ok(Box::new(FnB)),
                ScalarFuncSig::CastIntAsString => Ok(Box::new(FnC)),
                ScalarFuncSig::CastIntAsDecimal => Ok(Box::new(FnD)),
                _ => unreachable!(),
            }
        }

        let node_fn_b = {
            // Col1
            let mut node_col_1 = Expr::new();
            node_col_1.set_tp(ExprType::ColumnRef);
            node_col_1
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_col_1.mut_val().encode_i64(1).unwrap();

            // FnC
            let mut node_fn_c = Expr::new();
            node_fn_c.set_tp(ExprType::ScalarFunc);
            node_fn_c.set_sig(ScalarFuncSig::CastIntAsString);
            node_fn_c
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);

            // FnB
            let mut node_fn_b = Expr::new();
            node_fn_b.set_tp(ExprType::ScalarFunc);
            node_fn_b.set_sig(ScalarFuncSig::CastIntAsReal);
            node_fn_b
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_b.mut_children().push(node_col_1);
            node_fn_b.mut_children().push(node_fn_c);
            node_fn_b
        };

        let node_fn_a = {
            // Const1
            let mut node_const_1 = Expr::new();
            node_const_1.set_tp(ExprType::Int64);
            node_const_1
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_const_1.mut_val().encode_i64(7).unwrap();

            // Col0
            let mut node_col_0 = Expr::new();
            node_col_0.set_tp(ExprType::ColumnRef);
            node_col_0
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_col_0.mut_val().encode_i64(0).unwrap();

            // FnA
            let mut node_fn_a = Expr::new();
            node_fn_a.set_tp(ExprType::ScalarFunc);
            node_fn_a.set_sig(ScalarFuncSig::CastIntAsInt);
            node_fn_a
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_a.mut_children().push(node_const_1);
            node_fn_a.mut_children().push(node_fn_b);
            node_fn_a.mut_children().push(node_col_0);
            node_fn_a
        };

        // FnD
        let mut node_fn_d = Expr::new();
        node_fn_d.set_tp(ExprType::ScalarFunc);
        node_fn_d.set_sig(ScalarFuncSig::CastIntAsDecimal);
        node_fn_d
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        node_fn_d.mut_children().push(node_fn_a);

        // Build RPN expression from this expression tree.
        let exp =
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node_fn_d, fn_mapper, 2)
                .unwrap();

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
                col.mut_decoded().push_real(Some(0.5));
                col.mut_decoded().push_real(Some(-0.1));
                col.mut_decoded().push_real(Some(3.5));
                col
            },
        ]);
        let schema = &[FieldTypeTp::LongLong.into(), FieldTypeTp::Double.into()];

        let mut ctx = EvalContext::default();
        let result = exp.eval(&mut ctx, 3, schema, &mut columns);
        let val = result.unwrap();
        assert!(val.is_vector());
        assert_eq!(
            val.vector_value().unwrap().as_int_slice(),
            [Some(-13), Some(27), Some(574)]
        );
        assert_eq!(val.field_type().tp(), FieldTypeTp::LongLong);
    }
}
