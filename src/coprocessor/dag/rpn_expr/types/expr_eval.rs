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
    /// Evaluates the expression into a vector. If referred columns are not decoded, they will be
    /// decoded according to the given schema.
    pub fn eval<'a>(
        &'a self,
        context: &mut EvalContext,
        rows: usize,
        schema: &'a [FieldType],
        columns: &'a mut LazyBatchColumnVec,
    ) -> Result<RpnStackNode<'a>> {
        let mut stack = Vec::with_capacity(self.len());

        // First loop: ensure referred columns are decoded.
        for node in self.as_ref() {
            if let RpnExpressionNode::ColumnRef { ref offset, .. } = node {
                columns.ensure_column_decoded(*offset, &context.cfg.tz, &schema[*offset])?;
            }
        }

        // Second loop: evaluate RPN expressions.
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
