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

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::{Expr, ExprType, FieldType};

use super::function::RpnFunction;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::data_type::VectorLikeValueRef;
use crate::coprocessor::codec::data_type::{ScalarValue, VectorValue};
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::{Error, Result};

/// A structure for holding argument values and type information of arguments and return values.
///
/// It can simplify function signatures without losing performance where only argument values are
/// needed in most cases.
///
/// NOTE: This structure must be very fast to copy because it will be passed by value directly
/// (i.e. Copy), instead of by reference, for **EACH** function invocation.
#[derive(Clone, Copy)]
pub struct RpnFnCallPayload<'a> {
    raw_args: &'a [RpnStackNode<'a>],
    ret_field_type: &'a FieldType,
}

impl<'a> RpnFnCallPayload<'a> {
    /// The number of arguments.
    #[inline]
    pub fn args_len(&'a self) -> usize {
        self.raw_args.len()
    }

    /// Gets the raw argument at specific position.
    #[inline]
    pub fn raw_arg_at(&'a self, position: usize) -> &'a RpnStackNode<'a> {
        &self.raw_args[position]
    }

    /// Gets the field type of the argument at specific position.
    #[inline]
    pub fn field_type_at(&'a self, position: usize) -> &'a FieldType {
        self.raw_args[position].field_type()
    }

    /// Gets the field type of the return value.
    #[inline]
    pub fn return_field_type(&'a self) -> &'a FieldType {
        self.ret_field_type
    }
}

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

/// A type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function call.
    FnCall {
        func: Box<dyn RpnFunction>,

        /// The field type of the return value.
        field_type: FieldType,
    },

    /// Represents a scalar constant value.
    Constant {
        value: ScalarValue,

        /// The field type of the constant.
        field_type: FieldType,
    },

    /// Represents a reference to a column in the columns specified in `eval()`.
    ColumnRef {
        offset: usize,
        // The field type of the column itself is specified in `eval()`, so there is no
        // `field_type` here.
    },
}

impl RpnExpressionNode {
    /// Gets the field type.
    ///
    /// For `FnCall` variant, the field type of the return value is returned.
    /// For `Constant` variant, the field type of the value is returned.
    #[inline]
    pub fn field_type(&self) -> Option<&FieldType> {
        match self {
            RpnExpressionNode::FnCall { ref field_type, .. } => Some(field_type),
            RpnExpressionNode::Constant { ref field_type, .. } => Some(field_type),
            RpnExpressionNode::ColumnRef { .. } => None,
        }
    }

    /// Borrows the function instance for `FnCall` variant.
    #[inline]
    pub fn fn_call_func(&self) -> Option<&dyn RpnFunction> {
        match self {
            RpnExpressionNode::FnCall { ref func, .. } => Some(&*func),
            _ => None,
        }
    }

    /// Borrows the constant value for `Constant` variant.
    #[inline]
    pub fn constant_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnExpressionNode::Constant { ref value, .. } => Some(value),
            _ => None,
        }
    }

    /// Gets the column offset for `ColumnRef` variant.
    #[inline]
    pub fn column_ref_offset(&self) -> Option<usize> {
        match self {
            RpnExpressionNode::ColumnRef { ref offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

/// An RPN expression node list which represents an expression in Reverse Polish notation.
#[derive(Debug)]
pub struct RpnExpressionNodeVec(Vec<RpnExpressionNode>);

impl std::ops::Deref for RpnExpressionNodeVec {
    type Target = Vec<RpnExpressionNode>;

    fn deref(&self) -> &Vec<RpnExpressionNode> {
        &self.0
    }
}

impl std::ops::DerefMut for RpnExpressionNodeVec {
    fn deref_mut(&mut self) -> &mut Vec<RpnExpressionNode> {
        &mut self.0
    }
}

impl RpnExpressionNodeVec {
    /// Builds the RPN expression node list from an expression definition tree.
    // TODO: Deprecate it in Coprocessor V2 DAG interface.
    pub fn build_from_expr_tree(tree_node: Expr, time_zone: Tz) -> Result<Self> {
        let mut expr_nodes = Vec::new();
        Self::append_rpn_nodes_recursively(
            tree_node,
            time_zone,
            &mut expr_nodes,
            super::map_pb_sig_to_rpn_func,
        )?;
        Ok(RpnExpressionNodeVec(expr_nodes))
    }

    /// Evaluates the expression into a vector. If referred columns are not decoded, they will be
    /// decoded according to the given schema.
    pub fn eval<'a>(
        &'a self,
        context: &mut EvalContext,
        rows: usize,
        schema: &'a [FieldType],
        columns: &'a mut LazyBatchColumnVec,
    ) -> Result<RpnStackNode<'a>> {
        let mut stack = Vec::with_capacity(self.0.len());

        // First loop: ensure referred columns are decoded.
        for node in &self.0 {
            if let RpnExpressionNode::ColumnRef { ref offset, .. } = node {
                columns.ensure_column_decoded(*offset, &context.cfg.tz, &schema[*offset])?;
            }
        }

        // Second loop: evaluate RPN expressions.
        for node in &self.0 {
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

    /// Transforms eval tree nodes into RPN nodes.
    ///
    /// Suppose that we have a function call:
    ///
    /// ```ignore
    /// A(B, C(E, F, G), D)
    /// ```
    ///
    /// The eval tree looks like:
    ///
    /// ```ignore
    ///           +---+
    ///           | A |
    ///           +---+
    ///             |
    ///   +-------------------+
    ///   |         |         |
    /// +---+     +---+     +---+
    /// | B |     | C |     | D |
    /// +---+     +---+     +---+
    ///             |
    ///      +-------------+
    ///      |      |      |
    ///    +---+  +---+  +---+
    ///    | E |  | F |  | G |
    ///    +---+  +---+  +---+
    /// ```
    ///
    /// We need to transform the tree into RPN nodes:
    ///
    /// ```ignore
    /// B E F G C D A
    /// ```
    ///
    /// The transform process is very much like a post-order traversal. This function does it
    /// recursively.
    fn append_rpn_nodes_recursively<F>(
        mut tree_node: Expr,
        time_zone: Tz,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
        fn_mapper: F,
    ) -> Result<()>
    where
        F: Fn(tipb::expression::ScalarFuncSig) -> Result<Box<dyn RpnFunction>> + Copy,
    {
        // TODO: We should check whether node types match the function signature. Otherwise there
        // will be panics when the expression is evaluated.

        use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
        use crate::util::codec::number;
        use std::convert::{TryFrom, TryInto};

        #[inline]
        fn handle_node_null(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Int => ScalarValue::Int(None),
                EvalType::Real => ScalarValue::Real(None),
                EvalType::Decimal => ScalarValue::Decimal(None),
                EvalType::Bytes => ScalarValue::Bytes(None),
                EvalType::DateTime => ScalarValue::DateTime(None),
                EvalType::Duration => ScalarValue::Duration(None),
                EvalType::Json => ScalarValue::Json(None),
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_int64(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Int => {
                    let value = number::decode_i64(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Int(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_uint64(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Int => {
                    let value = number::decode_u64(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Int(Some(value as i64))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_bytes(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Bytes => ScalarValue::Bytes(Some(tree_node.take_val())),
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_float(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Real => {
                    let value = number::decode_f64(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Real(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_time(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            time_zone: Tz,
        ) -> Result<()> {
            let field_type = tree_node.take_field_type();
            let scalar_value = match eval_type {
                EvalType::DateTime => {
                    let v = number::decode_u64(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    let fsp = field_type.decimal() as i8;
                    let value =
                        Time::from_packed_u64(v, field_type.tp().try_into()?, fsp, &time_zone)
                            .map_err(|_| {
                                Error::Other(box_err!(
                                    "Unable to decode {:?} from the request",
                                    eval_type
                                ))
                            })?;;
                    ScalarValue::DateTime(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type,
            });
            Ok(())
        }

        #[inline]
        fn handle_node_duration(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Duration => {
                    let n = number::decode_i64(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    let value = Duration::from_nanos(n, MAX_FSP).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Duration(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_decimal(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Decimal => {
                    let value = Decimal::decode(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Decimal(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        #[inline]
        fn handle_node_json(
            mut tree_node: Expr,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Json => {
                    let value = Json::decode(&mut tree_node.get_val()).map_err(|_| {
                        Error::Other(box_err!(
                            "Unable to decode {:?} from the request",
                            eval_type
                        ))
                    })?;
                    ScalarValue::Json(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        tree_node.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type: tree_node.take_field_type(),
            });
            Ok(())
        }

        let eval_type = EvalType::try_from(tree_node.get_field_type().tp())
            .map_err(|e| Error::Other(box_err!(e)))?;

        match tree_node.get_tp() {
            ExprType::Null => handle_node_null(tree_node, eval_type, rpn_nodes)?,
            ExprType::Int64 => handle_node_int64(tree_node, eval_type, rpn_nodes)?,
            ExprType::Uint64 => handle_node_uint64(tree_node, eval_type, rpn_nodes)?,
            ExprType::String | ExprType::Bytes => {
                handle_node_bytes(tree_node, eval_type, rpn_nodes)?
            }
            ExprType::Float32 | ExprType::Float64 => {
                handle_node_float(tree_node, eval_type, rpn_nodes)?
            }
            ExprType::MysqlTime => handle_node_time(tree_node, eval_type, rpn_nodes, time_zone)?,
            ExprType::MysqlDuration => handle_node_duration(tree_node, eval_type, rpn_nodes)?,
            ExprType::MysqlDecimal => handle_node_decimal(tree_node, eval_type, rpn_nodes)?,
            ExprType::MysqlJson => handle_node_json(tree_node, eval_type, rpn_nodes)?,
            ExprType::ColumnRef => {
                let offset = number::decode_i64(&mut tree_node.get_val()).map_err(|_| {
                    Error::Other(box_err!(
                        "Unable to decode column reference offset from the request"
                    ))
                })? as usize;
                rpn_nodes.push(RpnExpressionNode::ColumnRef {
                    offset,
                    // Note: field type of the referred column is not stored here, since it will be
                    // specified in `eval()`.
                });
            }
            ExprType::ScalarFunc => {
                // Map pb func to `RpnFunction`.
                let func = fn_mapper(tree_node.get_sig())?;
                let args = tree_node.take_children().into_vec();
                if func.args_len() != args.len() {
                    return Err(box_err!(
                        "Unexpected arguments, expect {}, received {}",
                        func.args_len(),
                        args.len()
                    ));
                }
                for arg in args {
                    Self::append_rpn_nodes_recursively(arg, time_zone, rpn_nodes, fn_mapper)?;
                }
                rpn_nodes.push(RpnExpressionNode::FnCall {
                    func,
                    field_type: tree_node.take_field_type(),
                });
            }
            t => return Err(box_err!("Unexpected ExprType {:?}", t)),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::codec::batch::LazyBatchColumn;

    /// An RPN function for test. It accepts 1 int argument, returns the value in float.
    #[derive(Debug, Clone, Copy)]
    struct FnA;

    impl_template_fn! { 1 arg @ FnA }

    impl FnA {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v: &Option<i64>,
        ) -> Result<Option<f64>> {
            Ok(v.map(|v| v as f64))
        }
    }

    /// An RPN function for test. It accepts 2 float arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    struct FnB;

    impl_template_fn! { 2 arg @ FnB }

    impl FnB {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<f64>,
            v2: &Option<f64>,
        ) -> Result<Option<i64>> {
            if v1.is_none() || v2.is_none() {
                return Ok(None);
            }
            Ok(Some((v1.as_ref().unwrap() + v2.as_ref().unwrap()) as i64))
        }
    }

    /// An RPN function for test. It accepts 3 int arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    struct FnC;

    impl_template_fn! { 3 arg @ FnC }

    impl FnC {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<i64>,
            v2: &Option<i64>,
            v3: &Option<i64>,
        ) -> Result<Option<i64>> {
            if v1.is_none() || v2.is_none() || v3.is_none() {
                return Ok(None);
            }
            Ok(Some(
                v1.as_ref().unwrap() + v2.as_ref().unwrap() + v3.as_ref().unwrap(),
            ))
        }
    }

    /// An RPN function for test. It accepts 3 float arguments, returns their sum in float.
    #[derive(Debug, Clone, Copy)]
    struct FnD;

    impl_template_fn! { 3 arg @ FnD }

    impl FnD {
        #[inline(always)]
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            v1: &Option<f64>,
            v2: &Option<f64>,
            v3: &Option<f64>,
        ) -> Result<Option<f64>> {
            if v1.is_none() || v2.is_none() || v3.is_none() {
                return Ok(None);
            }
            Ok(Some(
                v1.as_ref().unwrap() + v2.as_ref().unwrap() + v3.as_ref().unwrap(),
            ))
        }
    }

    /// For testing `append_rpn_nodes_recursively`. It accepts protobuf function sig enum, which
    /// cannot be modified by us in tests to support FnA ~ FnD. So let's just hard code some
    /// substitute.
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

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_append_rpn_nodes_recursively() {
        use crate::util::codec::number::NumberEncoder;

        // Input:
        // FnD(a, FnA(FnC(b, c, d)), FnA(FnB(e, f))
        //
        // Tree:
        //           FnD
        // +----------+----------+
        // a         FnA        FnA
        //            |          |
        //           FnC        FnB
        //        +---+---+      +---+
        //        b   c   d      e   f
        //
        // RPN:
        // a b c d FnC FnA e f FnB FnA FnD

        let node_fn_a_1 = {
            // node b
            let mut node_b = Expr::new();
            node_b.set_tp(ExprType::Int64);
            node_b
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_b.mut_val().encode_i64(7).unwrap();

            // node c
            let mut node_c = Expr::new();
            node_c.set_tp(ExprType::Int64);
            node_c
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_c.mut_val().encode_i64(3).unwrap();

            // node d
            let mut node_d = Expr::new();
            node_d.set_tp(ExprType::Int64);
            node_d
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_d.mut_val().encode_i64(11).unwrap();

            // FnC
            let mut node_fn_c = Expr::new();
            node_fn_c.set_tp(ExprType::ScalarFunc);
            node_fn_c.set_sig(ScalarFuncSig::CastIntAsString);
            node_fn_c
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_fn_c.mut_children().push(node_b);
            node_fn_c.mut_children().push(node_c);
            node_fn_c.mut_children().push(node_d);

            // FnA
            let mut node_fn_a = Expr::new();
            node_fn_a.set_tp(ExprType::ScalarFunc);
            node_fn_a.set_sig(ScalarFuncSig::CastIntAsInt);
            node_fn_a
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_a.mut_children().push(node_fn_c);
            node_fn_a
        };

        let node_fn_a_2 = {
            // node e
            let mut node_e = Expr::new();
            node_e.set_tp(ExprType::Float64);
            node_e
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_e.mut_val().encode_f64(-1.5).unwrap();

            // node f
            let mut node_f = Expr::new();
            node_f.set_tp(ExprType::Float64);
            node_f
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_f.mut_val().encode_f64(100.12).unwrap();

            // FnB
            let mut node_fn_b = Expr::new();
            node_fn_b.set_tp(ExprType::ScalarFunc);
            node_fn_b.set_sig(ScalarFuncSig::CastIntAsReal);
            node_fn_b
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_fn_b.mut_children().push(node_e);
            node_fn_b.mut_children().push(node_f);

            // FnA
            let mut node_fn_a = Expr::new();
            node_fn_a.set_tp(ExprType::ScalarFunc);
            node_fn_a.set_sig(ScalarFuncSig::CastIntAsInt);
            node_fn_a
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_fn_a.mut_children().push(node_fn_b);
            node_fn_a
        };

        // node a (NULL)
        let mut node_a = Expr::new();
        node_a.set_tp(ExprType::Null);
        node_a
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Double);

        // FnD
        let mut node_fn_d = Expr::new();
        node_fn_d.set_tp(ExprType::ScalarFunc);
        node_fn_d.set_sig(ScalarFuncSig::CastIntAsDecimal);
        node_fn_d
            .mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Double);
        node_fn_d.mut_children().push(node_a);
        node_fn_d.mut_children().push(node_fn_a_1);
        node_fn_d.mut_children().push(node_fn_a_2);

        let mut vec = vec![];
        RpnExpressionNodeVec::append_rpn_nodes_recursively(
            node_fn_d,
            Tz::utc(),
            &mut vec,
            fn_mapper,
        )
        .unwrap();

        let mut it = vec.into_iter();

        // node a
        assert!(it
            .next()
            .unwrap()
            .constant_value()
            .unwrap()
            .as_real()
            .is_none());

        // node b
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_int()
                .unwrap(),
            7
        );

        // node c
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_int()
                .unwrap(),
            3
        );

        // node d
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_int()
                .unwrap(),
            11
        );

        // FnC
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name(), "FnC");

        // FnA
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name(), "FnA");

        // node e
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_real()
                .unwrap(),
            -1.5
        );

        // node f
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_real()
                .unwrap(),
            100.12
        );

        // FnB
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name(), "FnB");

        // FnA
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name(), "FnA");

        // FnD
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name(), "FnD");

        // Finish
        assert!(it.next().is_none())
    }

    fn new_gt_int_def(offset: usize, val: u64) -> Expr {
        // GTInt(ColumnRef(offset), Uint64(val))
        use crate::util::codec::number::NumberEncoder;

        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::GTInt);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.mut_children().push({
            let mut lhs = Expr::new();
            lhs.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            lhs.set_tp(ExprType::ColumnRef);
            lhs.mut_val().encode_i64(offset as i64).unwrap();
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::new();
            rhs.mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            rhs.set_tp(ExprType::Uint64);
            rhs.mut_val().encode_u64(val).unwrap();
            rhs
        });
        expr
    }

    #[test]
    fn test_build_from_expr_tree() {
        let expr = new_gt_int_def(1, 123);
        let rpn_nodes = RpnExpressionNodeVec::build_from_expr_tree(expr, Tz::utc()).unwrap();

        assert_eq!(rpn_nodes.len(), 3);
        assert!(rpn_nodes[0].field_type().is_none());
        assert_eq!(rpn_nodes[0].column_ref_offset().unwrap(), 1);
        assert_eq!(
            rpn_nodes[1].field_type().unwrap().tp(),
            FieldTypeTp::LongLong
        );
        assert_eq!(
            rpn_nodes[1].constant_value().unwrap().as_int().unwrap(),
            123
        );
        assert_eq!(
            rpn_nodes[2].field_type().unwrap().tp(),
            FieldTypeTp::LongLong
        );
        assert_eq!(rpn_nodes[2].fn_call_func().unwrap().name(), "RpnFnGTInt");

        // TODO: Nested
    }

    #[test]
    fn test_eval() {
        let expr = new_gt_int_def(0, 10);
        let rpn_nodes = RpnExpressionNodeVec::build_from_expr_tree(expr, Tz::utc()).unwrap();

        let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(100, EvalType::Int);
        col.mut_decoded().push_int(Some(1));
        col.mut_decoded().push_int(None);
        col.mut_decoded().push_int(Some(-1));
        col.mut_decoded().push_int(Some(10));
        col.mut_decoded().push_int(Some(35));
        col.mut_decoded().push_int(None);
        col.mut_decoded().push_int(Some(7));
        col.mut_decoded().push_int(Some(15));

        let mut ft = FieldType::new();
        ft.as_mut_accessor().set_tp(FieldTypeTp::LongLong);
        let schema = [ft];

        let mut cols = LazyBatchColumnVec::from(vec![col]);
        let mut ctx = EvalContext::default();
        let ret = rpn_nodes
            .eval(&mut ctx, cols.rows_len(), &schema, &mut cols)
            .unwrap();
        assert_eq!(ret.field_type().tp(), FieldTypeTp::LongLong);
        assert_eq!(
            ret.vector_value().unwrap().as_int_slice(),
            &[
                Some(0),
                None,
                Some(0),
                Some(0),
                Some(1),
                None,
                Some(0),
                Some(1)
            ]
        );
    }
}
