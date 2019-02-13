// Copyright 2018 PingCAP, Inc.
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

use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::{Expr, ExprType, FieldType};

use super::function::RpnFunction;
use crate::coprocessor::codec::batch::LazyBatchColumnVec;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalConfig;
use crate::coprocessor::data_type::{ScalarValue, ScalarValueRef, VectorValue, VectorValueRef};

/// Global variables needed in evaluating RPN expression.
#[derive(Debug)]
pub struct RpnExpressionEvalContext {
    pub config: Arc<EvalConfig>,
    // FIXME: warnings
}

impl RpnExpressionEvalContext {
    pub fn new(config: Arc<EvalConfig>) -> Self {
        Self { config }
    }
}

impl Default for RpnExpressionEvalContext {
    fn default() -> Self {
        Self::new(Arc::new(EvalConfig::default()))
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
    // There can be frequent stack push & pops, so we wrap this field in a `Box` to reduce move
    // cost.
    Owned(Box<VectorValue>),
    Ref(VectorValueRef<'a>),
}

impl<'a> RpnStackNodeVectorValue<'a> {
    #[inline]
    pub fn borrow(&'a self) -> VectorValueRef<'a> {
        match self {
            RpnStackNodeVectorValue::Owned(ref value) => value.borrow(),
            RpnStackNodeVectorValue::Ref(ref value) => *value,
        }
    }
}

/// Type for each node in the RPN evaluation stack.
pub enum RpnStackNode<'a> {
    /// Represents a scalar value. Comes from a constant node in expression list.
    Scalar {
        value: ScalarValueRef<'a>,
        field_type: &'a FieldType,
    },

    /// Represents a vector value. Comes from a column reference or evaluated result.
    Vector {
        value: RpnStackNodeVectorValue<'a>,
        field_type: &'a FieldType,
    },
}

impl<'a> RpnStackNode<'a> {
    #[inline]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnStackNode::Scalar { ref field_type, .. } => field_type,
            RpnStackNode::Vector { ref field_type, .. } => field_type,
        }
    }

    // TODO: Maybe returning Option<T> is better.

    #[inline]
    pub fn scalar_value(&self) -> ScalarValueRef {
        match self {
            RpnStackNode::Scalar { ref value, .. } => *value,
            RpnStackNode::Vector { .. } => panic!(),
        }
    }

    #[inline]
    pub fn vector_value(&self) -> VectorValueRef {
        match self {
            RpnStackNode::Scalar { .. } => panic!(),
            RpnStackNode::Vector { ref value, .. } => value.borrow(),
        }
    }

    #[inline]
    pub fn is_scalar(&self) -> bool {
        match self {
            RpnStackNode::Scalar { .. } => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_vector(&self) -> bool {
        match self {
            RpnStackNode::Vector { .. } => true,
            _ => false,
        }
    }
}

/// Type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function.
    Fn {
        func: RpnFunction,
        field_type: FieldType,
    },

    /// Represents a scalar constant value.
    Constant {
        value: ScalarValue,
        field_type: FieldType,
    },

    /// Represents a reference to a table column.
    TableColumnRef {
        offset: usize,

        // Although we can know `ColumnInfo` according to `offset` and columns info in scan
        // executors, its type is `ColumnInfo` instead of `FieldType`..
        field_type: FieldType,
    },
}

impl RpnExpressionNode {
    #[inline]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnExpressionNode::Fn { ref field_type, .. } => field_type,
            RpnExpressionNode::Constant { ref field_type, .. } => field_type,
            RpnExpressionNode::TableColumnRef { ref field_type, .. } => field_type,
        }
    }

    #[inline]
    pub fn fn_func(&self) -> Option<RpnFunction> {
        match self {
            RpnExpressionNode::Fn { ref func, .. } => Some(*func),
            _ => None,
        }
    }

    #[inline]
    pub fn constant_value(&self) -> Option<&ScalarValue> {
        match self {
            RpnExpressionNode::Constant { ref value, .. } => Some(value),
            _ => None,
        }
    }

    #[inline]
    pub fn table_column_ref_offset(&self) -> Option<usize> {
        match self {
            RpnExpressionNode::TableColumnRef { ref offset, .. } => Some(*offset),
            _ => None,
        }
    }
}

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
    pub fn build_from_def(def: Expr, time_zone: Tz) -> Self {
        let mut expr_nodes = Vec::new();
        Self::append_rpn_nodes_recursively(def, time_zone, &mut expr_nodes);
        expr_nodes.reverse();
        RpnExpressionNodeVec(expr_nodes)
    }

    pub fn eval<'a>(
        &'a self,
        context: &mut RpnExpressionEvalContext,
        rows: usize,
        columns: &'a LazyBatchColumnVec,
        // Related columns must be decoded before calling this function!
    ) -> RpnStackNode<'a> {
        let mut stack = Vec::with_capacity(self.0.len());
        let mut args = Vec::with_capacity(10);
        for node in &self.0 {
            match node {
                RpnExpressionNode::Constant {
                    ref value,
                    ref field_type,
                } => {
                    stack.push(RpnStackNode::Scalar {
                        value: value.borrow(),
                        field_type,
                    });
                }
                RpnExpressionNode::TableColumnRef {
                    ref offset,
                    ref field_type,
                } => {
                    let decoded_column = columns[*offset].decoded();
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Ref(decoded_column.borrow()),
                        field_type,
                    });
                }
                RpnExpressionNode::Fn {
                    ref func,
                    ref field_type,
                } => {
                    let args_len = func.args_len();
                    // pop `args_len` args from stack.
                    args.clear();
                    // TODO: Evaluate from left to right or right to left?
                    // Currently we are evaluating from right to left!
                    for _ in 0..args_len {
                        args.push(stack.pop().unwrap());
                    }
                    let ret = func.eval(rows, context, args.as_slice());
                    stack.push(RpnStackNode::Vector {
                        value: RpnStackNodeVectorValue::Owned(Box::new(ret)),
                        field_type,
                    });
                }
            }
        }

        assert_eq!(stack.len(), 1);
        stack.into_iter().next().unwrap()
    }

    /// Useful in selection executor
    pub fn eval_as_bools(
        &self,
        context: &mut RpnExpressionEvalContext,
        rows: usize,
        columns: &LazyBatchColumnVec,
        outputs: &mut [bool], // modify an existing buffer to avoid repeated allocation
    ) {
        use crate::coprocessor::data_type::AsBool;

        assert!(outputs.len() >= rows);
        let values = self.eval(context, rows, columns);
        match values {
            RpnStackNode::Scalar { value, .. } => {
                let b = value.as_bool();
                for i in 0..rows {
                    outputs[i] = b;
                }
            }
            RpnStackNode::Vector { value, .. } => match value {
                RpnStackNodeVectorValue::Owned(vec) => {
                    assert_eq!(vec.len(), rows);
                    vec.as_bools(outputs);
                }
                RpnStackNodeVectorValue::Ref(vec) => {
                    assert_eq!(vec.len(), rows);
                    vec.as_bools(outputs);
                }
            },
        }
    }

    fn append_rpn_nodes_recursively(
        mut def: Expr,
        time_zone: Tz,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
    ) {
        use crate::coprocessor::codec::mysql::{Decimal, Duration, Json, Time, MAX_FSP};
        use crate::util::codec::number;
        use std::convert::{TryFrom, TryInto};

        let field_type = def.take_field_type();
        let eval_type = EvalType::try_from(field_type.tp()).unwrap();
        // FIXME: Don't use `unwrap()`.

        match def.get_tp() {
            ExprType::Null => {
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
                    field_type,
                })
            }
            ExprType::Int64 => {
                let scalar_value = match eval_type {
                    EvalType::Int => {
                        // FIXME: Don't use `unwrap()`.
                        let value = number::decode_i64(&mut def.get_val()).unwrap();
                        ScalarValue::Int(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::Uint64 => {
                let scalar_value = match eval_type {
                    EvalType::Int => {
                        // FIXME: Don't use `unwrap()`.
                        let value = number::decode_u64(&mut def.get_val()).unwrap();
                        ScalarValue::Int(Some(value as i64))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::String | ExprType::Bytes => {
                let scalar_value = match eval_type {
                    EvalType::Bytes => ScalarValue::Bytes(Some(def.take_val())),
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::Float32 | ExprType::Float64 => {
                let scalar_value = match eval_type {
                    EvalType::Real => {
                        // FIXME: Don't use `unwrap()`.
                        let value = number::decode_f64(&mut def.get_val()).unwrap();
                        ScalarValue::Real(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::MysqlTime => {
                let scalar_value = match eval_type {
                    EvalType::DateTime => {
                        let v = number::decode_u64(&mut def.get_val()).unwrap();
                        let fsp = field_type.decimal() as i8;
                        // FIXME: Don't use `unwrap()`.
                        let value = Time::from_packed_u64(
                            v,
                            field_type.tp().try_into().unwrap(),
                            fsp,
                            time_zone,
                        )
                        .unwrap();
                        ScalarValue::DateTime(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::MysqlDuration => {
                let scalar_value = match eval_type {
                    EvalType::Duration => {
                        let n = number::decode_i64(&mut def.get_val()).unwrap();
                        let value = Duration::from_nanos(n, MAX_FSP).unwrap();
                        ScalarValue::Duration(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::MysqlDecimal => {
                let scalar_value = match eval_type {
                    EvalType::Decimal => {
                        let value = Decimal::decode(&mut def.get_val()).unwrap();
                        ScalarValue::Decimal(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::MysqlJson => {
                let scalar_value = match eval_type {
                    EvalType::Json => {
                        let value = Json::decode(&mut def.get_val()).unwrap();
                        ScalarValue::Json(Some(value))
                    }
                    _ => panic!(),
                };
                rpn_nodes.push(RpnExpressionNode::Constant {
                    value: scalar_value,
                    field_type,
                })
            }
            ExprType::ScalarFunc => {
                let func = RpnFunction::try_from(def.get_sig()).unwrap();
                let args = def.take_children().into_vec();
                // FIXME: Don't use assert_eq
                assert_eq!(func.args_len(), args.len());
                rpn_nodes.push(RpnExpressionNode::Fn { func, field_type });
                for arg in args {
                    Self::append_rpn_nodes_recursively(arg, time_zone, rpn_nodes);
                }
            }
            ExprType::ColumnRef => {
                let offset = number::decode_i64(&mut def.get_val()).unwrap() as usize;
                rpn_nodes.push(RpnExpressionNode::TableColumnRef { offset, field_type });
            }
            // FIXME: Return error.
            _ => panic!(),
            // expr_type => Err(box_err!("Unsupported expression type {:?}", expr_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::codec::batch::LazyBatchColumn;

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
    fn test_build_from_def() {
        let expr = new_gt_int_def(1, 123);
        let rpn_nodes = RpnExpressionNodeVec::build_from_def(expr, Tz::utc());

        assert_eq!(rpn_nodes.len(), 3);
        assert_eq!(rpn_nodes[0].field_type().tp(), FieldTypeTp::LongLong);
        assert_eq!(
            rpn_nodes[0].constant_value().unwrap().as_int().unwrap(),
            123
        );
        assert_eq!(rpn_nodes[1].field_type().tp(), FieldTypeTp::LongLong);
        assert_eq!(rpn_nodes[1].table_column_ref_offset().unwrap(), 1);
        assert_eq!(rpn_nodes[2].field_type().tp(), FieldTypeTp::LongLong);
        assert_eq!(rpn_nodes[2].fn_func().unwrap(), RpnFunction::GTInt);

        // TODO: Nested
    }

    #[test]
    fn test_eval() {
        let expr = new_gt_int_def(0, 10);
        let rpn_nodes = RpnExpressionNodeVec::build_from_def(expr, Tz::utc());

        let mut col = LazyBatchColumn::decoded_with_capacity_and_tp(100, EvalType::Int);
        col.mut_decoded().push_int(Some(1));
        col.mut_decoded().push_int(None);
        col.mut_decoded().push_int(Some(-1));
        col.mut_decoded().push_int(Some(10));
        col.mut_decoded().push_int(Some(35));
        col.mut_decoded().push_int(None);
        col.mut_decoded().push_int(Some(7));
        col.mut_decoded().push_int(Some(15));

        let cols = LazyBatchColumnVec::from(vec![col]);
        let mut ctx = RpnExpressionEvalContext::default();
        let ret = rpn_nodes.eval(&mut ctx, cols.rows_len(), &cols);
        assert_eq!(ret.field_type().tp(), FieldTypeTp::LongLong);
        assert_eq!(
            ret.vector_value().into_int_slice(),
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
