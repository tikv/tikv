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
use crate::coprocessor::codec::data_type::ScalarValue;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::{Error, Result};

/// A type for each node in the RPN expression list.
#[derive(Debug)]
pub enum RpnExpressionNode {
    /// Represents a function.
    Fn {
        func: Box<dyn RpnFunction>,
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
        // Maybe we can remove this field in future.
        field_type: FieldType,
    },
}

impl RpnExpressionNode {
    /// Gets the field type.
    #[inline]
    pub fn field_type(&self) -> &FieldType {
        match self {
            RpnExpressionNode::Fn { ref field_type, .. } => field_type,
            RpnExpressionNode::Constant { ref field_type, .. } => field_type,
            RpnExpressionNode::TableColumnRef { ref field_type, .. } => field_type,
        }
    }

    /// Borrows the function instance for `Fn` variant.
    #[inline]
    pub fn fn_func(&self) -> Option<&dyn RpnFunction> {
        match self {
            RpnExpressionNode::Fn { ref func, .. } => Some(&*func),
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

    /// Gets the column offset for `TableColumnRef` variant.
    #[inline]
    pub fn table_column_ref_offset(&self) -> Option<usize> {
        match self {
            RpnExpressionNode::TableColumnRef { ref offset, .. } => Some(*offset),
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
    pub fn build_from_expr_tree(def: Expr, time_zone: Tz) -> Result<Self> {
        let mut expr_nodes = Vec::new();
        Self::append_rpn_nodes_recursively(
            def,
            time_zone,
            &mut expr_nodes,
            super::map_pb_sig_to_rpn_func,
        )?;
        Ok(RpnExpressionNodeVec(expr_nodes))
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
        mut def: Expr,
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
        fn handle_expr_tree_node_null(
            field_type: FieldType,
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
                field_type,
            });
            Ok(())
        }

        #[inline]
        fn handle_expr_tree_node_int64(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Int => {
                    let value = number::decode_i64(&mut def_without_field_type.get_val()).map_err(
                        |_| {
                            Error::Other(box_err!(
                                "Unable to decode {:?} from the request",
                                eval_type
                            ))
                        },
                    )?;
                    ScalarValue::Int(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_uint64(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Int => {
                    let value = number::decode_u64(&mut def_without_field_type.get_val()).map_err(
                        |_| {
                            Error::Other(box_err!(
                                "Unable to decode {:?} from the request",
                                eval_type
                            ))
                        },
                    )?;
                    ScalarValue::Int(Some(value as i64))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_bytes(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            mut def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Bytes => ScalarValue::Bytes(Some(def_without_field_type.take_val())),
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_float(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Real => {
                    let value = number::decode_f64(&mut def_without_field_type.get_val()).map_err(
                        |_| {
                            Error::Other(box_err!(
                                "Unable to decode {:?} from the request",
                                eval_type
                            ))
                        },
                    )?;
                    ScalarValue::Real(Some(value))
                }
                t => {
                    return Err(box_err!(
                        "Unexpected eval type {:?} for ExprType {:?}",
                        t,
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_time(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
            time_zone: Tz,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::DateTime => {
                    let v = number::decode_u64(&mut def_without_field_type.get_val()).map_err(
                        |_| {
                            Error::Other(box_err!(
                                "Unable to decode {:?} from the request",
                                eval_type
                            ))
                        },
                    )?;
                    let fsp = field_type.decimal() as i8;
                    let value =
                        Time::from_packed_u64(v, field_type.tp().try_into()?, fsp, time_zone)
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
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_duration(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Duration => {
                    let n = number::decode_i64(&mut def_without_field_type.get_val()).map_err(
                        |_| {
                            Error::Other(box_err!(
                                "Unable to decode {:?} from the request",
                                eval_type
                            ))
                        },
                    )?;
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
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_decimal(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Decimal => {
                    let value =
                        Decimal::decode(&mut def_without_field_type.get_val()).map_err(|_| {
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
                        def_without_field_type.get_tp()
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
        fn handle_expr_tree_node_json(
            field_type: FieldType,
            eval_type: EvalType,
            rpn_nodes: &mut Vec<RpnExpressionNode>,
            def_without_field_type: Expr,
        ) -> Result<()> {
            let scalar_value = match eval_type {
                EvalType::Json => {
                    let value =
                        Json::decode(&mut def_without_field_type.get_val()).map_err(|_| {
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
                        def_without_field_type.get_tp()
                    ));
                }
            };
            rpn_nodes.push(RpnExpressionNode::Constant {
                value: scalar_value,
                field_type,
            });
            Ok(())
        }

        let field_type = def.take_field_type();
        let eval_type =
            EvalType::try_from(field_type.tp()).map_err(|e| Error::Other(box_err!(e)))?;

        match def.get_tp() {
            ExprType::Null => handle_expr_tree_node_null(field_type, eval_type, rpn_nodes)?,
            ExprType::Int64 => handle_expr_tree_node_int64(field_type, eval_type, rpn_nodes, def)?,
            ExprType::Uint64 => {
                handle_expr_tree_node_uint64(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::String | ExprType::Bytes => {
                handle_expr_tree_node_bytes(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::Float32 | ExprType::Float64 => {
                handle_expr_tree_node_float(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::MysqlTime => {
                handle_expr_tree_node_time(field_type, eval_type, rpn_nodes, def, time_zone)?
            }
            ExprType::MysqlDuration => {
                handle_expr_tree_node_duration(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::MysqlDecimal => {
                handle_expr_tree_node_decimal(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::MysqlJson => {
                handle_expr_tree_node_json(field_type, eval_type, rpn_nodes, def)?
            }
            ExprType::ColumnRef => {
                let offset = number::decode_i64(&mut def.get_val()).map_err(|_| {
                    Error::Other(box_err!(
                        "Unable to decode column reference offset from the request"
                    ))
                })? as usize;
                rpn_nodes.push(RpnExpressionNode::TableColumnRef { offset, field_type });
            }
            ExprType::ScalarFunc => {
                // Map pb func to `RpnFunction`.
                let func = fn_mapper(def.get_sig())?;
                let args = def.take_children().into_vec();
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
                rpn_nodes.push(RpnExpressionNode::Fn { func, field_type });
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

    /// An RPN function for test. It accepts 1 int argument, returns the value in float.
    #[derive(Debug, Clone, Copy)]
    struct FnA;

    impl_template_fn! { 1 arg @ FnA }

    /// An RPN function for test. It accepts 2 float arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    struct FnB;

    impl_template_fn! { 2 arg @ FnB }

    /// An RPN function for test. It accepts 3 int arguments, returns their sum in int.
    #[derive(Debug, Clone, Copy)]
    struct FnC;

    impl_template_fn! { 3 arg @ FnC }

    /// An RPN function for test. It accepts 3 float arguments, returns their sum in float.
    #[derive(Debug, Clone, Copy)]
    struct FnD;

    impl_template_fn! { 3 arg @ FnD }

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
        assert_eq!(it.next().unwrap().fn_func().unwrap().name(), "FnC");

        // FnA
        assert_eq!(it.next().unwrap().fn_func().unwrap().name(), "FnA");

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
        assert_eq!(it.next().unwrap().fn_func().unwrap().name(), "FnB");

        // FnA
        assert_eq!(it.next().unwrap().fn_func().unwrap().name(), "FnA");

        // FnD
        assert_eq!(it.next().unwrap().fn_func().unwrap().name(), "FnD");

        // Finish
        assert!(it.next().is_none())
    }
}
