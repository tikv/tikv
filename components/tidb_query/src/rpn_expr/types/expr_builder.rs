// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use codec::prelude::NumberDecoder;
use tidb_query_datatype::{EvalType, FieldTypeAccessor};
use tipb::{Expr, ExprType, FieldType};

use super::super::function::RpnFnMeta;
use super::expr::{RpnExpression, RpnExpressionNode};
use crate::codec::data_type::*;
use crate::codec::mysql::{JsonDecoder, Tz, MAX_FSP};
use crate::codec::{datum, Datum};
use crate::Result;

/// Helper to build an `RpnExpression`.
#[derive(Debug, Clone)]
pub struct RpnExpressionBuilder(Vec<RpnExpressionNode>);

impl RpnExpressionBuilder {
    /// Checks whether the given expression definition tree is supported.
    pub fn check_expr_tree_supported(c: &Expr) -> Result<()> {
        // TODO: This logic relies on the correctness of the passed in GROUP BY eval type. However
        // it can be different from the one we calculated (e.g. pass a column / fn with different
        // type).
        box_try!(EvalType::try_from(c.get_field_type().as_accessor().tp()));

        match c.get_tp() {
            ExprType::ScalarFunc => {
                super::super::map_expr_node_to_rpn_func(c)?;
                for n in c.get_children() {
                    RpnExpressionBuilder::check_expr_tree_supported(n)?;
                }
            }
            ExprType::Null => {}
            ExprType::Int64 => {}
            ExprType::Uint64 => {}
            ExprType::String | ExprType::Bytes => {}
            ExprType::Float32 | ExprType::Float64 => {}
            ExprType::MysqlTime => {}
            ExprType::MysqlDuration => {}
            ExprType::MysqlDecimal => {}
            ExprType::MysqlJson => {}
            ExprType::ColumnRef => {}
            _ => return Err(other_err!("Blacklist expression type {:?}", c.get_tp())),
        }

        Ok(())
    }

    /// Gets the result type when expression tree is converted to RPN expression and evaluated.
    /// The result type will be either scalar or vector.
    pub fn is_expr_eval_to_scalar(c: &Expr) -> Result<bool> {
        match c.get_tp() {
            ExprType::Null
            | ExprType::Int64
            | ExprType::Uint64
            | ExprType::String
            | ExprType::Bytes
            | ExprType::Float32
            | ExprType::Float64
            | ExprType::MysqlTime
            | ExprType::MysqlDuration
            | ExprType::MysqlDecimal
            | ExprType::MysqlJson => Ok(true),
            ExprType::ScalarFunc => Ok(false),
            ExprType::ColumnRef => Ok(false),
            _ => Err(other_err!("Unsupported expression type {:?}", c.get_tp())),
        }
    }

    /// Builds the RPN expression node list from an expression definition tree.
    pub fn build_from_expr_tree(
        tree_node: Expr,
        time_zone: &Tz,
        max_columns: usize,
    ) -> Result<RpnExpression> {
        let mut expr_nodes = Vec::new();
        append_rpn_nodes_recursively(
            tree_node,
            &mut expr_nodes,
            time_zone,
            super::super::map_expr_node_to_rpn_func,
            max_columns,
        )?;
        Ok(RpnExpression::from(expr_nodes))
    }

    /// Only used in tests, with a customized function mapper.
    #[cfg(test)]
    pub fn build_from_expr_tree_with_fn_mapper<F>(
        tree_node: Expr,
        fn_mapper: F,
        max_columns: usize,
    ) -> Result<RpnExpression>
    where
        F: Fn(&Expr) -> Result<RpnFnMeta> + Copy,
    {
        let mut expr_nodes = Vec::new();
        append_rpn_nodes_recursively(
            tree_node,
            &mut expr_nodes,
            &Tz::utc(),
            fn_mapper,
            max_columns,
        )?;
        Ok(RpnExpression::from(expr_nodes))
    }

    /// Creates a new builder instance.
    ///
    /// Only used in tests. Normal logic should use `build_from_expr_tree`.
    #[cfg(test)]
    pub fn new() -> Self {
        Self(Vec::new())
    }

    /// Pushes a `FnCall` node.
    #[cfg(test)]
    pub fn push_fn_call(
        mut self,
        func_meta: RpnFnMeta,
        args_len: usize,
        return_field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func_meta,
            args_len,
            field_type: return_field_type.into(),
            implicit_args: vec![],
        };
        self.0.push(node);
        self
    }

    #[cfg(test)]
    pub fn push_fn_call_with_implicit_args(
        mut self,
        func_meta: RpnFnMeta,
        args_len: usize,
        return_field_type: impl Into<FieldType>,
        implicit_args: Vec<ScalarValue>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func_meta,
            args_len,
            field_type: return_field_type.into(),
            implicit_args,
        };
        self.0.push(node);
        self
    }

    /// Pushes a `Constant` node. The field type will be auto inferred by choosing an arbitrary
    /// field type that matches the field type of the given value.
    #[cfg(test)]
    pub fn push_constant(mut self, value: impl Into<ScalarValue>) -> Self {
        let value = value.into();
        let field_type = value
            .eval_type()
            .into_certain_field_type_tp_for_test()
            .into();
        let node = RpnExpressionNode::Constant { value, field_type };
        self.0.push(node);
        self
    }

    /// Pushes a `Constant` node.
    #[cfg(test)]
    pub fn push_constant_with_field_type(
        mut self,
        value: impl Into<ScalarValue>,
        field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::Constant {
            value: value.into(),
            field_type: field_type.into(),
        };
        self.0.push(node);
        self
    }

    /// Pushes a `ColumnRef` node.
    #[cfg(test)]
    pub fn push_column_ref(mut self, offset: usize) -> Self {
        let node = RpnExpressionNode::ColumnRef { offset };
        self.0.push(node);
        self
    }

    /// Builds the `RpnExpression`.
    #[cfg(test)]
    pub fn build(self) -> RpnExpression {
        RpnExpression::from(self.0)
    }
}

impl AsRef<[RpnExpressionNode]> for RpnExpressionBuilder {
    fn as_ref(&self) -> &[RpnExpressionNode] {
        self.0.as_ref()
    }
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
    tree_node: Expr,
    rpn_nodes: &mut Vec<RpnExpressionNode>,
    time_zone: &Tz,
    fn_mapper: F,
    max_columns: usize,
    // TODO: Passing `max_columns` is only a workaround solution that works when we only check
    // column offset. To totally check whether or not the expression is valid, we need to pass in
    // the full schema instead.
) -> Result<()>
where
    F: Fn(&Expr) -> Result<RpnFnMeta> + Copy,
{
    match tree_node.get_tp() {
        ExprType::ScalarFunc => {
            handle_node_fn_call(tree_node, rpn_nodes, time_zone, fn_mapper, max_columns)
        }
        ExprType::ColumnRef => handle_node_column_ref(tree_node, rpn_nodes, max_columns),
        _ => handle_node_constant(tree_node, rpn_nodes, time_zone),
    }
}

#[inline]
fn handle_node_column_ref(
    tree_node: Expr,
    rpn_nodes: &mut Vec<RpnExpressionNode>,
    max_columns: usize,
) -> Result<()> {
    let offset = tree_node
        .get_val()
        .read_i64()
        .map_err(|_| other_err!("Unable to decode column reference offset from the request"))?
        as usize;
    if offset >= max_columns {
        return Err(other_err!(
            "Invalid column offset (schema has {} columns, access index {})",
            max_columns,
            offset
        ));
    }
    rpn_nodes.push(RpnExpressionNode::ColumnRef { offset });
    Ok(())
}

#[inline]
fn handle_node_fn_call<F>(
    mut tree_node: Expr,
    rpn_nodes: &mut Vec<RpnExpressionNode>,
    time_zone: &Tz,
    fn_mapper: F,
    max_columns: usize,
) -> Result<()>
where
    F: Fn(&Expr) -> Result<RpnFnMeta> + Copy,
{
    // Map pb func to `RpnFnMeta`.
    let func_meta = fn_mapper(&tree_node)?;

    // Validate the input expression.
    (func_meta.validator_ptr)(&tree_node).map_err(|e| {
        other_err!(
            "Invalid {} (sig = {:?}) signature: {}",
            func_meta.name,
            tree_node.get_sig(),
            e
        )
    })?;

    let args: Vec<_> = tree_node.take_children().into();
    let args_len = args.len();

    // Only Int/Real/Duration/Decimal/Bytes/Json will be decoded
    let datums = datum::decode(&mut tree_node.get_val())?;
    let mut implicit_args = Vec::with_capacity(datums.len());
    for d in datums {
        let arg = match d {
            Datum::I64(n) => ScalarValue::Int(Some(n)),
            Datum::U64(n) => ScalarValue::Int(Some(n as i64)),
            Datum::F64(n) => ScalarValue::Real(Real::new(n).ok()),
            Datum::Dur(dur) => ScalarValue::Duration(Some(dur)),
            Datum::Bytes(bytes) => ScalarValue::Bytes(Some(bytes)),
            Datum::Dec(dec) => ScalarValue::Decimal(Some(dec)),
            Datum::Json(json) => ScalarValue::Json(Some(json)),
            _ => return Err(other_err!("Unsupported push down datum {}", d)),
        };
        implicit_args.push(arg);
    }

    // Visit children first, then push current node, so that it is a post-order traversal.
    for arg in args {
        append_rpn_nodes_recursively(arg, rpn_nodes, time_zone, fn_mapper, max_columns)?;
    }
    rpn_nodes.push(RpnExpressionNode::FnCall {
        func_meta,
        args_len,
        field_type: tree_node.take_field_type(),
        implicit_args,
    });
    Ok(())
}

#[inline]
fn handle_node_constant(
    mut tree_node: Expr,
    rpn_nodes: &mut Vec<RpnExpressionNode>,
    time_zone: &Tz,
) -> Result<()> {
    let eval_type = box_try!(EvalType::try_from(
        tree_node.get_field_type().as_accessor().tp()
    ));

    let scalar_value = match tree_node.get_tp() {
        ExprType::Null => get_scalar_value_null(eval_type),
        ExprType::Int64 if eval_type == EvalType::Int => {
            extract_scalar_value_int64(tree_node.take_val())?
        }
        ExprType::Uint64 if eval_type == EvalType::Int => {
            extract_scalar_value_uint64(tree_node.take_val())?
        }
        ExprType::String | ExprType::Bytes if eval_type == EvalType::Bytes => {
            extract_scalar_value_bytes(tree_node.take_val())?
        }
        ExprType::Float32 | ExprType::Float64 if eval_type == EvalType::Real => {
            extract_scalar_value_float(tree_node.take_val())?
        }
        ExprType::MysqlTime if eval_type == EvalType::DateTime => extract_scalar_value_date_time(
            tree_node.take_val(),
            tree_node.get_field_type(),
            time_zone,
        )?,
        ExprType::MysqlDuration if eval_type == EvalType::Duration => {
            extract_scalar_value_duration(tree_node.take_val())?
        }
        ExprType::MysqlDecimal if eval_type == EvalType::Decimal => {
            extract_scalar_value_decimal(tree_node.take_val())?
        }
        ExprType::MysqlJson if eval_type == EvalType::Json => {
            extract_scalar_value_json(tree_node.take_val())?
        }
        expr_type => {
            return Err(other_err!(
                "Unexpected ExprType {:?} and EvalType {:?}",
                expr_type,
                eval_type
            ))
        }
    };
    rpn_nodes.push(RpnExpressionNode::Constant {
        value: scalar_value,
        field_type: tree_node.take_field_type(),
    });
    Ok(())
}

#[inline]
fn get_scalar_value_null(eval_type: EvalType) -> ScalarValue {
    match_template_evaluable! {
        TT, match eval_type {
            EvalType::TT => ScalarValue::TT(None),
        }
    }
}

#[inline]
fn extract_scalar_value_int64(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_i64()
        .map_err(|_| other_err!("Unable to decode int64 from the request"))?;
    Ok(ScalarValue::Int(Some(value)))
}

#[inline]
fn extract_scalar_value_uint64(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_u64()
        .map_err(|_| other_err!("Unable to decode uint64 from the request"))?;
    Ok(ScalarValue::Int(Some(value as i64)))
}

#[inline]
fn extract_scalar_value_bytes(val: Vec<u8>) -> Result<ScalarValue> {
    Ok(ScalarValue::Bytes(Some(val)))
}

#[inline]
fn extract_scalar_value_float(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_f64()
        .map_err(|_| other_err!("Unable to decode float from the request"))?;
    Ok(ScalarValue::Real(Real::new(value).ok()))
}

#[inline]
fn extract_scalar_value_date_time(
    val: Vec<u8>,
    field_type: &FieldType,
    time_zone: &Tz,
) -> Result<ScalarValue> {
    let v = val
        .as_slice()
        .read_u64()
        .map_err(|_| other_err!("Unable to decode date time from the request"))?;
    let fsp = field_type.decimal() as i8;
    let value =
        DateTime::from_packed_u64(v, field_type.as_accessor().tp().try_into()?, fsp, time_zone)
            .map_err(|_| other_err!("Unable to decode date time from the request"))?;
    Ok(ScalarValue::DateTime(Some(value)))
}

#[inline]
fn extract_scalar_value_duration(val: Vec<u8>) -> Result<ScalarValue> {
    let n = val
        .as_slice()
        .read_i64()
        .map_err(|_| other_err!("Unable to decode duration from the request"))?;
    let value = Duration::from_nanos(n, MAX_FSP)
        .map_err(|_| other_err!("Unable to decode duration from the request"))?;
    Ok(ScalarValue::Duration(Some(value)))
}

#[inline]
fn extract_scalar_value_decimal(val: Vec<u8>) -> Result<ScalarValue> {
    use crate::codec::mysql::DecimalDecoder;
    let value = val
        .as_slice()
        .read_decimal()
        .map_err(|_| other_err!("Unable to decode decimal from the request"))?;
    Ok(ScalarValue::Decimal(Some(value)))
}

#[inline]
fn extract_scalar_value_json(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_json()
        .map_err(|_| other_err!("Unable to decode json from the request"))?;
    Ok(ScalarValue::Json(Some(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use tidb_query_codegen::rpn_fn;
    use tidb_query_datatype::FieldTypeTp;
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use crate::codec::datum::{self, Datum};
    use crate::Result;

    /// An RPN function for test. It accepts 1 int argument, returns float.
    #[rpn_fn]
    fn fn_a(_v: &Option<i64>) -> Result<Option<Real>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 2 float arguments, returns int.
    #[rpn_fn]
    fn fn_b(_v1: &Option<Real>, _v2: &Option<Real>) -> Result<Option<i64>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 3 int arguments, returns int.
    #[rpn_fn]
    fn fn_c(_v1: &Option<i64>, _v2: &Option<i64>, _v3: &Option<i64>) -> Result<Option<i64>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 3 float arguments, returns float.
    #[rpn_fn]
    fn fn_d(_v1: &Option<Real>, _v2: &Option<Real>, _v3: &Option<Real>) -> Result<Option<Real>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn]
    fn fn_e(_v1: &Option<Int>, _v2: &Option<Real>) -> Result<Option<Bytes>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(varg)]
    fn fn_f(_v: &[&Option<Int>]) -> Result<Option<Real>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(varg, min_args = 2)]
    fn fn_g(_v: &[&Option<Real>]) -> Result<Option<Int>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(raw_varg, min_args = 1)]
    fn fn_h(_v: &[ScalarValueRef<'_>]) -> Result<Option<Real>> {
        unreachable!()
    }

    /// For testing `append_rpn_nodes_recursively`. It accepts protobuf function sig enum, which
    /// cannot be modified by us in tests to support fn_a ~ fn_d. So let's just hard code some
    /// substitute.
    fn fn_mapper(expr: &Expr) -> Result<RpnFnMeta> {
        // fn_a: CastIntAsInt
        // fn_b: CastIntAsReal
        // fn_c: CastIntAsString
        // fn_d: CastIntAsDecimal
        // fn_e: CastIntAsTime
        // fn_f: CastIntAsDuration
        // fn_g: CastIntAsJson
        // fn_h: CastRealAsInt
        Ok(match expr.get_sig() {
            ScalarFuncSig::CastIntAsInt => fn_a_fn_meta(),
            ScalarFuncSig::CastIntAsReal => fn_b_fn_meta(),
            ScalarFuncSig::CastIntAsString => fn_c_fn_meta(),
            ScalarFuncSig::CastIntAsDecimal => fn_d_fn_meta(),
            ScalarFuncSig::CastIntAsTime => fn_e_fn_meta(),
            ScalarFuncSig::CastIntAsDuration => fn_f_fn_meta(),
            ScalarFuncSig::CastIntAsJson => fn_g_fn_meta(),
            ScalarFuncSig::CastRealAsInt => fn_h_fn_meta(),
            _ => unreachable!(),
        })
    }

    #[test]
    fn test_validator_fixed_args_fn() {
        // Correct signature
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_real(3.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_real(3.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect number of arguments
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(1.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect argument type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());
    }

    #[test]
    fn test_validator_vargs_fn() {
        // Correct signature
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_int(5))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_int(5))
                .push_child(ExprDefBuilder::constant_int(4))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        // Incorrect return type
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect argument type
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(3.0))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(3.0))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());
    }

    #[test]
    fn test_validator_vargs_fn_with_min_args() {
        // Correct signature
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        // Insufficient arguments
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect types
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .push_child(ExprDefBuilder::constant_int(42))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());
    }

    #[test]
    fn test_validator_raw_vargs_fn_with_min_args() {
        // Correct signature
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_ok());

        // Insufficient arguments
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::Double).build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0);
        assert!(exp.is_err());
    }

    #[test]
    #[allow(clippy::float_cmp)]
    fn test_append_rpn_nodes_recursively() {
        // Input:
        // fn_d(a, fn_a(fn_c(b, c, d)), fn_a(fn_b(e, f))
        //
        // Tree:
        //          fn_d
        // +----------+----------+
        // a        fn_a       fn_a
        //            |          |
        //          fn_c       fn_b
        //        +---+---+      +---+
        //        b   c   d      e   f
        //
        // RPN:
        // a b c d fn_c fn_a e f fn_b fn_a fn_d

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDecimal, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_null(FieldTypeTp::Double))
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::Double)
                        .push_child(
                            ExprDefBuilder::scalar_func(
                                ScalarFuncSig::CastIntAsString,
                                FieldTypeTp::LongLong,
                            )
                            .push_child(ExprDefBuilder::constant_int(7))
                            .push_child(ExprDefBuilder::constant_int(3))
                            .push_child(ExprDefBuilder::constant_int(11)),
                        ),
                )
                .push_child(
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::Double)
                        .push_child(
                            ExprDefBuilder::scalar_func(
                                ScalarFuncSig::CastIntAsReal,
                                FieldTypeTp::LongLong,
                            )
                            .push_child(ExprDefBuilder::constant_real(-1.5))
                            .push_child(ExprDefBuilder::constant_real(100.12)),
                        ),
                )
                .build();

        let mut it = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0)
            .unwrap()
            .into_inner()
            .into_iter();

        // node a
        assert!(it.next().unwrap().constant_value().as_real().is_none());

        // node b
        assert_eq!(7, it.next().unwrap().constant_value().as_int().unwrap());

        // node c
        assert_eq!(3, it.next().unwrap().constant_value().as_int().unwrap());

        // node d
        assert_eq!(11, it.next().unwrap().constant_value().as_int().unwrap());

        // fn_c
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_c");

        // fn_a
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_a");

        // node e
        assert_eq!(
            &Real::new(-1.5).ok(),
            it.next().unwrap().constant_value().as_real()
        );

        // node f
        assert_eq!(
            &Real::new(100.12).ok(),
            it.next().unwrap().constant_value().as_real()
        );

        // fn_b
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_b");

        // fn_a
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_a");

        // fn_d
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_d");

        // Finish
        assert!(it.next().is_none())
    }

    #[test]
    fn test_max_columns_check() {
        // Col offset = 0. The minimum success max_columns is 1.
        let node = ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build();
        assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node.clone(),
            fn_mapper,
            0
        )
        .is_err());
        for i in 1..10 {
            assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node.clone(),
                fn_mapper,
                i
            )
            .is_ok());
        }

        // Col offset = 3. The minimum success max_columns is 4.
        let node = ExprDefBuilder::column_ref(3, FieldTypeTp::LongLong).build();
        for i in 0..=3 {
            assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node.clone(),
                fn_mapper,
                i
            )
            .is_err());
        }
        for i in 4..10 {
            assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node.clone(),
                fn_mapper,
                i
            )
            .is_ok());
        }

        // Col offset = 1, 2, 5. The minimum success max_columns is 6.
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsString, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::column_ref(5, FieldTypeTp::LongLong))
                .build();

        for i in 0..=5 {
            assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node.clone(),
                fn_mapper,
                i
            )
            .is_err());
        }
        for i in 6..10 {
            assert!(RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
                node.clone(),
                fn_mapper,
                i
            )
            .is_ok());
        }
    }

    #[test]
    #[allow(clippy::iter_skip_next)]
    fn test_expr_with_val() {
        fn build_expr() -> Expr {
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsReal, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_real(7.0))
                .push_child(ExprDefBuilder::constant_real(3.0))
                .build()
        }

        // Simple cases
        // bytes generated from TiDB
        // datum(0)
        let mut expr = build_expr();
        expr.mut_val().extend(&vec![8, 0]);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, &fn_mapper, 0)
            .unwrap()
            .into_inner();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, vec![ScalarValue::Int(Some(0))]);
            }
            _ => unreachable!(),
        }

        // datum(1)
        let mut expr = build_expr();
        expr.mut_val().extend(&vec![8, 2]);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, fn_mapper, 0)
            .unwrap()
            .into_inner();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, vec![ScalarValue::Int(Some(1))]);
            }
            _ => unreachable!(),
        }

        // bytes generated from TiKV
        // datum(0)
        let bytes = datum::encode_value(&[Datum::I64(0)]).unwrap();
        let mut expr = build_expr();
        expr.set_val(bytes);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, fn_mapper, 0)
            .unwrap()
            .into_inner();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, vec![ScalarValue::Int(Some(0))]);
            }
            _ => unreachable!(),
        }

        // datum(1)
        let bytes = datum::encode_value(&[Datum::I64(1)]).unwrap();
        let mut expr = build_expr();
        expr.set_val(bytes);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, fn_mapper, 0)
            .unwrap()
            .into_inner();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, vec![ScalarValue::Int(Some(1))]);
            }
            _ => unreachable!(),
        }

        // Combine various datums
        // bytes generated from TiDB
        // Int/Real/Duration/Decimal/Bytes/Json
        let mut expr = build_expr();
        expr.mut_val().extend(&vec![
            8, 0, 5, 191, 241, 153, 153, 153, 153, 153, 154, 2, 18, 102, 114, 111, 109, 32, 84,
            105, 68, 66, 6, 2, 1, 129, 5, 10, 4, 2, 7, 128, 0, 0, 0, 0, 0, 39, 16,
        ]);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, fn_mapper, 0)
            .unwrap()
            .into_inner();
        let args = vec![
            ScalarValue::Int(Some(0)),
            ScalarValue::Real(Some(Real::new(1.1).unwrap())),
            ScalarValue::Bytes(Some(b"from TiDB".to_vec())),
            ScalarValue::Decimal(Some(Decimal::from_bytes(b"1.5").unwrap().unwrap())),
            ScalarValue::Json(Some(Json::Boolean(false))),
            ScalarValue::Duration(Some(Duration::from_micros(10, 6).unwrap())),
        ];
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, args);
            }
            _ => unreachable!(),
        }

        // bytes generated from TiKV
        let datums = vec![
            Datum::I64(0),
            Datum::F64(1.1),
            Datum::Bytes(b"from TiKV".to_vec()),
            Datum::Dec(Decimal::from_bytes(b"1.5").unwrap().unwrap()),
            Datum::Json(Json::Boolean(false)),
            Datum::Dur(Duration::from_nanos(10000, 3).unwrap()),
        ];
        let bytes = datum::encode_value(&datums).unwrap();
        let mut expr = build_expr();
        expr.set_val(bytes);
        let vec = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(expr, fn_mapper, 0)
            .unwrap()
            .into_inner();
        let args = vec![
            ScalarValue::Int(Some(0)),
            ScalarValue::Real(Some(Real::new(1.1).unwrap())),
            ScalarValue::Bytes(Some(b"from TiKV".to_vec())),
            ScalarValue::Decimal(Some(Decimal::from_bytes(b"1.5").unwrap().unwrap())),
            ScalarValue::Json(Some(Json::Boolean(false))),
            ScalarValue::Duration(Some(Duration::from_nanos(10000, 3).unwrap())),
        ];
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { implicit_args, .. } => {
                assert_eq!(implicit_args, args);
            }
            _ => unreachable!(),
        }
    }
}
