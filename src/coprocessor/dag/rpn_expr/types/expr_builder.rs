// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use cop_datatype::{EvalType, FieldTypeAccessor};
use tikv_util::codec::number;
use tipb::expression::{Expr, ExprType, FieldType};

use super::super::function::RpnFnMeta;
use super::expr::{RpnExpression, RpnExpressionNode};
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::codec::mysql::MAX_FSP;
use crate::coprocessor::codec::{datum, Datum};
use crate::coprocessor::{Error, Result};

/// Helper to build an `RpnExpression`.
#[derive(Debug, Clone)]
pub struct RpnExpressionBuilder(Vec<RpnExpressionNode>);

impl RpnExpressionBuilder {
    /// Checks whether the given expression definition tree is supported.
    pub fn check_expr_tree_supported(c: &Expr) -> Result<()> {
        // TODO: This logic relies on the correctness of the passed in GROUP BY eval type. However
        // it can be different from the one we calculated (e.g. pass a column / fn with different
        // type).
        box_try!(EvalType::try_from(c.get_field_type().tp()));

        match c.get_tp() {
            ExprType::ScalarFunc => {
                let sig = c.get_sig();
                super::super::map_pb_sig_to_rpn_func(sig, c.get_children())?;
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
            _ => return Err(box_err!("Unsupported expression type {:?}", c.get_tp())),
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
            _ => Err(box_err!("Unsupported expression type {:?}", c.get_tp())),
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
            super::super::map_pb_sig_to_rpn_func,
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
        F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<RpnFnMeta> + Copy,
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

    #[cfg(test)]
    pub fn push_fn_call_with_imp_params(
        mut self,
        func: RpnFnMeta,
        imp_params: Vec<ScalarParameter>,
        return_field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func,
            field_type: return_field_type.into(),
            imp_params,
        };
        self.0.push(node);
        self
    }

    /// Pushes a `FnCall` node.
    #[cfg(test)]
    pub fn push_fn_call(
        mut self,
        func: RpnFnMeta,
        return_field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func,
            field_type: return_field_type.into(),
            imp_params: vec![],
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
    F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<RpnFnMeta> + Copy,
{
    // TODO: We should check whether node types match the function signature. Otherwise there
    // will be panics when the expression is evaluated.

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
    let offset = number::decode_i64(&mut tree_node.get_val()).map_err(|_| {
        Error::Other(box_err!(
            "Unable to decode column reference offset from the request"
        ))
    })? as usize;
    if offset >= max_columns {
        return Err(box_err!(
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
    F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<RpnFnMeta> + Copy,
{
    // Map pb func to `RpnFnMeta`.
    let func = fn_mapper(tree_node.get_sig(), tree_node.get_children())?;
    let args = tree_node.take_children().into_vec();

    // Only Int/Real/Duration/Decimal/Bytes/Json will be decoded
    let datums = datum::decode(&mut tree_node.get_val())?;
    let imp_params = datums
        .into_iter()
        .map(|d| match d {
            Datum::I64(n) => ScalarParameter::Int(n),
            Datum::U64(n) => ScalarParameter::Int(n as i64),
            Datum::F64(n) => ScalarParameter::Real(Real::new(n).unwrap()),
            Datum::Dur(dur) => ScalarParameter::Duration(dur),
            Datum::Bytes(bytes) => ScalarParameter::Bytes(bytes),
            Datum::Dec(dec) => ScalarParameter::Decimal(dec),
            Datum::Json(json) => ScalarParameter::Json(json),
            _ => unreachable!(),
        })
        .collect();

    if func.args_len != args.len() {
        return Err(box_err!(
            "Unexpected arguments, expect {}, received {}",
            func.args_len,
            args.len()
        ));
    }
    // Visit children first, then push current node, so that it is a post-order traversal.
    for arg in args {
        append_rpn_nodes_recursively(arg, rpn_nodes, time_zone, fn_mapper, max_columns)?;
    }
    rpn_nodes.push(RpnExpressionNode::FnCall {
        func,
        field_type: tree_node.take_field_type(),
        imp_params,
    });
    Ok(())
}

#[inline]
fn handle_node_constant(
    mut tree_node: Expr,
    rpn_nodes: &mut Vec<RpnExpressionNode>,
    time_zone: &Tz,
) -> Result<()> {
    let eval_type = box_try!(EvalType::try_from(tree_node.get_field_type().tp()));

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
            return Err(box_err!(
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
    let value = number::decode_i64(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode int64 from the request")))?;
    Ok(ScalarValue::Int(Some(value)))
}

#[inline]
fn extract_scalar_value_uint64(val: Vec<u8>) -> Result<ScalarValue> {
    let value = number::decode_u64(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode uint64 from the request")))?;
    Ok(ScalarValue::Int(Some(value as i64)))
}

#[inline]
fn extract_scalar_value_bytes(val: Vec<u8>) -> Result<ScalarValue> {
    Ok(ScalarValue::Bytes(Some(val)))
}

#[inline]
fn extract_scalar_value_float(val: Vec<u8>) -> Result<ScalarValue> {
    let value = number::decode_f64(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode float from the request")))?;
    Ok(ScalarValue::Real(Real::new(value).ok()))
}

#[inline]
fn extract_scalar_value_date_time(
    val: Vec<u8>,
    field_type: &FieldType,
    time_zone: &Tz,
) -> Result<ScalarValue> {
    let v = number::decode_u64(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode date time from the request")))?;
    let fsp = field_type.decimal() as i8;
    let value = DateTime::from_packed_u64(v, field_type.tp().try_into()?, fsp, time_zone)
        .map_err(|_| Error::Other(box_err!("Unable to decode date time from the request")))?;
    Ok(ScalarValue::DateTime(Some(value)))
}

#[inline]
fn extract_scalar_value_duration(val: Vec<u8>) -> Result<ScalarValue> {
    let n = number::decode_i64(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode duration from the request")))?;
    let value = Duration::from_nanos(n, MAX_FSP)
        .map_err(|_| Error::Other(box_err!("Unable to decode duration from the request")))?;
    Ok(ScalarValue::Duration(Some(value)))
}

#[inline]
fn extract_scalar_value_decimal(val: Vec<u8>) -> Result<ScalarValue> {
    let value = Decimal::decode(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode decimal from the request")))?;
    Ok(ScalarValue::Decimal(Some(value)))
}

#[inline]
fn extract_scalar_value_json(val: Vec<u8>) -> Result<ScalarValue> {
    let value = Json::decode(&mut val.as_slice())
        .map_err(|_| Error::Other(box_err!("Unable to decode json from the request")))?;
    Ok(ScalarValue::Json(Some(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use cop_codegen::rpn_fn;
    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::codec::datum::{self, Datum};
    use crate::coprocessor::Result;
    use tikv_util::codec::number::NumberEncoder;

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

    /// For testing `append_rpn_nodes_recursively`. It accepts protobuf function sig enum, which
    /// cannot be modified by us in tests to support fn_a ~ fn_d. So let's just hard code some
    /// substitute.
    fn fn_mapper(value: ScalarFuncSig, _children: &[Expr]) -> Result<RpnFnMeta> {
        // fn_a: CastIntAsInt
        // fn_b: CastIntAsReal
        // fn_c: CastIntAsString
        // fn_d: CastIntAsDecimal
        Ok(match value {
            ScalarFuncSig::CastIntAsInt => fn_a_fn_meta(),
            ScalarFuncSig::CastIntAsReal => fn_b_fn_meta(),
            ScalarFuncSig::CastIntAsString => fn_c_fn_meta(),
            ScalarFuncSig::CastIntAsDecimal => fn_d_fn_meta(),
            _ => unreachable!(),
        })
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

            // fn_c
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

            // fn_a
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

            // fn_b
            let mut node_fn_b = Expr::new();
            node_fn_b.set_tp(ExprType::ScalarFunc);
            node_fn_b.set_sig(ScalarFuncSig::CastIntAsReal);
            node_fn_b
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_fn_b.mut_children().push(node_e);
            node_fn_b.mut_children().push(node_f);

            // fn_a
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

        // fn_d
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
        append_rpn_nodes_recursively(node_fn_d, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();

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

        // fn_c
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name, "fn_c");

        // fn_a
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name, "fn_a");

        // node e
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_real()
                .unwrap()
                .into_inner(),
            -1.5
        );

        // node f
        assert_eq!(
            it.next()
                .unwrap()
                .constant_value()
                .unwrap()
                .as_real()
                .unwrap()
                .into_inner(),
            100.12
        );

        // fn_b
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name, "fn_b");

        // fn_a
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name, "fn_a");

        // fn_d
        assert_eq!(it.next().unwrap().fn_call_func().unwrap().name, "fn_d");

        // Finish
        assert!(it.next().is_none())
    }

    #[test]
    fn test_max_columns_check() {
        let mut vec = vec![];

        // Col offset = 0. The minimum success max_columns is 1.
        let mut node = Expr::new();
        node.set_tp(ExprType::ColumnRef);
        node.mut_val().encode_i64(0).unwrap();
        assert!(
            append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, 0).is_err()
        );
        for i in 1..10 {
            assert!(
                append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, i)
                    .is_ok()
            );
        }

        // Col offset = 3. The minimum success max_columns is 4.
        let mut node = Expr::new();
        node.set_tp(ExprType::ColumnRef);
        node.mut_val().encode_i64(3).unwrap();
        for i in 0..=3 {
            assert!(
                append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, i)
                    .is_err()
            );
        }
        for i in 4..10 {
            assert!(
                append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, i)
                    .is_ok()
            );
        }

        // Col offset = 1, 2, 5. The minimum success max_columns is 6.
        let mut node = Expr::new();
        node.set_tp(ExprType::ScalarFunc);
        node.set_sig(ScalarFuncSig::CastIntAsString); // fn_c
        node.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().encode_i64(1).unwrap();
            n
        });
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().encode_i64(2).unwrap();
            n
        });
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().encode_i64(5).unwrap();
            n
        });
        for i in 0..=5 {
            assert!(
                append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, i)
                    .is_err()
            );
        }
        for i in 6..10 {
            assert!(
                append_rpn_nodes_recursively(node.clone(), &mut vec, &Tz::utc(), fn_mapper, i)
                    .is_ok()
            );
        }
    }

    #[test]
    fn test_expr_with_val() {
        fn append_children(expr: &mut Expr) {
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
            expr.mut_children().push(node_b);
            expr.mut_children().push(node_c);
        }

        // Simple cases
        // bytes generated from TiDB
        // datum(0)
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.mut_val().extend(&vec![8, 0]);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, vec![ScalarParameter::Int(0)]);
            }
            _ => unreachable!(),
        }

        // datum(1)
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.mut_val().extend(&vec![8, 2]);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, vec![ScalarParameter::Int(1)]);
            }
            _ => unreachable!(),
        }

        // bytes generated from TiKV
        // datum(0)
        let bytes = datum::encode_value(&vec![Datum::I64(0)]).unwrap();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.set_val(bytes);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, vec![ScalarParameter::Int(0)]);
            }
            _ => unreachable!(),
        }

        // datum(1)
        let bytes = datum::encode_value(&vec![Datum::I64(1)]).unwrap();
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.set_val(bytes);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, vec![ScalarParameter::Int(1)]);
            }
            _ => unreachable!(),
        }

        // Combine various datums
        // bytes generated from TiDB
        // Int/Real/Duration/Decimal/Bytes/Json
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.mut_val().extend(&vec![
            8, 0, 5, 191, 241, 153, 153, 153, 153, 153, 154, 2, 18, 102, 114, 111, 109, 32, 84,
            105, 68, 66, 6, 2, 1, 129, 5, 10, 4, 2, 7, 128, 0, 0, 0, 0, 0, 39, 16,
        ]);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        let params = vec![
            ScalarParameter::Int(0),
            ScalarParameter::Real(Real::new(1.1).unwrap()),
            ScalarParameter::Bytes(b"from TiDB".to_vec()),
            ScalarParameter::Decimal(Decimal::from_bytes(b"1.5").unwrap().unwrap()),
            ScalarParameter::Json(Json::Boolean(false)),
            ScalarParameter::Duration(Duration::from_nanos(10000, 3).unwrap()),
        ];
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, params);
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
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::CastIntAsReal);
        expr.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        expr.set_val(bytes);
        append_children(&mut expr);
        let mut vec = vec![];
        append_rpn_nodes_recursively(expr, &mut vec, &Tz::utc(), fn_mapper, 0).unwrap();
        let params = vec![
            ScalarParameter::Int(0),
            ScalarParameter::Real(Real::new(1.1).unwrap()),
            ScalarParameter::Bytes(b"from TiKV".to_vec()),
            ScalarParameter::Decimal(Decimal::from_bytes(b"1.5").unwrap().unwrap()),
            ScalarParameter::Json(Json::Boolean(false)),
            ScalarParameter::Duration(Duration::from_nanos(10000, 3).unwrap()),
        ];
        match vec.into_iter().skip(2).next().unwrap() {
            RpnExpressionNode::FnCall { imp_params, .. } => {
                assert_eq!(imp_params, params);
            }
            _ => unreachable!(),
        }
    }
}
