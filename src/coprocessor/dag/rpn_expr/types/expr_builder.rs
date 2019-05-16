// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use codec::prelude::NumberDecoder;
use cop_datatype::{EvalType, FieldTypeAccessor};
use tipb::expression::{Expr, ExprType, FieldType};

use super::super::function::RpnFunction;
use super::expr::{RpnExpression, RpnExpressionNode};
use crate::coprocessor::{
    codec::{
        data_type::ScalarValue,
        mysql::{DecimalDecoder, Duration, JsonDecoder, Time, Tz, MAX_FSP},
    },
    Error, Result,
};

/// Helper to build an `RpnExpression`.
pub struct RpnExpressionBuilder(Vec<RpnExpressionNode>);

impl RpnExpressionBuilder {
    /// Checks whether the given expression definition tree is supported.
    pub fn check_expr_tree_supported(c: &Expr) -> Result<()> {
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
        F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<Box<dyn RpnFunction>> + Copy,
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
        func: impl RpnFunction,
        return_field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func: Box::new(func),
            field_type: return_field_type.into(),
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
    F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<Box<dyn RpnFunction>> + Copy,
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
    let offset = tree_node.get_val().read_i64().map_err(|_| {
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
    F: Fn(tipb::expression::ScalarFuncSig, &[Expr]) -> Result<Box<dyn RpnFunction>> + Copy,
{
    // Map pb func to `RpnFunction`.
    let func = fn_mapper(tree_node.get_sig(), tree_node.get_children())?;
    let args = tree_node.take_children().into_vec();
    if func.args_len() != args.len() {
        return Err(box_err!(
            "Unexpected arguments, expect {}, received {}",
            func.args_len(),
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
    match eval_type {
        EvalType::Int => ScalarValue::Int(None),
        EvalType::Real => ScalarValue::Real(None),
        EvalType::Decimal => ScalarValue::Decimal(None),
        EvalType::Bytes => ScalarValue::Bytes(None),
        EvalType::DateTime => ScalarValue::DateTime(None),
        EvalType::Duration => ScalarValue::Duration(None),
        EvalType::Json => ScalarValue::Json(None),
    }
}

#[inline]
fn extract_scalar_value_int64(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_i64()
        .map_err(|_| Error::Other(box_err!("Unable to decode int64 from the request")))?;
    Ok(ScalarValue::Int(Some(value)))
}

#[inline]
fn extract_scalar_value_uint64(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_u64()
        .map_err(|_| Error::Other(box_err!("Unable to decode uint64 from the request")))?;
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
        .map_err(|_| Error::Other(box_err!("Unable to decode float from the request")))?;
    Ok(ScalarValue::Real(Some(value)))
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
        .map_err(|_| Error::Other(box_err!("Unable to decode date time from the request")))?;
    let fsp = field_type.decimal() as i8;
    let value = Time::from_packed_u64(v, field_type.tp().try_into()?, fsp, time_zone)
        .map_err(|_| Error::Other(box_err!("Unable to decode date time from the request")))?;
    Ok(ScalarValue::DateTime(Some(value)))
}

#[inline]
fn extract_scalar_value_duration(val: Vec<u8>) -> Result<ScalarValue> {
    let n = val
        .as_slice()
        .read_i64()
        .map_err(|_| Error::Other(box_err!("Unable to decode duration from the request")))?;
    let value = Duration::from_nanos(n, MAX_FSP)
        .map_err(|_| Error::Other(box_err!("Unable to decode duration from the request")))?;
    Ok(ScalarValue::Duration(Some(value)))
}

#[inline]
fn extract_scalar_value_decimal(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .decode_decimal()
        .map_err(|_| Error::Other(box_err!("Unable to decode decimal from the request")))?;
    Ok(ScalarValue::Decimal(Some(value)))
}

#[inline]
fn extract_scalar_value_json(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .decode_json()
        .map_err(|_| Error::Other(box_err!("Unable to decode json from the request")))?;
    Ok(ScalarValue::Json(Some(value)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use super::super::RpnFnCallPayload;

    use cop_codegen::RpnFunction;
    use cop_datatype::FieldTypeTp;
    use tipb::expression::ScalarFuncSig;

    use crate::coprocessor::dag::expr::EvalContext;
    use crate::coprocessor::Result;
    use codec::prelude::NumberEncoder;

    /// An RPN function for test. It accepts 1 int argument, returns float.
    #[derive(Debug, Clone, Copy, RpnFunction)]
    #[rpn_function(args = 1)]
    struct FnA;

    impl FnA {
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            _v: &Option<i64>,
        ) -> Result<Option<f64>> {
            unreachable!()
        }
    }

    /// An RPN function for test. It accepts 2 float arguments, returns int.
    #[derive(Debug, Clone, Copy, RpnFunction)]
    #[rpn_function(args = 2)]
    struct FnB;

    impl FnB {
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            _v1: &Option<f64>,
            _v2: &Option<f64>,
        ) -> Result<Option<i64>> {
            unreachable!()
        }
    }

    /// An RPN function for test. It accepts 3 int arguments, returns int.
    #[derive(Debug, Clone, Copy, RpnFunction)]
    #[rpn_function(args = 3)]
    struct FnC;

    impl FnC {
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            _v1: &Option<i64>,
            _v2: &Option<i64>,
            _v3: &Option<i64>,
        ) -> Result<Option<i64>> {
            unreachable!()
        }
    }

    /// An RPN function for test. It accepts 3 float arguments, returns float.
    #[derive(Debug, Clone, Copy, RpnFunction)]
    #[rpn_function(args = 3)]
    struct FnD;

    impl FnD {
        fn call(
            _ctx: &mut EvalContext,
            _payload: RpnFnCallPayload<'_>,
            _v1: &Option<f64>,
            _v2: &Option<f64>,
            _v3: &Option<f64>,
        ) -> Result<Option<f64>> {
            unreachable!()
        }
    }

    /// For testing `append_rpn_nodes_recursively`. It accepts protobuf function sig enum, which
    /// cannot be modified by us in tests to support FnA ~ FnD. So let's just hard code some
    /// substitute.
    fn fn_mapper(value: ScalarFuncSig, _children: &[Expr]) -> Result<Box<dyn RpnFunction>> {
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
            node_b.mut_val().write_i64(7).unwrap();

            // node c
            let mut node_c = Expr::new();
            node_c.set_tp(ExprType::Int64);
            node_c
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_c.mut_val().write_i64(3).unwrap();

            // node d
            let mut node_d = Expr::new();
            node_d.set_tp(ExprType::Int64);
            node_d
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong);
            node_d.mut_val().write_i64(11).unwrap();

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
            node_e.mut_val().write_f64(-1.5).unwrap();

            // node f
            let mut node_f = Expr::new();
            node_f.set_tp(ExprType::Float64);
            node_f
                .mut_field_type()
                .as_mut_accessor()
                .set_tp(FieldTypeTp::Double);
            node_f.mut_val().write_f64(100.12).unwrap();

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

    #[test]
    fn test_max_columns_check() {
        let mut vec = vec![];

        // Col offset = 0. The minimum success max_columns is 1.
        let mut node = Expr::new();
        node.set_tp(ExprType::ColumnRef);
        node.mut_val().write_i64(0).unwrap();
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
        node.mut_val().write_i64(3).unwrap();
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
        node.set_sig(ScalarFuncSig::CastIntAsString); // FnC
        node.mut_field_type()
            .as_mut_accessor()
            .set_tp(FieldTypeTp::LongLong);
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().write_i64(1).unwrap();
            n
        });
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().write_i64(2).unwrap();
            n
        });
        node.mut_children().push({
            let mut n = Expr::new();
            n.set_tp(ExprType::ColumnRef);
            n.mut_val().write_i64(5).unwrap();
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
}
