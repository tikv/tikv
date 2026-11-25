// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::convert::{TryFrom, TryInto};

use codec::prelude::NumberDecoder;
use tidb_query_common::Result;
use tidb_query_datatype::{
    EvalType, FieldTypeAccessor,
    codec::{
        data_type::*,
        mysql::{EnumDecoder, JsonDecoder, MAX_FSP, VectorFloat32Decoder},
    },
    expr::{EvalContext, Flag},
    match_template_evaltype,
};
use tipb::{Expr, ExprType, FieldType, ScalarFuncSig};

use super::{
    super::function::RpnFnMeta,
    expr::{RpnExpression, RpnExpressionNode},
};
use crate::ShortCircuitFnMeta;

// Each active short-circuit call retains batch results and may retain row maps.
// Bound nesting to keep both the evaluator stack and retained batch state
// small.
const MAX_SHORT_CIRCUIT_NESTING_DEPTH: usize = 32;

/// Helper to build an `RpnExpression`.
#[derive(Debug)]
pub struct RpnExpressionBuilder(Vec<RpnExpressionNode>);

impl RpnExpressionBuilder {
    /// Checks whether the given expression definition tree is supported.
    pub fn check_expr_tree_supported(c: &Expr) -> Result<()> {
        // TODO: This logic relies on the correctness of the passed in GROUP BY eval
        // type. However it can be different from the one we calculated (e.g.
        // pass a column / fn with different type).
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
            ExprType::TiDbVectorFloat32 => {}
            ExprType::ColumnRef => {}
            _ => return Err(other_err!("Blacklist expression type {:?}", c.get_tp())),
        }

        Ok(())
    }

    /// Gets the result type when expression tree is converted to RPN expression
    /// and evaluated. The result type will be either scalar or vector.
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
            | ExprType::MysqlJson
            | ExprType::MysqlEnum
            | ExprType::TiDbVectorFloat32 => Ok(true),
            ExprType::ScalarFunc => Ok(false),
            ExprType::ColumnRef => Ok(false),
            _ => Err(other_err!("Unsupported expression type {:?}", c.get_tp())),
        }
    }

    /// Builds the RPN expression node list from an expression definition tree.
    pub fn build_from_expr_tree(
        tree_node: Expr,
        ctx: &mut EvalContext,
        max_columns: usize,
    ) -> Result<RpnExpression> {
        let mut expr_nodes = Vec::new();
        let mut build_ctx = RpnBuildContext::new(
            ctx,
            super::super::map_expr_node_to_rpn_func,
            super::super::map_expr_node_to_sc_func,
            max_columns,
        );
        build_ctx.append_rpn_nodes_recursively(
            tree_node,
            ScalarFuncSig::Unspecified,
            0,
            &mut expr_nodes,
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
        Self::build_from_expr_tree_with_fn_mapper_and_ctx(
            tree_node,
            &mut EvalContext::default(),
            fn_mapper,
            max_columns,
        )
    }

    /// Only used in tests, with a customized function mapper and evaluation
    /// context. The context controls request flags used while building.
    #[cfg(test)]
    pub fn build_from_expr_tree_with_fn_mapper_and_ctx<F>(
        tree_node: Expr,
        ctx: &mut EvalContext,
        fn_mapper: F,
        max_columns: usize,
    ) -> Result<RpnExpression>
    where
        F: Fn(&Expr) -> Result<RpnFnMeta> + Copy,
    {
        let mut expr_nodes = Vec::new();
        let mut build_ctx = RpnBuildContext::new(
            ctx,
            fn_mapper,
            super::super::map_expr_node_to_sc_func,
            max_columns,
        );
        build_ctx.append_rpn_nodes_recursively(
            tree_node,
            ScalarFuncSig::Unspecified,
            0,
            &mut expr_nodes,
        )?;
        Ok(RpnExpression::from(expr_nodes))
    }

    /// Creates a new builder instance.
    ///
    /// Only used in tests. Normal logic should use `build_from_expr_tree`.
    pub fn new_for_test() -> Self {
        Self(Vec::new())
    }

    /// Pushes a `FnCall` node.
    #[must_use]
    pub fn push_fn_call_for_test(
        mut self,
        func_meta: RpnFnMeta,
        args_len: usize,
        return_field_type: impl Into<FieldType>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func_meta,
            args_len,
            field_type: return_field_type.into(),
            metadata: Box::new(()),
        };
        self.0.push(node);
        self
    }

    #[cfg(test)]
    #[must_use]
    pub fn push_fn_call_with_metadata(
        mut self,
        func_meta: RpnFnMeta,
        args_len: usize,
        return_field_type: impl Into<FieldType>,
        metadata: Box<dyn std::any::Any + Send>,
    ) -> Self {
        let node = RpnExpressionNode::FnCall {
            func_meta,
            args_len,
            field_type: return_field_type.into(),
            metadata,
        };
        self.0.push(node);
        self
    }

    /// Pushes a `Constant` node. The field type will be auto inferred by
    /// choosing an arbitrary field type that matches the field type of the
    /// given value.
    #[must_use]
    pub fn push_constant_for_test(mut self, value: impl Into<ScalarValue>) -> Self {
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
    #[must_use]
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
    #[must_use]
    pub fn push_column_ref_for_test(mut self, offset: usize) -> Self {
        let node = RpnExpressionNode::ColumnRef { offset };
        self.0.push(node);
        self
    }

    /// Builds the `RpnExpression`.
    pub fn build_for_test(self) -> RpnExpression {
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
/// The transform process is mostly a post-order traversal. When short-circuit
/// evaluation is enabled, logical calls are converted with each argument in a
/// separate RPN expression; adjacent associative `AND`/`OR` calls may be
/// flattened to avoid recursive evaluation.
///
/// This context carries the dependencies shared by every recursive call.
struct RpnBuildContext<'a, F, SCF> {
    ctx: &'a mut EvalContext,
    fn_mapper: F,
    sc_fn_mapper: SCF,
    max_columns: usize,
    // TODO: Passing `max_columns` is only a workaround solution that works when we only check
    // column offset. To totally check whether or not the expression is valid, we need to pass in
    // the full schema instead.
}

impl<'a, F, SCF> RpnBuildContext<'a, F, SCF>
where
    F: Fn(&Expr) -> Result<RpnFnMeta>,
    SCF: Fn(&Expr) -> Option<ShortCircuitFnMeta>,
{
    fn new(ctx: &'a mut EvalContext, fn_mapper: F, sc_fn_mapper: SCF, max_columns: usize) -> Self {
        Self {
            ctx,
            fn_mapper,
            sc_fn_mapper,
            max_columns,
        }
    }

    fn append_rpn_nodes_recursively(
        &mut self,
        tree_node: Expr,
        parent_sig: ScalarFuncSig,
        depth: usize,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
    ) -> Result<()> {
        match tree_node.get_tp() {
            ExprType::ScalarFunc => {
                self.handle_node_fn_call(tree_node, parent_sig, depth, rpn_nodes)
            }
            ExprType::ColumnRef => {
                self.handle_node_column_ref(tree_node, rpn_nodes)?;
                Ok(())
            }
            _ => {
                self.handle_node_constant(tree_node, rpn_nodes)?;
                Ok(())
            }
        }
    }

    #[inline]
    fn handle_node_fn_call(
        &mut self,
        mut tree_node: Expr,
        parent_sig: ScalarFuncSig,
        depth: usize,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
    ) -> Result<()> {
        let short_circuit_func_meta = (self.sc_fn_mapper)(&tree_node);

        // Map, validate, and initialize metadata before taking the children because
        // each of these operations may inspect the original expression tree.
        let func_meta = (self.fn_mapper)(&tree_node)?;
        (func_meta.validator_ptr)(&tree_node).map_err(|e| {
            other_err!(
                "Invalid {} (sig = {:?}) signature: {}",
                func_meta.name,
                tree_node.get_sig(),
                e
            )
        })?;

        let metadata = (func_meta.metadata_expr_ptr)(&mut tree_node)?;
        let args: Vec<_> = tree_node.take_children().into();
        let args_len = args.len();

        let can_flatten_with_parent =
            short_circuit_func_meta.is_some_and(|func_meta| can_flatten(parent_sig, func_meta.sig));

        let short_circuit_depth = short_circuit_func_meta.map_or(depth, |_| {
            if can_flatten_with_parent {
                depth
            } else {
                depth.saturating_add(1)
            }
        });
        let can_short_circuit = short_circuit_func_meta.is_some()
            && self
                .ctx
                .cfg
                .flag
                .contains(Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION)
            && short_circuit_depth <= MAX_SHORT_CIRCUIT_NESTING_DEPTH;

        if can_short_circuit {
            let short_circuit_func_meta = short_circuit_func_meta.unwrap();
            let mut parsed_args = Vec::with_capacity(args_len);
            let mut is_short_circuit_worthwhile = false;
            for arg in args {
                let mut arg_nodes = Vec::new();

                self.append_rpn_nodes_recursively(
                    arg,
                    short_circuit_func_meta.sig,
                    short_circuit_depth,
                    &mut arg_nodes,
                )?;

                is_short_circuit_worthwhile |= should_flatten(short_circuit_func_meta, &arg_nodes)
                    || !is_simple_expr(&arg_nodes);

                parsed_args.push(arg_nodes);
            }

            if is_short_circuit_worthwhile {
                let mut short_circuit_args = Vec::with_capacity(args_len);
                for arg_nodes in parsed_args {
                    if can_flatten_with_parent {
                        // Defer flattening to the root of this same-operator chain so
                        // each argument is moved only once into the final list.
                        short_circuit_args.push(RpnExpression::from(arg_nodes));
                    } else {
                        append_short_circuit_arg(
                            short_circuit_func_meta,
                            arg_nodes,
                            &mut short_circuit_args,
                        );
                    }
                }

                rpn_nodes.push(RpnExpressionNode::ShortCircuitFnCall {
                    func_meta: short_circuit_func_meta,
                    args: short_circuit_args.into_boxed_slice(),
                    field_type: tree_node.take_field_type(),
                });
                return Ok(());
            }

            // The children have already been converted to RPN while deciding whether
            // short-circuit evaluation is worthwhile. Reuse them for the regular call
            // instead of traversing the expression tree again.
            for mut arg_nodes in parsed_args {
                rpn_nodes.append(&mut arg_nodes);
            }
        } else {
            // Visit children first, then push current node, so that it is a post-order
            // traversal.
            for arg in args {
                self.append_rpn_nodes_recursively(
                    arg,
                    tree_node.get_sig(),
                    short_circuit_depth,
                    rpn_nodes,
                )?
            }
        }
        rpn_nodes.push(RpnExpressionNode::FnCall {
            func_meta,
            args_len,
            field_type: tree_node.take_field_type(),
            metadata,
        });
        Ok(())
    }

    #[inline]
    fn handle_node_column_ref(
        &self,
        tree_node: Expr,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
    ) -> Result<()> {
        let offset =
            tree_node.get_val().read_i64().map_err(|_| {
                other_err!("Unable to decode column reference offset from the request")
            })? as usize;
        if offset >= self.max_columns {
            return Err(other_err!(
                "Invalid column offset (schema has {} columns, access index {})",
                self.max_columns,
                offset
            ));
        }
        rpn_nodes.push(RpnExpressionNode::ColumnRef { offset });
        Ok(())
    }

    #[inline]
    fn handle_node_constant(
        &mut self,
        mut tree_node: Expr,
        rpn_nodes: &mut Vec<RpnExpressionNode>,
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
            ExprType::MysqlTime if eval_type == EvalType::DateTime => {
                extract_scalar_value_date_time(
                    tree_node.take_val(),
                    tree_node.get_field_type(),
                    self.ctx,
                )?
            }
            ExprType::MysqlDuration if eval_type == EvalType::Duration => {
                extract_scalar_value_duration(tree_node.take_val())?
            }
            ExprType::MysqlDecimal if eval_type == EvalType::Decimal => {
                extract_scalar_value_decimal(tree_node.take_val())?
            }
            ExprType::MysqlJson if eval_type == EvalType::Json => {
                extract_scalar_value_json(tree_node.take_val())?
            }
            ExprType::MysqlEnum if eval_type == EvalType::Enum => {
                extract_scalar_value_enum(tree_node.take_val(), tree_node.get_field_type())?
            }
            ExprType::MysqlBit if eval_type == EvalType::Int => {
                extract_scalar_value_uint64_from_bits(tree_node.take_val())?
            }
            ExprType::TiDbVectorFloat32 if eval_type == EvalType::VectorFloat32 => {
                extract_scalar_value_vector_float32(tree_node.take_val())?
            }
            expr_type => {
                return Err(other_err!(
                    "Unexpected ExprType {:?} and EvalType {:?}",
                    expr_type,
                    eval_type
                ));
            }
        };
        rpn_nodes.push(RpnExpressionNode::Constant {
            value: scalar_value,
            field_type: tree_node.take_field_type(),
        });
        Ok(())
    }
}

#[inline]
fn can_flatten(father: ScalarFuncSig, son: ScalarFuncSig) -> bool {
    father == son && (father == ScalarFuncSig::LogicalOr || father == ScalarFuncSig::LogicalAnd)
}

#[inline]
fn should_flatten(father_func: ShortCircuitFnMeta, arg: &[RpnExpressionNode]) -> bool {
    assert!(!arg.is_empty());
    if arg.len() > 1 {
        return false;
    }

    matches!(
        arg.last().unwrap(),
        RpnExpressionNode::ShortCircuitFnCall {
            func_meta: son_func,
            ..
        } if can_flatten(father_func.sig, son_func.sig)
    )
}

#[inline]
fn is_simple_expr(arg: &[RpnExpressionNode]) -> bool {
    matches!(
        arg,
        [RpnExpressionNode::Constant { .. } | RpnExpressionNode::ColumnRef { .. }]
    )
}

fn append_short_circuit_arg(
    func: ShortCircuitFnMeta,
    mut arg_nodes: Vec<RpnExpressionNode>,
    output: &mut Vec<RpnExpression>,
) {
    if should_flatten(func, &arg_nodes) {
        match arg_nodes.pop().unwrap() {
            RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                for arg in args.into_vec() {
                    append_short_circuit_arg(func, arg.into_inner(), output);
                }
            }
            _ => unreachable!(),
        }
    } else {
        output.push(RpnExpression::from(arg_nodes));
    }
}

#[inline]
fn get_scalar_value_null(eval_type: EvalType) -> ScalarValue {
    match_template_evaltype! {
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
fn extract_scalar_value_uint64_from_bits(val: Vec<u8>) -> Result<ScalarValue> {
    debug_assert!(val.len() <= 8);
    let mut res = 0;
    for v in val {
        res <<= 8;
        res |= v as u64;
    }
    Ok(ScalarValue::Int(Some(res as i64)))
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
    ctx: &mut EvalContext,
) -> Result<ScalarValue> {
    let v = val
        .as_slice()
        .read_u64()
        .map_err(|_| other_err!("Unable to decode date time from the request"))?;
    let fsp = field_type.as_accessor().decimal() as i8;
    let value = DateTime::from_packed_u64(ctx, v, field_type.as_accessor().tp().try_into()?, fsp)
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
    use tidb_query_datatype::codec::mysql::DecimalDecoder;
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

#[inline]
fn extract_scalar_value_enum(val: Vec<u8>, field_type: &FieldType) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_enum_uint(field_type)
        .map_err(|_| other_err!("Unable to decode enum from the request"))?;
    Ok(ScalarValue::Enum(Some(value)))
}

#[inline]
fn extract_scalar_value_vector_float32(val: Vec<u8>) -> Result<ScalarValue> {
    let value = val
        .as_slice()
        .read_vector_float32()
        .map_err(|_| other_err!("Unable to decode vector float32 from the request"))?;
    Ok(ScalarValue::VectorFloat32(Some(value)))
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, thread};

    use tidb_query_codegen::rpn_fn;
    use tidb_query_common::Result;
    use tidb_query_datatype::{
        FieldTypeTp,
        codec::batch::LazyBatchColumnVec,
        expr::{EvalConfig, Flag},
    };
    use tikv_util::sys::thread::StdThreadBuildWrapper;
    use tipb::ScalarFuncSig;
    use tipb_helper::ExprDefBuilder;

    use super::*;

    fn short_circuit_context() -> EvalContext {
        EvalContext::new(Arc::new(EvalConfig::from_flag(
            Flag::ENABLE_SHORT_CIRCUIT_EXPRESSION,
        )))
    }

    fn short_circuit_depth(expr: &RpnExpression) -> usize {
        expr.iter()
            .map(|node| match node {
                RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                    1 + args.iter().map(short_circuit_depth).max().unwrap_or(0)
                }
                _ => 0,
            })
            .max()
            .unwrap_or(0)
    }

    fn nested_logical_sig(root_sig: ScalarFuncSig, level: usize) -> ScalarFuncSig {
        if level.is_multiple_of(2) {
            root_sig
        } else if root_sig == ScalarFuncSig::LogicalOr {
            ScalarFuncSig::LogicalAnd
        } else {
            ScalarFuncSig::LogicalOr
        }
    }

    fn nested_logical_expr(depth: usize, root_sig: ScalarFuncSig, wrap_in_cast: bool) -> Expr {
        // Make even the innermost logical call worthwhile, so `depth` is the
        // candidate short-circuit depth, without an off-by-one for a simple call.
        let mut node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(depth, FieldTypeTp::LongLong))
                .build();
        for level in (0..depth).rev() {
            if wrap_in_cast {
                node =
                    ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::LongLong)
                        .push_child(node)
                        .build();
            }
            node = ExprDefBuilder::scalar_func(
                nested_logical_sig(root_sig, level),
                FieldTypeTp::LongLong,
            )
            .push_child(ExprDefBuilder::column_ref(level, FieldTypeTp::LongLong))
            .push_child(node)
            .build();
        }
        node
    }

    fn same_logical_expr(depth: usize, sig: ScalarFuncSig) -> Expr {
        let mut node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsInt, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(depth, FieldTypeTp::LongLong))
                .build();
        for level in (0..depth).rev() {
            node = ExprDefBuilder::scalar_func(sig, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(level, FieldTypeTp::LongLong))
                .push_child(node)
                .build();
        }
        node
    }

    fn contains_regular_logical_call(expr: &RpnExpression) -> bool {
        expr.iter().any(|node| match node {
            RpnExpressionNode::FnCall { func_meta, .. } => {
                func_meta.name == "logical_or" || func_meta.name == "logical_and"
            }
            RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                args.iter().any(contains_regular_logical_call)
            }
            _ => false,
        })
    }

    fn assert_no_short_circuit_below_regular_logical(expr: &RpnExpression) -> bool {
        let mut stack = Vec::with_capacity(expr.len());
        for node in expr.iter() {
            let contains_short_circuit = match node {
                RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                    for arg in args {
                        assert_no_short_circuit_below_regular_logical(arg);
                    }
                    true
                }
                RpnExpressionNode::FnCall {
                    func_meta,
                    args_len,
                    ..
                } => {
                    assert!(stack.len() >= *args_len);
                    let args_begin = stack.len() - *args_len;
                    let args_contain_short_circuit = stack[args_begin..].iter().any(|&v| v);
                    if func_meta.name == "logical_or" || func_meta.name == "logical_and" {
                        assert!(
                            !args_contain_short_circuit,
                            "regular {} contains a short-circuit descendant",
                            func_meta.name
                        );
                    }
                    stack.truncate(args_begin);
                    args_contain_short_circuit
                }
                _ => false,
            };
            stack.push(contains_short_circuit);
        }

        assert_eq!(stack.len(), 1);
        stack[0]
    }

    fn nested_logical_columns(
        depth: usize,
        root_sig: ScalarFuncSig,
        partial: bool,
    ) -> LazyBatchColumnVec {
        let mut columns = Vec::with_capacity(depth + 1);
        for level in 0..=depth {
            let is_or = nested_logical_sig(root_sig, level) == ScalarFuncSig::LogicalOr;
            let values: Vec<_> = (0..2 * crate::BATCH_MAX_SIZE)
                .map(|physical_row| {
                    let row = physical_row / 2;
                    if level < depth && partial && row == level {
                        // Resolve just one new row at each level, retaining nearly
                        // full batches and row maps throughout the recursion.
                        Some(if is_or { 9 } else { 0 })
                    } else if row % 3 == 2 {
                        None
                    } else if level == depth {
                        Some(if row % 3 == 0 { 0 } else { -7 })
                    } else {
                        Some(if is_or { 0 } else { -11 })
                    }
                })
                .collect();
            columns.push(VectorValue::Int(values.into()));
        }
        LazyBatchColumnVec::from(columns)
    }

    /// An RPN function for test. It accepts 1 int argument, returns float.
    #[rpn_fn(nullable)]
    fn fn_a(_v: Option<&i64>) -> Result<Option<Real>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 2 float arguments, returns int.
    #[rpn_fn(nullable)]
    fn fn_b(_v1: Option<&Real>, _v2: Option<&Real>) -> Result<Option<i64>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 3 int arguments, returns int.
    #[rpn_fn(nullable)]
    fn fn_c(_v1: Option<&i64>, _v2: Option<&i64>, _v3: Option<&i64>) -> Result<Option<i64>> {
        unreachable!()
    }

    /// An RPN function for test. It accepts 3 float arguments, returns float.
    #[rpn_fn(nullable)]
    fn fn_d(_v1: Option<&Real>, _v2: Option<&Real>, _v3: Option<&Real>) -> Result<Option<Real>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(nullable)]
    fn fn_e(_v1: Option<&Int>, _v2: Option<&Real>) -> Result<Option<Bytes>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(nullable, varg)]
    fn fn_f(_v: &[Option<&Int>]) -> Result<Option<Real>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(nullable, varg, min_args = 2)]
    fn fn_g(_v: &[Option<&Real>]) -> Result<Option<Int>> {
        unreachable!()
    }

    /// This function is only used when testing with the validator.
    #[rpn_fn(nullable, raw_varg, min_args = 1)]
    fn fn_h(_v: &[ScalarValueRef<'_>]) -> Result<Option<Real>> {
        unreachable!()
    }

    /// For testing `append_rpn_nodes_recursively`. It accepts protobuf function
    /// sig enum, which cannot be modified by us in tests to support fn_a ~
    /// fn_d. So let's just hard code some substitute.
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
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_real(3.0))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect number of arguments
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(1.0))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect argument type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsTime, FieldTypeTp::VarChar)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();
    }

    #[test]
    fn test_validator_vargs_fn() {
        // Correct signature
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_int(5))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_int(5))
                .push_child(ExprDefBuilder::constant_int(4))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        // Incorrect return type
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect argument type
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_int(1))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(3.0))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsDuration, FieldTypeTp::Double)
                .push_child(ExprDefBuilder::constant_real(3.0))
                .push_child(ExprDefBuilder::constant_real(1.0))
                .push_child(ExprDefBuilder::constant_int(1))
                .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();
    }

    #[test]
    fn test_validator_vargs_fn_with_min_args() {
        // Correct signature
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        // Insufficient arguments
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect types
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsJson, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_real(5.0))
            .push_child(ExprDefBuilder::constant_int(42))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();
    }

    #[test]
    fn test_validator_raw_vargs_fn_with_min_args() {
        // Correct signature
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::Double)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap();

        // Insufficient arguments
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::Double).build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();

        // Incorrect return type
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::CastRealAsInt, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_real(3.0))
            .push_child(ExprDefBuilder::constant_int(5))
            .build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node, fn_mapper, 0).unwrap_err();
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
        assert_eq!(7, *it.next().unwrap().constant_value().as_int().unwrap());

        // node c
        assert_eq!(3, *it.next().unwrap().constant_value().as_int().unwrap());

        // node d
        assert_eq!(11, *it.next().unwrap().constant_value().as_int().unwrap());

        // fn_c
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_c");

        // fn_a
        assert_eq!(it.next().unwrap().fn_call_func().name, "fn_a");

        // node e
        assert_eq!(
            Real::new(-1.5).ok().as_ref(),
            it.next().unwrap().constant_value().as_real()
        );

        // node f
        assert_eq!(
            Real::new(100.12).ok().as_ref(),
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
    fn test_simple_logical_call_uses_regular_rpn() {
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::LogicalOr, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::constant_int(1))
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();

        let mut ctx = short_circuit_context();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper_and_ctx(
            node,
            &mut ctx,
            crate::map_expr_node_to_rpn_func,
            1,
        )
        .unwrap();

        assert_eq!(exp.len(), 3);
        assert!(matches!(exp[0], RpnExpressionNode::Constant { .. }));
        assert!(matches!(exp[1], RpnExpressionNode::ColumnRef { .. }));
        assert_eq!(exp[2].fn_call_func().name, "logical_or");
    }

    #[test]
    fn test_short_circuit_call_is_embedded_in_parent_rpn() {
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
            .push_child(
                ExprDefBuilder::scalar_func(ScalarFuncSig::LogicalOr, FieldTypeTp::LongLong)
                    .push_child(ExprDefBuilder::constant_int(1))
                    .push_child(
                        ExprDefBuilder::scalar_func(ScalarFuncSig::PlusInt, FieldTypeTp::LongLong)
                            .push_child(ExprDefBuilder::constant_int(0))
                            .push_child(ExprDefBuilder::constant_int(0)),
                    ),
            )
            .push_child(ExprDefBuilder::constant_int(3))
            .build();

        let eager_exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(
            node.clone(),
            crate::map_expr_node_to_rpn_func,
            0,
        )
        .unwrap();
        assert!(
            eager_exp
                .iter()
                .all(|node| !matches!(node, RpnExpressionNode::ShortCircuitFnCall { .. }))
        );

        let mut ctx = short_circuit_context();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper_and_ctx(
            node,
            &mut ctx,
            crate::map_expr_node_to_rpn_func,
            0,
        )
        .unwrap();

        assert_eq!(exp.len(), 3);
        match &exp[0] {
            RpnExpressionNode::ShortCircuitFnCall {
                func_meta, args, ..
            } => {
                assert_eq!(func_meta.sig, ScalarFuncSig::LogicalOr);
                assert_eq!(args.len(), 2);
            }
            node => panic!("expected short-circuit call, got {:?}", node),
        }
        assert!(matches!(exp[1], RpnExpressionNode::Constant { .. }));
        assert!(matches!(exp[2], RpnExpressionNode::FnCall { .. }));
    }

    #[test]
    fn test_adjacent_short_circuit_calls_are_flattened() {
        let node = ExprDefBuilder::scalar_func(ScalarFuncSig::LogicalOr, FieldTypeTp::LongLong)
            .push_child(
                ExprDefBuilder::scalar_func(ScalarFuncSig::LogicalOr, FieldTypeTp::LongLong)
                    .push_child(
                        ExprDefBuilder::scalar_func(
                            ScalarFuncSig::LogicalOr,
                            FieldTypeTp::LongLong,
                        )
                        .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
                        .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong)),
                    )
                    .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong)),
            )
            .push_child(ExprDefBuilder::column_ref(3, FieldTypeTp::LongLong))
            .build();

        let mut ctx = short_circuit_context();
        let exp = RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper_and_ctx(
            node,
            &mut ctx,
            crate::map_expr_node_to_rpn_func,
            4,
        )
        .unwrap();

        assert_eq!(exp.len(), 1);
        match &exp[0] {
            RpnExpressionNode::ShortCircuitFnCall {
                func_meta, args, ..
            } => {
                assert_eq!(func_meta.sig, ScalarFuncSig::LogicalOr);
                assert_eq!(args.len(), 3);
                assert_eq!(args[0].len(), 3);
                assert!(matches!(
                    args[0].last(),
                    Some(RpnExpressionNode::FnCall { .. })
                ));
            }
            node => panic!("expected short-circuit call, got {:?}", node),
        }
        assert_eq!(exp.node_count(), 6);
        // The flattened root has three binary OR operations even though the
        // outer two are represented by one short-circuit node.
        assert_eq!(exp.work_count(), 7);
        assert_eq!(exp.column_ref_count(), 4);
        assert_eq!(exp.referenced_column_offsets(), &[0, 1, 2, 3]);
    }

    #[test]
    fn test_flattened_calls_do_not_consume_nesting_budget() {
        let limit = MAX_SHORT_CIRCUIT_NESTING_DEPTH;
        for depth in [limit + 1, 8 * limit] {
            for sig in [ScalarFuncSig::LogicalOr, ScalarFuncSig::LogicalAnd] {
                let exp = thread::Builder::new()
                    .stack_size(16 * 1024 * 1024)
                    .spawn_wrapper(move || {
                        RpnExpressionBuilder::build_from_expr_tree(
                            same_logical_expr(depth, sig),
                            &mut short_circuit_context(),
                            depth + 1,
                        )
                        .unwrap()
                    })
                    .unwrap()
                    .join()
                    .unwrap();

                assert_eq!(short_circuit_depth(&exp), 1);
                assert!(!contains_regular_logical_call(&exp));
                match exp.last().unwrap() {
                    RpnExpressionNode::ShortCircuitFnCall { args, .. } => {
                        assert_eq!(args.len(), depth + 1);
                    }
                    node => panic!("expected flattened short-circuit call, got {:?}", node),
                }
            }
        }
    }

    #[test]
    fn test_flattened_chain_preserves_logical_work_count() {
        // 32 terms contain 31 binary logical operations. Flattening replaces
        // those 31 FnCall nodes with one short-circuit node, but the RU-v2 work
        // estimate must retain all 31 conceptual operations.
        let depth = 31;
        let lazy = RpnExpressionBuilder::build_from_expr_tree(
            same_logical_expr(depth, ScalarFuncSig::LogicalOr),
            &mut short_circuit_context(),
            depth + 1,
        )
        .unwrap();
        let eager = RpnExpressionBuilder::build_from_expr_tree(
            same_logical_expr(depth, ScalarFuncSig::LogicalOr),
            &mut EvalContext::default(),
            depth + 1,
        )
        .unwrap();

        assert_eq!(eager.node_count() - lazy.node_count(), 30);
        assert_eq!(lazy.work_count(), eager.node_count());
    }

    #[test]
    fn test_left_deep_short_circuit_chains_build_to_one_root_call() {
        // `same_logical_expr` constructs a left-deep chain. These widths exercise
        // the deferred flattening path without relying on machine-dependent timing
        // thresholds in the unit test.
        for depth in [32, 128, 512, 1024] {
            for sig in [ScalarFuncSig::LogicalOr, ScalarFuncSig::LogicalAnd] {
                let exp = thread::Builder::new()
                    .stack_size(16 * 1024 * 1024)
                    .spawn_wrapper(move || {
                        RpnExpressionBuilder::build_from_expr_tree(
                            same_logical_expr(depth, sig),
                            &mut short_circuit_context(),
                            depth + 1,
                        )
                        .unwrap()
                    })
                    .unwrap()
                    .join()
                    .unwrap();

                assert_eq!(exp.len(), 1, "depth={depth}, sig={sig:?}");
                assert_eq!(short_circuit_depth(&exp), 1, "depth={depth}, sig={sig:?}");
                assert!(
                    !contains_regular_logical_call(&exp),
                    "depth={depth}, sig={sig:?}"
                );
                match exp.last().unwrap() {
                    RpnExpressionNode::ShortCircuitFnCall {
                        func_meta, args, ..
                    } => {
                        assert_eq!(func_meta.sig, sig);
                        assert_eq!(args.len(), depth + 1);
                    }
                    node => panic!(
                        "expected root short-circuit call for depth={depth}, sig={sig:?}, got {node:?}"
                    ),
                }
                assert_eq!(
                    exp.referenced_column_offsets(),
                    &(0..=depth).collect::<Vec<_>>(),
                    "depth={depth}, sig={sig:?}"
                );
                assert_eq!(
                    exp.work_count(),
                    exp.node_count() + depth - 1,
                    "depth={depth}, sig={sig:?}"
                );
            }
        }
    }

    #[test]
    fn test_short_circuit_depth_limit_stress_on_small_stack() {
        let limit = MAX_SHORT_CIRCUIT_NESTING_DEPTH;
        for depth in [limit - 1, limit, limit + 1, 8 * limit] {
            for root_sig in [ScalarFuncSig::LogicalOr, ScalarFuncSig::LogicalAnd] {
                for wrap_in_cast in [false, true] {
                    // Build on a separate stack so the 2 MiB evaluation stack
                    // below tests runtime recursion, not AST construction.
                    let (lazy, eager) = thread::Builder::new()
                        .stack_size(16 * 1024 * 1024)
                        .spawn_wrapper(move || {
                            let build = |ctx: &mut EvalContext| {
                                RpnExpressionBuilder::build_from_expr_tree(
                                    nested_logical_expr(depth, root_sig, wrap_in_cast),
                                    ctx,
                                    depth + 1,
                                )
                                .unwrap()
                            };
                            (
                                build(&mut short_circuit_context()),
                                build(&mut EvalContext::default()),
                            )
                        })
                        .unwrap()
                        .join()
                        .unwrap();

                    thread::Builder::new()
                        .stack_size(2 * 1024 * 1024)
                        .spawn_wrapper(move || {
                            let case =
                                format!("depth={depth}, root={root_sig:?}, cast={wrap_in_cast}");
                            assert_eq!(short_circuit_depth(&lazy), depth.min(limit));
                            assert_eq!(short_circuit_depth(&eager), 0);
                            if depth > limit {
                                assert!(matches!(
                                    lazy.last(),
                                    Some(RpnExpressionNode::ShortCircuitFnCall { .. })
                                ));
                                assert!(contains_regular_logical_call(&lazy));
                                assert_no_short_circuit_below_regular_logical(&lazy);
                            }
                            // Collect metadata on the constrained stack too.
                            assert_eq!(lazy.node_count(), eager.node_count());
                            assert_eq!(lazy.column_ref_count(), depth + 1);
                            assert_eq!(
                                lazy.referenced_column_offsets(),
                                &(0..=depth).collect::<Vec<_>>()
                            );

                            let batch_size = crate::BATCH_MAX_SIZE;
                            let schema = vec![FieldTypeTp::LongLong.into(); depth + 1];
                            // Sparse, reversed rows force nested calls to maintain
                            // both output positions and physical row mappings.
                            let mut logical_rows: Vec<_> =
                                (0..batch_size).rev().map(|row| 2 * row + 1).collect();
                            for partial in [false, true] {
                                let mut columns = nested_logical_columns(depth, root_sig, partial);
                                let mut lazy_ctx = short_circuit_context();
                                let mut eager_ctx = EvalContext::default();
                                for batch in 0..8 {
                                    let expected = eager
                                        .eval(
                                            &mut eager_ctx,
                                            &schema,
                                            &mut columns,
                                            &logical_rows,
                                            batch_size,
                                        )
                                        .unwrap()
                                        .vector_value()
                                        .unwrap()
                                        .as_ref()
                                        .to_int_vec();
                                    assert!(expected.contains(&Some(0)));
                                    assert!(expected.contains(&Some(1)));
                                    assert!(expected.contains(&None));
                                    let result = lazy
                                        .eval(
                                            &mut lazy_ctx,
                                            &schema,
                                            &mut columns,
                                            &logical_rows,
                                            batch_size,
                                        )
                                        .unwrap();
                                    assert_eq!(
                                        result.vector_value().unwrap().as_ref().to_int_vec(),
                                        expected,
                                        "{case}, partial={partial}, batch={batch}"
                                    );
                                    logical_rows.rotate_left(1);
                                }
                            }
                        })
                        .unwrap()
                        .join()
                        .unwrap();
                }
            }
        }
    }

    #[test]
    fn test_max_columns_check() {
        // Col offset = 0. The minimum success max_columns is 1.
        let node = ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong).build();
        RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, 0)
            .unwrap_err();
        for i in 1..10 {
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, i)
                .unwrap();
        }

        // Col offset = 3. The minimum success max_columns is 4.
        let node = ExprDefBuilder::column_ref(3, FieldTypeTp::LongLong).build();
        for i in 0..=3 {
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, i)
                .unwrap_err();
        }
        for i in 4..10 {
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, i)
                .unwrap();
        }

        // Col offset = 1, 2, 5. The minimum success max_columns is 6.
        let node =
            ExprDefBuilder::scalar_func(ScalarFuncSig::CastIntAsString, FieldTypeTp::LongLong)
                .push_child(ExprDefBuilder::column_ref(1, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::column_ref(2, FieldTypeTp::LongLong))
                .push_child(ExprDefBuilder::column_ref(5, FieldTypeTp::LongLong))
                .build();

        for i in 0..=5 {
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, i)
                .unwrap_err();
        }
        for i in 6..10 {
            RpnExpressionBuilder::build_from_expr_tree_with_fn_mapper(node.clone(), fn_mapper, i)
                .unwrap();
        }
    }

    #[test]
    fn test_extract_scalar_value_uint64_from_bits() {
        let mut res = extract_scalar_value_uint64_from_bits(vec![0x01, 0x56, 0x12, 0x34]).unwrap();
        assert_eq!(ScalarValue::Int(Some(0x1561234)), res);
        res = extract_scalar_value_uint64_from_bits(vec![0x56, 0x34, 0x12, 0x78]).unwrap();
        assert_eq!(ScalarValue::Int(Some(0x56341278)), res);
        res = extract_scalar_value_uint64_from_bits(vec![0x78]).unwrap();
        assert_eq!(ScalarValue::Int(Some(0x78)), res);
    }
}
