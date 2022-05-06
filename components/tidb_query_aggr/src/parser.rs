// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_common::Result;
use tidb_query_datatype::expr::EvalContext;
use tidb_query_expr::{RpnExpression, RpnExpressionBuilder};
use tipb::{Expr, ExprType, FieldType};

use crate::{impl_bit_op::*, impl_max_min::*, impl_variance::*, AggrFunction};

/// Parse a specific aggregate function definition from protobuf.
///
/// All aggregate function implementations should include an impl for this trait as well as
/// add a match arm in `map_pb_sig_to_aggr_func_parser` so that the aggregate function can be
/// actually utilized.
pub trait AggrDefinitionParser {
    /// Checks whether the inner expression of the aggregate function definition is supported.
    /// It is ensured that `aggr_def.tp` maps the current parser instance.
    fn check_supported(&self, aggr_def: &Expr) -> Result<()>;

    /// Parses and transforms the aggregate function definition.
    ///
    /// The schema of this aggregate function will be appended in `out_schema` and the final
    /// RPN expression (maybe wrapped by some casting according to types) will be appended in
    /// `out_exp`.
    ///
    /// The parser may choose particular aggregate function implementation based on the data
    /// type, so `schema` is also needed in case of data type depending on the column.
    ///
    /// # Panic
    ///
    /// May panic if the aggregate function definition is not supported by this parser.
    fn parse(
        &self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        // Rewrite expression to insert CAST() if needed.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_schema.len())?;

        Self::parse_rpn(self, aggr_def, exp, ctx, src_schema, out_schema, out_exp)
    }

    #[inline]
    fn parse_rpn(
        &self,
        _root_expr: Expr,
        _exp: RpnExpression,
        _ctx: &mut EvalContext,
        _src_schema: &[FieldType],
        _out_schema: &mut Vec<FieldType>,
        _out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        unimplemented!(
            "This struct neither implemented parse nor parse_rpn, which is not expected."
        )
    }
}

#[inline]
fn map_pb_sig_to_aggr_func_parser(value: ExprType) -> Result<Box<dyn AggrDefinitionParser>> {
    match value {
        ExprType::Count => Ok(Box::new(super::impl_count::AggrFnDefinitionParserCount)),
        ExprType::Sum => Ok(Box::new(super::impl_sum::AggrFnDefinitionParserSum)),
        ExprType::Avg => Ok(Box::new(super::impl_avg::AggrFnDefinitionParserAvg)),
        ExprType::First => Ok(Box::new(super::impl_first::AggrFnDefinitionParserFirst)),
        ExprType::AggBitAnd => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitAnd>::new())),
        ExprType::AggBitOr => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitOr>::new())),
        ExprType::AggBitXor => Ok(Box::new(AggrFnDefinitionParserBitOp::<BitXor>::new())),
        ExprType::Max => Ok(Box::new(AggrFnDefinitionParserExtremum::<Max>::new())),
        ExprType::Min => Ok(Box::new(AggrFnDefinitionParserExtremum::<Min>::new())),
        ExprType::Variance | ExprType::VarPop => {
            Ok(Box::new(AggrFnDefinitionParserVariance::<Population>::new()))
        }
        ExprType::VarSamp => Ok(Box::new(AggrFnDefinitionParserVariance::<Sample>::new())),
        v => Err(other_err!(
            "Aggregation function meet blacklist aggr function {:?}",
            v
        )),
    }
}

/// Parse all aggregate function definition from protobuf.
pub struct AllAggrDefinitionParser;

impl AggrDefinitionParser for AllAggrDefinitionParser {
    /// Checks whether the aggregate function definition is supported.
    #[inline]
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        let parser = map_pb_sig_to_aggr_func_parser(aggr_def.get_tp())?;
        parser.check_supported(aggr_def).map_err(|e| {
            other_err!(
                "Aggregation function meet blacklist expr type {:?}: {}",
                aggr_def.get_tp(),
                e
            )
        })
    }

    /// Parses and transforms the aggregate function definition to generate corresponding
    /// `AggrFunction` instance.
    ///
    /// # Panic
    ///
    /// May panic if the aggregate function definition is not supported.
    #[inline]
    fn parse(
        &self,
        aggr_def: Expr,
        ctx: &mut EvalContext,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        let parser = map_pb_sig_to_aggr_func_parser(aggr_def.get_tp()).unwrap();
        parser.parse(aggr_def, ctx, src_schema, out_schema, out_exp)
    }
}
