// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::expression::{Expr, ExprType, FieldType};

use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::aggr_fn::AggrFunction;
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::{Error, Result};

/// Parse a specific aggregate function definition from ProtoBuf.
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
    /// # Panic
    ///
    /// May panic if the aggregate function definition is not supported by this parser.
    fn parse(
        &self,
        aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>>;
}

#[inline]
fn map_pb_sig_to_aggr_func_parser(value: ExprType) -> Result<Box<dyn AggrDefinitionParser>> {
    match value {
        ExprType::Count => Ok(Box::new(super::impl_count::AggrFnDefinitionParserCount)),
        ExprType::Avg => Ok(Box::new(super::impl_avg::AggrFnDefinitionParserAvg)),
        v => Err(box_err!(
            "Aggregation function expr type {:?} is not supported in batch mode",
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
            Error::Other(box_err!(
                "Aggregation function for expr type {:?} is not supported: {}",
                aggr_def.get_tp(),
                e
            ))
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
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>> {
        let parser = map_pb_sig_to_aggr_func_parser(aggr_def.get_tp()).unwrap();
        parser.parse(aggr_def, time_zone, max_columns, out_schema, out_exp)
    }
}
