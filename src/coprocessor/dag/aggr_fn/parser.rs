use tipb::expression::{Expr, ExprType, FieldType};

use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::aggr_fn::AggrFunction;
use crate::coprocessor::dag::rpn_expr::RpnExpression;
use crate::coprocessor::{Error, Result};

pub trait Parser {
    // TODO: Add comment
    fn check_supported(&self, aggr_def: &Expr) -> Result<()>;

    // TODO: Add comment
    fn parse(
        &self,
        aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn AggrFunction>>;
}

pub struct AggrDefinitionParser;

#[inline]
fn map_pb_sig_to_aggr_func_parser(value: ExprType) -> Result<Box<dyn Parser>> {
    match value {
        ExprType::Avg => Ok(Box::new(super::impl_avg::AggrFnDefinitionParserAvg)),
        v => Err(box_err!(
            "Aggregation function expr type {:?} is not supported in batch mode",
            v
        )),
    }
}

impl AggrDefinitionParser {
    // TODO: Comment
    #[inline]
    pub fn check_supported(aggr_def: &Expr) -> Result<()> {
        let parser = map_pb_sig_to_aggr_func_parser(aggr_def.get_tp())?;
        parser.check_supported(aggr_def).map_err(|e| {
            Error::Other(box_err!(
                "Aggregation function for expr type {:?} is not supported: {:?}",
                aggr_def.get_tp(),
                e
            ))
        })
    }

    // TODO: Comment
    #[inline]
    pub fn parse(
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
