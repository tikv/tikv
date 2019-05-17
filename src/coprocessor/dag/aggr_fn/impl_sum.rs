// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use cop_codegen::AggrFunction;
use cop_datatype::EvalType;
use tipb::expression::{Expr, ExprType, FieldType};

use super::summable::Summable;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// The parser for SUM aggregate function.
pub struct AggrFnDefinitionParserSum;

impl super::parser::AggrDefinitionParser for AggrFnDefinitionParserSum {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::Sum);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        assert_eq!(aggr_def.get_tp(), ExprType::Sum);

        // SUM outputs one column.
        out_schema.push(aggr_def.take_field_type());

        // Rewrite expression, inserting CAST if necessary. See `typeInfer4Sum` in TiDB.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let mut exp = RpnExpressionBuilder::build_from_expr_tree(child, time_zone, max_columns)?;
        // The rewrite should always success.
        super::util::rewrite_exp_for_sum_avg(schema, &mut exp).unwrap();

        let rewritten_eval_type = EvalType::try_from(exp.ret_field_type(schema).tp()).unwrap();
        out_exp.push(exp);

        // Choose a type-aware SUM implementation based on the eval type after rewriting exp.
        Ok(match rewritten_eval_type {
            EvalType::Decimal => Box::new(AggrFnSum::<Decimal>::new()),
            EvalType::Real => Box::new(AggrFnSum::<Real>::new()),
            // If we meet unexpected types after rewriting, it is an implementation fault.
            _ => unreachable!(),
        })
    }
}

/// The SUM aggregate function.
///
/// Note that there are `SUM(Decimal) -> Decimal` and `SUM(Double) -> Double`.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateSum::<T>::new())]
pub struct AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    _phantom: std::marker::PhantomData<T>,
}

impl<T> AggrFnSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

/// The state of the SUM aggregate function.
#[derive(Debug)]
pub struct AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    sum: T,
    has_value: bool,
}

impl<T> AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            has_value: false,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateSum<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;
    type ResultTargetType = Vec<Option<T>>;

    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, value)?;
                self.has_value = true;
                Ok(())
            }
        }
    }

    #[inline]
    fn push_result_concrete(
        &self,
        _ctx: &mut EvalContext,
        target: &mut Vec<Option<T>>,
    ) -> Result<()> {
        if !self.has_value {
            target.push(None);
        } else {
            target.push(Some(self.sum.clone()));
        }
        Ok(())
    }
}
