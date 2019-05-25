// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use cop_codegen::AggrFunction;
use cop_datatype::EvalType;
use tipb::expression::{Expr, ExprType, FieldType};

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::Result;

/// The parser for FIRST aggregate function.
pub struct AggrFnDefinitionParserFirst;

impl super::AggrDefinitionParser for AggrFnDefinitionParserFirst {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        if aggr_def.get_children().len() != 1 {
            return Err(box_err!(
                "Expect 1 parameter, but got {}",
                aggr_def.get_children().len()
            ));
        }

        // Check whether parameter expression is supported.
        RpnExpressionBuilder::check_expr_tree_supported(&aggr_def.get_children()[0])?;

        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().tp()).unwrap();

        // FIRST outputs one column with the same type as its child
        out_schema.push(aggr_def.take_field_type());

        // FIRST doesn't need to cast, so using the expression directly.
        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            max_columns,
        )?);

        match_template_evaluable! {
            TT, match eval_type {
                EvalType::TT => Ok(Box::new(AggrFnFirst::<TT>::new()))
            }
        }
    }
}

/// The FIRST aggregate function.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateFirst::<T>::new())]
pub struct AggrFnFirst<T>(PhantomData<T>)
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>;

impl<T> AggrFnFirst<T>
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>,
{
    fn new() -> Self {
        AggrFnFirst(PhantomData)
    }
}

/// The state of the FIRST aggregate function.
#[derive(Debug)]
pub enum AggrFnStateFirst<T>
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>,
{
    Empty,
    Valued(Option<T>),
}

impl<T> AggrFnStateFirst<T>
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>,
{
    pub fn new() -> Self {
        AggrFnStateFirst::Empty
    }
}

// default impl all `Evaluable` for all `AggrFnStateFirst` to make `AggrFnStateFirst`
// satisfy trait `AggrFunctionState`
impl<T1, T2> super::AggrFunctionStateUpdatePartial<T1> for AggrFnStateFirst<T2>
where
    T1: Evaluable,
    T2: Evaluable,
    VectorValue: VectorValueExt<T2>,
{
    #[inline]
    default fn update(&mut self, _ctx: &mut EvalContext, _value: &Option<T1>) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default fn update_repeat(
        &mut self,
        _ctx: &mut EvalContext,
        _value: &Option<T1>,
        _repeat_times: usize,
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }

    #[inline]
    default fn update_vector(
        &mut self,
        _ctx: &mut EvalContext,
        _values: &[Option<T1>],
    ) -> Result<()> {
        panic!("Unmatched parameter type")
    }
}

impl<T> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateFirst<T>
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>,
{
    #[inline]
    fn update(&mut self, _ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        if let AggrFnStateFirst::Empty = self {
            // TODO: avoid this clone
            *self = AggrFnStateFirst::Valued(value.as_ref().cloned());
        }
        Ok(())
    }

    #[inline]
    fn update_repeat(
        &mut self,
        ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        assert!(repeat_times > 0);
        self.update(ctx, value)
    }

    #[inline]
    fn update_vector(&mut self, ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()> {
        if let Some(v) = values.first() {
            self.update(ctx, v)?;
        }
        Ok(())
    }
}

impl<T> super::AggrFunctionState for AggrFnStateFirst<T>
where
    T: Evaluable,
    VectorValue: VectorValueExt<T>,
{
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        assert_eq!(target.len(), 1);
        let res = if let AggrFnStateFirst::Valued(v) = self {
            v.clone()
        } else {
            None
        };
        target[0].push(res);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    #[test]
    fn test_update() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<Int>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update(&mut ctx, &Some(1)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, Some(1)]);

        state.update(&mut ctx, &Some(2)).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, Some(1), Some(1)]);
    }

    #[test]
    fn test_update_repeat() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<Bytes>::new();
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Bytes)];

        state.update_repeat(&mut ctx, &Some(vec![1]), 2).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_bytes_slice(), &[Some(vec![1])]);

        state.update_repeat(&mut ctx, &Some(vec![2]), 3).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_bytes_slice(), &[Some(vec![1]), Some(vec![1])]);
    }

    #[test]
    fn test_update_vector() {
        let mut ctx = EvalContext::default();
        let function = AggrFnFirst::<Int>::new();
        let mut state = function.create_state();
        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.update_vector(&mut ctx, &[Some(0); 0]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        state.update_vector(&mut ctx, &[None, Some(2)]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, None]);

        state.update_vector(&mut ctx, &[Some(1)]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None, None, None]);
    }
}
