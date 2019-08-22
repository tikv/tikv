// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::marker::PhantomData;

use tidb_query_codegen::AggrFunction;
use tidb_query_datatype::EvalType;
use tipb::{Expr, ExprType, FieldType};

use crate::codec::data_type::*;
use crate::codec::mysql::Tz;
use crate::expr::EvalContext;
use crate::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::Result;

/// The parser for FIRST aggregate function.
pub struct AggrFnDefinitionParserFirst;

impl super::AggrDefinitionParser for AggrFnDefinitionParserFirst {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        super::util::check_aggr_exp_supported_one_child(aggr_def)
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        src_schema: &[FieldType],
        out_schema: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use std::convert::TryFrom;
        use tidb_query_datatype::FieldTypeAccessor;
        assert_eq!(aggr_def.get_tp(), ExprType::First);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().as_accessor().tp()).unwrap();

        // FIRST outputs one column with the same type as its child
        out_schema.push(aggr_def.take_field_type());

        out_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            src_schema.len(),
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

// Here we manually implement `AggrFunctionStateUpdatePartial` instead of implementing
// `ConcreteAggrFunctionState` so that `update_repeat` and `update_vector` can be faster.
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
    fn update_vector(
        &mut self,
        ctx: &mut EvalContext,
        physical_values: &[Option<T>],
        logical_rows: &[usize],
    ) -> Result<()> {
        if let Some(physical_index) = logical_rows.first() {
            self.update(ctx, &physical_values[*physical_index])?;
        }
        Ok(())
    }
}

// In order to make `AggrFnStateFirst` satisfy the `AggrFunctionState` trait, we default impl all
// `AggrFunctionStateUpdatePartial` of `Evaluable` for all `AggrFnStateFirst`.
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
        _physical_values: &[Option<T1>],
        _logical_rows: &[usize],
    ) -> Result<()> {
        panic!("Unmatched parameter type")
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

        state.update_vector(&mut ctx, &[Some(0); 0], &[]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        state.update_vector(&mut ctx, &[Some(1)], &[]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        state
            .update_vector(&mut ctx, &[None, Some(2)], &[0, 1])
            .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        result[0].clear();
        state.update_vector(&mut ctx, &[Some(1)], &[0]).unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[None]);

        // Reset state
        let mut state = function.create_state();

        result[0].clear();
        state
            .update_vector(&mut ctx, &[None, Some(2)], &[1, 0])
            .unwrap();
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(2)]);
    }
}
