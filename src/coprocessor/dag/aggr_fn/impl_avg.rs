// Copyright 2019 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO
#![allow(dead_code)]

//! There are `AVG(Decimal) -> (Int, Decimal)` and `AVG(Double) -> (Int, Double)`.

use cop_datatype::{EvalType, FieldTypeFlag, FieldTypeTp};
use tipb::expression::{Expr, ExprType, FieldType};

use super::summable::Summable;
use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::codec::mysql::Tz;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::dag::rpn_expr::{RpnExpression, RpnExpressionBuilder};
use crate::coprocessor::{Error, Result};

pub struct AggrSigAvgHandler;

impl super::AggrDefinitionParser for AggrSigAvgHandler {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
        if aggr_def.get_children().len() != 1 {
            return Err(box_err!(
                "Expect 1 parameter, but got {}",
                aggr_def.get_children().len()
            ));
        }

        // Check whether or not the children's field type is supported. Currently we only support
        // Double and Decimal and does not support other types (which need casting).
        let child = &aggr_def.get_children()[0];
        let eval_type = EvalType::try_from(child.get_field_type().tp())
            .map_err(|e| Error::Other(box_err!(e)))?;
        match eval_type {
            EvalType::Real | EvalType::Decimal => {}
            _ => return Err(box_err!("Cast from {:?} is not supported", eval_type)),
        }

        // Check whether parameter expression is supported.
        RpnExpressionBuilder::check_expr_tree_supported(child)?;

        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        time_zone: &Tz,
        max_columns: usize,
        output_schema: &mut Vec<FieldType>,
        output_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        use cop_datatype::FieldTypeAccessor;
        use std::convert::TryFrom;

        assert_eq!(aggr_def.get_tp(), ExprType::Avg);
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let eval_type = EvalType::try_from(child.get_field_type().tp()).unwrap();

        // AVG outputs two columns
        output_schema.push({
            let mut ft = FieldType::new();
            ft.as_mut_accessor()
                .set_tp(FieldTypeTp::LongLong)
                .set_flag(FieldTypeFlag::UNSIGNED);
            ft
        });
        output_schema.push(aggr_def.take_field_type());

        // Currently we don't insert CAST, so the built expression will be directly used.
        output_exp.push(RpnExpressionBuilder::build_from_expr_tree(
            child,
            time_zone,
            max_columns,
        )?);

        // Choose a type-aware AVG implementation based on eval type.
        match eval_type {
            EvalType::Real => Ok(Box::new(AggrFnAvg::<Real>::new())),
            EvalType::Decimal => Ok(Box::new(AggrFnAvg::<Decimal>::new())),
            _ => unreachable!(),
        }
    }
}

#[derive(Debug)]
pub struct AggrFnAvg<T: Summable> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Summable> AggrFnAvg<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T> super::AggrFunction for AggrFnAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    #[inline]
    fn name(&self) -> &'static str {
        "AggrFnAvg"
    }

    #[inline]
    fn create_state(&self) -> Box<dyn super::AggrFunctionState> {
        Box::new(AggrFnStateAvg::<T>::new())
    }
}

#[derive(Debug)]
pub struct AggrFnStateAvg<T: Summable> {
    sum: T,
    count: usize,
}

impl<T: Summable> AggrFnStateAvg<T> {
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            count: 0,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateAvg<T>
where
    T: Summable,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;
    type ResultTargetType = [VectorValue];

    #[inline]
    fn update_concrete(&mut self, ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add_assign(ctx, value)?;
                self.count += 1;
                Ok(())
            }
        }
    }

    // Note: The result of `AVG()` is returned as `[count, sum]`.

    #[inline]
    fn push_result_concrete(
        &self,
        _ctx: &mut EvalContext,
        target: &mut [VectorValue],
    ) -> Result<()> {
        assert_eq!(target.len(), 2);
        target[0].push_int(Some(self.count as Int));
        if self.count == 0 {
            target[1].push(None);
        } else {
            target[1].push(Some(self.sum.clone()))
        }
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
        let function = AggrFnAvg::<f64>::new();
        let mut state = function.create_state();

        let mut result = [
            VectorValue::with_capacity(0, EvalType::Int),
            VectorValue::with_capacity(0, EvalType::Real),
        ];
        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);
        assert_eq!(result[1].as_real_slice(), &[None]);

        state.update(&mut ctx, &Option::<Real>::None).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0), Some(0)]);
        assert_eq!(result[1].as_real_slice(), &[None, None]);

        state.update(&mut ctx, &Some(5.0)).unwrap();
        state.update(&mut ctx, &Option::<Real>::None).unwrap();
        state.update(&mut ctx, &Some(10.0)).unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0), Some(0), Some(2)]);
        assert_eq!(result[1].as_real_slice(), &[None, None, Some(15.0)]);

        state
            .update_vector(&mut ctx, &[Some(0.0), Some(-4.5), None])
            .unwrap();

        state.push_result(&mut ctx, &mut result[..]).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0), Some(0), Some(2), Some(4)]
        );
        assert_eq!(
            result[1].as_real_slice(),
            &[None, None, Some(15.0), Some(10.5)]
        );
    }
}
