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

use cop_datatype::EvalType;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

pub trait AvgDataType: Evaluable {
    fn zero() -> Self;

    fn add(&self, ctx: &mut EvalContext, other: &Self) -> Result<Self>;
}

impl AvgDataType for Decimal {
    #[inline]
    fn zero() -> Self {
        Decimal::zero()
    }

    #[inline]
    fn add(&self, _ctx: &mut EvalContext, other: &Self) -> Result<Self> {
        let r: crate::coprocessor::codec::Result<Decimal> = (self + other).into();
        Ok(r?)
        // TODO: If there is truncate error, should it be a warning instead?
    }
}

impl AvgDataType for Real {
    #[inline]
    fn zero() -> Self {
        0.0
    }

    #[inline]
    fn add(&self, _ctx: &mut EvalContext, other: &Self) -> Result<Self> {
        Ok(self + other)
    }
}

#[derive(Debug)]
pub struct AggrFnAvg<T: AvgDataType> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> super::AggrFunction for AggrFnAvg<T>
where
    T: AvgDataType,
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

    #[inline]
    fn update_type(&self) -> EvalType {
        T::EVAL_TYPE
    }

    #[inline]
    fn result_type(&self) -> &'static [EvalType] {
        &[EvalType::Int, T::EVAL_TYPE]
    }
}

#[derive(Debug)]
pub struct AggrFnStateAvg<T: AvgDataType> {
    sum: T,
    count: usize,
}

impl<T: AvgDataType> AggrFnStateAvg<T> {
    pub fn new() -> Self {
        Self {
            sum: T::zero(),
            count: 0,
        }
    }
}

impl<T> super::ConcreteAggrFunctionState for AggrFnStateAvg<T>
where
    T: AvgDataType,
    VectorValue: VectorValueExt<T>,
{
    type ParameterType = T;
    type ResultTargetType = [VectorValue];

    #[inline]
    fn update_concrete(
        &mut self,
        ctx: &mut EvalContext,
        value: &Option<Self::ParameterType>,
    ) -> Result<()> {
        match value {
            None => Ok(()),
            Some(value) => {
                self.sum.add(ctx, value)?;
                self.count += 1;
                Ok(())
            }
        }
    }

    #[inline]
    fn push_result_concrete(
        &self,
        _ctx: &mut EvalContext,
        target: &mut Self::ResultTargetType,
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
