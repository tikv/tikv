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

    fn add_assign(&mut self, ctx: &mut EvalContext, other: &Self) -> Result<()>;
}

impl AvgDataType for Decimal {
    #[inline]
    fn zero() -> Self {
        Decimal::zero()
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        let r: crate::coprocessor::codec::Result<Decimal> = (self as &Self + other).into();
        *self = r?;
        Ok(())
        // TODO: If there is truncate error, should it be a warning instead?
    }
}

impl AvgDataType for Real {
    #[inline]
    fn zero() -> Self {
        0.0
    }

    #[inline]
    fn add_assign(&mut self, _ctx: &mut EvalContext, other: &Self) -> Result<()> {
        *self += other;
        Ok(())
    }
}

#[derive(Debug)]
pub struct AggrFnAvg<T: AvgDataType> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: AvgDataType> AggrFnAvg<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
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
