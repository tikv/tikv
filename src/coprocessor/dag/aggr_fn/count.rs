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

#![allow(dead_code)]

use cop_datatype::EvalType;

use crate::coprocessor::codec::data_type::*;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(Debug)]
pub struct AggrFnCount<T: Evaluable> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Evaluable> AggrFnCount<T> {
    pub fn new() -> Self {
        Self {
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Evaluable> super::AggrFunction for AggrFnCount<T> {
    #[inline]
    fn name(&self) -> &'static str {
        "AggrFnCount"
    }

    #[inline]
    fn create_state(&self) -> Box<dyn super::AggrFunctionState> {
        Box::new(AggrFnStateCount::<T>::new())
    }

    #[inline]
    fn update_type(&self) -> EvalType {
        T::EVAL_TYPE
    }

    #[inline]
    fn result_type(&self) -> &'static [EvalType] {
        &[EvalType::Int]
    }
}

#[derive(Debug)]
pub struct AggrFnStateCount<T: Evaluable> {
    count: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: Evaluable> AggrFnStateCount<T> {
    pub fn new() -> Self {
        Self {
            count: 0,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<T: Evaluable> super::ConcreteAggrFunctionState for AggrFnStateCount<T> {
    type ParameterType = T;
    type ResultTargetType = Vec<Option<Int>>;

    #[inline]
    fn update_concrete(&mut self, _ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn push_result_concrete(
        &self,
        _ctx: &mut EvalContext,
        target: &mut Vec<Option<Int>>,
    ) -> Result<()> {
        target.push(Some(self.count as Int));
        Ok(())
    }
}
