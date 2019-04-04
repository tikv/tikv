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
pub struct AggrFnCount;

impl AggrFnCount {
    pub fn new() -> Self {
        Self
    }
}

impl super::AggrFunction for AggrFnCount {
    #[inline]
    fn name(&self) -> &'static str {
        "AggrFnCount"
    }

    #[inline]
    fn create_state(&self) -> Box<dyn super::AggrFunctionState> {
        Box::new(AggrFnStateCount::new())
    }
}

#[derive(Debug)]
pub struct AggrFnStateCount {
    count: usize,
}

impl AggrFnStateCount {
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

// Manually implement `AggrFunctionStateUpdatePartial` to achieve best performance for
// `update_repeat` and `update_vector`.

impl<T: Evaluable> super::AggrFunctionStateUpdatePartial<T> for AggrFnStateCount {
    #[inline]
    fn update(&mut self, _ctx: &mut EvalContext, value: &Option<T>) -> Result<()> {
        if value.is_some() {
            self.count += 1;
        }
        Ok(())
    }

    #[inline]
    fn update_repeat(
        &mut self,
        _ctx: &mut EvalContext,
        value: &Option<T>,
        repeat_times: usize,
    ) -> Result<()> {
        if value.is_some() {
            self.count += repeat_times;
        }
        Ok(())
    }

    #[inline]
    fn update_vector(&mut self, _ctx: &mut EvalContext, values: &[Option<T>]) -> Result<()> {
        for value in values {
            if value.is_some() {
                self.count += 1;
            }
        }
        Ok(())
    }
}

impl<T> super::AggrFunctionStateResultPartial<T> for AggrFnStateCount
where
    T: super::AggrResultAppendable + ?Sized,
{
    #[inline]
    default fn push_result(&self, _ctx: &mut EvalContext, _target: &mut T) -> Result<()> {
        panic!("Unmatched result append target type")
    }
}

impl super::AggrFunctionStateResultPartial<Vec<Option<Int>>> for AggrFnStateCount {
    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut Vec<Option<Int>>) -> Result<()> {
        target.push(Some(self.count as Int));
        Ok(())
    }
}

impl super::AggrFunctionState for AggrFnStateCount {}
