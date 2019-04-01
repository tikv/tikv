// Copyright 2018 PingCAP, Inc.
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

//! For demo purpose.

use super::types::RpnFnCallPayload;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(Debug, Clone, Copy)]
pub struct RpnFnDummy0ArgFunc;

impl_template_fn! { 0 arg @ RpnFnDummy0ArgFunc }

impl RpnFnDummy0ArgFunc {
    #[inline]
    fn call(_ctx: &mut EvalContext, _payload: RpnFnCallPayload<'_>) -> Result<Option<i64>> {
        Ok(Some(7))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnDummy1ArgFunc;

impl_template_fn! { 1 arg @ RpnFnDummy1ArgFunc }

impl RpnFnDummy1ArgFunc {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        _: &Option<i64>,
    ) -> Result<Option<i64>> {
        Ok(Some(5))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnDummy2ArgFunc;

impl_template_fn! { 2 arg @ RpnFnDummy2ArgFunc }

impl RpnFnDummy2ArgFunc {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        _: &Option<i64>,
        _: &Option<f64>,
    ) -> Result<Option<f64>> {
        Ok(Some(11.0))
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnDummy3ArgFunc;

impl_template_fn! { 3 arg @ RpnFnDummy3ArgFunc }

impl RpnFnDummy3ArgFunc {
    #[inline]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload<'_>,
        _: &Option<i64>,
        _: &Option<f64>,
        _: &Option<i64>,
    ) -> Result<Option<f64>> {
        Ok(Some(13.5))
    }
}

//
//#[inline]
//pub fn dummy_4_or_more_args_func(
//    arg0: &Option<f64>,
//    arg1: &Option<i64>,
//    arg2: &Option<f64>,
//    arg3: &Option<i64>,
//) -> Option<i64> {
//    // Evaluate to (arg0 + arg1) == (arg2 + arg3)
//    let l = match arg0 {
//        None => None,
//        Some(arg0) => match arg1 {
//            None => None,
//            Some(arg1) => arg0 + (arg1 as f64),
//        },
//    };
//    let r = match arg2 {
//        None => None,
//        Some(arg2) => match arg3 {
//            None => None,
//            Some(arg3) => arg2 + (arg3 as f64),
//        },
//    };
//}
//
//#[inline]
//pub fn dummy_variable_args_func(args: _) -> _ {
//    // Compile time unknown number of args.
//
//}
