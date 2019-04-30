// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! For demo purpose.

use cop_codegen::RpnFunction;

use super::types::RpnFnCallPayload;
use crate::coprocessor::dag::expr::EvalContext;
use crate::coprocessor::Result;

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 0)]
pub struct RpnFnDummy0ArgFunc;

impl RpnFnDummy0ArgFunc {
    #[inline]
    fn call(_ctx: &mut EvalContext, _payload: RpnFnCallPayload<'_>) -> Result<Option<i64>> {
        Ok(Some(7))
    }
}

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 1)]
pub struct RpnFnDummy1ArgFunc;

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

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 2)]
pub struct RpnFnDummy2ArgFunc;

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

#[derive(Debug, Clone, Copy, RpnFunction)]
#[rpn_function(args = 3)]
pub struct RpnFnDummy3ArgFunc;

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
