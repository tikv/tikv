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

use super::types::RpnRuntimeContext;
use tipb::expression::FieldType;

#[inline(always)]
pub fn dummy_0_arg_func(_ctx: &mut RpnRuntimeContext) -> Option<i64> {
    Some(7)
}

#[inline(always)]
pub fn dummy_1_arg_func(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    _v: &Option<i64>,
) -> Option<i64> {
    Some(5)
}

#[inline(always)]
pub fn dummy_2_args_func(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    _lhs: &Option<i64>,
    _: &FieldType,
    _rhs: &Option<f64>,
) -> Option<f64> {
    Some(11.0)
}

//
//#[inline(always)]
//pub fn dummy_3_args_func(
//    arg0: &Option<i64>,
//    arg1: &Option<f64>,
//    arg2: &Option<f64>,
//) -> Option<i64> {
//    // Evaluate to (arg0 + arg1) == arg2
//    let l = match arg0 {
//        None => None,
//        Some(arg0) => match arg1 {
//            None => None,
//            Some(arg1) => (arg0 as f64) + arg1,
//        },
//    };
//    let eq = (l == arg2);
//    Some(eq as i64)
//}
//
//#[inline(always)]
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
//#[inline(always)]
//pub fn dummy_variable_args_func(args: _) -> _ {
//    // Compile time unknown number of args.
//
//}
