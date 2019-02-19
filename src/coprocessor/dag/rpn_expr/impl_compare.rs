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

use super::types::RpnRuntimeContext;
use tipb::expression::FieldType;

#[inline(always)]
pub fn eq_real(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<f64>,
    _: &FieldType,
    arg1: &Option<f64>,
) -> Option<i64> {
    // FIXME: It really should be a `Result<Option<f64>>`.
    match (arg0, arg1) {
        (Some(ref arg0), Some(ref arg1)) => Some((*arg0 == *arg1) as i64),
        // TODO: Use `partial_cmp`.
        _ => None,
    }
}

#[inline(always)]
pub fn eq_int(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<i64>,
    _: &FieldType,
    arg1: &Option<i64>,
) -> Option<i64> {
    // FIXME: The algorithm here is incorrect. We should care about unsigned and signed.
    match (arg0, arg1) {
        (Some(ref arg0), Some(ref arg1)) => Some((*arg0 == *arg1) as i64),
        _ => None,
    }
}

#[inline(always)]
pub fn gt_int(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<i64>,
    _: &FieldType,
    arg1: &Option<i64>,
) -> Option<i64> {
    // FIXME: The algorithm here is incorrect. We should care about unsigned and signed.
    match (arg0, arg1) {
        (Some(ref arg0), Some(ref arg1)) => Some((*arg0 > *arg1) as i64),
        _ => None,
    }
}

#[inline(always)]
pub fn lt_int(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<i64>,
    _: &FieldType,
    arg1: &Option<i64>,
) -> Option<i64> {
    // FIXME: The algorithm here is incorrect. We should care about unsigned and signed.
    match (arg0, arg1) {
        (Some(ref arg0), Some(ref arg1)) => Some((*arg0 < *arg1) as i64),
        _ => None,
    }
}
