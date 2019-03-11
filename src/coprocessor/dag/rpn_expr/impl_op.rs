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

use super::types::RpnFnCallPayload;
use crate::coprocessor::dag::expr::EvalContext;

#[derive(Debug, Clone, Copy)]
pub struct RpnFnLogicalAnd;

impl_template_fn! { 2 arg @ RpnFnLogicalAnd }

impl RpnFnLogicalAnd {
    #[inline(always)]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Option<i64> {
        // Intentionally not merging `None` and `Some(0)` conditions to be clear.
        match arg0 {
            None => match arg1 {
                Some(0) => Some(0),
                _ => None,
            },
            Some(0) => Some(0),
            Some(_) => match arg1 {
                None => None,
                Some(0) => Some(0),
                Some(_) => Some(1),
            },
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct RpnFnLogicalOr;

impl_template_fn! { 2 arg @ RpnFnLogicalOr }

impl RpnFnLogicalOr {
    #[inline(always)]
    fn call(
        _ctx: &mut EvalContext,
        _payload: RpnFnCallPayload,
        arg0: &Option<i64>,
        arg1: &Option<i64>,
    ) -> Option<i64> {
        // Intentionally not merging `None` and `Some(0)` conditions to be clear.
        match arg0 {
            None => match arg1 {
                None => None,
                Some(0) => None,
                Some(_) => Some(1),
            },
            Some(0) => match arg1 {
                None => None,
                Some(0) => Some(0),
                Some(_) => Some(1),
            },
            Some(_) => Some(1),
        }
    }
}
