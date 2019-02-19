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
pub fn logical_and(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<i64>,
    _: &FieldType,
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

#[inline(always)]
pub fn logical_or(
    _ctx: &mut RpnRuntimeContext,
    _: &FieldType,
    arg0: &Option<i64>,
    _: &FieldType,
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
