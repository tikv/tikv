// Copyright 2017 PingCAP, Inc.
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

use std::{i64, f64};
use std::cmp::Ordering;
use coprocessor::codec::Datum;
use super::{Result, Expression, StatementContext, field_type_unsigned};

/// Compare two expressions which should be eval as int64.
pub fn lt_int_eval_int(lhs: &Expression,
                       rhs: &Expression,
                       row: &[Datum],
                       ctx: &StatementContext)
                       -> Result<Option<i64>> {
    let lhs_i = try!(lhs.eval_int(row, ctx));
    let rhs_i = try!(rhs.eval_int(row, ctx));
    match (lhs_i, rhs_i) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs_i), Some(rhs_i)) => {
            let lhs_unsigned = field_type_unsigned(&lhs.ret_type);
            let rhs_unsigned = field_type_unsigned(&rhs.ret_type);
            let ordering = cmp_i64(lhs_i, lhs_unsigned, rhs_i, rhs_unsigned);
            Ok(Some((ordering == Ordering::Less) as i64))
        }
    }
}

/// Compare two expressions which should be eval as float64.
pub fn lt_real_eval_int(lhs: &Expression,
                        rhs: &Expression,
                        row: &[Datum],
                        ctx: &StatementContext)
                        -> Result<Option<i64>> {
    let lhs = try!(lhs.eval_real(row, ctx));
    let rhs = try!(rhs.eval_real(row, ctx));
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => Ok(Some((cmp_f64(lhs, rhs) == Ordering::Less) as i64)),
    }
}

/// Compare two expressions which should be eval as decimal.
pub fn lt_decimal_eval_int(lhs: &Expression,
                           rhs: &Expression,
                           row: &[Datum],
                           ctx: &StatementContext)
                           -> Result<Option<i64>> {
    let lhs = try!(lhs.eval_decimal(row, ctx));
    let rhs = try!(rhs.eval_decimal(row, ctx));
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
    }
}

/// Compare two expressions which should be eval as string.
pub fn lt_string_eval_int(lhs: &Expression,
                          rhs: &Expression,
                          row: &[Datum],
                          ctx: &StatementContext)
                          -> Result<Option<i64>> {
    let lhs = try!(lhs.eval_string(row, ctx));
    let rhs = try!(rhs.eval_string(row, ctx));
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
    }
}

/// Compare two expressions which should be eval as time.
pub fn lt_time_eval_int(lhs: &Expression,
                        rhs: &Expression,
                        row: &[Datum],
                        ctx: &StatementContext)
                        -> Result<Option<i64>> {
    let lhs = try!(lhs.eval_time(row, ctx));
    let rhs = try!(rhs.eval_time(row, ctx));
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
    }
}

/// Compare two expressions which should be eval as duration.
pub fn lt_duration_eval_int(lhs: &Expression,
                            rhs: &Expression,
                            row: &[Datum],
                            ctx: &StatementContext)
                            -> Result<Option<i64>> {
    let lhs = try!(lhs.eval_duration(row, ctx));
    let rhs = try!(rhs.eval_duration(row, ctx));
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
    }
}

fn cmp_i64(lhs: i64, lhs_unsigned: bool, rhs: i64, rhs_unsigned: bool) -> Ordering {
    match (lhs_unsigned, rhs_unsigned) {
        (true, true) | (false, false) => lhs.cmp(&rhs),
        (true, false) => {
            if rhs < 0 || lhs as u64 > i64::MAX as u64 {
                Ordering::Greater
            } else {
                lhs.cmp(&rhs)
            }
        }
        (false, true) => {
            if lhs < 0 || rhs as u64 > i64::MAX as u64 {
                Ordering::Less
            } else {
                lhs.cmp(&rhs)
            }
        }
    }
}

fn cmp_f64(lhs: f64, rhs: f64) -> Ordering {
    if lhs - rhs < f64::EPSILON && rhs - lhs < f64::EPSILON {
        Ordering::Equal
    } else if lhs - rhs < 0.0 {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}
