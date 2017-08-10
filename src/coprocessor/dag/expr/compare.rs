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

use std::i64;
use std::cmp::Ordering;
use coprocessor::codec::Datum;
use super::*;

impl FnCall {
    pub fn lt_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => {
                let lhs_unsigned = field_type_unsigned(&self.children[0].get_tp());
                let rhs_unsigned = field_type_unsigned(&self.children[1].get_tp());
                let ordering = cmp_i64(lhs, lhs_unsigned, rhs, rhs_unsigned);
                Ok(Some((ordering == Ordering::Less) as i64))
            }
        }
    }

    pub fn lt_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some((cmp_f64(lhs, rhs) == Ordering::Less) as i64)),
        }
    }

    pub fn lt_decimal(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
        }
    }

    pub fn lt_string(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_string(ctx, row));
        let rhs = try!(self.children[1].eval_string(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
        }
    }

    pub fn lt_time(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_time(ctx, row));
        let rhs = try!(self.children[1].eval_time(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
        }
    }

    pub fn lt_duration(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_duration(ctx, row));
        let rhs = try!(self.children[1].eval_duration(ctx, row));
        match (lhs, rhs) {
            (None, _) | (_, None) => Ok(None),
            (Some(lhs), Some(rhs)) => Ok(Some((lhs.cmp(&rhs) == Ordering::Less) as i64)),
        }
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
    if lhs == rhs {
        Ordering::Equal
    } else if lhs < rhs {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}
