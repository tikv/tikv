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

use tipb::expression::ScalarFuncSig;

use coprocessor::codec::{datum, mysql, Datum};
use super::{Error, FnCall, Result, StatementContext};

#[derive(Clone, Copy, PartialEq)]
pub enum CmpOp {
    LT,
    LE,
    GT,
    GE,
    NE,
    EQ,
    NullEQ,
}

impl FnCall {
    pub fn compare_int(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_compare(lhs, rhs, op, |l, r| {
            let lhs_unsigned = mysql::has_unsigned_flag(self.children[0].get_tp().get_flag());
            let rhs_unsigned = mysql::has_unsigned_flag(self.children[1].get_tp().get_flag());
            Ok(cmp_i64_with_unsigned_flag(l, lhs_unsigned, r, rhs_unsigned))
        })
    }

    pub fn compare_real(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_compare(
            lhs,
            rhs,
            op,
            |l, r| datum::cmp_f64(l, r).map_err(Error::from),
        )
    }

    pub fn compare_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_compare(lhs, rhs, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_string(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_string(ctx, row));
        let rhs = try!(self.children[1].eval_string(ctx, row));
        do_compare(lhs, rhs, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_time(ctx, row));
        let rhs = try!(self.children[1].eval_time(ctx, row));
        do_compare(lhs, rhs, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_duration(ctx, row));
        let rhs = try!(self.children[1].eval_duration(ctx, row));
        do_compare(lhs, rhs, op, |l, r| Ok(l.cmp(&r)))
    }

    pub fn compare_json(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
        op: CmpOp,
    ) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_json(ctx, row));
        let rhs = try!(self.children[1].eval_json(ctx, row));
        do_compare(lhs, rhs, op, |l, r| Ok(l.cmp(&r)))
    }
}

fn do_compare<T, F>(lhs: Option<T>, rhs: Option<T>, op: CmpOp, get_order: F) -> Result<Option<i64>>
where
    F: Fn(T, T) -> Result<Ordering>,
{
    match (lhs, rhs) {
        (None, None) if op == CmpOp::NullEQ => Ok(Some(1)),
        (Some(lhs), Some(rhs)) => {
            let ordering = try!(get_order(lhs, rhs));
            let r = match op {
                CmpOp::LT => ordering == Ordering::Less,
                CmpOp::LE => ordering != Ordering::Greater,
                CmpOp::GT => ordering == Ordering::Greater,
                CmpOp::GE => ordering != Ordering::Less,
                CmpOp::NE => ordering != Ordering::Equal,
                CmpOp::EQ | CmpOp::NullEQ => ordering == Ordering::Equal,
            };
            Ok(Some(r as i64))
        }
        _ => match op {
            CmpOp::NullEQ => Ok(Some(0)),
            _ => Ok(None),
        },
    }
}

#[inline]
fn cmp_i64_with_unsigned_flag(
    lhs: i64,
    lhs_unsigned: bool,
    rhs: i64,
    rhs_unsigned: bool,
) -> Ordering {
    match (lhs_unsigned, rhs_unsigned) {
        (false, false) => lhs.cmp(&rhs),
        (true, true) => {
            let lhs = lhs as u64;
            let rhs = rhs as u64;
            lhs.cmp(&rhs)
        }
        (true, false) => if rhs < 0 || lhs as u64 > i64::MAX as u64 {
            Ordering::Greater
        } else {
            lhs.cmp(&rhs)
        },
        (false, true) => if lhs < 0 || rhs as u64 > i64::MAX as u64 {
            Ordering::Less
        } else {
            lhs.cmp(&rhs)
        },
    }
}

#[cfg(test)]
mod test {
    use std::{i64, u64};
    use super::*;

    #[test]
    fn test_cmp_i64_with_unsigned_flag() {
        let cases = vec![
            (5, false, 3, false, Ordering::Greater),
            (u64::MAX as i64, false, 5 as i64, false, Ordering::Less),

            (
                u64::MAX as i64,
                true,
                (u64::MAX - 1) as i64,
                true,
                Ordering::Greater,
            ),
            (u64::MAX as i64, true, 5 as i64, true, Ordering::Greater),

            (5, true, i64::MIN, false, Ordering::Greater),
            (u64::MAX as i64, true, i64::MIN, false, Ordering::Greater),
            (5, true, 3, false, Ordering::Greater),

            (i64::MIN, false, 3, true, Ordering::Less),
            (5, false, u64::MAX as i64, true, Ordering::Less),
            (5, false, 3, true, Ordering::Greater),
        ];
        for (a, b, c, d, e) in cases {
            let o = cmp_i64_with_unsigned_flag(a, b, c, d);
            assert_eq!(o, e);
        }
    }
}
