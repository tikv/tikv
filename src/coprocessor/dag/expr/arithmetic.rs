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

use std::{f64, i64, u64};
use std::borrow::Cow;
use coprocessor::codec::{datum, mysql, Datum};
use coprocessor::codec::mysql::{Decimal, MAX_FSP};
use super::{Error, FnCall, Result, StatementContext};

impl FnCall {
    pub fn plus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l + r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(r)
        })
    }

    pub fn plus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() + r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn plus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            if (l > 0 && r > i64::MAX - l) || (l < 0 && r < i64::MIN - l) {
                return Err(Error::Overflow);
            }
            Ok(l + r)
        })
    }

    pub fn plus_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let (l, r) = (l as u64, r as u64);
            if l > u64::MAX - r {
                return Err(Error::Overflow);
            }
            Ok((l + r) as i64)
        })
    }

    pub fn minus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l - r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(r)
        })
    }

    pub fn minus_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() - r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn minus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            if (l > 0 && -r > i64::MAX - l) || (l < 0 && -r < i64::MIN - l) {
                return Err(Error::Overflow);
            }
            Ok(l + r)
        })
    }

    pub fn minus_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let (l, r) = (l as u64, r as u64);
            if l < r {
                return Err(Error::Overflow);
            }
            Ok((l - r) as i64)
        })
    }

    pub fn multiply_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l * r;
            if !res.is_finite() {
                return Err(Error::Overflow);
            }
            Ok(r)
        })
    }

    pub fn multiply_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let lhs = try!(self.children[0].eval_decimal(ctx, row));
        let rhs = try!(self.children[1].eval_decimal(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let result: Result<Decimal> = (l.as_ref() * r.as_ref()).into();
            result.map(Cow::Owned)
        })
    }

    pub fn multiply_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let res = l * r;
            if l != 0 && res / l != r {
                return Err(Error::Overflow);
            }
            Ok(res)
        })
    }

    pub fn multiply_uint(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let lhs = try!(self.children[0].eval_int(ctx, row));
        let rhs = try!(self.children[1].eval_int(ctx, row));
        do_arithmetic(lhs, rhs, |l, r| {
            let (l, r) = (l as u64, r as u64);
            let res = l * r;
            if l != 0 && res / l != r {
                return Err(Error::Overflow);
            }
            Ok(res as i64)
        })
    }
}

#[inline]
fn do_arithmetic<T, F>(lhs: Option<T>, rhs: Option<T>, op: F) -> Result<Option<T>>
where
    F: Fn(T, T) -> Result<T>,
{
    match (lhs, rhs) {
        (None, _) | (_, None) => Ok(None),
        (Some(lhs), Some(rhs)) => op(lhs, rhs).map(|t| Some(t)),
    }
}
