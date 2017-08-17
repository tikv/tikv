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
//
use std::{i64, f64};
use coprocessor::codec::{datum, mysql, Datum};
use super::{FnCall, Result, StatementContext};

impl FnCall {
    pub fn plus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let lhs = try!(self.children[0].eval_real(ctx, row));
        let rhs = try!(self.children[1].eval_real(ctx, row));
        do_plus(lhs, rhs, |l, r| {
			let res = l + r;
			if !res.is_finite() {
				return Err(Error::Other("overflow"));
			}
            res
        })
    }

    pub fn plus_decimal<'a, 'b: 'a>(&'b self, ctx: &StatementContext, row: &'a [Datum]) -> Result<Option<Cow<'a, Decimal>>> {
    }

    pub fn plus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
    }
}

fn do_plus<T, F>(lhs: Option<T>, rhs: Option<T>, plus: F) -> Result<Option<T>>
where
    F: Fn(T, T) -> Result<Option<T>>
{
    Ok(None)
}
