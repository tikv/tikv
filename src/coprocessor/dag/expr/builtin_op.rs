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

// TODO: remove following later
#![allow(dead_code)]

use super::{FnCall, Result, StatementContext};
use coprocessor::codec::Datum;

impl FnCall {
    pub fn logic_and(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        match (arg0, arg1) {
            (None, None) => Ok(None),
            (Some(0), _) | (_, Some(0)) => Ok(Some(0)),
            _ => Ok(Some(1)),
        }
    }

    pub fn logic_or(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        match (arg0, arg1) {
            (None, None) => Ok(None),
            (None, Some(0)) | (Some(0), None) | (Some(0), Some(0)) => Ok(Some(0)),
            _ => Ok(Some(1)),
        }
    }

    pub fn logic_xor(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        match (arg0, arg1) {
            (None, _) | (_, None) => Ok(None),
            (Some(0), _) | (_, Some(0)) => Ok(Some(1)),
            _ => Ok(Some(0)),
        }
    }

    pub fn real_is_true(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_int(ctx, row));
        match input {
            None | Some(0) => Ok(Some(0)),
            _ => Ok(Some(1)),
        }
    }

    pub fn decimal_is_true(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_decimal(ctx, row));
        if input.is_none() || input.unwrap().into_owned().is_zero() {
            return Ok(Some(0));
        }
        Ok(Some(1))
    }

    pub fn int_is_true(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_int(ctx, row));
        match input {
            None | Some(0) => Ok(Some(0)),
            _ => Ok(Some(1)),
        }
    }

    pub fn real_is_false(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_real(ctx, row));
        // Shouldn't compare float to 0 directly, but for compatibility to TiDB
        if input.is_none() || input.unwrap() == 0.0 {
            return Ok(Some(0));
        }
        Ok(Some(1))
    }

    pub fn decimal_is_false(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_decimal(ctx, row));
        if input.is_none() || !input.unwrap().into_owned().is_zero() {
            return Ok(Some(0));
        }
        Ok(Some(1))
    }

    pub fn int_is_false(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let input = try!(self.children[0].eval_int(ctx, row));
        match input {
            Some(0) => Ok(Some(1)),
            _ => Ok(Some(0)),
        }
    }

    pub fn unary_not(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_int(ctx, row));
        match arg {
            None => Ok(None),
            Some(0) => Ok(Some(1)),
            _ => Ok(Some(0)),
        }
    }

    pub fn unary_minus_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn unary_minus_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn unary_minus_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn decimal_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_decimal(ctx, row));
        eval_is_null(arg)
    }

    pub fn int_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_int(ctx, row));
        eval_is_null(arg)
    }

    pub fn real_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_real(ctx, row));
        eval_is_null(arg)
    }

    pub fn string_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_string(ctx, row));
        eval_is_null(arg)
    }

    pub fn time_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_time(ctx, row));
        eval_is_null(arg)
    }

    pub fn duration_is_null(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg = try!(self.children[0].eval_duration(ctx, row));
        eval_is_null(arg)
    }
}

fn eval_is_null<T>(arg: Option<T>) -> Result<Option<i64>> {
    match arg {
        None => Ok(Some(1)),
        _ => Ok(Some(0)),
    }
}
