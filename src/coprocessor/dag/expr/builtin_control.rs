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
use coprocessor::codec::mysql::{Decimal, Duration, Time};

impl FnCall {
    pub fn if_null_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let arg0 = try!(self.children[0].eval_real(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0);
        }
        let arg1 = try!(self.children[1].eval_real(ctx, row));
        Ok(arg1)
    }

    pub fn if_null_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        let arg0 = try!(self.children[0].eval_decimal(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0.map(|x| x.into_owned()));
        }
        let arg1 = try!(self.children[1].eval_decimal(ctx, row));
        Ok(arg1.map(|x| x.into_owned()))
    }

    pub fn if_null_string(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Vec<u8>>> {
        let arg0 = try!(self.children[0].eval_string(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0.map(|x| x.into_owned()));
        }
        let arg1 = try!(self.children[1].eval_string(ctx, row));
        Ok(arg1.map(|x| x.into_owned()))
    }

    pub fn if_null_time(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Time>> {
        let arg0 = try!(self.children[0].eval_time(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0.map(|x| x.into_owned()));
        }
        let arg1 = try!(self.children[1].eval_time(ctx, row));
        Ok(arg1.map(|x| x.into_owned()))
    }

    pub fn if_null_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Duration>> {
        let arg0 = try!(self.children[0].eval_duration(ctx, row));
        if !arg0.is_none() {
            return Ok(arg0.map(|x| x.into_owned()));
        }
        let arg1 = try!(self.children[1].eval_duration(ctx, row));
        Ok(arg1.map(|x| x.into_owned()))
    }

    pub fn if_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_int(ctx, row));
        let arg2 = try!(self.children[2].eval_int(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_real(ctx, row));
        let arg2 = try!(self.children[2].eval_real(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2),
            _ => Ok(arg1),
        }
    }

    pub fn if_decimal(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Decimal>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_decimal(ctx, row));
        let arg2 = try!(self.children[2].eval_decimal(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2.map(|x| x.into_owned())),
            _ => Ok(arg1.map(|x| x.into_owned())),
        }
    }

    pub fn if_string(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Vec<u8>>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_string(ctx, row));
        let arg2 = try!(self.children[2].eval_string(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2.map(|x| x.into_owned())),
            _ => Ok(arg1.map(|x| x.into_owned())),
        }
    }

    pub fn if_time(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Time>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_time(ctx, row));
        let arg2 = try!(self.children[2].eval_time(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2.map(|x| x.into_owned())),
            _ => Ok(arg1.map(|x| x.into_owned())),
        }
    }

    pub fn if_duration(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<Duration>> {
        let arg0 = try!(self.children[0].eval_int(ctx, row));
        let arg1 = try!(self.children[1].eval_duration(ctx, row));
        let arg2 = try!(self.children[2].eval_duration(ctx, row));
        match arg0 {
            None | Some(0) => Ok(arg2.map(|x| x.into_owned())),
            _ => Ok(arg1.map(|x| x.into_owned())),
        }
    }
}
