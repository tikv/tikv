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

use std::borrow::Cow;

use coprocessor::codec::Datum;
use coprocessor::codec::mysql::{Decimal, Duration, Json, Time};
use super::{Constant, Error, Result, StatementContext};

#[inline]
pub fn datum_as_int(d: &Datum) -> Result<Option<i64>> {
    match *d {
        Datum::I64(i) => Ok(Some(i)),
        Datum::U64(u) => Ok(Some(u as i64)),
        _ => Err(Error::Other("Can't eval_int from Constant")),
    }
}

#[inline]
pub fn datum_as_real(d: &Datum) -> Result<Option<f64>> {
    match *d {
        Datum::F64(f) => Ok(Some(f)),
        _ => Err(Error::Other("Can't eval_real from Datum")),
    }
}

#[inline]
pub fn datum_as_decimal(d: &Datum) -> Result<Option<Cow<Decimal>>> {
    match *d {
        Datum::Dec(ref d) => Ok(Some(Cow::Borrowed(d))),
        _ => Err(Error::Other("Can't eval_decimal from Datum")),
    }
}

#[inline]
pub fn datum_as_string(d: &Datum) -> Result<Option<Cow<Vec<u8>>>> {
    match *d {
        Datum::Bytes(ref b) => Ok(Some(Cow::Borrowed(b))),
        _ => Err(Error::Other("Can't eval_string from Datum")),
    }
}

#[inline]
pub fn datum_as_time(d: &Datum) -> Result<Option<Cow<Time>>> {
    match *d {
        Datum::Time(ref t) => Ok(Some(Cow::Borrowed(t))),
        _ => Err(Error::Other("Can't eval_time from Datum")),
    }
}

#[inline]
pub fn datum_as_duration(d: &Datum) -> Result<Option<Cow<Duration>>> {
    match *d {
        Datum::Dur(ref d) => Ok(Some(Cow::Borrowed(d))),
        _ => Err(Error::Other("Can't eval_duration from Datum")),
    }
}

#[inline]
pub fn datum_as_json(d: &Datum) -> Result<Option<Cow<Json>>> {
    match *d {
        Datum::Json(ref j) => Ok(Some(Cow::Borrowed(j))),
        _ => Err(Error::Other("Can't eval_json from Datum")),
    }
}

impl Constant {
    #[inline]
    pub fn eval_int(&self, ctx: &StatementContext) -> Result<Option<i64>> {
        datum_as_int(&self.val)
    }

    #[inline]
    pub fn eval_real(&self, ctx: &StatementContext) -> Result<Option<f64>> {
        datum_as_real(&self.val)
    }

    #[inline]
    pub fn eval_decimal(&self, ctx: &StatementContext) -> Result<Option<Cow<Decimal>>> {
        datum_as_decimal(&self.val)
    }

    #[inline]
    pub fn eval_string(&self, ctx: &StatementContext) -> Result<Option<Cow<Vec<u8>>>> {
        datum_as_string(&self.val)
    }

    #[inline]
    pub fn eval_time(&self, ctx: &StatementContext) -> Result<Option<Cow<Time>>> {
        datum_as_time(&self.val)
    }

    #[inline]
    pub fn eval_duration(&self, ctx: &StatementContext) -> Result<Option<Cow<Duration>>> {
        datum_as_duration(&self.val)
    }

    #[inline]
    pub fn eval_json(&self, ctx: &StatementContext) -> Result<Option<Cow<Json>>> {
        datum_as_json(&self.val)
    }
}
