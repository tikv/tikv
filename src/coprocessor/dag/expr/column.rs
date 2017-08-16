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
use super::{constant, Column, Error, Result};

impl Column {
    fn check_offset(&self, row: &[Datum]) -> Result<()> {
        if self.offset >= row.len() {
            return Err(Error::ColumnOffset(self.offset));
        }
        Ok(())
    }

    #[inline]
    pub fn eval_int(&self, row: &[Datum]) -> Result<Option<i64>> {
        try!(self.check_offset(row));
        constant::datum_as_int(&row[self.offset])
    }

    #[inline]
    pub fn eval_real(&self, row: &[Datum]) -> Result<Option<f64>> {
        try!(self.check_offset(row));
        constant::datum_as_real(&row[self.offset])
    }

    #[inline]
    pub fn eval_decimal<'a, 'b: 'a>(
        &'b self,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        try!(self.check_offset(row));
        constant::datum_as_decimal(&row[self.offset])
    }

    #[inline]
    pub fn eval_string<'a, 'b: 'a>(&'b self, row: &'a [Datum]) -> Result<Option<Cow<'a, Vec<u8>>>> {
        try!(self.check_offset(row));
        constant::datum_as_string(&row[self.offset])
    }

    #[inline]
    pub fn eval_time<'a, 'b: 'a>(&'b self, row: &'a [Datum]) -> Result<Option<Cow<'a, Time>>> {
        try!(self.check_offset(row));
        constant::datum_as_time(&row[self.offset])
    }

    #[inline]
    pub fn eval_duration<'a, 'b: 'a>(
        &'b self,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        try!(self.check_offset(row));
        constant::datum_as_duration(&row[self.offset])
    }

    #[inline]
    pub fn eval_json<'a, 'b: 'a>(&'b self, row: &'a [Datum]) -> Result<Option<Cow<'a, Json>>> {
        try!(self.check_offset(row));
        constant::datum_as_json(&row[self.offset])
    }
}
