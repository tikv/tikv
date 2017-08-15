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

// FIXME(shirly): remove following later
#![allow(dead_code)]

use std::{str, i64, u64};

use coprocessor::codec::{mysql, Datum};
use coprocessor::codec::mysql::{Decimal, Duration, Time};
use coprocessor::codec::mysql::decimal::RoundMode;
use coprocessor::codec::convert::{self, convert_float_to_int, convert_float_to_uint,
                                  convert_int_to_uint};
use coprocessor::codec::mysql::types;

use super::{FnCall, Result, StatementContext};

impl FnCall {
    pub fn cast_int_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    pub fn cast_real_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try!(self.children[0].eval_real(ctx, row));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap();
        if mysql::has_unsigned_flag(self.tp.get_flag() as u64) {
            let uval = try!(convert_float_to_uint(val, u64::MAX, types::DOUBLE));
            Ok(Some(uval as i64))
        } else {
            let res = try!(convert_float_to_int(val, i64::MIN, i64::MAX, types::DOUBLE));
            Ok(Some(res))
        }
    }

    pub fn cast_decimal_as_int(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        let val = try!(self.children[0].eval_decimal(ctx, row));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap().round(0, RoundMode::HalfEven).unwrap();
        if mysql::has_unsigned_flag(self.tp.get_flag() as u64) {
            let uint = val.as_u64().unwrap();
            // TODO:handle overflow
            Ok(Some(uint as i64))
        } else {
            let val = val.as_i64().unwrap();
            // TODO:handle overflow
            Ok(Some(val))
        }
    }

    pub fn cast_str_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        if self.children[0].is_hybrid_type() {
            return self.children[0].eval_int(ctx, row);
        }
        let val = try!(self.children[0].eval_string(ctx, row));
        if val.is_none() {
            return Ok(None);
        }
        let val = try!(String::from_utf8(val.unwrap()));
        let val = val.trim();
        if val.starts_with('-') {
            // negative
            let v = try!(convert::bytes_to_int(ctx, val.as_bytes()));
            // TODO: if overflow, don't append this warning
            Ok(Some(v))
        } else {
            let urs = try!(convert::bytes_to_uint(ctx, val.as_bytes()));
            // TODO: process overflow
            Ok(Some(urs as i64))
        }
    }

    pub fn cast_time_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn cast_duration_as_int(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        unimplemented!()
    }

    pub fn cast_int_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try!(self.children[0].eval_int(ctx, row));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap();
        if !mysql::has_unsigned_flag(self.children[0].get_tp().get_flag() as u64) {
            Ok(Some(val as f64))
        } else {
            let uval = try!(convert_int_to_uint(val, u64::MAX, types::LONG_LONG));
            Ok(Some(uval as f64))
        }
    }

    pub fn cast_real_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        unimplemented!()
    }

    pub fn cast_decimal_as_real(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        unimplemented!()
    }

    pub fn cast_str_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        unimplemented!()
    }

    pub fn cast_time_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        unimplemented!()
    }

    pub fn cast_duration_as_real(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        unimplemented!()
    }

    pub fn cast_int_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        let val = try!(self.children[0].eval_int(ctx, row));
        if val.is_none() {
            return Ok(None);
        }
        let val = val.unwrap();
        let field_type = &self.children[0].get_tp();
        let res = if !mysql::has_unsigned_flag(field_type.get_flag() as u64) {
            Decimal::from(val)
        } else {
            let uval = try!(convert_int_to_uint(val, u64::MAX, types::LONG_LONG));
            Decimal::from(uval)
        };
        let flen = field_type.get_flen();
        let decimal = field_type.get_decimal();
        if flen == convert::UNSPECIFIED_LENGTH || decimal == convert::UNSPECIFIED_LENGTH {
            return Ok(Some(res));
        }
        let res = try!(res.convert_to(ctx, flen as u8, decimal as u8));
        Ok(Some(res))
    }

    pub fn cast_real_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    pub fn cast_decimal_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    pub fn cast_str_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    pub fn cast_time_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    pub fn cast_duration_as_decimal(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Decimal>> {
        unimplemented!()
    }

    pub fn cast_int_as_str(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_real_as_str(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_decimal_as_str(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_str_as_str(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_time_as_strl(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_duration_as_str(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<u8>>> {
        unimplemented!()
    }

    pub fn cast_int_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_real_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_decimal_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_str_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_time_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_duration_as_time(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Time>>> {
        unimplemented!()
    }

    pub fn cast_int_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }

    pub fn cast_real_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }

    pub fn cast_decimal_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }

    pub fn cast_str_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }

    pub fn cast_time_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }

    pub fn cast_duration_as_duration(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Vec<Duration>>> {
        unimplemented!()
    }
}
