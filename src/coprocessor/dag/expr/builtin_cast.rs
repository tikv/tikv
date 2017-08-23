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
use std::ascii::AsciiExt;
use std::borrow::Cow;

use coprocessor::codec::{mysql, Datum};
use coprocessor::codec::mysql::{charset, types, Decimal, Duration, Json, Res, Time};
use coprocessor::codec::mysql::decimal::RoundMode;
use coprocessor::codec::convert::{self, convert_float_to_int, convert_float_to_uint};

use super::{FnCall, Result, StatementContext};

impl FnCall {
    pub fn cast_int_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    pub fn cast_real_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
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
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let val = val.into_owned().round(0, RoundMode::HalfEven).unwrap();
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
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let negative_flag = b'-';
        let is_negative = match val.iter().skip_while(|x| x.is_ascii_whitespace()).next() {
            Some(&negative_flag) => true,
            _ => false,
        };
        if is_negative {
            // negative
            let v = try!(convert::bytes_to_int(ctx, &val));
            // TODO: if overflow, don't append this warning
            Ok(Some(v))
        } else {
            let urs = try!(convert::bytes_to_uint(ctx, &val));
            // TODO: process overflow
            Ok(Some(urs as i64))
        }
    }

    pub fn cast_time_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dec = try!(val.to_decimal());
        let dec = dec.round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
            .unwrap();
        let res = dec.as_i64().unwrap();
        Ok(Some(res))
    }

    pub fn cast_duration_as_int(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let dec = try!(val.to_decimal());
        let dec = dec.round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
            .unwrap();
        let res = dec.as_i64().unwrap();
        Ok(Some(res))
    }

    pub fn cast_json_as_int(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let res = val.cast_to_int();
        Ok(Some(res))
    }

    pub fn cast_int_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        if !mysql::has_unsigned_flag(self.children[0].get_tp().get_flag() as u64) {
            Ok(Some(
                try!(self.produce_float_with_specified_tp(ctx, val as f64)),
            ))
        } else {
            let uval = val as u64;
            Ok(Some(
                try!(self.produce_float_with_specified_tp(ctx, uval as f64)),
            ))
        }
    }

    pub fn cast_real_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, val))))
    }

    pub fn cast_decimal_as_real(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let res = try!(val.as_f64());
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, res))))
    }

    pub fn cast_str_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        if self.children[0].is_hybrid_type() {
            return self.children[0].eval_real(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let res = try!(convert::bytes_to_f64(ctx, &val));
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, res))))
    }

    pub fn cast_time_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let val = try!(val.to_decimal());
        let res = try!(val.as_f64());
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, res))))
    }

    pub fn cast_duration_as_real(
        &self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let val = try!(val.to_decimal());
        let res = try!(val.as_f64());
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, res))))
    }

    pub fn cast_json_as_real(&self, ctx: &StatementContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val = val.cast_to_real();
        Ok(Some(try!(self.produce_float_with_specified_tp(ctx, val))))
    }

    pub fn cast_int_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let field_type = &self.children[0].get_tp();
        let res = if !mysql::has_unsigned_flag(field_type.get_flag() as u64) {
            Cow::Owned(Decimal::from(val))
        } else {
            let uval = val as u64;
            Cow::Owned(Decimal::from(uval))
        };
        self.produce_dec_with_specified_tp(ctx, res).map(Some)
    }

    pub fn cast_real_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let res = try!(Decimal::from_f64(val));
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(res))
            .map(Some)
    }

    pub fn cast_decimal_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        self.produce_dec_with_specified_tp(ctx, val).map(Some)

    }

    pub fn cast_str_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        if self.children[0].is_hybrid_type() {
            return self.children[0].eval_decimal(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let dec = match try!(Decimal::from_bytes(&val)) {
            Res::Ok(d) => Cow::Owned(d),
            Res::Truncated(d) | Res::Overflow(d) => {
                try!(convert::handle_truncate(ctx, true));
                Cow::Owned(d)
            }
        };
        self.produce_dec_with_specified_tp(ctx, dec).map(Some)
    }

    pub fn cast_time_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dec = try!(val.to_decimal());
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_duration_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let dec = try!(val.to_decimal());
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }


    pub fn cast_json_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val = val.cast_to_real();
        let dec = try!(Decimal::from_f64(val));
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_int_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = if mysql::has_unsigned_flag(self.children[0].get_tp().get_flag() as u64) {
            let uval = val as u64;
            format!("{}", uval)
        } else {
            format!("{}", val)
        };
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_real_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_decimal_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_str_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        self.produce_str_with_specified_tp(ctx, val).map(Some)
    }

    pub fn cast_time_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }


    pub fn cast_duration_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_json_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Vec<u8>>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.to_string();
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_int_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = format!("{}", val);
        Ok(Some(try!(self.produce_time_with_str(ctx, s))))
    }

    pub fn cast_real_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        Ok(Some(try!(self.produce_time_with_str(ctx, s))))
    }

    pub fn cast_decimal_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        Ok(Some(try!(self.produce_time_with_str(ctx, s))))
    }

    pub fn cast_str_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let s = try!(String::from_utf8(val.into_owned()));
        Ok(Some(try!(self.produce_time_with_str(ctx, s))))
    }

    pub fn cast_time_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut val = val.into_owned();
        try!(val.round_frac(self.tp.get_decimal() as i8));
        // TODO: tidb only update tp when tp is Date
        try!(val.set_tp(self.tp.get_tp() as u8));
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_duration_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut val = try!(Time::from_duration(&ctx.tz, val.as_ref()));
        try!(val.round_frac(self.tp.get_decimal() as i8));
        try!(val.set_tp(self.tp.get_tp() as u8));
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_json_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = try!(val.unquote());
        Ok(Some(try!(self.produce_time_with_str(ctx, s))))
    }

    pub fn cast_int_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = format!("{}", val);
        let dur = try!(Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_real_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        let dur = try!(Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_decimal_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        let dur = try!(Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_str_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        // TODO: tidb would handle truncate here
        let dur = try!(Duration::parse(val.as_ref(), self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_time_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut res = try!(val.to_duration());
        try!(res.round_frac(self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(res)))
    }

    pub fn cast_duration_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut res = val.into_owned();
        try!(res.round_frac(self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(res)))
    }

    pub fn cast_json_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = try!(val.unquote());
        // TODO: tidb would handle truncate here
        let d = try!(Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8));
        Ok(Some(Cow::Owned(d)))
    }

    pub fn cast_int_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let j = if mysql::has_unsigned_flag(self.children[0].get_tp().get_flag() as u64) {
            Json::U64(val as u64)
        } else {
            Json::I64(val)
        };
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_real_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let j = Json::Double(val);
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_decimal_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let val = try!(val.as_f64());
        let j = Json::Double(val);
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_str_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let s = try!(String::from_utf8(val.into_owned()));
        if self.tp.get_decimal() == 0 {
            let j: Json = try!(s.parse());
            Ok(Some(Cow::Owned(j)))
        } else {
            Ok(Some(Cow::Owned(Json::String(s))))
        }
    }

    pub fn cast_time_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut val = val.into_owned();
        if val.get_tp() == types::DATETIME || val.get_tp() == types::TIMESTAMP {
            val.set_fsp(mysql::MAX_FSP as u8);
        }
        let s = format!("{}", val);
        Ok(Some(Cow::Owned(Json::String(s))))
    }

    pub fn cast_duration_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut val = val.into_owned();
        val.fsp = mysql::MAX_FSP as u8;
        let s = format!("{}", val);
        Ok(Some(Cow::Owned(Json::String(s))))
    }

    pub fn cast_json_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.children[0].eval_json(ctx, row)
    }

    fn produce_dec_with_specified_tp<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        val: Cow<'a, Decimal>,
    ) -> Result<Cow<'a, Decimal>> {
        let flen = self.tp.get_flen();
        let decimal = self.tp.get_decimal();
        if flen == convert::UNSPECIFIED_LENGTH || decimal == convert::UNSPECIFIED_LENGTH {
            return Ok(val);
        }
        let res = try!(val.into_owned().convert_to(ctx, flen as u8, decimal as u8));
        Ok(Cow::Owned(res))
    }

    /// `produce_str_with_specified_tp`(`ProduceStrWithSpecifiedTp` in tidb) produces
    /// a new string according to `flen` and `chs`.
    fn produce_str_with_specified_tp<'a, 'b: 'a>(
        &'b self,
        ctx: &StatementContext,
        s: Cow<'a, Vec<u8>>,
    ) -> Result<Cow<'a, Vec<u8>>> {
        let flen = self.tp.get_flen();
        let chs = self.tp.get_charset();
        if flen < 0 {
            return Ok(s);
        }
        let flen = flen as usize;
        // flen is the char length, not byte length, for UTF8 charset, we need to calculate the
        // char count and truncate to flen chars if it is too long.
        if chs == charset::CHARSET_UTF8 || chs == charset::CHARSET_UTF8MB4 {
            let s_binary = s.into_owned();
            let s = try!(String::from_utf8(s_binary));
            let char_count = s.char_indices().count();
            if char_count <= flen {
                return Ok(Cow::Owned(s.into_bytes()));
            }

            if convert::handle_truncate_as_error(ctx) {
                return Err(box_err!(
                    "Data Too Long, field len {}, data len {}",
                    flen,
                    char_count
                ));
            }
            let (truncate_pos, _) = s.char_indices().nth(flen).unwrap();
            let res = convert::truncate_binary(s.into_bytes(), truncate_pos as isize);
            return Ok(Cow::Owned(res));
        }

        if s.len() > flen {
            if convert::handle_truncate_as_error(ctx) {
                return Err(box_err!(
                    "Data Too Long, field len {}, data len {}",
                    flen,
                    s.len()
                ));
            }
            let res = convert::truncate_binary(s.into_owned(), flen as isize);
            return Ok(Cow::Owned(res));
        }

        if self.tp.get_tp() == types::STRING as i32 && s.len() < flen {
            let to_pad = flen - s.len();
            let mut s = s.into_owned();
            s.resize(to_pad, 0);
            return Ok(Cow::Owned(s));
        }
        Ok(s)
    }

    fn produce_time_with_str(&self, ctx: &StatementContext, s: String) -> Result<Cow<Time>> {
        let mut t = try!(Time::parse_datetime(
            s.as_ref(),
            self.tp.get_decimal() as i8,
            &ctx.tz
        ));
        try!(t.set_tp(self.tp.get_tp() as u8));
        Ok(Cow::Owned(t))
    }

    /// `produce_float_with_specified_tp`(`ProduceFloatWithSpecifiedTp` in tidb) produces
    /// a new float64 according to `flen` and `decimal` in `self.tp`.
    /// TODO port tests from tidb(tidb haven't implemented now)
    fn produce_float_with_specified_tp(&self, ctx: &StatementContext, f: f64) -> Result<f64> {
        let flen = self.tp.get_flen();
        let decimal = self.tp.get_decimal();
        if flen == convert::UNSPECIFIED_LENGTH || decimal == convert::UNSPECIFIED_LENGTH {
            return Ok(f);
        }
        match convert::truncate_f64(f, flen as u8, decimal as u8) {
            Res::Ok(d) => Ok(d),
            Res::Overflow(d) | Res::Truncated(d) => {
                //TODO process warning with ctx
                try!(convert::handle_truncate(ctx, true));
                Ok(d)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use tipb::expression::{Expr, ExprType, FieldType, ScalarFuncSig};
    use protobuf::RepeatedField;

    use coprocessor::codec::{convert, Datum};
    use coprocessor::codec::mysql::{self, charset, types, Decimal, Duration, Json, Time};
    use coprocessor::dag::expr::{Expression, StatementContext};
    use util::codec::number::NumberEncoder;
    use chrono::Utc;

    pub fn col_expr(col_id: i64, tp: i32) -> Expr {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ColumnRef);
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(col_id).unwrap();
        expr.set_val(buf);
        let mut fp = FieldType::new();
        fp.set_tp(tp);
        expr.set_field_type(fp);
        expr
    }

    fn expr_with_flen_and_decimal(
        sig: ScalarFuncSig,
        children: Vec<Expr>,
        cols: usize,
        flen: i32,
        decimal: i32,
    ) -> Expression {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_children(RepeatedField::from_vec(children));
        expr.set_sig(sig);
        let mut fp = FieldType::new();
        fp.set_flen(flen);
        fp.set_decimal(decimal);
        fp.set_charset(String::from(charset::CHARSET_UTF8));
        expr.set_field_type(fp);
        Expression::build(expr, cols).unwrap()
    }

    fn expr_with_tp_and_decimal(
        sig: ScalarFuncSig,
        children: Vec<Expr>,
        cols: usize,
        tp: i32,
        decimal: i32,
    ) -> Expression {
        let mut expr = Expr::new();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_children(RepeatedField::from_vec(children));
        expr.set_sig(sig);
        let mut fp = FieldType::new();
        fp.set_tp(tp);
        fp.set_decimal(decimal);
        fp.set_charset(String::from(charset::CHARSET_UTF8));
        expr.set_field_type(fp);
        Expression::build(expr, cols).unwrap()
    }

    #[test]
    fn test_cast_as_decimal() {
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true;
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        let int_t = 20121212120023u64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsDecimal,
                types::LONG_LONG,
                vec![Datum::I64(1)],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastIntAsDecimal,
                types::LONG_LONG,
                vec![Datum::I64(1234)],
                7,
                3,
                Decimal::from_f64(1234.000).unwrap(),
            ),
            (
                ScalarFuncSig::CastStringAsDecimal,
                types::STRING,
                vec![Datum::Bytes(b"1".to_vec())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastStringAsDecimal,
                types::STRING,
                vec![Datum::Bytes(b"1234".to_vec())],
                7,
                3,
                Decimal::from_f64(1234.000).unwrap(),
            ),
            (
                ScalarFuncSig::CastRealAsDecimal,
                types::DOUBLE,
                vec![Datum::F64(1f64)],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastRealAsDecimal,
                types::DOUBLE,
                vec![Datum::F64(1234.123)],
                8,
                4,
                Decimal::from_f64(1234.1230).unwrap(),
            ),
            (
                ScalarFuncSig::CastTimeAsDecimal,
                types::DATETIME,
                vec![Datum::Time(t.clone())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(int_t),
            ),
            (
                ScalarFuncSig::CastTimeAsDecimal,
                types::DATETIME,
                vec![Datum::Time(t)],
                15,
                1,
                format!("{}.0", int_t).parse::<Decimal>().unwrap(),
            ),
            (
                ScalarFuncSig::CastDurationAsDecimal,
                types::DURATION,
                vec![Datum::Dur(duration_t.clone())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(120023),
            ),
            (
                ScalarFuncSig::CastDurationAsDecimal,
                types::DURATION,
                vec![Datum::Dur(duration_t)],
                7,
                1,
                Decimal::from_f64(120023.0).unwrap(),
            ),
            (
                ScalarFuncSig::CastJsonAsDecimal,
                types::JSON,
                vec![Datum::Json(Json::I64(1))],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastJsonAsDecimal,
                types::JSON,
                vec![Datum::Json(Json::I64(1))],
                2,
                1,
                Decimal::from_f64(1.0).unwrap(),
            ),
            (
                ScalarFuncSig::CastDecimalAsDecimal,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1))],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastDecimalAsDecimal,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1))],
                2,
                1,
                Decimal::from_f64(1.0).unwrap(),
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, flen, decimal, exp) in cases {
            let col_expr = col_expr(0, tp as i32);
            let e = expr_with_flen_and_decimal(sig, vec![col_expr], 1, flen as i32, decimal as i32);
            let res = e.eval_decimal(&ctx, &col).unwrap();
            assert_eq!(res.unwrap().into_owned(), exp);
            // test None
            let res = e.eval_decimal(&ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_str() {
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true;
        let t_str = "2012-12-12 12:00:23";
        let t = Time::parse_utc_datetime(t_str, 0).unwrap();
        let int_t = 20121212120023u64;
        let dur_str = b"12:00:23";
        let duration_t = Duration::parse(dur_str, 0).unwrap();
        let s = "您好world";
        let exp_s = "您好w";
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsString,
                types::LONG_LONG,
                vec![Datum::I64(1)],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastIntAsString,
                types::LONG_LONG,
                vec![Datum::I64(1234)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                types::STRING,
                vec![Datum::Bytes(b"1234".to_vec())],
                convert::UNSPECIFIED_LENGTH,
                b"1234".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                types::STRING,
                vec![Datum::Bytes(s.as_bytes().to_vec())],
                3,
                exp_s.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                types::DOUBLE,
                vec![Datum::F64(1f64)],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                types::DOUBLE,
                vec![Datum::F64(1234.123)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                types::DATETIME,
                vec![Datum::Time(t.clone())],
                convert::UNSPECIFIED_LENGTH,
                t_str.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                types::DATETIME,
                vec![Datum::Time(t)],
                3,
                t_str[0..3].as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                types::DURATION,
                vec![Datum::Dur(duration_t.clone())],
                convert::UNSPECIFIED_LENGTH,
                dur_str.to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                types::DURATION,
                vec![Datum::Dur(duration_t)],
                3,
                dur_str[0..3].to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                types::JSON,
                vec![Datum::Json(Json::I64(1))],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                types::JSON,
                vec![Datum::Json(Json::I64(1234))],
                2,
                b"12".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1))],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1234))],
                2,
                b"12".to_vec(),
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, flen, exp) in cases {
            let col_expr = col_expr(0, tp as i32);
            let e = expr_with_flen_and_decimal(
                sig,
                vec![col_expr],
                1,
                flen as i32,
                convert::UNSPECIFIED_LENGTH as i32,
            );
            let res = e.eval_string(&ctx, &col).unwrap();
            assert_eq!(
                res.unwrap().into_owned(),
                exp,
                "sig: {:?} with flen {} failed",
                sig,
                flen
            );
            // test None
            let res = e.eval_string(&ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_time() {
        let mut ctx = StatementContext::default();
        ctx.ignore_truncate = true;
        let today = Utc::now();
        let t_dur_str = format!("{}", today.format("%H:%M:%S"));
        let t_date_str = format!("{}", today.format("%Y-%m-%d"));
        let t_time_str = format!("{}", today.format("%Y-%m-%d %H:%M:%S"));
        let t_time = Time::parse_utc_datetime(t_time_str.as_ref(), 0).unwrap();
        let t_date = {
            let mut date = t_time.clone();
            date.set_tp(types::DATE).unwrap();
            date
        };
        let t_int = format!("{}", today.format("%Y%m%d%H%M%S"))
            .parse::<u64>()
            .unwrap();

        let dur_str = "12:00:23";
        let duration_t = Duration::parse(dur_str.as_bytes(), 0).unwrap();
        let dur_to_time_str = format!("{} 12:00:23", t_date_str);
        let dur_to_time = Time::parse_utc_datetime(&dur_to_time_str, 0).unwrap();
        let mut dur_to_date = dur_to_time.clone();
        dur_to_date.set_tp(types::DATE).unwrap();

        let json_cols = vec![Datum::Json(Json::String(t_time_str.clone()))];
        let int_cols = vec![Datum::U64(t_int)];
        let str_cols = vec![Datum::Bytes(t_time_str.as_bytes().to_vec())];
        let f64_cols = vec![Datum::F64(t_int as f64)];
        let time_cols = vec![Datum::Time(t_time.clone())];
        let duration_cols = vec![Datum::Dur(duration_t)];
        let dec_cols = vec![Datum::Dec(Decimal::from(t_int))];

        let cases = vec![
            (
                // cast int as time
                ScalarFuncSig::CastIntAsTime,
                types::LONG_LONG,
                &int_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                // cast int as datetime(6)  TODO
                ScalarFuncSig::CastIntAsTime,
                types::LONG_LONG,
                &int_cols,
                mysql::MAX_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                ScalarFuncSig::CastStringAsTime,
                types::STRING,
                &str_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                // cast string as datetime(6)
                ScalarFuncSig::CastStringAsTime,
                types::STRING,
                &str_cols,
                mysql::MAX_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                ScalarFuncSig::CastRealAsTime,
                types::DOUBLE,
                &f64_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                // cast real as date(0)
                ScalarFuncSig::CastRealAsTime,
                types::DOUBLE,
                &f64_cols,
                mysql::DEFAULT_FSP,
                types::DATE,
                &t_date,
            ),
            (
                ScalarFuncSig::CastTimeAsTime,
                types::DATETIME,
                &time_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                // cast time as date
                ScalarFuncSig::CastTimeAsTime,
                types::DATETIME,
                &time_cols,
                mysql::DEFAULT_FSP,
                types::DATE,
                &t_date,
            ),
            (
                ScalarFuncSig::CastDurationAsTime,
                types::DURATION,
                &duration_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &dur_to_time,
            ),
            (
                // cast duration as date
                ScalarFuncSig::CastDurationAsTime,
                types::DURATION,
                &duration_cols,
                mysql::MAX_FSP,
                types::DATE,
                &dur_to_date,
            ),
            (
                ScalarFuncSig::CastJsonAsTime,
                types::JSON,
                &json_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                ScalarFuncSig::CastJsonAsTime,
                types::JSON,
                &json_cols,
                mysql::DEFAULT_FSP,
                types::DATE,
                &t_date,
            ),
            (
                ScalarFuncSig::CastDecimalAsTime,
                types::NEW_DECIMAL,
                &dec_cols,
                mysql::UN_SPECIFIED_FSP,
                types::DATETIME,
                &t_time,
            ),
            (
                // cast decimal as date
                ScalarFuncSig::CastDecimalAsTime,
                types::NEW_DECIMAL,
                &dec_cols,
                mysql::DEFAULT_FSP,
                types::DATE,
                &t_date,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, to_fsp, to_tp, exp) in cases {
            let col_expr = col_expr(0, tp as i32);
            let e = expr_with_tp_and_decimal(sig, vec![col_expr], 1, to_tp as i32, to_fsp as i32);
            let res = e.eval_time(&ctx, col).unwrap();
            let data = res.unwrap().into_owned();
            let mut expt = exp.clone();
            if to_fsp != mysql::UN_SPECIFIED_FSP {
                expt.set_fsp(to_fsp as u8);
            }
            assert_eq!(
                data.to_string(),
                expt.to_string(),
                "sig: {:?} with to tp {} and fsp {} failed",
                sig,
                to_tp,
                to_fsp,
            );
            // test None
            let res = e.eval_time(&ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }
}
