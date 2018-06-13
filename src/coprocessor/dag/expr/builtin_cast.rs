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
use std::{str, i64, u64};

use coprocessor::codec::convert::{self, convert_float_to_int, convert_float_to_uint};
use coprocessor::codec::mysql::decimal::RoundMode;
use coprocessor::codec::mysql::{charset, types, Decimal, Duration, Json, Res, Time};
use coprocessor::codec::{mysql, Datum};

use super::{Error, EvalContext, FnCall, Result};

impl FnCall {
    pub fn cast_int_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    pub fn cast_real_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        if mysql::has_unsigned_flag(u64::from(self.tp.get_flag())) {
            let uval = convert_float_to_uint(val, u64::MAX, types::DOUBLE)?;
            Ok(Some(uval as i64))
        } else {
            let res = convert_float_to_int(val, i64::MIN, i64::MAX, types::DOUBLE)?;
            Ok(Some(res))
        }
    }

    pub fn cast_decimal_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let val = val.into_owned().round(0, RoundMode::HalfEven).unwrap();
        let (overflow, res) = if mysql::has_unsigned_flag(u64::from(self.tp.get_flag())) {
            let uint = val.as_u64();
            (uint.is_overflow(), uint.unwrap() as i64)
        } else {
            let val = val.as_i64();
            (val.is_overflow(), val.unwrap())
        };

        if overflow {
            if !ctx.cfg.overflow_as_warning {
                return Err(Error::overflow("CastDecimalAsInt", &format!("{}", val)));
            }
            ctx.warnings
                .append_warning(Error::truncated_wrong_val("DECIMAL", &format!("{}", val)));
        }
        Ok(Some(res))
    }

    pub fn cast_str_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        if self.children[0].is_hybrid_type() {
            return self.children[0].eval_int(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let is_negative = match val.iter().skip_while(|x| x.is_ascii_whitespace()).next() {
            Some(&b'-') => true,
            _ => false,
        };
        let res = if is_negative {
            convert::bytes_to_int(ctx, &val).map(|v| {
                ctx.warnings
                    .append_warning(Error::cast_neg_int_as_unsigned());
                v
            })
        } else {
            convert::bytes_to_uint(ctx, &val).map(|urs| {
                if !mysql::has_unsigned_flag(u64::from(self.tp.get_flag()))
                    && urs > (i64::MAX as u64)
                {
                    ctx.warnings
                        .append_warning(Error::cast_as_signed_overflow());
                }
                urs as i64
            })
        };

        match res {
            Ok(v) => Ok(Some(v)),
            Err(e) => if e.is_overflow() {
                ctx.overflow_from_cast_str_as_int(&val, e, is_negative)
                    .map(Some)
            } else {
                Err(e)
            },
        }
    }

    pub fn cast_time_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dec = val.to_decimal()?;
        let dec = dec.round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
            .unwrap();
        let res = dec.as_i64().unwrap();
        Ok(Some(res))
    }

    pub fn cast_duration_as_int(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let dec = val.to_decimal()?;
        let dec = dec.round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
            .unwrap();
        let res = dec.as_i64().unwrap();
        Ok(Some(res))
    }

    pub fn cast_json_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let res = val.cast_to_int();
        Ok(Some(res))
    }

    pub fn cast_int_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        if !mysql::has_unsigned_flag(u64::from(self.children[0].get_tp().get_flag())) {
            Ok(Some(self.produce_float_with_specified_tp(ctx, val as f64)?))
        } else {
            let uval = val as u64;
            Ok(Some(self.produce_float_with_specified_tp(
                ctx,
                uval as f64,
            )?))
        }
    }

    pub fn cast_real_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        Ok(Some(self.produce_float_with_specified_tp(ctx, val)?))
    }

    pub fn cast_decimal_as_real(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let res = val.as_f64()?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_str_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        if self.children[0].is_hybrid_type() {
            return self.children[0].eval_real(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let res = convert::bytes_to_f64(ctx, &val)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_time_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let val = val.to_decimal()?;
        let res = val.as_f64()?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_duration_as_real(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let val = val.to_decimal()?;
        let res = val.as_f64()?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_json_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val = val.cast_to_real();
        Ok(Some(self.produce_float_with_specified_tp(ctx, val)?))
    }

    pub fn cast_int_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let field_type = &self.children[0].get_tp();
        let res = if !mysql::has_unsigned_flag(u64::from(field_type.get_flag())) {
            Cow::Owned(Decimal::from(val))
        } else {
            let uval = val as u64;
            Cow::Owned(Decimal::from(uval))
        };
        self.produce_dec_with_specified_tp(ctx, res).map(Some)
    }

    pub fn cast_real_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let res = Decimal::from_f64(val)?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(res))
            .map(Some)
    }

    pub fn cast_decimal_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        self.produce_dec_with_specified_tp(ctx, val).map(Some)
    }

    pub fn cast_str_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let dec = if self.children[0].is_hybrid_type() {
            try_opt!(self.children[0].eval_decimal(ctx, row))
        } else {
            let val = try_opt!(self.children[0].eval_string(ctx, row));
            match Decimal::from_bytes(&val)? {
                Res::Ok(d) => Cow::Owned(d),
                Res::Truncated(d) | Res::Overflow(d) => {
                    ctx.handle_truncate(true)?;
                    Cow::Owned(d)
                }
            }
        };
        self.produce_dec_with_specified_tp(ctx, dec).map(Some)
    }

    pub fn cast_time_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dec = val.to_decimal()?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_duration_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let dec = val.to_decimal()?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_json_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val = val.cast_to_real();
        let dec = Decimal::from_f64(val)?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_int_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = if mysql::has_unsigned_flag(u64::from(self.children[0].get_tp().get_flag())) {
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
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_decimal_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_str_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        self.produce_str_with_specified_tp(ctx, val).map(Some)
    }

    pub fn cast_time_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_duration_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let s = format!("{}", val);
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_json_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.to_string();
        self.produce_str_with_specified_tp(ctx, Cow::Owned(s.into_bytes()))
            .map(Some)
    }

    pub fn cast_int_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = format!("{}", val);
        Ok(Some(self.produce_time_with_str(ctx, s)?))
    }

    pub fn cast_real_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        Ok(Some(self.produce_time_with_str(ctx, s)?))
    }

    pub fn cast_decimal_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        Ok(Some(self.produce_time_with_str(ctx, s)?))
    }

    pub fn cast_str_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let s = String::from_utf8(val.into_owned())?;
        Ok(Some(self.produce_time_with_str(ctx, s)?))
    }

    pub fn cast_time_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut val = val.into_owned();
        val.round_frac(self.tp.get_decimal() as i8)?;
        // TODO: tidb only update tp when tp is Date
        val.set_tp(self.tp.get_tp() as u8)?;
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_duration_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut val = Time::from_duration(&ctx.cfg.tz, self.tp.get_tp() as u8, val.as_ref())?;
        val.round_frac(self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_json_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.unquote()?;
        Ok(Some(self.produce_time_with_str(ctx, s)?))
    }

    pub fn cast_int_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = format!("{}", val);
        // TODO: port NumberToDuration from tidb.
        match Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8) {
            Ok(dur) => Ok(Some(Cow::Owned(dur))),
            Err(e) => if e.is_overflow() {
                ctx.handle_overflow(e)?;
                Ok(None)
            } else {
                Err(e)
            },
        }
    }

    pub fn cast_real_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        let dur = Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_decimal_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        let dur = Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_str_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let dur = Duration::parse(val.as_ref(), self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(dur)))
    }

    pub fn cast_time_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut res = val.to_duration()?;
        res.round_frac(self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(res)))
    }

    pub fn cast_duration_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut res = val.into_owned();
        res.round_frac(self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(res)))
    }

    pub fn cast_json_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Duration>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.unquote()?;
        // TODO: tidb would handle truncate here
        let d = Duration::parse(s.as_bytes(), self.tp.get_decimal() as i8)?;
        Ok(Some(Cow::Owned(d)))
    }

    pub fn cast_int_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let flag = self.children[0].get_tp().get_flag();
        let j = if mysql::has_is_boolean_flag(flag) {
            Json::Boolean(val != 0)
        } else if mysql::has_unsigned_flag(flag) {
            Json::U64(val as u64)
        } else {
            Json::I64(val)
        };
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_real_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let j = Json::Double(val);
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_decimal_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let val = val.as_f64()?;
        let j = Json::Double(val);
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_str_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let s = String::from_utf8(val.into_owned())?;
        if mysql::has_parse_to_json_flag(self.tp.get_flag()) {
            let j: Json = s.parse()?;
            Ok(Some(Cow::Owned(j)))
        } else {
            Ok(Some(Cow::Owned(Json::String(s))))
        }
    }

    pub fn cast_time_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
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
        ctx: &mut EvalContext,
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
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.children[0].eval_json(ctx, row)
    }

    fn produce_dec_with_specified_tp<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        val: Cow<'a, Decimal>,
    ) -> Result<Cow<'a, Decimal>> {
        let flen = self.tp.get_flen();
        let decimal = self.tp.get_decimal();
        if flen == convert::UNSPECIFIED_LENGTH || decimal == convert::UNSPECIFIED_LENGTH {
            return Ok(val);
        }
        let res = val.into_owned().convert_to(ctx, flen as u8, decimal as u8)?;
        Ok(Cow::Owned(res))
    }

    /// `produce_str_with_specified_tp`(`ProduceStrWithSpecifiedTp` in tidb) produces
    /// a new string according to `flen` and `chs`.
    fn produce_str_with_specified_tp<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        s: Cow<'a, [u8]>,
    ) -> Result<Cow<'a, [u8]>> {
        let flen = self.tp.get_flen();
        let chs = self.tp.get_charset();
        if flen < 0 {
            return Ok(s);
        }
        let flen = flen as usize;
        // flen is the char length, not byte length, for UTF8 charset, we need to calculate the
        // char count and truncate to flen chars if it is too long.
        if chs == charset::CHARSET_UTF8 || chs == charset::CHARSET_UTF8MB4 {
            let truncate_info = {
                let s = str::from_utf8(s.as_ref())?;
                let mut indices = s.char_indices().skip(flen);
                if let Some((truncate_pos, _)) = indices.next() {
                    let char_count = flen + 1 + indices.count();
                    Some((char_count, truncate_pos))
                } else {
                    None
                }
            };
            if truncate_info.is_none() {
                return Ok(s);
            }
            let (char_count, truncate_pos) = truncate_info.unwrap();
            ctx.handle_truncate_err(Error::data_too_long(format!(
                "Data Too Long, field len {}, data len {}",
                flen, char_count
            )))?;

            let mut res = s.into_owned();
            convert::truncate_binary(&mut res, truncate_pos as isize);
            return Ok(Cow::Owned(res));
        }

        if s.len() > flen {
            ctx.handle_truncate_err(Error::data_too_long(format!(
                "Data Too Long, field len {}, data len {}",
                flen,
                s.len()
            )))?;
            let mut res = s.into_owned();
            convert::truncate_binary(&mut res, flen as isize);
            return Ok(Cow::Owned(res));
        }

        if self.tp.get_tp() == i32::from(types::STRING) && s.len() < flen {
            let mut s = s.into_owned();
            s.resize(flen, 0);
            return Ok(Cow::Owned(s));
        }
        Ok(s)
    }

    fn produce_time_with_str(&self, ctx: &mut EvalContext, s: String) -> Result<Cow<Time>> {
        let mut t = Time::parse_datetime(s.as_ref(), self.tp.get_decimal() as i8, &ctx.cfg.tz)?;
        t.set_tp(self.tp.get_tp() as u8)?;
        Ok(Cow::Owned(t))
    }

    /// `produce_float_with_specified_tp`(`ProduceFloatWithSpecifiedTp` in tidb) produces
    /// a new float64 according to `flen` and `decimal` in `self.tp`.
    /// TODO port tests from tidb(tidb haven't implemented now)
    fn produce_float_with_specified_tp(&self, ctx: &mut EvalContext, f: f64) -> Result<f64> {
        let flen = self.tp.get_flen();
        let decimal = self.tp.get_decimal();
        if flen == convert::UNSPECIFIED_LENGTH || decimal == convert::UNSPECIFIED_LENGTH {
            return Ok(f);
        }
        match convert::truncate_f64(f, flen as u8, decimal as u8) {
            Res::Ok(d) => Ok(d),
            Res::Overflow(d) | Res::Truncated(d) => {
                //TODO process warning with ctx
                ctx.handle_truncate(true)?;
                Ok(d)
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;
    use std::{i64, u64};

    use tipb::expression::{Expr, FieldType, ScalarFuncSig};

    use chrono::{FixedOffset, Utc};

    use coprocessor::codec::error::*;
    use coprocessor::codec::mysql::{self, charset, types, Decimal, Duration, Json, Time};
    use coprocessor::codec::{convert, Datum};
    use coprocessor::dag::expr::test::{col_expr as base_col_expr, fncall_expr};
    use coprocessor::dag::expr::{self, EvalConfig, EvalContext, Expression, FLAG_IGNORE_TRUNCATE};

    pub fn col_expr(col_id: i64, tp: i32) -> Expr {
        let mut expr = base_col_expr(col_id);
        let mut fp = FieldType::new();
        fp.set_tp(tp);
        expr.set_field_type(fp);
        expr
    }

    #[test]
    fn test_cast_as_int() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        #[allow(inconsistent_digit_grouping)]
        let time_int = 2012_12_12_12_00_23i64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsInt,
                types::LONG_LONG,
                Some(types::UNSIGNED_FLAG),
                vec![Datum::U64(1)],
                1,
            ),
            (
                ScalarFuncSig::CastIntAsInt,
                types::LONG_LONG,
                None,
                vec![Datum::I64(-1)],
                -1,
            ),
            (
                ScalarFuncSig::CastStringAsInt,
                types::STRING,
                None,
                vec![Datum::Bytes(b"1".to_vec())],
                1,
            ),
            (
                ScalarFuncSig::CastRealAsInt,
                types::DOUBLE,
                None,
                vec![Datum::F64(1f64)],
                1,
            ),
            (
                ScalarFuncSig::CastRealAsInt,
                types::DOUBLE,
                None,
                vec![Datum::F64(1234.000)],
                1234,
            ),
            (
                ScalarFuncSig::CastTimeAsInt,
                types::DATETIME,
                None,
                vec![Datum::Time(t)],
                time_int,
            ),
            (
                ScalarFuncSig::CastDurationAsInt,
                types::DURATION,
                None,
                vec![Datum::Dur(duration_t)],
                120023,
            ),
            (
                ScalarFuncSig::CastJsonAsInt,
                types::JSON,
                None,
                vec![Datum::Json(Json::I64(-1))],
                -1,
            ),
            (
                ScalarFuncSig::CastJsonAsInt,
                types::JSON,
                None,
                vec![Datum::Json(Json::U64(1))],
                1,
            ),
            (
                ScalarFuncSig::CastDecimalAsInt,
                types::NEW_DECIMAL,
                None,
                vec![Datum::Dec(Decimal::from(1))],
                1,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, flag, col, expect) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let mut exp = fncall_expr(sig, &[col_expr]);
            if flag.is_some() {
                exp.mut_field_type().set_flag(flag.unwrap() as u32);
            }
            let e = Expression::build(&mut ctx, exp).unwrap();
            let res = e.eval_int(&mut ctx, &col).unwrap();
            assert_eq!(res.unwrap(), expect);
            // test None
            let res = e.eval_int(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_real() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        #[allow(inconsistent_digit_grouping)]
        let int_t = 2012_12_12_12_00_23u64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsReal,
                types::LONG_LONG,
                vec![Datum::I64(1)],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastIntAsReal,
                types::LONG_LONG,
                vec![Datum::I64(1234)],
                7,
                3,
                1234.000,
            ),
            (
                ScalarFuncSig::CastStringAsReal,
                types::STRING,
                vec![Datum::Bytes(b"1".to_vec())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastStringAsReal,
                types::STRING,
                vec![Datum::Bytes(b"1234".to_vec())],
                7,
                3,
                1234.000,
            ),
            (
                ScalarFuncSig::CastRealAsReal,
                types::DOUBLE,
                vec![Datum::F64(1f64)],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastRealAsReal,
                types::DOUBLE,
                vec![Datum::F64(1234.123)],
                8,
                4,
                1234.1230,
            ),
            (
                ScalarFuncSig::CastTimeAsReal,
                types::DATETIME,
                vec![Datum::Time(t.clone())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                int_t as f64,
            ),
            (
                ScalarFuncSig::CastTimeAsReal,
                types::DATETIME,
                vec![Datum::Time(t)],
                15,
                1,
                format!("{}.0", int_t).parse::<f64>().unwrap(),
            ),
            (
                ScalarFuncSig::CastDurationAsReal,
                types::DURATION,
                vec![Datum::Dur(duration_t.clone())],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                120023f64,
            ),
            (
                ScalarFuncSig::CastDurationAsReal,
                types::DURATION,
                vec![Datum::Dur(duration_t)],
                7,
                1,
                120023.0,
            ),
            (
                ScalarFuncSig::CastJsonAsReal,
                types::JSON,
                vec![Datum::Json(Json::I64(1))],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastJsonAsReal,
                types::JSON,
                vec![Datum::Json(Json::I64(1))],
                2,
                1,
                1.0,
            ),
            (
                ScalarFuncSig::CastDecimalAsReal,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1))],
                convert::UNSPECIFIED_LENGTH,
                convert::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastDecimalAsReal,
                types::NEW_DECIMAL,
                vec![Datum::Dec(Decimal::from(1))],
                2,
                1,
                1.0,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, flen, decimal, expect) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let mut exp = fncall_expr(sig, &[col_expr]);
            exp.mut_field_type().set_flen(flen as i32);
            exp.mut_field_type().set_decimal(decimal as i32);
            let e = Expression::build(&mut ctx, exp).unwrap();
            let res = e.eval_real(&mut ctx, &col).unwrap();
            assert_eq!(format!("{}", res.unwrap()), format!("{}", expect));
            // test None
            let res = e.eval_real(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_decimal() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
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
        for (sig, tp, col, flen, decimal, expect) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let mut exp = fncall_expr(sig, &[col_expr]);
            exp.mut_field_type().set_flen(flen as i32);
            exp.mut_field_type().set_decimal(decimal as i32);
            let e = Expression::build(&mut ctx, exp).unwrap();
            let res = e.eval_decimal(&mut ctx, &col).unwrap();
            assert_eq!(res.unwrap().into_owned(), expect);
            // test None
            let res = e.eval_decimal(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_str() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let t_str = "2012-12-12 12:00:23";
        let t = Time::parse_utc_datetime(t_str, 0).unwrap();
        let dur_str = b"12:00:23";
        let duration_t = Duration::parse(dur_str, 0).unwrap();
        let s = "您好world";
        let exp_s = "您好w";
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsString,
                types::LONG_LONG,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::I64(1)],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastIntAsString,
                types::LONG_LONG,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::I64(1234)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                types::STRING,
                charset::CHARSET_ASCII,
                Some(i32::from(types::STRING)),
                vec![Datum::Bytes(b"1234".to_vec())],
                6,
                b"1234\0\0".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                types::STRING,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Bytes(s.as_bytes().to_vec())],
                3,
                exp_s.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                types::DOUBLE,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::F64(1f64)],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                types::DOUBLE,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::F64(1234.123)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                types::DATETIME,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Time(t.clone())],
                convert::UNSPECIFIED_LENGTH,
                t_str.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                types::DATETIME,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Time(t)],
                3,
                t_str[0..3].as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                types::DURATION,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dur(duration_t.clone())],
                convert::UNSPECIFIED_LENGTH,
                dur_str.to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                types::DURATION,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dur(duration_t)],
                3,
                dur_str[0..3].to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                types::JSON,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Json(Json::I64(1))],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                types::JSON,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Json(Json::I64(1234))],
                2,
                b"12".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                types::NEW_DECIMAL,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dec(Decimal::from(1))],
                convert::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                types::NEW_DECIMAL,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dec(Decimal::from(1234))],
                2,
                b"12".to_vec(),
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, charset, to_tp, col, flen, exp) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let mut ex = fncall_expr(sig, &[col_expr]);
            ex.mut_field_type().set_flen(flen as i32);
            ex.mut_field_type().set_decimal(convert::UNSPECIFIED_LENGTH);
            if to_tp.is_some() {
                ex.mut_field_type().set_tp(to_tp.unwrap());
            }
            ex.mut_field_type().set_charset(String::from(charset));
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_string(&mut ctx, &col).unwrap();
            assert_eq!(
                res.unwrap().into_owned(),
                exp,
                "sig: {:?} with flen {} failed",
                sig,
                flen
            );
            // test None
            let res = e.eval_string(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_time() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let today = Utc::now();
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
                // cast int as datetime(6)
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
            let col_expr = col_expr(0, i32::from(tp));
            let mut ex = fncall_expr(sig, &[col_expr]);
            ex.mut_field_type().set_decimal(i32::from(to_fsp));
            ex.mut_field_type().set_tp(i32::from(to_tp));
            let e = Expression::build(&mut ctx, ex).unwrap();

            let res = e.eval_time(&mut ctx, col).unwrap();
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
            let res = e.eval_time(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_duration() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let today = Utc::now();
        let t_date_str = format!("{}", today.format("%Y-%m-%d"));

        let dur_str = "12:00:23";
        let dur_int = 120023u64;
        let duration = Duration::parse(dur_str.as_bytes(), 0).unwrap();
        let dur_to_time_str = format!("{} 12:00:23", t_date_str);
        let dur_to_time = Time::parse_utc_datetime(&dur_to_time_str, 0).unwrap();
        let mut dur_to_date = dur_to_time.clone();
        dur_to_date.set_tp(types::DATE).unwrap();

        let json_cols = vec![Datum::Json(Json::String(String::from(dur_str)))];
        let int_cols = vec![Datum::U64(dur_int)];
        let str_cols = vec![Datum::Bytes(dur_str.as_bytes().to_vec())];
        let f64_cols = vec![Datum::F64(dur_int as f64)];
        let time_cols = vec![Datum::Time(dur_to_time)];
        let duration_cols = vec![Datum::Dur(duration.clone())];
        let dec_cols = vec![Datum::Dec(Decimal::from(dur_int))];

        let cases = vec![
            (
                // cast int as duration
                ScalarFuncSig::CastIntAsDuration,
                types::LONG_LONG,
                &int_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast int as duration
                ScalarFuncSig::CastIntAsDuration,
                types::LONG_LONG,
                &int_cols,
                mysql::MAX_FSP,
                &duration,
            ),
            (
                // string as duration
                ScalarFuncSig::CastStringAsDuration,
                types::STRING,
                &str_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast string as duration
                ScalarFuncSig::CastStringAsDuration,
                types::STRING,
                &str_cols,
                4,
                &duration,
            ),
            (
                // cast real as duration
                ScalarFuncSig::CastRealAsDuration,
                types::DOUBLE,
                &f64_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast real as duration
                ScalarFuncSig::CastRealAsDuration,
                types::DOUBLE,
                &f64_cols,
                1,
                &duration,
            ),
            (
                // cast time as duration
                ScalarFuncSig::CastTimeAsDuration,
                types::DATETIME,
                &time_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast time as duration
                ScalarFuncSig::CastTimeAsDuration,
                types::DATETIME,
                &time_cols,
                5,
                &duration,
            ),
            (
                ScalarFuncSig::CastDurationAsDuration,
                types::DURATION,
                &duration_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast duration as duration
                ScalarFuncSig::CastDurationAsDuration,
                types::DURATION,
                &duration_cols,
                mysql::MAX_FSP,
                &duration,
            ),
            (
                // cast json as duration
                ScalarFuncSig::CastJsonAsDuration,
                types::JSON,
                &json_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                ScalarFuncSig::CastJsonAsDuration,
                types::JSON,
                &json_cols,
                5,
                &duration,
            ),
            (
                // cast decimal as duration
                ScalarFuncSig::CastDecimalAsDuration,
                types::NEW_DECIMAL,
                &dec_cols,
                mysql::UN_SPECIFIED_FSP,
                &duration,
            ),
            (
                // cast decimal as duration
                ScalarFuncSig::CastDecimalAsDuration,
                types::NEW_DECIMAL,
                &dec_cols,
                2,
                &duration,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, to_fsp, exp) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let mut ex = fncall_expr(sig, &[col_expr]);
            ex.mut_field_type().set_decimal(i32::from(to_fsp));
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_duration(&mut ctx, col).unwrap();
            let data = res.unwrap().into_owned();
            let mut expt = exp.clone();
            if to_fsp != mysql::UN_SPECIFIED_FSP {
                expt.fsp = to_fsp as u8;
            }
            assert_eq!(
                data.to_string(),
                expt.to_string(),
                "sig: {:?} with fsp {} failed",
                sig,
                to_fsp,
            );
            // test None
            let res = e.eval_duration(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_int_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (
                Some(types::UNSIGNED_FLAG),
                vec![Datum::U64(32)],
                Some(Json::U64(32)),
            ),
            (
                Some(types::UNSIGNED_FLAG | types::IS_BOOLEAN_FLAG),
                vec![Datum::U64(1)],
                Some(Json::Boolean(true)),
            ),
            (
                Some(types::UNSIGNED_FLAG | types::IS_BOOLEAN_FLAG),
                vec![Datum::I64(0)],
                Some(Json::Boolean(false)),
            ),
            (None, vec![Datum::I64(-1)], Some(Json::I64(-1))),
            (None, vec![Datum::Null], None),
        ];
        for (flag, cols, exp) in cases {
            let mut col_expr = col_expr(0, i32::from(types::LONG_LONG));
            if flag.is_some() {
                col_expr.mut_field_type().set_flag(flag.unwrap() as u32);
            }
            let ex = fncall_expr(ScalarFuncSig::CastIntAsJson, &[col_expr]);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_real_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (vec![Datum::F64(32.0001)], Some(Json::Double(32.0001))),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(types::DOUBLE));
            let ex = fncall_expr(ScalarFuncSig::CastRealAsJson, &[col_expr]);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_decimal_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (
                vec![Datum::Dec(Decimal::from_f64(32.0001).unwrap())],
                Some(Json::Double(32.0001)),
            ),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(types::NEW_DECIMAL));
            let ex = fncall_expr(ScalarFuncSig::CastDecimalAsJson, &[col_expr]);

            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_str_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (
                false,
                vec![Datum::Bytes(b"[1,2,3]".to_vec())],
                Some(Json::String(String::from("[1,2,3]"))),
            ),
            (
                true,
                vec![Datum::Bytes(b"[1,2,3]".to_vec())],
                Some(Json::Array(vec![Json::I64(1), Json::I64(2), Json::I64(3)])),
            ),
            (false, vec![Datum::Null], None),
            (true, vec![Datum::Null], None),
        ];
        for (by_parse, cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(types::STRING));
            let mut ex = fncall_expr(ScalarFuncSig::CastStringAsJson, &[col_expr]);
            if by_parse {
                let mut flag = ex.get_field_type().get_flag();
                flag |= types::PARSE_TO_JSON_FLAG as u32;
                ex.mut_field_type().set_flag(flag);
            }
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_time_as_json() {
        let mut cfg = EvalConfig::default();
        cfg.ignore_truncate = true;
        let mut ctx = EvalContext::new(Arc::new(cfg));
        let time_str = "2012-12-12 11:11:11";
        let date_str = "2012-12-12";
        let tz = FixedOffset::east(0);
        let time = Time::parse_utc_datetime(time_str, mysql::DEFAULT_FSP).unwrap();
        let time_stamp = {
            let t = time.to_packed_u64();
            Time::from_packed_u64(t, types::TIMESTAMP, mysql::DEFAULT_FSP, &tz).unwrap()
        };
        let date = {
            let mut t = time.clone();
            t.set_tp(types::DATE).unwrap();
            t
        };

        let cases = vec![
            (
                types::DATETIME,
                vec![Datum::Time(time)],
                Some(Json::String(format!("{}.000000", time_str))),
            ),
            (
                types::TIMESTAMP,
                vec![Datum::Time(time_stamp)],
                Some(Json::String(format!("{}.000000", time_str))),
            ),
            (
                types::DATE,
                vec![Datum::Time(date)],
                Some(Json::String(String::from(date_str))),
            ),
            (0, vec![Datum::Null], None),
        ];
        for (tp, cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(tp));
            let ex = fncall_expr(ScalarFuncSig::CastTimeAsJson, &[col_expr]);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_duration_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let dur_str = "11:12:08";
        let dur_str_expect = "11:12:08.000000";

        let cases = vec![
            (
                vec![Datum::Dur(Duration::parse(dur_str.as_bytes(), 0).unwrap())],
                Some(Json::String(String::from(dur_str_expect))),
            ),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(types::STRING));
            let ex = fncall_expr(ScalarFuncSig::CastDurationAsJson, &[col_expr]);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_cast_json_as_json() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::new(0, FLAG_IGNORE_TRUNCATE).unwrap()));
        let cases = vec![
            (
                vec![Datum::Json(Json::Boolean(true))],
                Some(Json::Boolean(true)),
            ),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, i32::from(types::STRING));
            let ex = fncall_expr(ScalarFuncSig::CastJsonAsJson, &[col_expr]);
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_json(&mut ctx, &cols).unwrap();
            if exp.is_none() {
                assert!(res.is_none());
                continue;
            }
            assert_eq!(res.unwrap().into_owned(), exp.unwrap());
        }
    }

    #[test]
    fn test_dec_as_int_with_overflow() {
        let cases = vec![
            (
                0,
                vec![
                    Datum::Dec(Decimal::from_f64(i64::MAX as f64 + 100.5).unwrap()),
                ],
                i64::MAX,
            ),
            (
                types::UNSIGNED_FLAG,
                vec![
                    Datum::Dec(Decimal::from_f64(u64::MAX as f64 + 100.5).unwrap()),
                ],
                u64::MAX as i64,
            ),
        ];
        for (flag, cols, exp) in cases {
            let mut col_expr = col_expr(0, i32::from(types::NEW_DECIMAL));
            let mut ex = fncall_expr(ScalarFuncSig::CastDecimalAsInt, &[col_expr]);
            ex.mut_field_type().set_flag(flag as u32);

            // test with overflow as warning
            let mut ctx = EvalContext::new(Arc::new(
                EvalConfig::new(0, expr::FLAG_OVERFLOW_AS_WARNING).unwrap(),
            ));
            let e = Expression::build(&mut ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(ctx.warnings.warning_cnt, 1);
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_TRUNCATE_WRONG_VALUE
            );

            // test overflow as error
            ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_int(&mut ctx, &cols);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_str_as_int() {
        let cases = vec![
            (
                0,
                vec![Datum::Bytes(b"18446744073709551615".to_vec())],
                u64::MAX as i64,
                1,
            ),
            (
                types::UNSIGNED_FLAG,
                vec![Datum::Bytes(b"18446744073709551615".to_vec())],
                u64::MAX as i64,
                0,
            ),
            (0, vec![Datum::Bytes(b"-1".to_vec())], -1, 1),
        ];

        for (flag, cols, exp, warnings_cnt) in cases {
            let mut col_expr = col_expr(0, i32::from(types::STRING));
            let mut ex = fncall_expr(ScalarFuncSig::CastStringAsInt, &[col_expr]);
            ex.mut_field_type().set_flag(flag as u32);

            let mut ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&mut ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(ctx.warnings.warning_cnt, warnings_cnt);
            if warnings_cnt > 0 {
                assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_UNKNOWN);
            }
        }

        let cases = vec![
            (
                vec![Datum::Bytes(b"-9223372036854775810".to_vec())],
                i64::MIN,
            ),
            (
                vec![Datum::Bytes(b"18446744073709551616".to_vec())],
                u64::MAX as i64,
            ),
        ];

        for (cols, exp) in cases {
            let mut col_expr = col_expr(0, i32::from(types::STRING));
            let ex = fncall_expr(ScalarFuncSig::CastStringAsInt, &[col_expr]);
            // test with overflow as warning && in select stmt
            let mut ctx = EvalContext::new(Arc::new(
                EvalConfig::new(
                    0,
                    expr::FLAG_OVERFLOW_AS_WARNING | expr::FLAG_IN_SELECT_STMT,
                ).unwrap(),
            ));
            let e = Expression::build(&mut ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(ctx.warnings.warning_cnt, 1);
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_TRUNCATE_WRONG_VALUE
            );

            // test overflow as error
            ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&mut ctx, ex).unwrap();
            let res = e.eval_int(&mut ctx, &cols);
            assert!(res.is_err());
        }
    }

    // This test should work when NumberToDuration ported from tidb.
    // #[test]
    // fn test_int_as_duration_with_overflow() {
    //     let cols = vec![Datum::I64(3020400)];

    //     let col_expr = col_expr(0, i32::from(types::LONG_LONG));
    //     let ex = fncall_expr(ScalarFuncSig::CastIntAsDuration, &[col_expr]);

    //     // test with overflow as warning
    //     let mut ctx = EvalContext::new(Arc::new(
    //         EvalConfig::new(0, expr::FLAG_OVERFLOW_AS_WARNING).unwrap(),
    //     ));
    //     let e = Expression::build(&mut ctx, ex.clone()).unwrap();
    //     let res = e.eval_duration(&mut ctx, &cols).unwrap();
    //     assert!(res.is_none());
    //     assert_eq!(ctx.warnings.warning_cnt, 1);
    //     assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_DATA_OUT_OF_RANGE);

    //     // test overflow as error
    //     ctx = EvalContext::new(Arc::new(EvalConfig::default()));
    //     let e = Expression::build(&mut ctx, ex).unwrap();
    //     let res = e.eval_duration(&mut ctx, &cols);
    //     assert!(res.is_err());
    // }
}
