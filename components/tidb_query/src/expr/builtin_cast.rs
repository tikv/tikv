// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::convert::TryInto;
use std::{i64, str, u64};

use tidb_query_datatype::prelude::*;
use tidb_query_datatype::{self, FieldTypeFlag, FieldTypeTp};

use super::{Error, EvalContext, Result, ScalarFunc};
use crate::codec::convert::*;
use crate::codec::mysql::decimal::RoundMode;
use crate::codec::mysql::{charset, Decimal, Duration, Json, Res, Time, TimeType};
use crate::codec::{mysql, Datum};
use crate::expr::Flag;

// TODO: remove it after CAST function use `in_union` function
#[allow(dead_code)]

/// Indicates whether the current expression is evaluated in union statement
/// See: https://github.com/pingcap/tidb/blob/1e403873d905b2d0ad3be06bd8cd261203d84638/expression/builtin.go#L260
fn in_union(implicit_args: &[Datum]) -> bool {
    implicit_args.get(0) == Some(&Datum::I64(1))
}

impl ScalarFunc {
    pub fn cast_int_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        self.children[0].eval_int(ctx, row)
    }

    pub fn cast_real_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        if self
            .field_type
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED)
        {
            let uval = val.to_uint(ctx, FieldTypeTp::LongLong)?;
            Ok(Some(uval as i64))
        } else {
            let res = val.to_int(ctx, FieldTypeTp::LongLong)?;
            Ok(Some(res))
        }
    }

    pub fn cast_decimal_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let val = val.into_owned().round(0, RoundMode::HalfEven).unwrap();
        let (overflow, res) = if self
            .field_type
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED)
        {
            let uint = val.as_u64();
            (uint.is_overflow(), uint.unwrap() as i64)
        } else {
            let val = val.as_i64();
            (val.is_overflow(), val.unwrap())
        };

        if overflow {
            if !ctx.cfg.flag.contains(Flag::OVERFLOW_AS_WARNING) {
                return Err(Error::overflow("CastDecimalAsInt", &format!("{}", val)));
            }
            ctx.warnings
                .append_warning(Error::truncated_wrong_val("DECIMAL", &format!("{}", val)));
        }
        Ok(Some(res))
    }

    pub fn cast_str_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        if self.children[0].field_type().is_hybrid() {
            return self.children[0].eval_int(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let is_negative = match val.iter().skip_while(|x| x.is_ascii_whitespace()).next() {
            Some(&b'-') => true,
            _ => false,
        };
        let res = if is_negative {
            val.to_int(ctx, FieldTypeTp::LongLong).map(|v| {
                // TODO: handle inUion flag
                if self
                    .field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    ctx.warnings
                        .append_warning(Error::cast_neg_int_as_unsigned());
                }
                v
            })
        } else {
            val.to_uint(ctx, FieldTypeTp::LongLong).map(|urs| {
                if !self
                    .field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
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
            Err(e) => {
                if e.is_overflow() {
                    ctx.overflow_from_cast_str_as_int(&val, e, is_negative)
                        .map(Some)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn cast_time_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dec: Decimal = val.convert(ctx)?;
        let dec = dec
            .round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
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
        let dec: Decimal = val.convert(ctx)?;
        let dec = dec
            .round(mysql::DEFAULT_FSP as i8, RoundMode::HalfEven)
            .unwrap();
        let res = dec.as_i64().unwrap();
        Ok(Some(res))
    }

    pub fn cast_json_as_int(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let res = val.to_int(ctx, FieldTypeTp::LongLong)?;
        Ok(Some(res))
    }

    pub fn cast_int_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        if !self.children[0].is_unsigned() {
            Ok(Some(self.produce_float_with_specified_tp(ctx, val as f64)?))
        } else {
            let uval = val as u64;
            Ok(Some(
                self.produce_float_with_specified_tp(ctx, uval as f64)?,
            ))
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
        let res = val.convert(ctx)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_str_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        if self.children[0].field_type().is_hybrid() {
            return self.children[0].eval_real(ctx, row);
        }
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let res = val.convert(ctx)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_time_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let res = val.convert(ctx)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_duration_as_real(
        &self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let val: Decimal = val.convert(ctx)?;
        let res = val.convert(ctx)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, res)?))
    }

    pub fn cast_json_as_real(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<f64>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val = val.convert(ctx)?;
        Ok(Some(self.produce_float_with_specified_tp(ctx, val)?))
    }

    pub fn cast_int_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &[Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let field_type = self.children[0].field_type();
        let res = if !field_type
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::UNSIGNED)
        {
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
        let val: f64 = try_opt!(self.children[0].eval_real(ctx, row));
        let res: Decimal = val.convert(ctx)?;
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
        let dec = if self.children[0].field_type().is_hybrid() {
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
        let dec = val.convert(ctx)?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_duration_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let dec: Decimal = val.convert(ctx)?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_json_as_decimal<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Decimal>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let val: f64 = val.convert(ctx)?;
        let dec: Decimal = val.convert(ctx)?;
        self.produce_dec_with_specified_tp(ctx, Cow::Owned(dec))
            .map(Some)
    }

    pub fn cast_int_as_str<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = if self.children[0].is_unsigned() {
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
        Ok(Some(self.produce_time_with_str(ctx, &s)?))
    }

    pub fn cast_real_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        Ok(Some(self.produce_time_with_str(ctx, &s)?))
    }

    pub fn cast_decimal_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        Ok(Some(self.produce_time_with_float_str(ctx, &s)?))
    }

    pub fn cast_str_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        Ok(Some(self.produce_time_with_str(ctx, &val)?))
    }

    pub fn cast_time_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut val = val.into_owned();
        val.round_frac(self.field_type.decimal() as i8)?;
        // TODO: tidb only update tp when tp is Date
        val.set_time_type(self.field_type.as_accessor().tp().try_into()?)?;
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_duration_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let mut val = Time::from_duration(
            &ctx.cfg.tz,
            self.field_type.as_accessor().tp().try_into()?,
            val,
        )?;
        val.round_frac(self.field_type.decimal() as i8)?;
        Ok(Some(Cow::Owned(val)))
    }

    pub fn cast_json_as_time<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Time>>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.unquote()?;
        Ok(Some(self.produce_time_with_str(ctx, &s)?))
    }

    pub fn cast_int_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let s = format!("{}", val);
        // TODO: port NumberToDuration from tidb.
        match Duration::parse(s.as_bytes(), self.field_type.decimal() as i8) {
            Ok(dur) => Ok(Some(dur)),
            Err(e) => {
                if e.is_overflow() {
                    ctx.handle_overflow_err(e)?;
                    Ok(None)
                } else {
                    Err(e)
                }
            }
        }
    }

    pub fn cast_real_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_real(ctx, row));
        let s = format!("{}", val);
        let dur = Duration::parse(s.as_bytes(), self.field_type.decimal() as i8)?;
        Ok(Some(dur))
    }

    pub fn cast_decimal_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_decimal(ctx, row));
        let s = val.to_string();
        let dur = Duration::parse(s.as_bytes(), self.field_type.decimal() as i8)?;
        Ok(Some(dur))
    }

    pub fn cast_str_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_string(ctx, row));
        let dur = Duration::parse(val.as_ref(), self.field_type.decimal() as i8)?;
        Ok(Some(dur))
    }

    pub fn cast_time_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let dur: Duration = val.convert(ctx)?;
        let res = dur.round_frac(self.field_type.decimal() as i8)?;
        Ok(Some(res))
    }

    pub fn cast_duration_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_duration(ctx, row));
        let res = val.round_frac(self.field_type.decimal() as i8)?;
        Ok(Some(res))
    }

    pub fn cast_json_as_duration<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Duration>> {
        let val = try_opt!(self.children[0].eval_json(ctx, row));
        let s = val.unquote()?;
        // TODO: tidb would handle truncate here
        let d = Duration::parse(s.as_bytes(), self.field_type.decimal() as i8)?;
        Ok(Some(d))
    }

    pub fn cast_int_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_int(ctx, row));
        let flag = self.children[0].field_type().as_accessor().flag();
        let j = if flag.contains(FieldTypeFlag::IS_BOOLEAN) {
            Json::Boolean(val != 0)
        } else if flag.contains(FieldTypeFlag::UNSIGNED) {
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
        let val = val.convert(ctx)?;
        let j = Json::Double(val);
        Ok(Some(Cow::Owned(j)))
    }

    pub fn cast_str_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        if self
            .field_type
            .as_accessor()
            .flag()
            .contains(FieldTypeFlag::PARSE_TO_JSON)
        {
            let j: Json = val.parse()?;
            Ok(Some(Cow::Owned(j)))
        } else {
            Ok(Some(Cow::Owned(Json::String(val.into_owned()))))
        }
    }

    pub fn cast_time_as_json<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let val = try_opt!(self.children[0].eval_time(ctx, row));
        let mut val = val.into_owned();
        if val.get_time_type() == TimeType::DateTime || val.get_time_type() == TimeType::Timestamp {
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
        let mut val = try_opt!(self.children[0].eval_duration(ctx, row));
        val = val.maximize_fsp();
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
        let flen = self.field_type.as_accessor().flen();
        let decimal = self.field_type.as_accessor().decimal();
        if flen == tidb_query_datatype::UNSPECIFIED_LENGTH
            || decimal == tidb_query_datatype::UNSPECIFIED_LENGTH
        {
            return Ok(val);
        }
        let res = val
            .into_owned()
            .convert_to(ctx, flen as u8, decimal as u8)?;
        Ok(Cow::Owned(res))
    }

    /// `produce_str_with_specified_tp`(`ProduceStrWithSpecifiedTp` in tidb) produces
    /// a new string according to `flen` and `chs`.
    fn produce_str_with_specified_tp<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        s: Cow<'a, [u8]>,
    ) -> Result<Cow<'a, [u8]>> {
        let flen = self.field_type.flen();
        let chs = self.field_type.get_charset();
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
            truncate_binary(&mut res, truncate_pos as isize);
            return Ok(Cow::Owned(res));
        }

        if s.len() > flen {
            ctx.handle_truncate_err(Error::data_too_long(format!(
                "Data Too Long, field len {}, data len {}",
                flen,
                s.len()
            )))?;
            let mut res = s.into_owned();
            truncate_binary(&mut res, flen as isize);
            return Ok(Cow::Owned(res));
        }

        if self.field_type.as_accessor().tp() == FieldTypeTp::String && s.len() < flen {
            let mut s = s.into_owned();
            s.resize(flen, 0);
            return Ok(Cow::Owned(s));
        }
        Ok(s)
    }

    fn produce_time_with_str(&self, ctx: &mut EvalContext, s: &str) -> Result<Cow<'_, Time>> {
        let mut t = Time::parse_datetime(s, self.field_type.decimal() as i8, &ctx.cfg.tz)?;
        t.set_time_type(self.field_type.as_accessor().tp().try_into()?)?;
        Ok(Cow::Owned(t))
    }

    fn produce_time_with_float_str(&self, ctx: &mut EvalContext, s: &str) -> Result<Cow<'_, Time>> {
        let mut t = Time::parse_datetime_from_float_string(
            s,
            self.field_type.decimal() as i8,
            &ctx.cfg.tz,
        )?;
        t.set_time_type(self.field_type.as_accessor().tp().try_into()?)?;
        Ok(Cow::Owned(t))
    }

    /// `produce_float_with_specified_tp`(`ProduceFloatWithSpecifiedTp` in tidb) produces
    /// a new float64 according to `flen` and `decimal` in `self.tp`.
    /// TODO port tests from tidb(tidb haven't implemented now)
    fn produce_float_with_specified_tp(&self, ctx: &mut EvalContext, f: f64) -> Result<f64> {
        let flen = self.field_type.as_accessor().flen();
        let decimal = self.field_type.as_accessor().decimal();
        if flen == tidb_query_datatype::UNSPECIFIED_LENGTH
            || decimal == tidb_query_datatype::UNSPECIFIED_LENGTH
        {
            return Ok(f);
        }
        match truncate_f64(f, flen as u8, decimal as u8) {
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
mod tests {
    use std::str::FromStr;
    use std::sync::Arc;
    use std::{i64, u64};

    use tidb_query_datatype::{self, FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
    use tipb::{Expr, FieldType, ScalarFuncSig};

    use chrono::Utc;

    use crate::codec::error::*;
    use crate::codec::mysql::{self, charset, Decimal, Duration, Json, Time, TimeType, Tz};
    use crate::codec::Datum;
    use crate::expr::ctx::Flag;
    use crate::expr::tests::{col_expr as base_col_expr, scalar_func_expr};
    use crate::expr::{EvalConfig, EvalContext, Expression};

    pub fn col_expr(col_id: i64, tp: FieldTypeTp) -> Expr {
        let mut expr = base_col_expr(col_id);
        let mut fp = FieldType::default();
        fp.as_mut_accessor().set_tp(tp);
        if tp == FieldTypeTp::String {
            fp.set_charset(charset::CHARSET_UTF8.to_owned());
        }
        expr.set_field_type(fp);
        expr
    }

    #[test]
    fn test_cast_as_int() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        #[allow(clippy::inconsistent_digit_grouping)]
        let time_int = 2012_12_12_12_00_23i64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsInt,
                FieldTypeTp::LongLong,
                Some(FieldTypeFlag::UNSIGNED),
                vec![Datum::U64(1)],
                1,
            ),
            (
                ScalarFuncSig::CastIntAsInt,
                FieldTypeTp::LongLong,
                None,
                vec![Datum::I64(-1)],
                -1,
            ),
            (
                ScalarFuncSig::CastStringAsInt,
                FieldTypeTp::String,
                None,
                vec![Datum::Bytes(b"1".to_vec())],
                1,
            ),
            (
                ScalarFuncSig::CastRealAsInt,
                FieldTypeTp::Double,
                None,
                vec![Datum::F64(1f64)],
                1,
            ),
            (
                ScalarFuncSig::CastRealAsInt,
                FieldTypeTp::Double,
                None,
                vec![Datum::F64(1234.000)],
                1234,
            ),
            (
                ScalarFuncSig::CastTimeAsInt,
                FieldTypeTp::DateTime,
                None,
                vec![Datum::Time(t)],
                time_int,
            ),
            (
                ScalarFuncSig::CastDurationAsInt,
                FieldTypeTp::Duration,
                None,
                vec![Datum::Dur(duration_t)],
                120023,
            ),
            (
                ScalarFuncSig::CastJsonAsInt,
                FieldTypeTp::JSON,
                None,
                vec![Datum::Json(Json::I64(-1))],
                -1,
            ),
            (
                ScalarFuncSig::CastJsonAsInt,
                FieldTypeTp::JSON,
                None,
                vec![Datum::Json(Json::U64(1))],
                1,
            ),
            (
                ScalarFuncSig::CastDecimalAsInt,
                FieldTypeTp::NewDecimal,
                None,
                vec![Datum::Dec(Decimal::from(1))],
                1,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, flag, col, expect) in cases {
            let col_expr = col_expr(0, tp);
            let mut exp = scalar_func_expr(sig, &[col_expr]);
            if let Some(flag) = flag {
                exp.mut_field_type().as_mut_accessor().set_flag(flag);
            }
            let e = Expression::build(&ctx, exp).unwrap();
            let res = e.eval_int(&mut ctx, &col).unwrap();
            assert_eq!(res.unwrap(), expect);
            // test None
            let res = e.eval_int(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }

        let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
        let cases = vec![
            (
                ScalarFuncSig::CastDecimalAsInt,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(
                    Decimal::from_str("1111111111111111111111111").unwrap(),
                )],
                9223372036854775807,
            ),
            (
                ScalarFuncSig::CastDecimalAsInt,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(
                    Decimal::from_str("-1111111111111111111111111").unwrap(),
                )],
                -9223372036854775808,
            ),
        ];
        for (sig, tp, col, expect) in cases {
            let col_expr = col_expr(0, tp);
            let exp = scalar_func_expr(sig, &[col_expr]);
            let e = Expression::build(&ctx, exp).unwrap();
            let res = e.eval_int(&mut ctx, &col).unwrap();
            assert_eq!(res.unwrap(), expect);
        }
    }

    #[test]
    fn test_cast_as_real() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        #[allow(clippy::inconsistent_digit_grouping)]
        let int_t = 2012_12_12_12_00_23u64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsReal,
                FieldTypeTp::LongLong,
                vec![Datum::I64(1)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastIntAsReal,
                FieldTypeTp::LongLong,
                vec![Datum::I64(1234)],
                7,
                3,
                1234.000,
            ),
            (
                ScalarFuncSig::CastStringAsReal,
                FieldTypeTp::String,
                vec![Datum::Bytes(b"1".to_vec())],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastStringAsReal,
                FieldTypeTp::String,
                vec![Datum::Bytes(b"1234".to_vec())],
                7,
                3,
                1234.000,
            ),
            (
                ScalarFuncSig::CastRealAsReal,
                FieldTypeTp::Double,
                vec![Datum::F64(1f64)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastRealAsReal,
                FieldTypeTp::Double,
                vec![Datum::F64(1234.123)],
                8,
                4,
                1234.1230,
            ),
            (
                ScalarFuncSig::CastTimeAsReal,
                FieldTypeTp::DateTime,
                vec![Datum::Time(t.clone())],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                int_t as f64,
            ),
            (
                ScalarFuncSig::CastTimeAsReal,
                FieldTypeTp::DateTime,
                vec![Datum::Time(t)],
                15,
                1,
                format!("{}.0", int_t).parse::<f64>().unwrap(),
            ),
            (
                ScalarFuncSig::CastDurationAsReal,
                FieldTypeTp::Duration,
                vec![Datum::Dur(duration_t)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                120023f64,
            ),
            (
                ScalarFuncSig::CastDurationAsReal,
                FieldTypeTp::Duration,
                vec![Datum::Dur(duration_t)],
                7,
                1,
                120023.0,
            ),
            (
                ScalarFuncSig::CastJsonAsReal,
                FieldTypeTp::JSON,
                vec![Datum::Json(Json::I64(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastJsonAsReal,
                FieldTypeTp::JSON,
                vec![Datum::Json(Json::I64(1))],
                2,
                1,
                1.0,
            ),
            (
                ScalarFuncSig::CastDecimalAsReal,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(Decimal::from(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                1f64,
            ),
            (
                ScalarFuncSig::CastDecimalAsReal,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(Decimal::from(1))],
                2,
                1,
                1.0,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, flen, decimal, expect) in cases {
            let col_expr = col_expr(0, tp);
            let mut exp = scalar_func_expr(sig, &[col_expr]);
            exp.mut_field_type()
                .as_mut_accessor()
                .set_flen(flen)
                .set_decimal(decimal);
            let e = Expression::build(&ctx, exp).unwrap();
            let res = e.eval_real(&mut ctx, &col).unwrap();
            assert_eq!(format!("{}", res.unwrap()), format!("{}", expect));
            // test None
            let res = e.eval_real(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    fn f64_to_decimal(ctx: &mut EvalContext, f: f64) -> Result<Decimal> {
        use crate::codec::convert::ConvertTo;
        let val = f.convert(ctx)?;
        Ok(val)
    }

    #[test]
    fn test_cast_as_decimal() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let t = Time::parse_utc_datetime("2012-12-12 12:00:23", 0).unwrap();
        let int_t = 20121212120023u64;
        let duration_t = Duration::parse(b"12:00:23", 0).unwrap();
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsDecimal,
                FieldTypeTp::LongLong,
                vec![Datum::I64(1)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastIntAsDecimal,
                FieldTypeTp::LongLong,
                vec![Datum::I64(1234)],
                7,
                3,
                f64_to_decimal(&mut ctx, 1234.000).unwrap(),
            ),
            (
                ScalarFuncSig::CastStringAsDecimal,
                FieldTypeTp::String,
                vec![Datum::Bytes(b"1".to_vec())],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastStringAsDecimal,
                FieldTypeTp::String,
                vec![Datum::Bytes(b"1234".to_vec())],
                7,
                3,
                f64_to_decimal(&mut ctx, 1234.000).unwrap(),
            ),
            (
                ScalarFuncSig::CastRealAsDecimal,
                FieldTypeTp::Double,
                vec![Datum::F64(1f64)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastRealAsDecimal,
                FieldTypeTp::Double,
                vec![Datum::F64(1234.123)],
                8,
                4,
                f64_to_decimal(&mut ctx, 1234.1230).unwrap(),
            ),
            (
                ScalarFuncSig::CastTimeAsDecimal,
                FieldTypeTp::DateTime,
                vec![Datum::Time(t.clone())],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(int_t),
            ),
            (
                ScalarFuncSig::CastTimeAsDecimal,
                FieldTypeTp::DateTime,
                vec![Datum::Time(t)],
                15,
                1,
                format!("{}.0", int_t).parse::<Decimal>().unwrap(),
            ),
            (
                ScalarFuncSig::CastDurationAsDecimal,
                FieldTypeTp::Duration,
                vec![Datum::Dur(duration_t)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(120023),
            ),
            (
                ScalarFuncSig::CastDurationAsDecimal,
                FieldTypeTp::Duration,
                vec![Datum::Dur(duration_t)],
                7,
                1,
                f64_to_decimal(&mut ctx, 120023.0).unwrap(),
            ),
            (
                ScalarFuncSig::CastJsonAsDecimal,
                FieldTypeTp::JSON,
                vec![Datum::Json(Json::I64(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastJsonAsDecimal,
                FieldTypeTp::JSON,
                vec![Datum::Json(Json::I64(1))],
                2,
                1,
                f64_to_decimal(&mut ctx, 1.0).unwrap(),
            ),
            (
                ScalarFuncSig::CastDecimalAsDecimal,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(Decimal::from(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                Decimal::from(1),
            ),
            (
                ScalarFuncSig::CastDecimalAsDecimal,
                FieldTypeTp::NewDecimal,
                vec![Datum::Dec(Decimal::from(1))],
                2,
                1,
                f64_to_decimal(&mut ctx, 1.0).unwrap(),
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, flen, decimal, expect) in cases {
            let col_expr = col_expr(0, tp);
            let mut exp = scalar_func_expr(sig, &[col_expr]);
            exp.mut_field_type()
                .as_mut_accessor()
                .set_flen(flen)
                .set_decimal(decimal);
            let e = Expression::build(&ctx, exp).unwrap();
            let res = e.eval_decimal(&mut ctx, &col).unwrap();
            assert_eq!(res.unwrap().into_owned(), expect);
            // test None
            let res = e.eval_decimal(&mut ctx, &null_cols).unwrap();
            assert!(res.is_none());
        }
    }

    #[test]
    fn test_cast_as_str() {
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let t_str = "2012-12-12 12:00:23";
        let t = Time::parse_utc_datetime(t_str, 0).unwrap();
        let dur_str = b"12:00:23";
        let duration_t = Duration::parse(dur_str, 0).unwrap();
        let s = "您好world";
        let exp_s = "您好w";
        let cases = vec![
            (
                ScalarFuncSig::CastIntAsString,
                FieldTypeTp::LongLong,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::I64(1)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastIntAsString,
                FieldTypeTp::LongLong,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::I64(1234)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                FieldTypeTp::String,
                charset::CHARSET_ASCII,
                Some(FieldTypeTp::String),
                vec![Datum::Bytes(b"1234".to_vec())],
                6,
                b"1234\0\0".to_vec(),
            ),
            (
                ScalarFuncSig::CastStringAsString,
                FieldTypeTp::String,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Bytes(s.as_bytes().to_vec())],
                3,
                exp_s.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                FieldTypeTp::Double,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::F64(1f64)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastRealAsString,
                FieldTypeTp::Double,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::F64(1234.123)],
                3,
                b"123".to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                FieldTypeTp::DateTime,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Time(t.clone())],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                t_str.as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastTimeAsString,
                FieldTypeTp::DateTime,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Time(t)],
                3,
                t_str[0..3].as_bytes().to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                FieldTypeTp::Duration,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dur(duration_t)],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                dur_str.to_vec(),
            ),
            (
                ScalarFuncSig::CastDurationAsString,
                FieldTypeTp::Duration,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dur(duration_t)],
                3,
                dur_str[0..3].to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                FieldTypeTp::JSON,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Json(Json::I64(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastJsonAsString,
                FieldTypeTp::JSON,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Json(Json::I64(1234))],
                2,
                b"12".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                FieldTypeTp::NewDecimal,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dec(Decimal::from(1))],
                tidb_query_datatype::UNSPECIFIED_LENGTH,
                b"1".to_vec(),
            ),
            (
                ScalarFuncSig::CastDecimalAsString,
                FieldTypeTp::NewDecimal,
                charset::CHARSET_UTF8,
                None,
                vec![Datum::Dec(Decimal::from(1234))],
                2,
                b"12".to_vec(),
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, charset, to_tp, col, flen, exp) in cases {
            let col_expr = col_expr(0, tp);
            let mut ex = scalar_func_expr(sig, &[col_expr]);
            ex.mut_field_type()
                .as_mut_accessor()
                .set_flen(flen)
                .set_decimal(tidb_query_datatype::UNSPECIFIED_LENGTH);
            if let Some(to_tp) = to_tp {
                ex.mut_field_type().as_mut_accessor().set_tp(to_tp);
            }
            ex.mut_field_type().set_charset(String::from(charset));
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let today = Utc::now();
        let t_date_str = format!("{}", today.format("%Y-%m-%d"));
        let t_time_str = format!("{}", today.format("%Y-%m-%d %H:%M:%S"));
        let t_time = Time::parse_utc_datetime(t_time_str.as_ref(), 0).unwrap();
        let t_date = {
            let mut date = t_time.clone();
            date.set_time_type(TimeType::Date).unwrap();
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
        dur_to_date.set_time_type(TimeType::Date).unwrap();

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
                FieldTypeTp::LongLong,
                &int_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                // cast int as datetime(6)
                ScalarFuncSig::CastIntAsTime,
                FieldTypeTp::LongLong,
                &int_cols,
                mysql::MAX_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                ScalarFuncSig::CastStringAsTime,
                FieldTypeTp::String,
                &str_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                // cast string as datetime(6)
                ScalarFuncSig::CastStringAsTime,
                FieldTypeTp::String,
                &str_cols,
                mysql::MAX_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                ScalarFuncSig::CastRealAsTime,
                FieldTypeTp::Double,
                &f64_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                // cast real as date(0)
                ScalarFuncSig::CastRealAsTime,
                FieldTypeTp::Double,
                &f64_cols,
                mysql::DEFAULT_FSP,
                FieldTypeTp::Date,
                &t_date,
            ),
            (
                ScalarFuncSig::CastTimeAsTime,
                FieldTypeTp::DateTime,
                &time_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                // cast time as date
                ScalarFuncSig::CastTimeAsTime,
                FieldTypeTp::DateTime,
                &time_cols,
                mysql::DEFAULT_FSP,
                FieldTypeTp::Date,
                &t_date,
            ),
            (
                ScalarFuncSig::CastDurationAsTime,
                FieldTypeTp::Duration,
                &duration_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &dur_to_time,
            ),
            (
                // cast duration as date
                ScalarFuncSig::CastDurationAsTime,
                FieldTypeTp::Duration,
                &duration_cols,
                mysql::MAX_FSP,
                FieldTypeTp::Date,
                &dur_to_date,
            ),
            (
                ScalarFuncSig::CastJsonAsTime,
                FieldTypeTp::JSON,
                &json_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                ScalarFuncSig::CastJsonAsTime,
                FieldTypeTp::JSON,
                &json_cols,
                mysql::DEFAULT_FSP,
                FieldTypeTp::Date,
                &t_date,
            ),
            (
                ScalarFuncSig::CastDecimalAsTime,
                FieldTypeTp::NewDecimal,
                &dec_cols,
                mysql::UNSPECIFIED_FSP,
                FieldTypeTp::DateTime,
                &t_time,
            ),
            (
                // cast decimal as date
                ScalarFuncSig::CastDecimalAsTime,
                FieldTypeTp::NewDecimal,
                &dec_cols,
                mysql::DEFAULT_FSP,
                FieldTypeTp::Date,
                &t_date,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, to_fsp, to_tp, exp) in cases {
            let col_expr = col_expr(0, tp);
            let mut ex = scalar_func_expr(sig, &[col_expr]);
            ex.mut_field_type()
                .as_mut_accessor()
                .set_decimal(isize::from(to_fsp))
                .set_tp(to_tp);
            let e = Expression::build(&ctx, ex).unwrap();

            let res = e.eval_time(&mut ctx, col).unwrap();
            let data = res.unwrap().into_owned();
            let mut expt = exp.clone();
            if to_fsp != mysql::UNSPECIFIED_FSP {
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let today = Utc::now();
        let t_date_str = format!("{}", today.format("%Y-%m-%d"));

        let dur_str = "12:00:23";
        let dur_int = 120023u64;
        let duration = Duration::parse(dur_str.as_bytes(), 0).unwrap();
        let dur_to_time_str = format!("{} 12:00:23", t_date_str);
        let dur_to_time = Time::parse_utc_datetime(&dur_to_time_str, 0).unwrap();
        let mut dur_to_date = dur_to_time.clone();
        dur_to_date.set_time_type(TimeType::Date).unwrap();

        let json_cols = vec![Datum::Json(Json::String(String::from(dur_str)))];
        let int_cols = vec![Datum::U64(dur_int)];
        let str_cols = vec![Datum::Bytes(dur_str.as_bytes().to_vec())];
        let f64_cols = vec![Datum::F64(dur_int as f64)];
        let time_cols = vec![Datum::Time(dur_to_time)];
        let duration_cols = vec![Datum::Dur(duration)];
        let dec_cols = vec![Datum::Dec(Decimal::from(dur_int))];

        let cases = vec![
            (
                // cast int as duration
                ScalarFuncSig::CastIntAsDuration,
                FieldTypeTp::LongLong,
                &int_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast int as duration
                ScalarFuncSig::CastIntAsDuration,
                FieldTypeTp::LongLong,
                &int_cols,
                mysql::MAX_FSP,
                &duration,
            ),
            (
                // string as duration
                ScalarFuncSig::CastStringAsDuration,
                FieldTypeTp::String,
                &str_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast string as duration
                ScalarFuncSig::CastStringAsDuration,
                FieldTypeTp::String,
                &str_cols,
                4,
                &duration,
            ),
            (
                // cast real as duration
                ScalarFuncSig::CastRealAsDuration,
                FieldTypeTp::Double,
                &f64_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast real as duration
                ScalarFuncSig::CastRealAsDuration,
                FieldTypeTp::Double,
                &f64_cols,
                1,
                &duration,
            ),
            (
                // cast time as duration
                ScalarFuncSig::CastTimeAsDuration,
                FieldTypeTp::DateTime,
                &time_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast time as duration
                ScalarFuncSig::CastTimeAsDuration,
                FieldTypeTp::DateTime,
                &time_cols,
                5,
                &duration,
            ),
            (
                ScalarFuncSig::CastDurationAsDuration,
                FieldTypeTp::Duration,
                &duration_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast duration as duration
                ScalarFuncSig::CastDurationAsDuration,
                FieldTypeTp::Duration,
                &duration_cols,
                mysql::MAX_FSP,
                &duration,
            ),
            (
                // cast json as duration
                ScalarFuncSig::CastJsonAsDuration,
                FieldTypeTp::JSON,
                &json_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                ScalarFuncSig::CastJsonAsDuration,
                FieldTypeTp::JSON,
                &json_cols,
                5,
                &duration,
            ),
            (
                // cast decimal as duration
                ScalarFuncSig::CastDecimalAsDuration,
                FieldTypeTp::NewDecimal,
                &dec_cols,
                mysql::UNSPECIFIED_FSP,
                &duration,
            ),
            (
                // cast decimal as duration
                ScalarFuncSig::CastDecimalAsDuration,
                FieldTypeTp::NewDecimal,
                &dec_cols,
                2,
                &duration,
            ),
        ];

        let null_cols = vec![Datum::Null];
        for (sig, tp, col, to_fsp, exp) in cases {
            let col_expr = col_expr(0, tp);
            let mut ex = scalar_func_expr(sig, &[col_expr]);
            ex.mut_field_type()
                .as_mut_accessor()
                .set_decimal(isize::from(to_fsp));
            let e = Expression::build(&ctx, ex).unwrap();
            let res = e.eval_duration(&mut ctx, col).unwrap();
            let data = res.unwrap();
            let mut expt = *exp;
            if to_fsp != mysql::UNSPECIFIED_FSP {
                expt = expt.round_frac(to_fsp).expect("fail to round");
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec![
            (
                Some(FieldTypeFlag::UNSIGNED),
                vec![Datum::U64(32)],
                Some(Json::U64(32)),
            ),
            (
                Some(FieldTypeFlag::UNSIGNED | FieldTypeFlag::IS_BOOLEAN),
                vec![Datum::U64(1)],
                Some(Json::Boolean(true)),
            ),
            (
                Some(FieldTypeFlag::UNSIGNED | FieldTypeFlag::IS_BOOLEAN),
                vec![Datum::I64(0)],
                Some(Json::Boolean(false)),
            ),
            (None, vec![Datum::I64(-1)], Some(Json::I64(-1))),
            (None, vec![Datum::Null], None),
        ];
        for (flag, cols, exp) in cases {
            let mut col_expr = col_expr(0, FieldTypeTp::LongLong);
            if let Some(flag) = flag {
                col_expr.mut_field_type().as_mut_accessor().set_flag(flag);
            }
            let ex = scalar_func_expr(ScalarFuncSig::CastIntAsJson, &[col_expr]);
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec![
            (vec![Datum::F64(32.0001)], Some(Json::Double(32.0001))),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, FieldTypeTp::Double);
            let ex = scalar_func_expr(ScalarFuncSig::CastRealAsJson, &[col_expr]);
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec![
            (
                vec![Datum::Dec(f64_to_decimal(&mut ctx, 32.0001).unwrap())],
                Some(Json::Double(32.0001)),
            ),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, FieldTypeTp::NewDecimal);
            let ex = scalar_func_expr(ScalarFuncSig::CastDecimalAsJson, &[col_expr]);

            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
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
            let col_expr = col_expr(0, FieldTypeTp::String);
            let mut ex = scalar_func_expr(ScalarFuncSig::CastStringAsJson, &[col_expr]);
            if by_parse {
                let mut flag = ex.get_field_type().as_accessor().flag();
                flag |= FieldTypeFlag::PARSE_TO_JSON;
                ex.mut_field_type().as_mut_accessor().set_flag(flag);
            }
            let e = Expression::build(&ctx, ex).unwrap();
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
        let cfg = EvalConfig::default_for_test();
        let mut ctx = EvalContext::new(Arc::new(cfg));
        let time_str = "2012-12-12 11:11:11";
        let date_str = "2012-12-12";
        let tz = Tz::utc();
        let time = Time::parse_utc_datetime(time_str, mysql::DEFAULT_FSP).unwrap();
        let time_stamp = {
            let t = time.to_packed_u64();
            Time::from_packed_u64(t, TimeType::Timestamp, mysql::DEFAULT_FSP, &tz).unwrap()
        };
        let date = {
            let mut t = time.clone();
            t.set_time_type(TimeType::Date).unwrap();
            t
        };

        let cases = vec![
            (
                FieldTypeTp::DateTime,
                vec![Datum::Time(time)],
                Some(Json::String(format!("{}.000000", time_str))),
            ),
            (
                FieldTypeTp::Timestamp,
                vec![Datum::Time(time_stamp)],
                Some(Json::String(format!("{}.000000", time_str))),
            ),
            (
                FieldTypeTp::Date,
                vec![Datum::Time(date)],
                Some(Json::String(String::from(date_str))),
            ),
            (FieldTypeTp::Unspecified, vec![Datum::Null], None),
        ];
        for (tp, cols, exp) in cases {
            let col_expr = col_expr(0, tp);
            let ex = scalar_func_expr(ScalarFuncSig::CastTimeAsJson, &[col_expr]);
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
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
            let col_expr = col_expr(0, FieldTypeTp::String);
            let ex = scalar_func_expr(ScalarFuncSig::CastDurationAsJson, &[col_expr]);
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::new(Arc::new(EvalConfig::default_for_test()));
        let cases = vec![
            (
                vec![Datum::Json(Json::Boolean(true))],
                Some(Json::Boolean(true)),
            ),
            (vec![Datum::Null], None),
        ];
        for (cols, exp) in cases {
            let col_expr = col_expr(0, FieldTypeTp::String);
            let ex = scalar_func_expr(ScalarFuncSig::CastJsonAsJson, &[col_expr]);
            let e = Expression::build(&ctx, ex).unwrap();
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
        let mut ctx = EvalContext::default();
        let cases = vec![
            (
                FieldTypeFlag::empty(),
                vec![Datum::Dec(
                    f64_to_decimal(&mut ctx, i64::MAX as f64 + 100.5).unwrap(),
                )],
                i64::MAX,
            ),
            (
                FieldTypeFlag::UNSIGNED,
                vec![Datum::Dec(
                    f64_to_decimal(&mut ctx, u64::MAX as f64 + 100.5).unwrap(),
                )],
                u64::MAX as i64,
            ),
        ];
        for (flag, cols, exp) in cases {
            let col_expr = col_expr(0, FieldTypeTp::NewDecimal);
            let mut ex = scalar_func_expr(ScalarFuncSig::CastDecimalAsInt, &[col_expr]);
            ex.mut_field_type().as_mut_accessor().set_flag(flag);

            // test with overflow as warning
            let mut ctx =
                EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
            let e = Expression::build(&ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(ctx.warnings.warning_cnt, 1);
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_TRUNCATE_WRONG_VALUE
            );

            // test overflow as error
            ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&ctx, ex).unwrap();
            let res = e.eval_int(&mut ctx, &cols);
            assert!(res.is_err());
        }
    }

    #[test]
    fn test_str_as_int() {
        let cases = vec![
            (
                FieldTypeFlag::empty(),
                vec![Datum::Bytes(b"18446744073709551615".to_vec())],
                u64::MAX as i64,
                1,
            ),
            (
                FieldTypeFlag::UNSIGNED,
                vec![Datum::Bytes(b"18446744073709551615".to_vec())],
                u64::MAX as i64,
                0,
            ),
            (
                FieldTypeFlag::UNSIGNED,
                vec![Datum::Bytes(b"-1".to_vec())],
                -1,
                1,
            ),
        ];

        for (flag, cols, exp, warnings_cnt) in cases {
            let col_expr = col_expr(0, FieldTypeTp::String);
            let mut ex = scalar_func_expr(ScalarFuncSig::CastStringAsInt, &[col_expr]);
            ex.mut_field_type().as_mut_accessor().set_flag(flag);

            let mut ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(
                ctx.warnings.warning_cnt, warnings_cnt,
                "unexpected warning: {:?}",
                ctx.warnings.warnings
            );
            if warnings_cnt > 0 {
                assert_eq!(
                    ctx.warnings.warnings[0].get_code(),
                    ERR_UNKNOWN,
                    "unexpected warning: {:?}",
                    ctx.warnings.warnings
                );
            }
        }

        let cases = vec![
            (
                vec![Datum::Bytes(b"-9223372036854775810".to_vec())],
                i64::MIN,
                FieldTypeFlag::empty(),
            ),
            (
                vec![Datum::Bytes(b"18446744073709551616".to_vec())],
                u64::MAX as i64,
                FieldTypeFlag::UNSIGNED,
            ),
        ];

        for (cols, exp, flag) in cases {
            let col_expr = col_expr(0, FieldTypeTp::String);
            let mut ex = scalar_func_expr(ScalarFuncSig::CastStringAsInt, &[col_expr]);
            ex.mut_field_type().as_mut_accessor().set_flag(flag);

            // test with overflow as warning && in select stmt
            let mut cfg = EvalConfig::new();
            cfg.set_flag(Flag::OVERFLOW_AS_WARNING | Flag::IN_SELECT_STMT);
            let mut ctx = EvalContext::new(Arc::new(cfg));
            let e = Expression::build(&ctx, ex.clone()).unwrap();
            let res = e.eval_int(&mut ctx, &cols).unwrap().unwrap();
            assert_eq!(res, exp);
            assert_eq!(
                ctx.warnings.warning_cnt, 1,
                "unexpected warning: {:?}",
                ctx.warnings.warnings
            );
            assert_eq!(
                ctx.warnings.warnings[0].get_code(),
                ERR_DATA_OUT_OF_RANGE,
                "unexpected warning: {:?}",
                ctx.warnings.warnings
            );

            // test overflow as error
            ctx = EvalContext::new(Arc::new(EvalConfig::default()));
            let e = Expression::build(&ctx, ex).unwrap();
            let res = e.eval_int(&mut ctx, &cols);
            assert!(res.is_err());
        }
    }

    // This test should work when NumberToDuration ported from tidb.
    // #[test]
    // fn test_int_as_duration_with_overflow() {
    //     let cols = vec![Datum::I64(3020400)];

    //     let col_expr = col_expr(0, i32::from(FieldTypeTp::LongLong));
    //     let ex = scalar_func_expr(ScalarFuncSig::CastIntAsDuration, &[col_expr]);

    //     // test with overflow as warning
    //     let mut ctx = EvalContext::new(Arc::new(EvalConfig::from_flag(Flag::OVERFLOW_AS_WARNING)));
    //     let e = Expression::build(&ctx, ex.clone()).unwrap();
    //     let res = e.eval_duration(&mut ctx, &cols).unwrap();
    //     assert!(res.is_none());
    //     assert_eq!(ctx.warnings.warning_cnt, 1);
    //     assert_eq!(ctx.warnings.warnings[0].get_code(), ERR_DATA_OUT_OF_RANGE);

    //     // test overflow as error
    //     ctx = EvalContext::new(Arc::new(EvalConfig::default()));
    //     let e = Expression::build(&ctx, ex).unwrap();
    //     let res = e.eval_duration(&mut ctx, &cols);
    //     assert!(res.is_err());
    // }

    #[test]
    fn test_in_union() {
        use super::*;

        // empty implicit arguments
        assert!(!in_union(&[]));

        // single implicit arguments
        assert!(!in_union(&[Datum::I64(0)]));
        assert!(in_union(&[Datum::I64(1)]));

        // multiple implicit arguments
        assert!(!in_union(&[Datum::I64(0), Datum::I64(1)]));
        assert!(in_union(&[Datum::I64(1), Datum::I64(0)]));
    }
}
