// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

//! DO NOT MOVE THIS FILE. IT WILL BE PARSED BY `fuzz/cli.rs`. SEE `discover_fuzz_targets()`.

mod util;

use std::io::Cursor;

use anyhow::Result;
use tidb_query_datatype::{
    codec::datum_codec::DatumFlagAndPayloadEncoder,
    expr::{EvalConfig, EvalContext},
};

use self::util::ReadLiteralExt;

#[inline(always)]
pub fn fuzz_codec_bytes(data: &[u8]) -> Result<()> {
    let _ = tikv_util::codec::bytes::encode_bytes(data);
    let _ = tikv_util::codec::bytes::encode_bytes_desc(data);
    let _ = tikv_util::codec::bytes::encoded_bytes_len(data, true);
    let _ = tikv_util::codec::bytes::encoded_bytes_len(data, false);
    Ok(())
}

#[inline(always)]
pub fn fuzz_codec_number(data: &[u8]) -> Result<()> {
    use tikv_util::codec::number::NumberEncoder;
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u64()?;
        let mut buf = vec![];
        let _ = buf.encode_u64(n);
        let _ = buf.encode_u64_le(n);
        let _ = buf.encode_u64_desc(n);
        let _ = buf.encode_var_u64(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_i64()?;
        let mut buf = vec![];
        let _ = buf.encode_i64(n);
        let _ = buf.encode_i64_le(n);
        let _ = buf.encode_i64_desc(n);
        let _ = buf.encode_var_i64(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_f64()?;
        let mut buf = vec![];
        let _ = buf.encode_f64(n);
        let _ = buf.encode_f64_le(n);
        let _ = buf.encode_f64_desc(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u32()?;
        let mut buf = vec![];
        let _ = buf.encode_u32(n);
        let _ = buf.encode_u32_le(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_i32()?;
        let mut buf = vec![];
        let _ = buf.encode_i32_le(n);
    }
    {
        let mut cursor = Cursor::new(data);
        let n = cursor.read_as_u16()?;
        let mut buf = vec![];
        let _ = buf.encode_u16(n);
        let _ = buf.encode_u16_le(n);
    }
    {
        let buf = data.to_owned();
        let _ = tikv_util::codec::number::decode_u64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64_desc(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_f64_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u32(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u32_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_i32_le(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u16(&mut buf.as_slice());
        let _ = tikv_util::codec::number::decode_u16_le(&mut buf.as_slice());
    }
    Ok(())
}

trait ReadAsDecimalRoundMode: ReadLiteralExt {
    fn read_as_decimal_round_mode(
        &mut self,
    ) -> Result<::tidb_query_datatype::codec::mysql::decimal::RoundMode> {
        Ok(match self.read_as_u8()? % 3 {
            0 => tidb_query_datatype::codec::mysql::decimal::RoundMode::HalfEven,
            1 => tidb_query_datatype::codec::mysql::decimal::RoundMode::Truncate,
            _ => tidb_query_datatype::codec::mysql::decimal::RoundMode::Ceiling,
        })
    }
}

impl<T: ReadLiteralExt> ReadAsDecimalRoundMode for T {}

#[inline(always)]
pub fn fuzz_coprocessor_codec_decimal(data: &[u8]) -> Result<()> {
    use tidb_query_datatype::codec::{convert::ConvertTo, data_type::Decimal};

    fn fuzz(lhs: &Decimal, rhs: &Decimal, cursor: &mut Cursor<&[u8]>) -> Result<()> {
        let _ = lhs.abs();
        let _ = lhs.ceil();
        let _ = lhs.floor();
        let _ = lhs.prec_and_frac();

        let mode = cursor.read_as_decimal_round_mode()?;
        let frac = cursor.read_as_i8()?;
        let _ = lhs.round(frac, mode);

        let shift = cursor.read_as_u64()? as isize;
        let _ = lhs.shift(shift);

        let _ = lhs.as_i64();
        let _ = lhs.as_u64();
        let _ = lhs.is_zero();
        let _ = lhs.approximate_encoded_size();

        let _ = lhs > rhs;

        let _ = lhs + rhs;
        let _ = lhs - rhs;
        let _ = lhs * rhs;
        let _ = lhs / rhs;
        let _ = *lhs % *rhs;
        let _ = -*lhs;
        Ok(())
    }

    let mut cursor = Cursor::new(data);
    let mut ctx = EvalContext::default();
    let decimal1: Decimal = cursor.read_as_f64()?.convert(&mut ctx)?;
    let decimal2: Decimal = cursor.read_as_f64()?.convert(&mut ctx)?;
    let _ = fuzz(&decimal1, &decimal2, &mut cursor);
    let _ = fuzz(&decimal2, &decimal1, &mut cursor);
    Ok(())
}

#[inline(always)]
pub fn fuzz_hash_decimal(data: &[u8]) -> Result<()> {
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    use tidb_query_datatype::codec::{data_type::Decimal, mysql::DecimalDecoder};

    fn fuzz_eq_then_hash(lhs: &Decimal, rhs: &Decimal) -> Result<()> {
        if lhs == rhs {
            let mut lhasher = DefaultHasher::new();
            lhs.hash(&mut lhasher);
            let mut rhasher = DefaultHasher::new();
            rhs.hash(&mut rhasher);
            if lhasher.finish() == rhasher.finish() {
                Ok(())
            } else {
                panic!("eq but not hash eq");
            }
        } else {
            Ok(())
        }
    }
    let mut cursor = Cursor::new(data);
    let decimal1 = cursor.read_decimal()?;
    let decimal2 = cursor.read_decimal()?;

    fuzz_eq_then_hash(&decimal1, &decimal2)
}

trait ReadAsTimeType: ReadLiteralExt {
    fn read_as_time_type(&mut self) -> Result<::tidb_query_datatype::codec::mysql::TimeType> {
        Ok(match self.read_as_u8()? % 3 {
            0 => tidb_query_datatype::codec::mysql::TimeType::Date,
            1 => tidb_query_datatype::codec::mysql::TimeType::DateTime,
            _ => tidb_query_datatype::codec::mysql::TimeType::Timestamp,
        })
    }
}

impl<T: ReadLiteralExt> ReadAsTimeType for T {}

fn fuzz_time(t: tidb_query_datatype::codec::mysql::Time, mut cursor: Cursor<&[u8]>) -> Result<()> {
    use tidb_query_datatype::codec::{
        convert::ConvertTo,
        data_type::{Decimal, Duration},
        mysql::TimeEncoder,
    };

    let mut ctx = EvalContext::default();
    let _ = t.clone().set_time_type(cursor.read_as_time_type()?);
    let _ = t.is_zero();
    let _ = t.invalid_zero();
    let _ = t.to_packed_u64(&mut ctx);
    let _ = t.round_frac(&mut ctx, cursor.read_as_i8()?);
    let _ = t.is_leap_year();
    let _ = t.last_day_of_month();
    let _ = t.to_string();
    let mut v = Vec::new();
    let _ = v.write_time(t);

    let _: i64 = t.convert(&mut ctx)?;
    let _: u64 = t.convert(&mut ctx)?;
    let _: f64 = t.convert(&mut ctx)?;
    let _: Vec<u8> = t.convert(&mut ctx)?;
    let _: Decimal = t.convert(&mut ctx)?;
    let _: Duration = t.convert(&mut ctx)?;
    Ok(())
}

pub fn fuzz_coprocessor_codec_time_from_parse(data: &[u8]) -> Result<()> {
    use std::io::Read;

    use tidb_query_datatype::codec::mysql::{Time, Tz};

    let mut cursor = Cursor::new(data);
    let tz = Tz::from_offset(cursor.read_as_i64()?).unwrap_or_else(Tz::utc);
    let mut ctx = EvalContext::new(std::sync::Arc::new(EvalConfig {
        tz,
        ..EvalConfig::default()
    }));
    let fsp = cursor.read_as_i8()?;
    let mut buf: [u8; 32] = [b' '; 32];
    cursor.read_exact(&mut buf)?;
    let t = Time::parse_datetime(&mut ctx, ::std::str::from_utf8(&buf)?, fsp, false)?;
    fuzz_time(t, cursor)
}

pub fn fuzz_coprocessor_codec_time_from_u64(data: &[u8]) -> Result<()> {
    use tidb_query_datatype::codec::mysql::{Time, Tz};

    let mut cursor = Cursor::new(data);
    let u = cursor.read_as_u64()?;
    let time_type = cursor.read_as_time_type()?;
    let tz = Tz::from_offset(cursor.read_as_i64()?).unwrap_or_else(Tz::utc);
    let mut ctx = EvalContext::new(std::sync::Arc::new(EvalConfig {
        tz,
        ..EvalConfig::default()
    }));
    let fsp = cursor.read_as_i8()?;
    let t = Time::from_packed_u64(&mut ctx, u, time_type, fsp)?;
    fuzz_time(t, cursor)
}

// Duration
fn fuzz_duration(
    t: tidb_query_datatype::codec::mysql::Duration,
    mut cursor: Cursor<&[u8]>,
) -> Result<()> {
    use tidb_query_datatype::codec::{convert::ConvertTo, mysql::decimal::Decimal};

    let _ = t.fsp();
    let u = t;
    u.round_frac(cursor.read_as_i8()?)?;
    let _ = t.hours();
    let _ = t.minutes();
    let _ = t.secs();
    let _ = t.subsec_micros();
    let _ = t.to_secs_f64();
    let _ = t.is_zero();

    let u = t;
    u.round_frac(cursor.read_as_i8()?)?;
    let mut v = Vec::new();
    let _ = v.write_datum_duration_int(t);

    let mut ctx = EvalContext::default();
    let _: Decimal = t.convert(&mut ctx)?;

    Ok(())
}

pub fn fuzz_coprocessor_codec_duration_from_nanos(data: &[u8]) -> Result<()> {
    use tidb_query_datatype::codec::mysql::Duration;

    let mut cursor = Cursor::new(data);
    let nanos = cursor.read_as_i64()?;
    let fsp = cursor.read_as_i8()?;
    fuzz_duration(Duration::from_nanos(nanos, fsp)?, cursor)
}

pub fn fuzz_coprocessor_codec_duration_from_parse(data: &[u8]) -> Result<()> {
    use std::io::Read;

    use tidb_query_datatype::codec::mysql::Duration;

    let mut cursor = Cursor::new(data);
    let fsp = cursor.read_as_i8()?;
    let mut buf: [u8; 32] = [b' '; 32];
    cursor.read_exact(&mut buf)?;
    let d = Duration::parse(
        &mut EvalContext::default(),
        &std::str::from_utf8(&buf)?,
        fsp,
    )?;
    fuzz_duration(d, cursor)
}

pub fn fuzz_coprocessor_codec_row_v2_binary_search(data: &[u8]) -> Result<()> {
    use tidb_query_datatype::codec::row::v2::RowSlice;

    let mut cursor = Cursor::new(data);
    let id = cursor.read_as_i64()?;
    let first_byte = cursor.read_as_u8()?;

    if first_byte == 128 {
        let row_slice = RowSlice::from_bytes(&data[8..])?;
        let _ = row_slice.search_in_non_null_ids(id);
        let _ = row_slice.search_in_null_ids(id);
    }

    Ok(())
}
