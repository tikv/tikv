// Copyright 2018 PingCAP, Inc.
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

//! DO NOT MOVE THIS FILE. IT WILL BE PARSED BY `fuzz/cli.rs`. SEE `discover_fuzz_targets()`.

extern crate byteorder;
extern crate failure;
extern crate tikv;

mod util;

use self::util::ReadLiteralExt;
use failure::Error;
use std::io::Cursor;

#[inline(always)]
pub fn fuzz_codec_bytes(data: &[u8]) -> Result<(), Error> {
    let _ = tikv::util::codec::bytes::encode_bytes(data);
    let _ = tikv::util::codec::bytes::encode_bytes_desc(data);
    let _ = tikv::util::codec::bytes::encoded_bytes_len(data, true);
    let _ = tikv::util::codec::bytes::encoded_bytes_len(data, false);
    Ok(())
}

#[inline(always)]
pub fn fuzz_codec_number(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
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
        let _ = tikv::util::codec::number::decode_u64(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u64_desc(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u64_le(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_i64(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_i64_desc(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_i64_le(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_f64(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_f64_desc(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_f64_le(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u32(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u32_le(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_i32_le(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u16(&mut buf.as_slice());
        let _ = tikv::util::codec::number::decode_u16_le(&mut buf.as_slice());
    }
    Ok(())
}

trait ReadAsDecimalRoundMode: ReadLiteralExt {
    fn read_as_decimal_round_mode(
        &mut self,
    ) -> Result<::tikv::coprocessor::codec::mysql::decimal::RoundMode, Error> {
        Ok(match self.read_as_u8()? % 3 {
            0 => ::tikv::coprocessor::codec::mysql::decimal::RoundMode::HalfEven,
            1 => ::tikv::coprocessor::codec::mysql::decimal::RoundMode::Truncate,
            _ => ::tikv::coprocessor::codec::mysql::decimal::RoundMode::Ceiling,
        })
    }
}

impl<T: ReadLiteralExt> ReadAsDecimalRoundMode for T {}

#[inline(always)]
pub fn fuzz_coprocessor_codec_decimal(data: &[u8]) -> Result<(), Error> {
    use tikv::coprocessor::codec::mysql::decimal::Decimal;

    fn fuzz(lhs: &Decimal, rhs: &Decimal, cursor: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let _ = lhs.clone().abs();
        let _ = lhs.ceil();
        let _ = lhs.floor();
        let _ = lhs.prec_and_frac();

        let mode = cursor.read_as_decimal_round_mode()?;
        let frac = cursor.read_as_i8()?;
        let _ = lhs.clone().round(frac, mode.clone());

        let shift = cursor.read_as_u64()? as isize;
        let _ = lhs.clone().shift(shift);

        let _ = lhs.as_i64();
        let _ = lhs.as_u64();
        let _ = lhs.as_f64();
        let _ = lhs.is_zero();
        let _ = lhs.approximate_encoded_size();

        let _ = lhs > rhs;

        let _ = lhs + rhs;
        let _ = lhs - rhs;
        let _ = lhs * rhs;
        let _ = lhs.clone() / rhs.clone();
        let _ = lhs.clone() % rhs.clone();
        let _ = -lhs.clone();
        Ok(())
    }

    let mut cursor = Cursor::new(data);
    let decimal1 = Decimal::from_f64(cursor.read_as_f64()?)?;
    let decimal2 = Decimal::from_f64(cursor.read_as_f64()?)?;
    let _ = fuzz(&decimal1, &decimal2, &mut cursor);
    let _ = fuzz(&decimal2, &decimal1, &mut cursor);
    Ok(())
}

trait ReadAsTimeType: ReadLiteralExt {
    fn read_as_time_type(&mut self) -> Result<::tikv::coprocessor::codec::mysql::TimeType, Error> {
        Ok(match self.read_as_u8()? % 3 {
            0 => ::tikv::coprocessor::codec::mysql::TimeType::Date,
            1 => ::tikv::coprocessor::codec::mysql::TimeType::DateTime,
            _ => ::tikv::coprocessor::codec::mysql::TimeType::Timestamp,
        })
    }
}

impl<T: ReadLiteralExt> ReadAsTimeType for T {}

fn fuzz_time(
    t: ::tikv::coprocessor::codec::mysql::Time,
    mut cursor: Cursor<&[u8]>,
) -> Result<(), Error> {
    use tikv::coprocessor::codec::mysql::TimeEncoder;
    let _ = t.clone().set_time_type(cursor.read_as_time_type()?);
    let _ = t.is_zero();
    let _ = t.invalid_zero();
    let _ = t.to_decimal();
    let _ = t.to_f64();
    let _ = t.to_duration();
    let _ = t.to_packed_u64();
    let _ = t.clone().round_frac(cursor.read_as_i8()?);
    let _ = t.is_leap_year();
    let _ = t.last_day_of_month();
    let _ = t.to_string();
    let mut v = Vec::new();
    let _ = v.encode_time(&t);
    Ok(())
}

pub fn fuzz_coprocessor_codec_time_from_parse(data: &[u8]) -> Result<(), Error> {
    use std::io::Read;
    use tikv::coprocessor::codec::mysql::{Time, Tz};
    let mut cursor = Cursor::new(data);
    let tz = Tz::from_offset(cursor.read_as_i64()?).unwrap_or_else(Tz::utc);
    let fsp = cursor.read_as_i8()?;
    let mut buf: [u8; 32] = [b' '; 32];
    cursor.read_exact(&mut buf)?;
    let t = Time::parse_datetime(::std::str::from_utf8(&buf)?, fsp, tz)?;
    fuzz_time(t, cursor)
}

pub fn fuzz_coprocessor_codec_time_from_u64(data: &[u8]) -> Result<(), Error> {
    use tikv::coprocessor::codec::mysql::{Time, Tz};
    let mut cursor = Cursor::new(data);
    let u = cursor.read_as_u64()?;
    let time_type = cursor.read_as_time_type()?;
    let tz = Tz::from_offset(cursor.read_as_i64()?).unwrap_or_else(Tz::utc);
    let fsp = cursor.read_as_i8()?;
    let t = Time::from_packed_u64(u, time_type, fsp, tz)?;
    fuzz_time(t, cursor)
}
