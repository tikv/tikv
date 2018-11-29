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

extern crate byteorder;
extern crate failure;
extern crate tikv;

#[macro_use]
mod framework;
mod util;

use self::util::ReadLiteralExt;
use failure::Error;
use std::io::Cursor;

fn fuzz_codec_bytes_encode_bytes(data: &[u8]) -> Result<(), Error> {
    let _ = tikv::util::codec::bytes::encode_bytes(data);
    Ok(())
}

fn fuzz_codec_bytes_encode_bytes_desc(data: &[u8]) -> Result<(), Error> {
    let _ = tikv::util::codec::bytes::encode_bytes_desc(data);
    Ok(())
}

fn fuzz_codec_bytes_encoded_bytes_len(data: &[u8]) -> Result<(), Error> {
    let _ = tikv::util::codec::bytes::encoded_bytes_len(data, true);
    let _ = tikv::util::codec::bytes::encoded_bytes_len(data, false);
    Ok(())
}

fn fuzz_codec_number_u64_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_u64()?;
    let mut buf = vec![];
    let _ = buf.encode_u64(n);
    let _ = buf.encode_u64_le(n);
    let _ = buf.encode_u64_desc(n);
    let _ = buf.encode_var_u64(n);
    Ok(())
}

fn fuzz_codec_number_u64_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_u64(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_u64_desc(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_u64_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_codec_number_i64_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_i64()?;
    let mut buf = vec![];
    let _ = buf.encode_i64(n);
    let _ = buf.encode_i64_le(n);
    let _ = buf.encode_i64_desc(n);
    let _ = buf.encode_var_i64(n);
    Ok(())
}

fn fuzz_codec_number_i64_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_i64(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_i64_desc(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_i64_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_codec_number_f64_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_f64()?;
    let mut buf = vec![];
    let _ = buf.encode_f64(n);
    let _ = buf.encode_f64_le(n);
    let _ = buf.encode_f64_desc(n);
    Ok(())
}

fn fuzz_codec_number_f64_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_f64(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_f64_desc(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_f64_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_codec_number_u32_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_u32()?;
    let mut buf = vec![];
    let _ = buf.encode_u32(n);
    let _ = buf.encode_u32_le(n);
    Ok(())
}

fn fuzz_codec_number_u32_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_u32(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_u32_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_codec_number_i32_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_i32()?;
    let mut buf = vec![];
    let _ = buf.encode_i32_le(n);
    Ok(())
}

fn fuzz_codec_number_i32_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_i32_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_codec_number_u16_encode(data: &[u8]) -> Result<(), Error> {
    use tikv::util::codec::number::NumberEncoder;
    let mut cursor = Cursor::new(data);
    let n = cursor.read_as_u16()?;
    let mut buf = vec![];
    let _ = buf.encode_u16(n);
    let _ = buf.encode_u16_le(n);
    Ok(())
}

fn fuzz_codec_number_u16_decode(data: &[u8]) -> Result<(), Error> {
    let buf = data.to_owned();
    let _ = tikv::util::codec::number::decode_u16(&mut buf.as_slice());
    let _ = tikv::util::codec::number::decode_u16_le(&mut buf.as_slice());
    Ok(())
}

fn fuzz_coprocessor_codec_decimal(data: &[u8]) -> Result<(), Error> {
    use tikv::coprocessor::codec::mysql::decimal::{Decimal, RoundMode};

    fn fuzz(lhs: &Decimal, rhs: &Decimal, cursor: &mut Cursor<&[u8]>) -> Result<(), Error> {
        let _ = lhs.clone().abs();
        let _ = lhs.ceil();
        let _ = lhs.floor();
        let _ = lhs.prec_and_frac();

        let mode = match cursor.read_as_u8()? % 3 {
            0 => RoundMode::HalfEven,
            1 => RoundMode::Truncate,
            _ => RoundMode::Ceiling,
        };
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

register_fuzz!(
    fuzz_codec_bytes_encode_bytes,
    fuzz_codec_bytes_encode_bytes_desc,
    fuzz_codec_bytes_encoded_bytes_len,
    fuzz_codec_number_u64_encode,
    fuzz_codec_number_u64_decode,
    fuzz_codec_number_i64_encode,
    fuzz_codec_number_i64_decode,
    fuzz_codec_number_f64_encode,
    fuzz_codec_number_f64_decode,
    fuzz_codec_number_u32_encode,
    fuzz_codec_number_u32_decode,
    fuzz_codec_number_i32_encode,
    fuzz_codec_number_i32_decode,
    fuzz_codec_number_u16_encode,
    fuzz_codec_number_u16_decode,
    fuzz_coprocessor_codec_decimal,
);
