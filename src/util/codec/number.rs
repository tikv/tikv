// Copyright 2016 PingCAP, Inc.
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

use byteorder::{BigEndian, ByteOrder, LittleEndian, WriteBytesExt};
use std::io::{self, ErrorKind, Write};
use std::mem;

use super::{BytesSlice, Error, Result};

const SIGN_MARK: u64 = 0x8000000000000000;
pub const MAX_VAR_I64_LEN: usize = 10;
pub const MAX_VAR_U64_LEN: usize = 10;
pub const U64_SIZE: usize = 8;
pub const I64_SIZE: usize = 8;
pub const F64_SIZE: usize = 8;

fn order_encode_i64(v: i64) -> u64 {
    v as u64 ^ SIGN_MARK
}

fn order_decode_i64(u: u64) -> i64 {
    (u ^ SIGN_MARK) as i64
}

fn order_encode_f64(v: f64) -> u64 {
    let u: u64 = unsafe { mem::transmute(v) };
    if v.is_sign_positive() {
        u | SIGN_MARK
    } else {
        !u
    }
}

fn order_decode_f64(u: u64) -> f64 {
    let u = if u & SIGN_MARK > 0 {
        u & (!SIGN_MARK)
    } else {
        !u
    };
    f64::from_bits(u)
}

pub trait NumberEncoder: Write {
    /// `encode_i64` writes the encoded value to buf.
    /// `encode_i64` guarantees that the encoded value is in ascending order for comparison.
    fn encode_i64(&mut self, v: i64) -> Result<()> {
        let u = order_encode_i64(v);
        self.encode_u64(u)
    }

    /// `encode_i64_desc` writes the encoded value to buf.
    /// `encode_i64_desc` guarantees that the encoded value is in descending order for comparison.
    fn encode_i64_desc(&mut self, v: i64) -> Result<()> {
        let u = order_encode_i64(v);
        self.encode_u64_desc(u)
    }

    /// `encode_u64` writes the encoded value to slice buf.
    /// `encode_u64` guarantees that the encoded value is in ascending order for comparison.
    fn encode_u64(&mut self, v: u64) -> Result<()> {
        self.write_u64::<BigEndian>(v).map_err(From::from)
    }

    /// `encode_u64_desc` writes the encoded value to slice buf.
    /// `encode_u64_desc` guarantees that the encoded value is in descending order for comparison.
    fn encode_u64_desc(&mut self, v: u64) -> Result<()> {
        self.write_u64::<BigEndian>(!v).map_err(From::from)
    }

    fn encode_u32(&mut self, v: u32) -> Result<()> {
        self.write_u32::<BigEndian>(v).map_err(From::from)
    }

    fn encode_u16(&mut self, v: u16) -> Result<()> {
        self.write_u16::<BigEndian>(v).map_err(From::from)
    }

    /// `encode_var_i64` writes the encoded value to slice buf.
    /// Note that the encoded result is not memcomparable.
    fn encode_var_i64(&mut self, v: i64) -> Result<()> {
        let mut vx = (v as u64) << 1;
        if v < 0 {
            vx = !vx;
        }
        self.encode_var_u64(vx)
    }

    /// `encode_var_u64` writes the encoded value to slice buf.
    /// Note that the encoded result is not memcomparable.
    fn encode_var_u64(&mut self, mut v: u64) -> Result<()> {
        while v >= 0x80 {
            self.write_u8(v as u8 | 0x80)?;
            v >>= 7;
        }
        self.write_u8(v as u8).map_err(From::from)
    }

    /// `encode_f64` writes the encoded value to slice buf.
    /// `encode_f64` guarantees that the encoded value is in ascending order for comparison.
    fn encode_f64(&mut self, f: f64) -> Result<()> {
        let u = order_encode_f64(f);
        self.encode_u64(u)
    }

    /// `encode_f64_desc` writes the encoded value to slice buf.
    /// `encode_f64_desc` guarantees that the encoded value is in descending order for comparison.
    fn encode_f64_desc(&mut self, f: f64) -> Result<()> {
        let u = order_encode_f64(f);
        self.encode_u64_desc(u)
    }

    fn encode_u16_le(&mut self, v: u16) -> Result<()> {
        self.write_u16::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_u32_le(&mut self, v: u32) -> Result<()> {
        self.write_u32::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_i32_le(&mut self, v: i32) -> Result<()> {
        self.write_i32::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_f64_le(&mut self, v: f64) -> Result<()> {
        self.write_f64::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_i64_le(&mut self, v: i64) -> Result<()> {
        self.write_i64::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_u64_le(&mut self, v: u64) -> Result<()> {
        self.write_u64::<LittleEndian>(v).map_err(From::from)
    }
}

impl<T: Write> NumberEncoder for T {}

#[inline]
fn read_num_bytes<T, F>(size: usize, data: &mut &[u8], f: F) -> Result<T>
where
    F: Fn(&[u8]) -> T,
{
    if data.len() >= size {
        let buf = &data[..size];
        *data = &data[size..];
        return Ok(f(buf));
    }
    Err(Error::unexpected_eof())
}

/// `decode_i64` decodes value encoded by `encode_i64` before.
#[inline]
pub fn decode_i64(data: &mut BytesSlice) -> Result<i64> {
    decode_u64(data).map(order_decode_i64)
}

/// `decode_i64_desc` decodes value encoded by `encode_i64_desc` before.
#[inline]
pub fn decode_i64_desc(data: &mut BytesSlice) -> Result<i64> {
    decode_u64_desc(data).map(order_decode_i64)
}

/// `decode_u64` decodes value encoded by `encode_u64` before.
#[inline]
pub fn decode_u64(data: &mut BytesSlice) -> Result<u64> {
    read_num_bytes(mem::size_of::<u64>(), data, BigEndian::read_u64)
}

/// `decode_u32` decodes value encoded by `encode_u32` before.
#[inline]
pub fn decode_u32(data: &mut BytesSlice) -> Result<u32> {
    read_num_bytes(mem::size_of::<u32>(), data, BigEndian::read_u32)
}

/// `decode_u16` decodes value encoded by `encode_u16` before.
#[inline]
pub fn decode_u16(data: &mut BytesSlice) -> Result<u16> {
    read_num_bytes(mem::size_of::<u16>(), data, BigEndian::read_u16)
}

/// `decode_u64_desc` decodes value encoded by `encode_u64_desc` before.
#[inline]
pub fn decode_u64_desc(data: &mut BytesSlice) -> Result<u64> {
    let v = decode_u64(data)?;
    Ok(!v)
}

/// `decode_var_i64` decodes value encoded by `encode_var_i64` before.
#[inline]
pub fn decode_var_i64(data: &mut BytesSlice) -> Result<i64> {
    let v = decode_var_u64(data)?;
    let vx = v >> 1;
    if v & 1 == 0 {
        Ok(vx as i64)
    } else {
        Ok(!vx as i64)
    }
}

/// `decode_var_u64` decodes value encoded by `encode_var_u64` before.
#[inline]
pub fn decode_var_u64(data: &mut BytesSlice) -> Result<u64> {
    if !data.is_empty() {
        // process with value < 127 independently at first
        // since it matches most of the cases.
        if data[0] < 0x80 {
            let res = u64::from(data[0]) & 0x7f;
            *data = unsafe { data.get_unchecked(1..) };
            return Ok(res);
        }

        // process with data's len >=10 or data ends with var u64
        if data.len() >= 10 || *data.last().unwrap() < 0x80 {
            let mut res = 0;
            for i in 0..9 {
                let b = unsafe { *data.get_unchecked(i) };
                res |= (u64::from(b) & 0x7f) << (i * 7);
                if b < 0x80 {
                    *data = unsafe { data.get_unchecked(i + 1..) };
                    return Ok(res);
                }
            }
            let b = unsafe { *data.get_unchecked(9) };
            if b <= 1 {
                res |= ((u64::from(b)) & 0x7f) << (9 * 7);
                *data = unsafe { data.get_unchecked(10..) };
                return Ok(res);
            }
            return Err(Error::Io(io::Error::new(
                ErrorKind::InvalidData,
                "overflow",
            )));
        }
    }

    // process data's len < 10 && data not end with var u64.
    let mut res = 0;
    for i in 0..data.len() {
        let b = data[i];
        res |= (u64::from(b) & 0x7f) << (i * 7);
        if b < 0x80 {
            *data = unsafe { data.get_unchecked(i + 1..) };
            return Ok(res);
        }
    }
    Err(Error::unexpected_eof())
}

/// `decode_f64` decodes value encoded by `encode_f64` before.
#[inline]
pub fn decode_f64(data: &mut BytesSlice) -> Result<f64> {
    decode_u64(data).map(order_decode_f64)
}

/// `decode_f64_desc` decodes value encoded by `encode_f64_desc` before.
#[inline]
pub fn decode_f64_desc(data: &mut BytesSlice) -> Result<f64> {
    decode_u64_desc(data).map(order_decode_f64)
}

/// `decode_u16_le` decodes value encoded by `encode_u16_le` before.
#[inline]
pub fn decode_u16_le(data: &mut BytesSlice) -> Result<u16> {
    read_num_bytes(mem::size_of::<u16>(), data, LittleEndian::read_u16)
}

/// `decode_u32_le` decodes value encoded by `encode_u32_le` before.
#[inline]
pub fn decode_u32_le(data: &mut BytesSlice) -> Result<u32> {
    read_num_bytes(mem::size_of::<u32>(), data, LittleEndian::read_u32)
}

/// `decode_i32_le` decodes value encoded by `encode_i32_le` before.
#[inline]
pub fn decode_i32_le(data: &mut BytesSlice) -> Result<i32> {
    read_num_bytes(mem::size_of::<i32>(), data, LittleEndian::read_i32)
}

/// `decode_f64_le` decodes value encoded by `encode_f64_le` before.
#[inline]
pub fn decode_f64_le(data: &mut BytesSlice) -> Result<f64> {
    read_num_bytes(mem::size_of::<f64>(), data, LittleEndian::read_f64)
}

/// `decode_i64_le` decodes value encoded by `encode_i64_le` before.
#[inline]
pub fn decode_i64_le(data: &mut BytesSlice) -> Result<i64> {
    let v = decode_u64_le(data)?;
    Ok(v as i64)
}

/// `decode_u64_le` decodes value encoded by `encode_u64_le` before.
#[inline]
pub fn decode_u64_le(data: &mut BytesSlice) -> Result<u64> {
    read_num_bytes(mem::size_of::<u64>(), data, LittleEndian::read_u64)
}

#[inline]
pub fn read_u8(data: &mut BytesSlice) -> Result<u8> {
    if !data.is_empty() {
        let v = data[0];
        *data = &data[1..];
        Ok(v)
    } else {
        Err(Error::unexpected_eof())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::Error;

    use protobuf::CodedOutputStream;
    use std::io::ErrorKind;
    use std::{f32, f64, i16, i32, i64, u16, u32, u64};

    const U16_TESTS: &[u16] = &[
        i16::MIN as u16,
        i16::MAX as u16,
        u16::MIN,
        u16::MAX,
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        1024,
    ];

    const U32_TESTS: &[u32] = &[
        i32::MIN as u32,
        i32::MAX as u32,
        u32::MIN,
        u32::MAX,
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        1024,
    ];

    const U64_TESTS: &[u64] = &[
        i64::MIN as u64,
        i64::MAX as u64,
        u64::MIN,
        u64::MAX,
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        1024,
    ];
    const I64_TESTS: &[i64] = &[
        i64::MIN,
        i64::MAX,
        u64::MIN as i64,
        u64::MAX as i64,
        -1,
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        1024,
        -1023,
    ];

    const F64_TESTS: &[f64] = &[
        -1.0,
        0.0,
        1.0,
        f64::MAX,
        f64::MIN,
        f32::MAX as f64,
        f32::MIN as f64,
        f64::MIN_POSITIVE,
        f32::MIN_POSITIVE as f64,
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];

    const I32_TESTS: &[i32] = &[
        i32::MIN,
        i32::MAX,
        0,
        1,
        2,
        10,
        20,
        63,
        64,
        65,
        127,
        128,
        129,
        255,
        256,
        257,
        -1024,
    ];

    // use macro to generate order tests for number codecs.
    macro_rules! test_order {
        ($arr:expr, $sorted:expr, $enc:ident, $dec:ident) => {
            let mut encoded: Vec<_> = $arr
                .iter()
                .map(|e| {
                    let mut buf = vec![];
                    buf.$enc(*e).unwrap();
                    buf
                })
                .collect();
            encoded.sort();
            let decoded: Vec<_> = encoded
                .iter()
                .map(|b| $dec(&mut b.as_slice()).unwrap())
                .collect();
            assert_eq!(decoded, $sorted);
        };
    }

    // use macro to generate serialization tests for number codecs.
    macro_rules! test_serialize {
        ($tag:ident, $enc:ident, $dec:ident, $cases:expr) => {
            #[test]
            fn $tag() {
                for &v in $cases {
                    let mut buf = vec![];
                    buf.$enc(v).unwrap();
                    assert!(buf.len() <= MAX_VAR_I64_LEN);
                    assert_eq!(v, $dec(&mut buf.as_slice()).unwrap());
                }
            }
        };
    }

    // use macro to generate serialization and order tests for number codecs.
    macro_rules! test_codec {
        ($enc:ident, $dec:ident, $compare:expr, $cases:expr) => {
            #[allow(unused_imports)]
            #[allow(float_cmp)]
            mod $enc {
                use super::{F64_TESTS, I64_TESTS, U16_TESTS, U32_TESTS, U64_TESTS};
                use util::codec::number::*;

                test_serialize!(serialize, $enc, $dec, $cases);

                #[test]
                fn test_order() {
                    let mut ordered_case = $cases.to_vec();
                    ordered_case.sort_by($compare);
                    test_order!($cases, ordered_case, $enc, $dec);
                }
            }
        };
    }

    test_codec!(encode_i64, decode_i64, |a, b| a.cmp(b), I64_TESTS);
    test_codec!(encode_u32, decode_u32, |a, b| a.cmp(b), U32_TESTS);
    test_codec!(encode_u16, decode_u16, |a, b| a.cmp(b), U16_TESTS);
    test_codec!(encode_i64_desc, decode_i64_desc, |a, b| b.cmp(a), I64_TESTS);
    test_codec!(encode_u64, decode_u64, |a, b| a.cmp(b), U64_TESTS);
    test_codec!(encode_u64_desc, decode_u64_desc, |a, b| b.cmp(a), U64_TESTS);
    test_codec!(
        encode_f64,
        decode_f64,
        |a, b| a.partial_cmp(b).unwrap(),
        F64_TESTS
    );
    test_codec!(
        encode_f64_desc,
        decode_f64_desc,
        |a, b| b.partial_cmp(a).unwrap(),
        F64_TESTS
    );

    test_serialize!(
        var_i64_little_endian_codec,
        encode_i64_le,
        decode_i64_le,
        I64_TESTS
    );

    test_serialize!(
        var_u64_little_endian_codec,
        encode_u64_le,
        decode_u64_le,
        U64_TESTS
    );

    test_serialize!(
        var_i32_little_endian_codec,
        encode_i32_le,
        decode_i32_le,
        I32_TESTS
    );

    test_serialize!(var_u16_codec, encode_u16_le, decode_u16_le, U16_TESTS);
    test_serialize!(var_u32_codec, encode_u32_le, decode_u32_le, U32_TESTS);

    test_serialize!(var_i64_codec, encode_var_i64, decode_var_i64, I64_TESTS);

    #[test]
    #[allow(float_cmp)]
    fn test_var_f64_le() {
        for &v in F64_TESTS {
            let mut buf = vec![];
            buf.encode_f64_le(v).unwrap();
            let value = decode_f64_le(&mut buf.as_slice()).unwrap();
            assert_eq!(v, value);
        }
    }

    #[test]
    fn test_var_u64_codec() {
        for &v in U64_TESTS {
            let mut buf = vec![];
            let mut p_buf = vec![];
            {
                let mut writer = CodedOutputStream::new(&mut p_buf);
                writer.write_uint64_no_tag(v).unwrap();
                writer.flush().unwrap();
            }
            buf.encode_var_u64(v).unwrap();
            assert!(buf.len() <= MAX_VAR_I64_LEN);
            assert_eq!(buf, p_buf);
            let decoded = decode_var_u64(&mut buf.as_slice()).unwrap();
            assert_eq!(v, decoded);
        }
    }

    // test if a `Result` is expected io error.
    macro_rules! check_error {
        ($e:expr, $k:expr) => {
            match $e {
                Err(Error::Io(e)) => assert_eq!(e.kind(), $k),
                o => panic!("expect {:?}, got {:?}", $k, o),
            }
        };
    }

    // generate bound check test for number codecs.
    macro_rules! test_eof {
        ($tag:ident, $enc:ident, $dec:ident, $case:expr) => {
            #[test]
            fn $tag() {
                let mut buf = vec![0; 7];
                check_error!(buf.as_mut_slice().$enc($case), ErrorKind::WriteZero);
                check_error!($dec(&mut buf.as_slice()), ErrorKind::UnexpectedEof);
            }
        };
    }

    test_eof!(i64_eof, encode_i64, decode_i64, 1);
    test_eof!(u64_eof, encode_u64, decode_u64, 1);
    test_eof!(f64_eof, encode_f64, decode_f64, 1.0);
    test_eof!(i64_desc_eof, encode_i64_desc, decode_i64_desc, 1);
    test_eof!(u64_desc_eof, encode_u64_desc, decode_u64_desc, 1);
    test_eof!(f64_desc_eof, encode_f64_desc, decode_f64_desc, 1.0);

    #[test]
    fn test_var_eof() {
        let mut buf = vec![0x80; 9];
        buf.push(0x2);
        check_error!(decode_var_u64(&mut buf.as_slice()), ErrorKind::InvalidData);
        check_error!(decode_var_i64(&mut buf.as_slice()), ErrorKind::InvalidData);

        buf = vec![0x80; 3];
        check_error!(
            decode_var_u64(&mut buf.as_slice()),
            ErrorKind::UnexpectedEof
        );

        buf.push(0);
        assert_eq!(0, decode_var_u64(&mut buf.as_slice()).unwrap());
    }
}
