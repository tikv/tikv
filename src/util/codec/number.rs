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

use byteorder::{BigEndian, LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{self, ErrorKind, Write, Read};
use std::mem;

use super::{Result, Error};

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
    unsafe { mem::transmute(u) }
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
            try!(self.write_u8(v as u8 | 0x80));
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

    fn encode_f64_le(&mut self, v: f64) -> Result<()> {
        self.write_f64::<LittleEndian>(v).map_err(From::from)
    }

    fn encode_i64_le(&mut self, v: i64) -> Result<()> {
        self.write_i64::<LittleEndian>(v).map_err(From::from)
    }
}

impl<T: Write> NumberEncoder for T {}

pub trait NumberDecoder: Read {
    /// `decode_i64` decodes value encoded by `encode_i64` before.
    fn decode_i64(&mut self) -> Result<i64> {
        self.decode_u64().map(order_decode_i64)
    }

    /// `decode_i64_desc` decodes value encoded by `encode_i64_desc` before.
    fn decode_i64_desc(&mut self) -> Result<i64> {
        self.decode_u64_desc().map(order_decode_i64)
    }

    /// `decode_u64` decodes value encoded by `encode_u64` before.
    fn decode_u64(&mut self) -> Result<u64> {
        self.read_u64::<BigEndian>().map_err(From::from)
    }

    /// `decode_u64_desc` decodes value encoded by `encode_u64_desc` before.
    fn decode_u64_desc(&mut self) -> Result<u64> {
        let v = try!(self.read_u64::<BigEndian>());
        Ok(!v)
    }

    /// `decode_var_i64` decodes value encoded by `encode_var_i64` before.
    fn decode_var_i64(&mut self) -> Result<i64> {
        let v = try!(self.decode_var_u64());
        let mut vx = v >> 1;
        if v & 1 != 0 {
            vx = !vx;
        }
        Ok(vx as i64)
    }

    /// `decode_var_u64` decodes value encoded by `encode_var_u64` before.
    fn decode_var_u64(&mut self) -> Result<u64> {
        let (mut x, mut s, mut i) = (0, 0, 0);
        loop {
            let b = try!(self.read_u8());
            if b < 0x80 {
                if i > 9 || i == 9 && b > 1 {
                    return Err(Error::Io(io::Error::new(ErrorKind::InvalidData, "overflow")));
                }
                return Ok(x | ((b as u64) << s));
            }
            x |= ((b & 0x7f) as u64) << s;
            s += 7;
            i += 1;
        }
    }

    /// `decode_f64` decodes value encoded by `encode_f64` before.
    fn decode_f64(&mut self) -> Result<f64> {
        self.decode_u64().map(order_decode_f64)
    }

    /// `decode_f64_desc` decodes value encoded by `encode_f64_desc` before.
    fn decode_f64_desc(&mut self) -> Result<f64> {
        self.decode_u64_desc().map(order_decode_f64)
    }

    fn decode_u16_le(&mut self) -> Result<u16> {
        self.read_u16::<LittleEndian>().map_err(From::from)
    }

    fn decode_u32_le(&mut self) -> Result<u32> {
        self.read_u32::<LittleEndian>().map_err(From::from)
    }

    fn decode_f64_le(&mut self) -> Result<f64> {
        self.read_f64::<LittleEndian>().map_err(From::from)
    }

    fn decode_i64_le(&mut self) -> Result<i64> {
        self.read_i64::<LittleEndian>().map_err(From::from)
    }
}

impl<T: Read> NumberDecoder for T {}

#[cfg(test)]
mod test {
    use super::*;
    use util::codec::Error;

    use std::{i64, u64, i32, u32, i16, u16, f64, f32};
    use protobuf::CodedOutputStream;
    use std::io::ErrorKind;

    const U16_TESTS: &'static [u16] = &[i16::MIN as u16,
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
                                        1024];

    const U32_TESTS: &'static [u32] = &[i32::MIN as u32,
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
                                        1024];

    const U64_TESTS: &'static [u64] = &[i64::MIN as u64,
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
                                        1024];
    const I64_TESTS: &'static [i64] = &[i64::MIN,
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
                                        -1023];

    const F64_TESTS: &'static [f64] = &[-1.0,
                                        0.0,
                                        1.0,
                                        f64::MAX,
                                        f64::MIN,
                                        f32::MAX as f64,
                                        f32::MIN as f64,
                                        f64::MIN_POSITIVE,
                                        f32::MIN_POSITIVE as f64,
                                        f64::INFINITY,
                                        f64::NEG_INFINITY];

    // use macro to generate order tests for number codecs.
    macro_rules! test_order {
        ($arr:expr, $sorted:expr, $enc:ident, $dec:ident) => {
            let mut encoded: Vec<_> = $arr.iter().map(|e| {
                let mut buf = vec![];
                buf.$enc(*e).unwrap();
                buf
            }).collect();
            encoded.sort();
            let decoded: Vec<_> = encoded.iter().map(|b| b.as_slice().$dec().unwrap()).collect();
            assert_eq!(decoded, $sorted);
        }
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
                    assert_eq!(v, buf.as_slice().$dec().unwrap());
                }
            }
        }
    }

    // use macro to generate serialization and order tests for number codecs.
    macro_rules! test_codec {
        ($enc:ident, $dec:ident, $compare:expr, $cases:expr) => {
            #[allow(unused_imports)]
            #[allow(float_cmp)]
            mod $enc {
                use super::{I64_TESTS, U64_TESTS, U32_TESTS,U16_TESTS,F64_TESTS};
                use util::codec::number::*;

                test_serialize!(serialize, $enc, $dec, $cases);

                #[test]
                fn test_order() {
                    let mut ordered_case = $cases.to_vec();
                    ordered_case.sort_by($compare);
                    test_order!($cases, ordered_case, $enc, $dec);
                }
            }
        }
    }

    test_codec!(encode_i64, decode_i64, |a, b| a.cmp(b), I64_TESTS);
    test_codec!(encode_i64_desc, decode_i64_desc, |a, b| b.cmp(a), I64_TESTS);
    test_codec!(encode_u64, decode_u64, |a, b| a.cmp(b), U64_TESTS);
    test_codec!(encode_u64_desc, decode_u64_desc, |a, b| b.cmp(a), U64_TESTS);
    test_codec!(encode_f64,
                decode_f64,
                |a, b| a.partial_cmp(b).unwrap(),
                F64_TESTS);
    test_codec!(encode_f64_desc,
                decode_f64_desc,
                |a, b| b.partial_cmp(a).unwrap(),
                F64_TESTS);

    test_serialize!(var_i64_little_endian_codec,
                    encode_i64_le,
                    decode_i64_le,
                    I64_TESTS);

    test_serialize!(var_u16_codec, encode_u16_le, decode_u16_le, U16_TESTS);
    test_serialize!(var_u32_codec, encode_u32_le, decode_u32_le, U32_TESTS);

    test_serialize!(var_i64_codec, encode_var_i64, decode_var_i64, I64_TESTS);

    #[test]
    #[allow(float_cmp)]
    fn test_var_f64_le() {
        for &v in F64_TESTS {
            let mut buf = vec![];
            buf.encode_f64_le(v).unwrap();
            let value = buf.as_slice().decode_f64_le().unwrap();
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
            let decoded = buf.as_slice().decode_var_u64().unwrap();
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
        }
    }

    // generate bound check test for number codecs.
    macro_rules! test_eof {
        ($tag:ident, $enc:ident, $dec:ident, $case:expr) => {
            #[test]
            fn $tag() {
                let mut buf = vec![0; 7];
                check_error!(buf.as_mut_slice().$enc($case), ErrorKind::WriteZero);
                check_error!(buf.as_slice().$dec(), ErrorKind::UnexpectedEof);
            }
        }
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
        check_error!(buf.as_slice().decode_var_u64(), ErrorKind::InvalidData);
        check_error!(buf.as_slice().decode_var_i64(), ErrorKind::InvalidData);

        buf = vec![0x80; 3];
        check_error!(buf.as_slice().decode_var_u64(), ErrorKind::UnexpectedEof);

        buf.push(0);
        assert_eq!(0, buf.as_slice().decode_var_u64().unwrap());
    }
}
