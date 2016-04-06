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

use byteorder::{ByteOrder, BigEndian};
use super::{check_bound, Result, Error};

const SIGN_MARK: u64 = 0x8000000000000000;

fn order_encode(v: i64) -> u64 {
    v as u64 ^ SIGN_MARK
}

fn order_decode(u: u64) -> i64 {
    (u ^ SIGN_MARK) as i64
}

/// `encode_i64` writes the encoded value to buf.
/// `encode_i64` guarantees that the encoded value is in ascending order for comparison.
pub fn encode_i64(buf: &mut [u8], v: i64) -> Result<()> {
    try!(check_bound(buf, 8));
    let u = order_encode(v);
    encode_u64(buf, u)
}

/// `encode_i64_desc` writes the encoded value to buf.
/// `encode_i64_desc` guarantees that the encoded value is in descending order for comparison.
pub fn encode_i64_desc(buf: &mut [u8], v: i64) -> Result<()> {
    try!(check_bound(buf, 8));
    let u = order_encode(v);
    encode_u64_desc(buf, u)
}

/// `decode_i64` decodes value encoded by `encode_i64` before.
pub fn decode_i64(buf: &[u8]) -> Result<i64> {
    try!(check_bound(buf, 8));
    let u = try!(decode_u64(buf));
    Ok(order_decode(u))
}

/// `decode_i64_desc` decodes value encoded by `encode_i64_desc` before.
pub fn decode_i64_desc(buf: &[u8]) -> Result<i64> {
    try!(check_bound(buf, 8));
    let u = try!(decode_u64_desc(buf));
    Ok(order_decode(u))
}

/// `encode_u64` writes the encoded value to slice buf.
/// `encode_u64` guarantees that the encoded value is in ascending order for comparison.
pub fn encode_u64(buf: &mut [u8], v: u64) -> Result<()> {
    try!(check_bound(buf, 8));
    BigEndian::write_u64(buf, v);
    Ok(())
}

/// `encode_u64_desc` writes the encoded value to slice buf.
/// `encode_u64_desc` guarantees that the encoded value is in descending order for comparison.
pub fn encode_u64_desc(buf: &mut [u8], v: u64) -> Result<()> {
    try!(check_bound(buf, 8));
    BigEndian::write_u64(buf, !v);
    Ok(())
}

/// `decode_u64` decodes value encoded by `encode_u64` before.
pub fn decode_u64(buf: &[u8]) -> Result<u64> {
    try!(check_bound(buf, 8));
    let v = BigEndian::read_u64(buf);
    Ok(v)
}

/// `decode_u64_desc` decodes value encoded by `encode_u64_desc` before.
pub fn decode_u64_desc(buf: &[u8]) -> Result<u64> {
    try!(check_bound(buf, 8));
    let v = BigEndian::read_u64(buf);
    Ok(!v)
}

/// `encode_var_i64` writes the encoded value to slice buf.
/// Note that the encoded result is not memcomparable.
pub fn encode_var_i64(buf: &mut [u8], v: i64) -> usize {
    let mut vx = (v as u64) << 1;
    if v < 0 {
        vx = !vx;
    }
    encode_var_u64(buf, vx)
}

/// `decode_var_i64` decodes value encoded by `encode_var_i64` before.
pub fn decode_var_i64(buf: &[u8]) -> Result<(i64, usize)> {
    let (v, n) = try!(decode_var_u64(buf));
    let mut vx = v >> 1;
    if v & 1 != 0 {
        vx = !vx;
    }
    Ok((vx as i64, n))
}

/// `encode_var_u64` writes the encoded value to slice buf.
/// Note that the encoded result is not memcomparable.
pub fn encode_var_u64(buf: &mut [u8], mut v: u64) -> usize {
    let mut i = 0;
    while v >= 0x80 {
        buf[i] = v as u8 | 0x80;
        v >>= 7;
        i += 1;
    }
    buf[i] = v as u8;
    i + 1
}

/// `decode_var_u64` decodes value encoded by `encode_var_u64` before.
pub fn decode_var_u64(buf: &[u8]) -> Result<(u64, usize)> {
    let (mut x, mut s) = (0, 0);
    for (i, &b) in buf.iter().enumerate() {
        if b < 0x80 {
            if i > 9 || i == 9 && b > 1 {
                return Err(Error::OutOfBound(8, i));
            }
            return Ok((x | ((b as u64) << s), i + 1));
        }
        x |= ((b & 0x7f) as u64) << s;
        s += 7;
    }
    Err(Error::Eof)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{i64, u64};
    use util::codec::{Result, Error};
    use std::fmt::Debug;
    use protobuf::CodedOutputStream;

    type TestCodecPair<T> = (Box<Fn(&mut [u8], T) -> Result<()>>, Box<Fn(&[u8]) -> Result<T>>);

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

    fn test_order<T: Ord + Debug + Copy>(pair: &TestCodecPair<T>, arr: &[T], sorted: &[T]) {
        let mut buf = vec![0; 8];
        let mut encoded: Vec<Vec<u8>> = arr.iter()
                                           .map(|e| {
                                               pair.0(&mut buf, *e).unwrap();
                                               buf.to_vec()
                                           })
                                           .collect();
        encoded.sort();
        let decoded: Vec<T> = encoded.iter()
                                     .map(|b| pair.1(b).unwrap())
                                     .collect();
        assert_eq!(decoded, sorted);
    }

    #[test]
    fn test_u64_codec() {
        let test_func: Vec<TestCodecPair<u64>> = vec![
			(box encode_u64, box decode_u64),
			(box encode_u64_desc, box decode_u64_desc),
		];
        test_codec(U64_TESTS, test_func);
    }

    #[test]
    fn test_i64_codec() {
        let test_func: Vec<TestCodecPair<i64>> = vec![
			(box encode_i64, box decode_i64),
			(box encode_i64_desc, box decode_i64_desc),
		];
        test_codec(I64_TESTS, test_func);
    }

    fn test_codec<T: Debug + Copy + Ord>(tests: &[T], test_func: Vec<TestCodecPair<T>>) {
        let mut buf = vec![0; 8];
        for &(ref enc, ref dec) in &test_func {
            for &v in tests {
                enc(&mut buf, v).unwrap();
                assert_eq!(v, dec(&buf).unwrap());
            }
        }

        let mut ordered_case = tests.to_vec();
        ordered_case.sort();
        test_order(&test_func[0], tests, &ordered_case);

        ordered_case.reverse();
        test_order(&test_func[1], tests, &ordered_case);
    }

    #[test]
    fn test_var_i64_codec() {
        for &v in I64_TESTS {
            let mut buf = vec![0; 10];
            assert!(encode_var_i64(&mut buf, v) <= 10);
            assert_eq!(v, decode_var_i64(&buf).unwrap().0);
        }
    }

    #[test]
    fn test_var_u64_codec() {
        for &v in U64_TESTS {
            let mut buf = vec![0; 10];
            let mut p_buf = vec![];
            {
                let mut writer = CodedOutputStream::new(&mut p_buf);
                writer.write_uint64_no_tag(v).unwrap();
                writer.flush().unwrap();
            }
            let n = encode_var_u64(&mut buf, v);
            assert!(n <= 10);
            assert_eq!(buf[..n], *p_buf);
            assert_eq!(v, decode_var_u64(&buf).unwrap().0);
        }
    }

    fn test_encode_out_of_bound<T, B, R, F: Fn(B, T) -> Result<R>>(f: F, t: T, buf: B) {
        if let Err(Error::OutOfBound(_, _)) = f(buf, t) {
            return;
        }
        panic!("out_of_bound should be thrown!");
    }

    fn test_decode_out_of_bound<B, R, F: Fn(B) -> Result<R>>(f: F, buf: B) {
        if let Err(Error::OutOfBound(_, _)) = f(buf) {
            return;
        }
        panic!("out_of_bound should be thrown!");
    }

    #[test]
    fn test_bad_case() {
        let mut buf = vec![0; 7];
        test_encode_out_of_bound(encode_i64, 1, &mut buf);
        test_encode_out_of_bound(encode_u64, 1, &mut buf);
        test_encode_out_of_bound(encode_i64_desc, 1, &mut buf);
        test_encode_out_of_bound(encode_u64, 1, &mut buf);
        test_decode_out_of_bound(decode_i64, &buf);
        test_decode_out_of_bound(decode_u64, &buf);
        test_decode_out_of_bound(decode_i64_desc, &buf);
        test_decode_out_of_bound(decode_u64_desc, &buf);

        buf = vec![0x80; 9];
        buf.push(0x2);
        test_decode_out_of_bound(decode_var_u64, &buf);
        test_decode_out_of_bound(decode_var_i64, &buf);

        buf = vec![0x80; 3];
        match decode_var_u64(&buf) {
            Err(Error::Eof) => {}
            o => panic!("eof is epxpected, but we got: {:?}", o),
        };

        buf.push(0);
        assert_eq!(0, decode_var_u64(&buf).unwrap().0);
    }
}
