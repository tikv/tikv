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
use super::{check_bound, Result};

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

// `decode_u64` decodes value encoded by `encode_u64` before.
// It returns the leftover un-decoded slice, decoded value if no error.
pub fn decode_u64(buf: &[u8]) -> Result<u64> {
    try!(check_bound(buf, 8));
    let v = BigEndian::read_u64(buf);
    Ok(v)
}

// `decode_u64_desc` decodes value encoded by `encode_u64_desc` before.
// It returns the leftover un-decoded slice, decoded value if no error.
pub fn decode_u64_desc(buf: &[u8]) -> Result<u64> {
    try!(check_bound(buf, 8));
    let v = BigEndian::read_u64(buf);
    Ok(!v)
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{i64, u64};
    use util::codec::Result;
    use std::fmt::Debug;

    type TestCodecPair<T> = (Box<Fn(&mut [u8], T) -> Result<()>>, Box<Fn(&[u8]) -> Result<T>>);

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
        let test_values = vec![u64::MAX, u64::MIN, 2, 3, 0, 4, 4, 1024];
        let test_func: Vec<TestCodecPair<u64>> = vec![
			(box encode_u64, box decode_u64),
			(box encode_u64_desc, box decode_u64_desc),
		];
        test_codec(test_values, test_func);
    }

    #[test]
    fn test_i64_codec() {
        let test_values = vec![i64::MAX, i64::MIN, -2, -3, 0, 0, 4, 1024];
        let test_func: Vec<TestCodecPair<i64>> = vec![
			(box encode_i64, box decode_i64),
			(box encode_i64_desc, box decode_i64_desc),
		];
        test_codec(test_values, test_func);
    }

    fn test_codec<T: Debug + Copy + Ord>(test_values: Vec<T>, test_func: Vec<TestCodecPair<T>>) {
        let mut buf = vec![0; 8];
        for &(ref enc, ref dec) in &test_func {
            for &v in &test_values {
                enc(&mut buf, v).unwrap();
                assert_eq!(v, dec(&buf).unwrap());
            }
        }

        let mut ordered_case = test_values.clone();
        ordered_case.sort();
        test_order(&test_func[0], &test_values, &ordered_case);

        ordered_case.reverse();
        test_order(&test_func[1], &test_values, &ordered_case);
    }
}
