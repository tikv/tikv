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

use std::vec::Vec;
use std::io::Write;

use super::{Result, Error, number};
use util::codec;

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

// returns the maximum encoded bytes size.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
pub fn encode_bytes(key: &[u8]) -> Vec<u8> {
    let cap = max_encoded_bytes_size(key.len());
    let mut encoded = vec![0; cap];
    let len = encode_bytes_to_buf(&mut encoded, key).unwrap();
    encoded.truncate(len);
    encoded
}

pub fn encode_bytes_to_buf(mut buf: &mut [u8], key: &[u8]) -> Result<usize> {
    let len = key.len();
    let mut index = 0;
    let origin_offset = buf.as_ptr() as usize;
    while index <= len {
        let remain = len - index;
        let mut pad: usize = 0;
        if remain > ENC_GROUP_SIZE {
            try!(buf.write_all(&key[index..index + ENC_GROUP_SIZE]));
        } else {
            pad = ENC_GROUP_SIZE - remain;
            try!(buf.write_all(&key[index..]));
            try!(buf.write_all(&ENC_PADDING[..pad]));
        }
        try!(buf.write_all(&[ENC_MARKER - (pad as u8)]));
        index += ENC_GROUP_SIZE;
    }
    Ok(buf.as_ptr() as usize - origin_offset)
}

pub fn decode_bytes(data: &[u8]) -> Result<(Vec<u8>, usize)> {
    let mut key = Vec::<u8>::with_capacity(data.len());
    let mut read: usize = 0;
    for chunk in data.chunks(ENC_GROUP_SIZE + 1) {
        if chunk.len() != ENC_GROUP_SIZE + 1 {
            return Err(Error::KeyLength);
        }
        read += ENC_GROUP_SIZE + 1;

        let (marker, bytes) = chunk.split_last().unwrap();
        let pad_size = (ENC_MARKER - *marker) as usize;
        if pad_size == 0 {
            key.write(bytes).unwrap();
            continue;
        }
        if pad_size > ENC_GROUP_SIZE {
            return Err(Error::KeyPadding);
        }
        let (bytes, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
        key.write(bytes).unwrap();
        if padding.iter().any(|x| *x != 0) {
            return Err(Error::KeyPadding);
        }
        return Ok((key, read));
    }
    Err(Error::KeyLength)
}

/// `extract_encoded_bytes` extracts encoded bytes from the buffer, without decoding.
pub fn extract_encoded_bytes(data: &[u8]) -> Result<(&[u8], usize)> {
    let mut len: usize = 0;
    for chunk in data.chunks(ENC_GROUP_SIZE + 1) {
        if chunk.len() != ENC_GROUP_SIZE + 1 {
            return Err(Error::KeyLength);
        }
        len += ENC_GROUP_SIZE + 1;

        let (marker, bytes) = chunk.split_last().unwrap();
        let pad_size = (ENC_MARKER - *marker) as usize;
        if pad_size == 0 {
            continue;
        }
        if pad_size > ENC_GROUP_SIZE {
            return Err(Error::KeyPadding);
        }
        let (_, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
        if padding.iter().any(|x| *x != 0) {
            return Err(Error::KeyPadding);
        }
        return Ok((&data[0..len], len));
    }
    Err(Error::KeyLength)
}

/// `encode_compact_bytes` joins bytes with its length into a byte slice. It is more
/// efficient in both space and time compare to `encode_bytes`. Note that the encoded
/// result is not memcomparable.
pub fn encode_compact_bytes(buf: &mut [u8], data: &[u8]) -> Result<usize> {
    try!(codec::check_bound(buf, number::MAX_VAR_I64_LEN + data.len()));
    let vn = number::encode_var_i64(buf, data.len() as i64);
    try!((&mut buf[vn..]).write(data));
    Ok(vn + data.len())
}

/// `decode_compact_bytes` decodes bytes which is encoded by `encode_compact_bytes` before.
pub fn decode_compact_bytes(buf: &[u8]) -> Result<(Vec<u8>, usize)> {
    let (v, vn) = try!(number::decode_var_i64(buf));
    try!(codec::check_bound(&buf[vn..], v as usize));
    let readed = vn + v as usize;
    Ok((buf[vn..readed].to_vec(), readed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::codec::{number, bytes};
    use std::cmp::Ordering;

    #[test]
    fn test_enc_dec_bytes() {
        let pairs = vec![(vec![], vec![0, 0, 0, 0, 0, 0, 0, 0, 247]),
                         (vec![1, 2, 3], vec![1, 2, 3, 0, 0, 0, 0, 0, 250]),
                         (vec![0], vec![0, 0, 0, 0, 0, 0, 0, 0, 248]),
                         (vec![1, 2, 3], vec![1, 2, 3, 0, 0, 0, 0, 0, 250]),
                         (vec![1, 2, 3, 0], vec![1, 2, 3, 0, 0, 0, 0, 0, 251]),
                         (vec![1, 2, 3, 4, 5, 6, 7], vec![1, 2, 3, 4, 5, 6, 7, 0, 254]),

                         (vec![0, 0, 0, 0, 0, 0, 0, 0],
                          vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]),

                         (vec![1, 2, 3, 4, 5, 6, 7, 8],
                          vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247]),

                         (vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                          vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248])];

        for (x, y) in pairs {
            assert_eq!(encode_bytes(&x), y);
            let (key, size) = decode_bytes(&y).unwrap();
            assert_eq!(key, x);
            assert_eq!(size, y.len());
        }
    }

    #[test]
    fn test_dec_bytes_fail() {
        let invalid_bytes = vec![vec![1, 2, 3, 4],
                                 vec![0, 0, 0, 0, 0, 0, 0, 247],
                                 vec![0, 0, 0, 0, 0, 0, 0, 0, 246],
                                 vec![0, 0, 0, 0, 0, 0, 0, 1, 247],
                                 vec![1, 2, 3, 4, 5, 6, 7, 8, 0],
                                 vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1],
                                 vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8],
                                 vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255],
                                 vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0]];

        for x in invalid_bytes {
            assert!(decode_bytes(&x).is_err());
        }
    }

    #[test]
    fn test_encode_bytes_compare() {
        let pairs: Vec<(&[u8], &[u8], _)> = vec![(b"", b"\x00", Ordering::Less),
                                                 (b"\x00", b"\x00", Ordering::Equal),
                                                 (b"\xFF", b"\x00", Ordering::Greater),
                                                 (b"\xFF", b"\xFF\x00", Ordering::Less),
                                                 (b"a", b"b", Ordering::Less),
                                                 (b"a", b"\x00", Ordering::Greater),
                                                 (b"\x00", b"\x01", Ordering::Less),
                                                 (b"\x00\x01", b"\x00\x00", Ordering::Greater),
                                                 (b"\x00\x00\x00", b"\x00\x00", Ordering::Greater),
                                                 (b"\x00\x00\x00", b"\x00\x00", Ordering::Greater),

                                                 (b"\x00\x00\x00\x00\x00\x00\x00\x00",
                                                  b"\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                                                  Ordering::Less),

                                                 (b"\x01\x02\x03\x00",
                                                  b"\x01\x02\x03",
                                                  Ordering::Greater),
                                                 (b"\x01\x03\x03\x04",
                                                  b"\x01\x03\x03\x05",
                                                  Ordering::Less),

                                                 (b"\x01\x02\x03\x04\x05\x06\x07",
                                                  b"\x01\x02\x03\x04\x05\x06\x07\x08",
                                                  Ordering::Less),

                                                 (b"\x01\x02\x03\x04\x05\x06\x07\x08\x09",
                                                  b"\x01\x02\x03\x04\x05\x06\x07\x08",
                                                  Ordering::Greater),

                                                 (b"\x01\x02\x03\x04\x05\x06\x07\x08\x00",
                                                  b"\x01\x02\x03\x04\x05\x06\x07\x08",
                                                  Ordering::Greater)];

        for (x, y, ord) in pairs {
            assert_eq!(encode_bytes(x).cmp(&encode_bytes(y)), ord);
        }
    }

    #[test]
    fn test_max_encoded_bytes_size() {
        let n = bytes::ENC_GROUP_SIZE;
        let tbl: Vec<(usize, usize)> = vec![(0, n + 1), (n / 2, n + 1), (n, 2 * (n + 1))];
        for (x, y) in tbl {
            assert_eq!(max_encoded_bytes_size(x), y);
        }
    }

    #[test]
    fn test_compact_codec() {
        let tests = vec!["", "hello", "世界"];
        for &s in &tests {
            let max_size = s.len() + number::MAX_VAR_I64_LEN;
            let mut buf = vec![0; max_size];
            let written = encode_compact_bytes(&mut buf, s.as_bytes()).unwrap();
            assert!(written <= max_size);
            let (decoded, readed) = decode_compact_bytes(&buf).unwrap();
            assert_eq!(readed, written);
            assert_eq!(decoded, s.as_bytes());
        }
    }

    use test::Bencher;

    #[bench]
    fn bench_encode(b: &mut Bencher) {
        let key = [b'x'; 20];
        b.iter(|| encode_bytes(&key));
    }

    #[bench]
    fn bench_decode(b: &mut Bencher) {
        let key = [b'x'; 20];
        let encoded = encode_bytes(&key);
        b.iter(|| decode_bytes(&encoded));
    }
}
