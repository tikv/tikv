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

use super::{Result, Error};

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
    let mut encoded = Vec::<u8>::with_capacity(cap);
    let len = key.len();
    let mut index = 0;
    while index <= len {
        let remain = len - index;
        let mut pad: usize = 0;
        if remain > ENC_GROUP_SIZE {
            encoded.write(&key[index..index + ENC_GROUP_SIZE]).unwrap();
        } else {
            pad = ENC_GROUP_SIZE - remain;
            encoded.write(&key[index..]).unwrap();
            encoded.write(&ENC_PADDING[..pad]).unwrap();
        }
        encoded.push(ENC_MARKER - (pad as u8));
        index += ENC_GROUP_SIZE;
    }
    encoded
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

#[cfg(test)]
mod tests {
    use super::{encode_bytes, decode_bytes, max_encoded_bytes_size, ENC_GROUP_SIZE};
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
        let n = ENC_GROUP_SIZE;
        let tbl: Vec<(usize, usize)> = vec![(0, n + 1), (n / 2, n + 1), (n, 2 * (n + 1))];
        for (x, y) in tbl {
            assert_eq!(max_encoded_bytes_size(x), y);
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
