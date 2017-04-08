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
use std::io::{Read, Write};

use super::{Result, Error};
use util::codec::number::{NumberEncoder, NumberDecoder};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];

// returns the maximum encoded bytes size.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

pub trait BytesEncoder: NumberEncoder {
    /// Refer: https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format
    fn encode_bytes(&mut self, key: &[u8], desc: bool) -> Result<()> {
        let len = key.len();
        let mut index = 0;
        let mut buf = [0; ENC_GROUP_SIZE];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                try!(self.write_all(adjust_bytes_order(&key[index..index + ENC_GROUP_SIZE],
                                                       desc,
                                                       &mut buf)));
            } else {
                pad = ENC_GROUP_SIZE - remain;
                try!(self.write_all(adjust_bytes_order(&key[index..], desc, &mut buf)));
                try!(self.write_all(adjust_bytes_order(&ENC_PADDING[..pad], desc, &mut buf)));
            }
            try!(self.write_all(adjust_bytes_order(&[ENC_MARKER - (pad as u8)], desc, &mut buf)));
            index += ENC_GROUP_SIZE;
        }
        Ok(())
    }

    /// `encode_compact_bytes` joins bytes with its length into a byte slice. It is more
    /// efficient in both space and time compare to `encode_bytes`. Note that the encoded
    /// result is not memcomparable.
    fn encode_compact_bytes(&mut self, data: &[u8]) -> Result<()> {
        try!(self.encode_var_i64(data.len() as i64));
        self.write_all(data).map_err(From::from)
    }
}

fn adjust_bytes_order<'a>(bs: &'a [u8], desc: bool, buf: &'a mut [u8]) -> &'a [u8] {
    if desc {
        let mut buf_idx = 0;
        for &b in bs {
            buf[buf_idx] = !b;
            buf_idx += 1;
        }
        &buf[..buf_idx]
    } else {
        bs
    }
}

impl<T: Write> BytesEncoder for T {}

pub fn encode_bytes(bs: &[u8]) -> Vec<u8> {
    encode_order_bytes(bs, false)
}

pub fn encode_bytes_desc(bs: &[u8]) -> Vec<u8> {
    encode_order_bytes(bs, true)
}

fn encode_order_bytes(bs: &[u8], desc: bool) -> Vec<u8> {
    let cap = max_encoded_bytes_size(bs.len());
    let mut encoded = Vec::with_capacity(cap);
    encoded.encode_bytes(bs, desc).unwrap();
    encoded.shrink_to_fit();
    encoded
}

/// Get the first compactly encoded bytes's length in encoded.
///
/// Please note that, this function won't check the whether the bytes
/// is encoded correctly.
pub fn encoded_compact_len(mut encoded: &[u8]) -> usize {
    let last_encoded = encoded.as_ptr() as usize;
    let total_len = encoded.len();
    let vn = match encoded.decode_var_i64() {
        Ok(vn) => vn as usize,
        Err(e) => {
            debug!("failed to decode bytes' length: {:?}", e);
            return total_len;
        }
    };
    vn + (encoded.as_ptr() as usize - last_encoded)
}

pub trait CompactBytesDecoder: NumberDecoder {
    /// `decode_compact_bytes` decodes bytes which is encoded by `encode_compact_bytes` before.
    fn decode_compact_bytes(&mut self) -> Result<Vec<u8>> {
        let vn = try!(self.decode_var_i64()) as usize;
        let mut data = vec![0; vn];
        try!(self.read_exact(&mut data));
        Ok(data)
    }
}

impl<T: Read> CompactBytesDecoder for T {}

/// Get the first encoded bytes's length in encoded.
///
/// Please note that, this function won't check the whether the bytes
/// is encoded correctly.
pub fn encoded_bytes_len(encoded: &[u8], desc: bool) -> usize {
    let mut idx = ENC_GROUP_SIZE;
    loop {
        if encoded.len() < idx + 1 {
            return encoded.len();
        }
        let marker = encoded[idx];
        if desc && marker != 0 || !desc && marker != ENC_MARKER {
            return idx + 1;
        }
        idx += ENC_GROUP_SIZE + 1;
    }
}

pub trait BytesDecoder: NumberDecoder + CompactBytesDecoder {
    /// Get the remaining length in bytes of current reader.
    fn remaining(&self) -> usize;

    fn peak_u8(&self) -> Option<u8>;

    fn decode_bytes(&mut self, desc: bool) -> Result<Vec<u8>> {
        let mut key = Vec::with_capacity(self.remaining());
        let mut chunk = [0; ENC_GROUP_SIZE + 1];
        loop {
            try!(self.read_exact(&mut chunk));
            let (&marker, bytes) = chunk.split_last().unwrap();
            let pad_size = if desc {
                marker as usize
            } else {
                (ENC_MARKER - marker) as usize
            };
            if pad_size == 0 {
                key.write_all(bytes).unwrap();
                continue;
            }
            if pad_size > ENC_GROUP_SIZE {
                return Err(Error::KeyPadding);
            }
            let (bytes, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
            key.write_all(bytes).unwrap();
            let pad_byte = if desc { !0 } else { 0 };
            if padding.iter().any(|x| *x != pad_byte) {
                return Err(Error::KeyPadding);
            }
            key.shrink_to_fit();
            if desc {
                for k in &mut key {
                    *k = !*k;
                }
            }
            return Ok(key);
        }
    }
}

impl<'a> BytesDecoder for &'a [u8] {
    fn remaining(&self) -> usize {
        self.len()
    }

    fn peak_u8(&self) -> Option<u8> {
        if self.is_empty() { None } else { Some(self[0]) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use util::codec::{number, bytes};
    use std::cmp::Ordering;

    #[test]
    fn test_enc_dec_bytes() {
        let pairs = vec![(vec![],
                          vec![0, 0, 0, 0, 0, 0, 0, 0, 247],
                          vec![255, 255, 255, 255, 255, 255, 255, 255, 8]),
                         (vec![0],
                          vec![0, 0, 0, 0, 0, 0, 0, 0, 248],
                          vec![255, 255, 255, 255, 255, 255, 255, 255, 7]),
                         (vec![1, 2, 3],
                          vec![1, 2, 3, 0, 0, 0, 0, 0, 250],
                          vec![254, 253, 252, 255, 255, 255, 255, 255, 5]),
                         (vec![1, 2, 3, 0],
                          vec![1, 2, 3, 0, 0, 0, 0, 0, 251],
                          vec![254, 253, 252, 255, 255, 255, 255, 255, 4]),
                         (vec![1, 2, 3, 4, 5, 6, 7],
                          vec![1, 2, 3, 4, 5, 6, 7, 0, 254],
                          vec![254, 253, 252, 251, 250, 249, 248, 255, 1]),
                         (vec![0, 0, 0, 0, 0, 0, 0, 0],
                          vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                          vec![255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255,
                               255, 255, 255, 255, 8]),
                         (vec![1, 2, 3, 4, 5, 6, 7, 8],
                          vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                          vec![254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255,
                               255, 255, 255, 255, 8]),
                         (vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                          vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
                          vec![254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255,
                               255, 255, 255, 255, 7])];

        for (source, asc, desc) in pairs {
            assert_eq!(encode_bytes(&source), asc);
            assert_eq!(encode_bytes_desc(&source), desc);

            let asc_offset = asc.as_ptr() as usize;
            let mut asc_input = asc.as_slice();
            assert_eq!(source, asc_input.decode_bytes(false).unwrap());
            assert_eq!(asc_input.as_ptr() as usize - asc_offset, asc.len());

            let desc_offset = desc.as_ptr() as usize;
            let mut desc_input = desc.as_slice();
            assert_eq!(source, desc_input.decode_bytes(true).unwrap());
            assert_eq!(desc_input.as_ptr() as usize - desc_offset, desc.len());
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
            assert!(x.as_slice().decode_bytes(false).is_err());
        }
    }

    #[test]
    fn test_encode_bytes_compare() {
        let pairs: Vec<(&[u8], &[u8], _)> =
            vec![(b"", b"\x00", Ordering::Less),
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

                 (b"\x01\x02\x03\x00", b"\x01\x02\x03", Ordering::Greater),
                 (b"\x01\x03\x03\x04", b"\x01\x03\x03\x05", Ordering::Less),

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
            assert_eq!(encode_bytes_desc(x).cmp(&encode_bytes_desc(y)),
                       ord.reverse());
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
            let mut buf = Vec::with_capacity(max_size);
            buf.encode_compact_bytes(s.as_bytes()).unwrap();
            assert!(buf.len() <= max_size);
            let mut input = buf.as_slice();
            let decoded = input.decode_compact_bytes().unwrap();
            assert!(input.is_empty());
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
        let key = [b'x'; 2000000];
        let encoded = encode_bytes(&key);
        b.iter(|| encoded.as_slice().decode_bytes(false));
    }
}
