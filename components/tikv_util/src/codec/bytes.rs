// Copyright 2016 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{BufRead, Write},
    ptr,
};

use byteorder::ReadBytesExt;

use super::{BytesSlice, Error, Result};
use crate::codec::number::{self, NumberEncoder};

const ENC_GROUP_SIZE: usize = 8;
const ENC_MARKER: u8 = b'\xff';
const ENC_ASC_PADDING: [u8; ENC_GROUP_SIZE] = [0; ENC_GROUP_SIZE];
const ENC_DESC_PADDING: [u8; ENC_GROUP_SIZE] = [!0; ENC_GROUP_SIZE];

/// Returns the maximum encoded bytes size.
pub fn max_encoded_bytes_size(n: usize) -> usize {
    (n / ENC_GROUP_SIZE + 1) * (ENC_GROUP_SIZE + 1)
}

pub trait BytesEncoder: NumberEncoder {
    /// Refer: <https://github.com/facebook/mysql-5.6/wiki/MyRocks-record-format#memcomparable-format>
    fn encode_bytes(&mut self, key: &[u8], desc: bool) -> Result<()> {
        let len = key.len();
        let mut index = 0;
        let mut buf = [0; ENC_GROUP_SIZE];
        while index <= len {
            let remain = len - index;
            let mut pad: usize = 0;
            if remain > ENC_GROUP_SIZE {
                self.write_all(adjust_bytes_order(
                    &key[index..index + ENC_GROUP_SIZE],
                    desc,
                    &mut buf,
                ))?;
            } else {
                pad = ENC_GROUP_SIZE - remain;
                self.write_all(adjust_bytes_order(&key[index..], desc, &mut buf))?;
                if desc {
                    self.write_all(&ENC_DESC_PADDING[..pad])?;
                } else {
                    self.write_all(&ENC_ASC_PADDING[..pad])?;
                }
            }
            self.write_all(adjust_bytes_order(
                &[ENC_MARKER - (pad as u8)],
                desc,
                &mut buf,
            ))?;
            index += ENC_GROUP_SIZE;
        }
        Ok(())
    }

    /// Joins bytes with its length into a byte slice. It is more
    /// efficient in both space and time compared to `encode_bytes`. Note that the encoded
    /// result is not memcomparable.
    fn encode_compact_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.encode_var_i64(data.len() as i64)?;
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
    encoded
}

/// Gets the first encoded bytes' length in compactly encoded data.
///
/// Compact-encoding includes a VarInt encoded length prefix (1 ~ 9 bytes) and N bytes payload.
/// This function gets the total bytes length of compact-encoded data, including the length prefix.
///
/// Note:
///     - This function won't check whether the bytes are encoded correctly.
///     - There can be multiple compact-encoded data, placed one by one. This function only returns
///       the length of the first one.
pub fn encoded_compact_len(mut encoded: &[u8]) -> usize {
    let last_encoded = encoded.as_ptr() as usize;
    let total_len = encoded.len();
    let vn = match number::decode_var_i64(&mut encoded) {
        Ok(vn) => vn as usize,
        Err(e) => {
            debug!("failed to decode bytes' length: {:?}", e);
            return total_len;
        }
    };
    vn + (encoded.as_ptr() as usize - last_encoded)
}

pub trait CompactBytesFromFileDecoder: BufRead {
    /// Decodes bytes which are encoded by `encode_compact_bytes` before.
    fn decode_compact_bytes(&mut self) -> Result<Vec<u8>> {
        let mut var_data = Vec::with_capacity(number::MAX_VAR_I64_LEN);
        while var_data.len() < number::MAX_VAR_U64_LEN {
            let b = self.read_u8()?;
            var_data.push(b);
            if b < 0x80 {
                break;
            }
        }
        let vn = number::decode_var_i64(&mut var_data.as_slice())? as usize;
        let mut data = vec![0; vn];
        self.read_exact(&mut data)?;
        Ok(data)
    }
}

impl<T: BufRead> CompactBytesFromFileDecoder for T {}

/// Gets the first encoded bytes' length in memcomparable-encoded data.
///
/// Memcomparable-encoding includes a VarInt encoded length prefix (1 ~ 9 bytes) and N bytes payload.
/// This function gets the total bytes length of memcomparable-encoded data, including the length prefix.
///
/// Note:
///     - This function won't check whether the bytes are encoded correctly.
///     - There can be multiple memcomparable-encoded data, placed one by one. This function only returns
///       the length of the first one.
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

/// Decodes bytes which are encoded by `encode_compact_bytes` before.
pub fn decode_compact_bytes(data: &mut BytesSlice<'_>) -> Result<Vec<u8>> {
    let vn = number::decode_var_i64(data)? as usize;
    if data.len() >= vn {
        let bs = data[0..vn].to_vec();
        *data = &data[vn..];
        return Ok(bs);
    }
    Err(Error::unexpected_eof())
}

/// Decodes bytes which are encoded by `encode_bytes` before.
///
/// Please note that, data is a mut reference to slice. After calling this the
/// slice that data point to would change.
pub fn decode_bytes(data: &mut BytesSlice<'_>, desc: bool) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(data.len() / (ENC_GROUP_SIZE + 1) * ENC_GROUP_SIZE);
    let mut offset = 0;
    let chunk_len = ENC_GROUP_SIZE + 1;
    loop {
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        let next_offset = offset + chunk_len;
        let chunk = if next_offset <= data.len() {
            &data[offset..next_offset]
        } else {
            return Err(Error::unexpected_eof());
        };
        offset = next_offset;
        // the last byte in decode unit is for marker which indicates pad size
        let (&marker, bytes) = chunk.split_last().unwrap();
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };
        // no padding, just push 8 bytes
        if pad_size == 0 {
            key.write_all(bytes).unwrap();
            continue;
        }
        if pad_size > ENC_GROUP_SIZE {
            return Err(Error::KeyPadding);
        }
        // if has padding, split the padding pattern and push rest bytes
        let (bytes, padding) = bytes.split_at(ENC_GROUP_SIZE - pad_size);
        key.write_all(bytes).unwrap();
        let pad_byte = if desc { !0 } else { 0 };
        // check the padding pattern whether validate or not
        if padding.iter().any(|x| *x != pad_byte) {
            return Err(Error::KeyPadding);
        }

        if desc {
            for k in &mut key {
                *k = !*k;
            }
        }
        // data will point to following unencoded bytes, maybe timestamp
        *data = &data[offset..];
        return Ok(key);
    }
}

/// Decodes bytes which are encoded by `encode_bytes` before just in place without malloc.
/// Please use this instead of `decode_bytes` if possible.
pub fn decode_bytes_in_place(data: &mut Vec<u8>, desc: bool) -> Result<()> {
    let mut write_offset = 0;
    let mut read_offset = 0;
    loop {
        let marker_offset = read_offset + ENC_GROUP_SIZE;
        if marker_offset >= data.len() {
            return Err(Error::unexpected_eof());
        };

        unsafe {
            // it is semantically equivalent to C's memmove()
            // and the src and dest may overlap
            // if src == dest do nothing
            ptr::copy(
                data.as_ptr().add(read_offset),
                data.as_mut_ptr().add(write_offset),
                ENC_GROUP_SIZE,
            );
        }
        write_offset += ENC_GROUP_SIZE;
        // everytime make ENC_GROUP_SIZE + 1 elements as a decode unit
        read_offset += ENC_GROUP_SIZE + 1;

        // the last byte in decode unit is for marker which indicates pad size
        let marker = data[marker_offset];
        let pad_size = if desc {
            marker as usize
        } else {
            (ENC_MARKER - marker) as usize
        };

        if pad_size > 0 {
            if pad_size > ENC_GROUP_SIZE {
                return Err(Error::KeyPadding);
            }

            // check the padding pattern whether validate or not
            let padding_slice = if desc {
                &ENC_DESC_PADDING[..pad_size]
            } else {
                &ENC_ASC_PADDING[..pad_size]
            };
            if &data[write_offset - pad_size..write_offset] != padding_slice {
                return Err(Error::KeyPadding);
            }
            unsafe {
                data.set_len(write_offset - pad_size);
            }
            if desc {
                for k in data {
                    *k = !*k;
                }
            }
            return Ok(());
        }
    }
}

/// Returns whether `encoded` bytes is encoded from `raw`. Returns `false` if `encoded` is invalid.
pub fn is_encoded_from(encoded: &[u8], raw: &[u8], desc: bool) -> bool {
    let check_single_chunk = |encoded: &[u8], raw: &[u8]| {
        let len = raw.len();
        let pad = (ENC_GROUP_SIZE - len) as u8;
        if desc {
            encoded[ENC_GROUP_SIZE] == !(ENC_MARKER - pad)
                && encoded[..len]
                    .iter()
                    .zip(raw)
                    .all(|(&enc, &raw)| enc == !raw)
                && encoded[len..encoded.len() - 1]
                    .iter()
                    .all(|&v| v == ENC_MARKER)
        } else {
            encoded[ENC_GROUP_SIZE] == (ENC_MARKER - pad)
                && &encoded[..len] == raw
                && encoded[len..encoded.len() - 1]
                    .iter()
                    .all(|&v| v == !ENC_MARKER)
        }
    };

    let mut rev_encoded_chunks = encoded.rchunks_exact(ENC_GROUP_SIZE + 1);
    // Valid encoded bytes must has complete chunks
    if !rev_encoded_chunks.remainder().is_empty() {
        return false;
    }

    // Bytes are compared in reverse order because in real cases like TiDB, if two keys
    // are different, the last a few bytes are more likely to be different.

    let raw_chunks = raw.chunks_exact(ENC_GROUP_SIZE);
    // Check the last chunk first
    match rev_encoded_chunks.next() {
        Some(encoded_chunk) if check_single_chunk(encoded_chunk, raw_chunks.remainder()) => {}
        _ => return false,
    }

    // The count of the remaining chunks must be the same. Using `size_hint` here is both safe and
    // efficient because chunk iterators implement trait `TrustedLen`.
    if rev_encoded_chunks.size_hint() != raw_chunks.size_hint() {
        return false;
    }

    for (encoded_chunk, raw_chunk) in rev_encoded_chunks.zip(raw_chunks.rev()) {
        if !check_single_chunk(encoded_chunk, raw_chunk) {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::codec::{bytes, number};

    #[test]
    fn test_enc_dec_bytes() {
        let pairs = vec![
            (
                vec![],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 8],
            ),
            (
                vec![0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![255, 255, 255, 255, 255, 255, 255, 255, 7],
            ),
            (
                vec![1, 2, 3],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 250],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 5],
            ),
            (
                vec![1, 2, 3, 0],
                vec![1, 2, 3, 0, 0, 0, 0, 0, 251],
                vec![254, 253, 252, 255, 255, 255, 255, 255, 4],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7],
                vec![1, 2, 3, 4, 5, 6, 7, 0, 254],
                vec![254, 253, 252, 251, 250, 249, 248, 255, 1],
            ),
            (
                vec![0, 0, 0, 0, 0, 0, 0, 0],
                vec![0, 0, 0, 0, 0, 0, 0, 0, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    255, 255, 255, 255, 255, 255, 255, 255, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 0, 0, 0, 0, 0, 0, 0, 0, 247],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 255, 255, 255, 255, 255, 255, 255,
                    255, 8,
                ],
            ),
            (
                vec![1, 2, 3, 4, 5, 6, 7, 8, 9],
                vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 9, 0, 0, 0, 0, 0, 0, 0, 248],
                vec![
                    254, 253, 252, 251, 250, 249, 248, 247, 0, 246, 255, 255, 255, 255, 255, 255,
                    255, 7,
                ],
            ),
        ];

        for (source, mut asc, mut desc) in pairs {
            assert_eq!(encode_bytes(&source), asc);
            assert_eq!(encode_bytes_desc(&source), desc);

            // apppend timestamp, the timestamp bytes should not affect decode result
            asc.encode_u64_desc(0).unwrap();
            desc.encode_u64_desc(0).unwrap();
            {
                let asc_offset = asc.as_ptr() as usize;
                let mut asc_input = asc.as_slice();
                assert_eq!(source, decode_bytes(&mut asc_input, false).unwrap());
                assert_eq!(
                    asc_input.as_ptr() as usize - asc_offset,
                    asc.len() - number::U64_SIZE
                );
            }
            decode_bytes_in_place(&mut asc, false).unwrap();
            assert_eq!(source, asc);

            {
                let desc_offset = desc.as_ptr() as usize;
                let mut desc_input = desc.as_slice();
                assert_eq!(source, decode_bytes(&mut desc_input, true).unwrap());
                assert_eq!(
                    desc_input.as_ptr() as usize - desc_offset,
                    desc.len() - number::U64_SIZE
                );
            }
            decode_bytes_in_place(&mut desc, true).unwrap();
            assert_eq!(source, desc);
        }
    }

    #[test]
    fn test_dec_bytes_fail() {
        let invalid_bytes = vec![
            vec![1, 2, 3, 4],
            vec![0, 0, 0, 0, 0, 0, 0, 247],
            vec![0, 0, 0, 0, 0, 0, 0, 0, 246],
            vec![0, 0, 0, 0, 0, 0, 0, 1, 247],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 0],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 255],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 2, 3, 4, 5, 6, 7, 8, 0],
            vec![1, 2, 3, 4, 5, 6, 7, 8, 255, 1, 0, 0, 0, 0, 0, 0, 0, 247],
        ];

        for mut x in invalid_bytes {
            assert!(decode_bytes(&mut x.as_slice(), false).is_err());
            assert!(decode_bytes_in_place(&mut x, false).is_err());
        }
    }

    #[test]
    fn test_is_encoded_from() {
        for raw_len in 0..=24 {
            let raw: Vec<u8> = (1..=raw_len).collect();
            for &desc in &[true, false] {
                let encoded = encode_order_bytes(&raw, desc);
                assert!(
                    is_encoded_from(&encoded, &raw, desc),
                    "Encoded: {:?}, Raw: {:?}, desc: {}",
                    encoded,
                    raw,
                    desc
                );

                // Should return false if we modify one byte in raw
                for i in 0..raw.len() {
                    let mut invalid_raw = raw.clone();
                    invalid_raw[i] = raw[i].wrapping_add(1);
                    assert!(
                        !is_encoded_from(&encoded, &invalid_raw, desc),
                        "Encoded: {:?}, Raw: {:?}, desc: {}",
                        encoded,
                        invalid_raw,
                        desc
                    );
                }

                // Should return false if we modify one byte in encoded
                for i in 0..encoded.len() {
                    let mut invalid_encoded = encoded.clone();
                    invalid_encoded[i] = encoded[i].wrapping_add(1);
                    assert!(
                        !is_encoded_from(&invalid_encoded, &raw, desc),
                        "Encoded: {:?}, Raw: {:?}, desc: {}",
                        invalid_encoded,
                        raw,
                        desc
                    );
                }

                // Should return false if encoded length is not a multiple of 9
                let invalid_encoded = &encoded[..encoded.len() - 1];
                assert!(
                    !is_encoded_from(invalid_encoded, &raw, desc),
                    "Encoded: {:?}, Raw: {:?}, desc: {}",
                    invalid_encoded,
                    raw,
                    desc
                );

                // Should return false if encoded has less or more chunks
                let shorter_encoded = &encoded[..encoded.len() - ENC_GROUP_SIZE - 1];
                assert!(
                    !is_encoded_from(shorter_encoded, &raw, desc),
                    "Encoded: {:?}, Raw: {:?}, desc: {}",
                    shorter_encoded,
                    raw,
                    desc
                );
                let mut longer_encoded = encoded.clone();
                longer_encoded.extend(&[0, 0, 0, 0, 0, 0, 0, 0, 0xFF]);
                assert!(
                    !is_encoded_from(&longer_encoded, &raw, desc),
                    "Encoded: {:?}, Raw: {:?}, desc: {}",
                    longer_encoded,
                    raw,
                    desc
                );

                // Should return false if raw is longer or shorter
                if !raw.is_empty() {
                    let shorter_raw = &raw[..raw.len() - 1];
                    assert!(
                        !is_encoded_from(&encoded, shorter_raw, desc),
                        "Encoded: {:?}, Raw: {:?}, desc: {}",
                        encoded,
                        shorter_raw,
                        desc
                    );
                }
                let mut longer_raw = raw.to_vec();
                longer_raw.push(0);
                assert!(
                    !is_encoded_from(&encoded, &longer_raw, desc),
                    "Encoded: {:?}, Raw: {:?}, desc: {}",
                    encoded,
                    longer_raw,
                    desc
                );
            }
        }
    }

    #[test]
    fn test_encode_bytes_compare() {
        let pairs: Vec<(&[u8], &[u8], _)> = vec![
            (b"", b"\x00", Ordering::Less),
            (b"\x00", b"\x00", Ordering::Equal),
            (b"\xFF", b"\x00", Ordering::Greater),
            (b"\xFF", b"\xFF\x00", Ordering::Less),
            (b"a", b"b", Ordering::Less),
            (b"a", b"\x00", Ordering::Greater),
            (b"\x00", b"\x01", Ordering::Less),
            (b"\x00\x01", b"\x00\x00", Ordering::Greater),
            (b"\x00\x00\x00", b"\x00\x00", Ordering::Greater),
            (b"\x00\x00\x00", b"\x00\x00", Ordering::Greater),
            (
                b"\x00\x00\x00\x00\x00\x00\x00\x00",
                b"\x00\x00\x00\x00\x00\x00\x00\x00\x00",
                Ordering::Less,
            ),
            (b"\x01\x02\x03\x00", b"\x01\x02\x03", Ordering::Greater),
            (b"\x01\x03\x03\x04", b"\x01\x03\x03\x05", Ordering::Less),
            (
                b"\x01\x02\x03\x04\x05\x06\x07",
                b"\x01\x02\x03\x04\x05\x06\x07\x08",
                Ordering::Less,
            ),
            (
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x09",
                b"\x01\x02\x03\x04\x05\x06\x07\x08",
                Ordering::Greater,
            ),
            (
                b"\x01\x02\x03\x04\x05\x06\x07\x08\x00",
                b"\x01\x02\x03\x04\x05\x06\x07\x08",
                Ordering::Greater,
            ),
        ];

        for (x, y, ord) in pairs {
            assert_eq!(encode_bytes(x).cmp(&encode_bytes(y)), ord);
            assert_eq!(
                encode_bytes_desc(x).cmp(&encode_bytes_desc(y)),
                ord.reverse()
            );
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
            let decoded = decode_compact_bytes(&mut input).unwrap();
            assert!(input.is_empty());
            assert_eq!(decoded, s.as_bytes());
        }
    }

    #[test]
    fn test_compact_codec_for_file() {
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
        let key = [b'x'; 10000];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            let encoded = encoded.clone();
            decode_bytes(&mut encoded.as_slice(), false).unwrap();
        });
    }

    #[bench]
    fn bench_decode_inplace(b: &mut Bencher) {
        let key = [b'x'; 10000];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            let mut encoded = encoded.clone();
            decode_bytes_in_place(&mut encoded, false).unwrap();
        });
    }

    #[bench]
    fn bench_decode_small(b: &mut Bencher) {
        let key = [b'x'; 30];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            let encoded = encoded.clone();
            decode_bytes(&mut encoded.as_slice(), false).unwrap();
        });
    }

    #[bench]
    fn bench_decode_inplace_small(b: &mut Bencher) {
        let key = [b'x'; 30];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            let mut encoded = encoded.clone();
            decode_bytes_in_place(&mut encoded, false).unwrap();
        });
    }

    #[bench]
    fn bench_is_encoded_from(b: &mut Bencher) {
        let key = [b'x'; 10000];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            assert!(is_encoded_from(&encoded, &key, false));
        });
    }

    #[bench]
    fn bench_is_encoded_from_small(b: &mut Bencher) {
        let key = [b'x'; 30];
        let encoded = encode_bytes(&key);
        b.iter(|| {
            assert!(is_encoded_from(&encoded, &key, false));
        });
    }
}
