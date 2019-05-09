// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A set of high performance codecs for TiDB tables.

use std::intrinsics::{likely, unlikely};

use codec::number::{NumberCodec, NumberDecoder};

use crate::coprocessor::codec::mysql::Json;
use crate::coprocessor::codec::{datum, mysql};
use crate::coprocessor::Result;
use codec::byte::{CompactByteCodec, MemComparableByteCodec};

/// Encoder and decoder for table row key.
///
/// The format of the record key is `tXXXXXXXX_rYYYYYYYY` where `XXXXXXXX` is the table id
/// and `YYYYYYYY` is the handle.
pub struct RowKeyCodec;

impl RowKeyCodec {
    /// Decodes a complete row key. Returns table id and handle.
    ///
    /// # Error
    ///
    /// Returns error if the row key is not valid.
    pub fn decode() -> Result<(i64, i64)> {
        unimplemented!()
    }

    /// Decodes only the handle from a complete row key.
    ///
    /// # Error
    ///
    /// Returns error if the row key is not valid.
    #[inline]
    pub fn decode_only_handle(record_key: &[u8]) -> Result<i64> {
        if unsafe { unlikely(record_key.len() < 1 + 8 + 2 + 8) } {
            return Err(box_err!("Unexpected EOF in record key"));
        }
        let mut i = 0;
        if unsafe { unlikely(record_key[i] != b't') } {
            return Err(box_err!("Bad row key"));
        }
        i += 1; // skip `t`
        i += 8; // skip table id
        if unsafe { unlikely(record_key[i] != b'_' || record_key[i + 1] != b'r') } {
            return Err(box_err!("Bad row key"));
        }
        i += 2; // skip `_r`
        Ok(NumberCodec::decode_i64(&record_key[i..]))
    }
}

/// Decoder for table row values.
pub struct RowValueCodec;

impl RowValueCodec {
    /// Decodes the row format version from the row value.
    // TODO: Remove this `unused` attribute once we have row format v2.
    #[allow(unused)]
    pub fn decode_format_version(value: &[u8]) -> u8 {
        if unsafe { unlikely(value.is_empty()) } {
            return 0;
        }
        if unsafe { likely(value[0] < 128) } {
            return 0;
        }
        value[0] - 127 // So that version will be 1 when `value[0] == 128`.
    }
}

pub trait DatumDecoder: NumberDecoder {
    fn peek_datum_slice(&mut self) -> Result<&[u8]> {
        let buf = self.bytes();
        if unsafe { unlikely(buf.is_empty()) } {
            return Err(box_err!("Unexpected EOF"));
        }
        let flag = buf[0];
        let len = match flag {
            datum::NIL_FLAG => 0,
            datum::VAR_INT_FLAG | datum::VAR_UINT_FLAG => {
                NumberCodec::get_first_encoded_var_int_len(&buf[1..])
            }
            datum::INT_FLAG => codec::number::I64_SIZE,
            datum::UINT_FLAG => codec::number::U64_SIZE,
            datum::FLOAT_FLAG => codec::number::F64_SIZE,
            datum::BYTES_FLAG => MemComparableByteCodec::get_first_encoded_len(&buf[1..]),
            datum::COMPACT_BYTES_FLAG => CompactByteCodec::get_first_encoded_len(&buf[1..]),
            datum::DURATION_FLAG => codec::number::I64_SIZE,
            datum::DECIMAL_FLAG => mysql::dec_encoded_len(&buf[1..])?,
            datum::JSON_FLAG => {
                let mut v = &buf[1..];
                let l = v.len();
                Json::decode(&mut v)?;
                l - v.len()
            }
            _ => return Err(box_err!("Unsupported datum flag {}", flag)),
        };
        if len + 1 > buf.len() {
            return Err(box_err!("Unexpected EOF"));
        }
        Ok(&buf[..=len])
    }
}

impl<T: codec::buffer::BufferReader> DatumDecoder for T {}

pub trait RowValueDecoder: NumberDecoder + DatumDecoder {
    #[inline]
    fn read_v1_column_id(&mut self) -> Result<i64> {
        let flag = box_try!(self.read_u8());
        if unsafe { unlikely(flag != datum::VAR_INT_FLAG) } {
            return Err(box_err!("Bad row value: column id must be VAR_INT"));
        }
        Ok(box_try!(self.read_var_i64()))
    }

    fn peek_v1_column_data(&mut self) -> Result<&[u8]> {
        self.peek_datum_slice()
    }
}

impl<T: codec::buffer::BufferReader> RowValueDecoder for T {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_key_codec_decode_only_handle() {
        let ok_cases = vec![
            (
                b"t\x80\x00\x00\x00\x00\x00\x01\x02_r\x80\x40\x00\x00\x00\x00\x2C\x0C".to_vec(),
                0x40000000002C0C,
            ),
            (
                b"t\x80\x00\x00\x00\x00\x00\x01\x02_r\x80\x00\x00\x00\x00\x00\x00\xAA\x00".to_vec(),
                0xAA,
            ),
        ];
        for (input, expect_output) in ok_cases {
            assert_eq!(
                RowKeyCodec::decode_only_handle(&input).unwrap(),
                expect_output
            );
        }

        let fail_cases = vec![
            b"".to_vec(),
            b"t\x80\x00\x00".to_vec(),
            b"t\x80\x00\x00\x00\x00\x00\x01\x02".to_vec(),
            b"t\x80\x00\x00\x00\x00\x00\x01\x02_i\x80\x40\x00\x00\x00\x00\x2C\x0C".to_vec(),
            b"x\x80\x00\x00\x00\x00\x00\x01\x02_r\x80\x40\x00\x00\x00\x00\x2C\x0C".to_vec(),
        ];
        for input in fail_cases {
            assert!(RowKeyCodec::decode_only_handle(&input).is_err());
        }
    }
}
