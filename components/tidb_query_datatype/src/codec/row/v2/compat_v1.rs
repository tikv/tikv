// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A compatible layer for converting V2 row datum into V1 row datum.

use crate::{FieldTypeAccessor, FieldTypeTp};
use codec::number::NumberCodec;
use codec::prelude::BufferWriter;

use crate::codec::datum_codec::DatumFlagAndPayloadEncoder;
use crate::codec::{datum, Error, Result};

#[inline]
pub fn decode_v2_u64(v: &[u8]) -> Result<u64> {
    // See `decodeInt` in TiDB.
    match v.len() {
        1 => Ok(u64::from(v[0])),
        2 => Ok(u64::from(NumberCodec::decode_u16_le(v))),
        4 => Ok(u64::from(NumberCodec::decode_u32_le(v))),
        8 => Ok(NumberCodec::decode_u64_le(v)),
        _ => Err(Error::InvalidDataType(
            "Failed to decode row v2 data as u64".to_owned(),
        )),
    }
}

#[inline]
fn decode_v2_i64(v: &[u8]) -> Result<i64> {
    // See `decodeUint` in TiDB.
    match v.len() {
        1 => Ok(i64::from(v[0] as i8)),
        2 => Ok(i64::from(NumberCodec::decode_u16_le(v) as i16)),
        4 => Ok(i64::from(NumberCodec::decode_u32_le(v) as i32)),
        8 => Ok(NumberCodec::decode_u64_le(v) as i64),
        _ => Err(Error::InvalidDataType(
            "Failed to decode row v2 data as i64".to_owned(),
        )),
    }
}

pub trait V1CompatibleEncoder: DatumFlagAndPayloadEncoder {
    fn write_v2_as_datum_i64(&mut self, src: &[u8]) -> Result<()> {
        self.write_datum_i64(decode_v2_i64(src)?)
    }

    fn write_v2_as_datum_u64(&mut self, src: &[u8]) -> Result<()> {
        self.write_datum_u64(decode_v2_u64(src)?)
    }

    fn write_v2_as_datum_duration(&mut self, src: &[u8]) -> Result<()> {
        self.write_u8(datum::DURATION_FLAG)?;
        self.write_datum_payload_i64(decode_v2_i64(src)?)
    }

    fn write_v2_as_datum(&mut self, src: &[u8], ft: &dyn FieldTypeAccessor) -> Result<()> {
        // See `fieldType2Flag.go` in TiDB.
        match ft.tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong => {
                if ft.is_unsigned() {
                    self.write_v2_as_datum_u64(src)?;
                } else {
                    self.write_v2_as_datum_i64(src)?;
                }
            }
            FieldTypeTp::Float | FieldTypeTp::Double => {
                self.write_u8(datum::FLOAT_FLAG)?;
                // Copy datum payload as it is
                self.write_bytes(src)?;
            }
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob
            | FieldTypeTp::Blob => {
                self.write_datum_compact_bytes(src)?;
            }
            FieldTypeTp::Date
            | FieldTypeTp::DateTime
            | FieldTypeTp::Timestamp
            | FieldTypeTp::Enum
            | FieldTypeTp::Bit
            | FieldTypeTp::Set => {
                self.write_v2_as_datum_u64(src)?;
            }
            FieldTypeTp::Year => {
                self.write_v2_as_datum_i64(src)?;
            }
            FieldTypeTp::Duration => {
                // This implementation is different from TiDB. TiDB encodes v2 duration into v1
                // with datum flag VarInt, but we will encode with datum flag Duration, since
                // Duration datum flag results in fixed-length datum payload, which is faster
                // to encode and decode.
                self.write_v2_as_datum_duration(src)?;
            }
            FieldTypeTp::NewDecimal => {
                self.write_u8(datum::DECIMAL_FLAG)?;
                // Copy datum payload as it is
                self.write_bytes(src)?;
            }
            FieldTypeTp::JSON => {
                self.write_u8(datum::JSON_FLAG)?;
                // Copy datum payload as it is
                self.write_bytes(src)?;
            }
            FieldTypeTp::Null => {
                self.write_u8(datum::NIL_FLAG)?;
            }
            fp => {
                return Err(Error::InvalidDataType(format!(
                    "Unsupported FieldType {:?}",
                    fp
                )))
            }
        }
        Ok(())
    }
}

impl<T: BufferWriter> V1CompatibleEncoder for T {}

/// These tests mainly focus on transfer the v2 encoding to v1-compatible encoding.
///
/// The test path is:
/// 1. Encode value using v2
/// 2. Use `V1CompatibleEncoder` to transfer the encoded bytes from v2 to v1-compatible
/// 3. Use `RawDatumDecoder` decode the encoded bytes, check the result.
///
/// Note: a value encoded using v2 then transfer to v1-compatible encoding, is not always equals the
/// encoded-bytes using v1 directly.
#[cfg(test)]
mod tests {
    use super::super::encoder_for_test::{Column, ScalarValueEncoder};
    use super::V1CompatibleEncoder;
    use crate::FieldTypeTp;
    use crate::{
        codec::{data_type::*, datum_codec::RawDatumDecoder},
        expr::EvalContext,
    };
    use std::{f64, i16, i32, i64, i8, u16, u32, u64, u8};

    fn encode_to_v1_compatible(mut ctx: &mut EvalContext, col: &Column) -> Vec<u8> {
        let mut buf_v2 = vec![];
        buf_v2.write_value(&mut ctx, &col).unwrap();
        let mut buf_v1 = vec![];
        buf_v1.write_v2_as_datum(&buf_v2, col.ft()).unwrap();
        buf_v1
    }

    #[test]
    fn test_int() {
        let cases = vec![
            0,
            i64::from(i8::MIN),
            i64::from(u8::MAX),
            i64::from(i8::MAX),
            i64::from(i16::MIN),
            i64::from(u16::MAX),
            i64::from(i16::MAX),
            i64::from(i32::MIN),
            i64::from(u32::MAX),
            i64::from(i32::MAX),
            i64::MAX,
            i64::MIN,
        ];
        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value).with_tp(FieldTypeTp::LongLong);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Int = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_uint() {
        let cases = vec![
            0,
            i8::MAX as u64,
            u64::from(u8::MAX),
            i16::MAX as u64,
            u64::from(u16::MAX),
            i32::MAX as u64,
            u64::from(u32::MAX),
            i64::MAX as u64,
            u64::MAX,
        ];
        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value as i64)
                .with_unsigned()
                .with_tp(FieldTypeTp::LongLong);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Int = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got as u64);
        }
    }

    #[test]
    fn test_real() {
        let cases = vec![
            Real::new(0.0).unwrap(),
            Real::new(1.3).unwrap(),
            Real::new(-1.234).unwrap(),
            Real::new(f64::MAX).unwrap(),
            Real::new(f64::MIN).unwrap(),
            Real::new(f64::MIN_POSITIVE).unwrap(),
            Real::new(f64::INFINITY).unwrap(),
            Real::new(f64::NEG_INFINITY).unwrap(),
        ];
        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value).with_tp(FieldTypeTp::Double);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Real = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_decimal() {
        use std::str::FromStr;
        let cases = vec![
            Decimal::from(1i64),
            Decimal::from(i64::MIN),
            Decimal::from(i64::MAX),
            Decimal::from_str("10.123").unwrap(),
            Decimal::from_str("-10.123").unwrap(),
            Decimal::from_str("10.111").unwrap(),
            Decimal::from_str("-10.111").unwrap(),
        ];
        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value).with_tp(FieldTypeTp::NewDecimal);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Decimal = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_bytes() {
        let cases = vec![b"".to_vec(), b"abc".to_vec(), "数据库".as_bytes().to_vec()];
        let mut ctx = EvalContext::default();

        for value in cases {
            let col = Column::new(1, value.clone()).with_tp(FieldTypeTp::String);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Bytes = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_datetime() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            DateTime::parse_date(&mut ctx, "2019-12-31").unwrap(),
            DateTime::parse_datetime(&mut ctx, "2019-09-16 10:11:12", 0, false).unwrap(),
            DateTime::parse_timestamp(&mut ctx, "2019-09-16 10:11:12.111", 3, false).unwrap(),
            DateTime::parse_timestamp(&mut ctx, "2019-09-16 10:11:12.67", 2, true).unwrap(),
        ];

        for value in cases {
            let col = Column::new(1, value).with_tp(FieldTypeTp::DateTime);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: DateTime = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_json() {
        let cases: Vec<Json> = vec![
            r#"[1,"sdf",2,[3,4]]"#.parse().unwrap(),
            r#"{"1":"sdf","2":{"3":4},"asd":"qwe"}"#.parse().unwrap(),
            r#""hello""#.parse().unwrap(),
        ];

        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value.clone()).with_tp(FieldTypeTp::JSON);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Json = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }

    #[test]
    fn test_duration() {
        let mut ctx = EvalContext::default();
        let cases = vec![
            Duration::parse(&mut ctx, "31 11:30:45.123", 4).unwrap(),
            Duration::parse(&mut ctx, "-11:30:45.9233456", 4).unwrap(),
        ];

        let mut ctx = EvalContext::default();
        for value in cases {
            let col = Column::new(1, value)
                .with_tp(FieldTypeTp::Duration)
                .with_decimal(4);
            let buf = encode_to_v1_compatible(&mut ctx, &col);
            let got: Duration = buf.decode(col.ft(), &mut ctx).unwrap().unwrap();
            assert_eq!(value, got);
        }
    }
}
