// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::{Error, Result};
use codec::number::NumberCodec;
use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};

fn decode_uint(v: &[u8]) -> Result<u64> {
    match v.len() {
        1 => Ok(u64::from(v[0])),
        2 => Ok(u64::from(NumberCodec::decode_u16_le(v))),
        4 => Ok(u64::from(NumberCodec::decode_u32_le(v))),
        8 => Ok(NumberCodec::decode_u64_le(v)),
        _ => Err(Error::InvalidDataType(
            "Failed to decode data as u64".to_owned(),
        )),
    }
}

#[inline]
fn decode_int(v: &[u8]) -> Result<i64> {
    match v.len() {
        1 => Ok(i64::from(v[0])),
        2 => Ok(i64::from(NumberCodec::decode_i16_le(v))),
        4 => Ok(i64::from(NumberCodec::decode_i32_le(v))),
        8 => Ok(NumberCodec::decode_i64_le(v)),
        _ => Err(Error::InvalidDataType(
            "Failed to decode data as i64".to_owned(),
        )),
    }
}

///  Encode `v` which is originally encoded using v2 format to `buf` using the v1 format
pub fn encode_to_v1_binary(buf: &mut Vec<u8>, v: &[u8], ft: &dyn FieldTypeAccessor) -> Result<()> {
    use crate::codec::datum;
    use codec::prelude::*;
    match ft.tp() {
        FieldTypeTp::Tiny
        | FieldTypeTp::Short
        | FieldTypeTp::Int24
        | FieldTypeTp::Long
        | FieldTypeTp::LongLong
        | FieldTypeTp::Year => {
            if ft.is_unsigned() {
                buf.push(datum::VAR_UINT_FLAG);
                buf.write_var_u64(decode_uint(v)?)?;
            } else {
                buf.push(datum::VAR_INT_FLAG);
                buf.write_var_i64(decode_int(v)?)?;
            }
        }
        FieldTypeTp::VarChar
        | FieldTypeTp::VarString
        | FieldTypeTp::String
        | FieldTypeTp::TinyBlob
        | FieldTypeTp::MediumBlob
        | FieldTypeTp::LongBlob
        | FieldTypeTp::Blob => {
            buf.push(datum::COMPACT_BYTES_FLAG);
            buf.write_compact_bytes(v)?
        }
        FieldTypeTp::Date
        | FieldTypeTp::DateTime
        | FieldTypeTp::Timestamp
        | FieldTypeTp::Set
        | FieldTypeTp::Enum
        | FieldTypeTp::Bit => {
            buf.push(datum::UINT_FLAG);
            buf.write_u64(decode_uint(v)?)?;
        }
        FieldTypeTp::Double | FieldTypeTp::Float => {
            buf.push(datum::FLOAT_FLAG);
            buf.write_f64(f64::from_bits(decode_uint(v)?))?
        }
        FieldTypeTp::NewDecimal => {
            buf.push(datum::DECIMAL_FLAG);
            buf.extend_from_slice(v);
        }
        FieldTypeTp::JSON => {
            buf.push(datum::JSON_FLAG);
            buf.extend_from_slice(v);
        }
        FieldTypeTp::Duration => {
            buf.push(datum::DURATION_FLAG);
            buf.write_i64(decode_int(v)?)?;
        }
        FieldTypeTp::NewDate
        | FieldTypeTp::Geometry
        | FieldTypeTp::Null
        | FieldTypeTp::Unspecified => unreachable!(),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::encoder::{Column, ScalarValueEncoder};
    use super::encode_to_v1_binary;
    use crate::codec::data_type::*;
    use crate::codec::mysql::{Duration, Json, Time};
    use crate::codec::raw_datum::RawDatumDecoder;
    use crate::expr::EvalContext;
    use std::str::FromStr;
    use std::{i16, i32, i64};
    use tidb_query_datatype::{builder::FieldTypeBuilder, FieldTypeTp};

    #[test]
    fn test_encode_to_v1_binary() {
        let mut ctx = EvalContext::default();
        let date = Time::parse_date(&mut ctx, "2012-12-12").unwrap();
        let datetime = Time::parse_datetime(&mut ctx, "2019-09-16 10:11:12", 0, false).unwrap();
        let timestamp =
            Time::parse_timestamp(&mut ctx, "2012-12-12 12:13:14.999", 0, false).unwrap();
        let cases: Vec<(FieldTypeTp, ScalarValue)> = vec![
            (FieldTypeTp::Tiny, 1.into()),
            (FieldTypeTp::Short, i64::from(i16::MAX).into()),
            (FieldTypeTp::Int24, ((1 << 23) - 1).into()),
            (FieldTypeTp::Long, i64::from(i32::MAX).into()),
            (FieldTypeTp::LongLong, i64::MAX.into()),
            (FieldTypeTp::Year, 12.into()),
            (FieldTypeTp::Float, 12.5.into()),
            (FieldTypeTp::Double, (-128.55).into()),
            (FieldTypeTp::VarChar, b"abc".to_vec().into()),
            (FieldTypeTp::VarString, b"abc".to_vec().into()),
            (FieldTypeTp::String, b"abc".to_vec().into()),
            (FieldTypeTp::TinyBlob, b"abc".to_vec().into()),
            (FieldTypeTp::MediumBlob, b"abc".to_vec().into()),
            (FieldTypeTp::LongBlob, b"abc".to_vec().into()),
            (FieldTypeTp::Blob, b"abc".to_vec().into()),
            (FieldTypeTp::Date, date.into()),
            (FieldTypeTp::DateTime, datetime.into()),
            (FieldTypeTp::Timestamp, timestamp.into()),
            (
                FieldTypeTp::NewDecimal,
                Decimal::from_str("12.78").unwrap().into(),
            ),
            (
                FieldTypeTp::JSON,
                Json::from_str(r#"{"key":"value"}"#).unwrap().into(),
            ),
            (
                FieldTypeTp::Duration,
                Duration::from_nanos(10000, 0).unwrap().into(),
            ),
        ];

        for (tp, value) in cases {
            let ft = FieldTypeBuilder::new().tp(tp).build();
            let c = Column::new_with_ft(1, value.clone(), ft);
            let mut buf = vec![];
            buf.write_value(&mut ctx, &c).unwrap();

            let mut result = vec![];
            encode_to_v1_binary(&mut result, &buf, c.field_type()).unwrap();

            match_template_evaluable! {
                TT, match value {
                    ScalarValue::TT(value) => {
                        assert_eq!(
                            result.decode(c.field_type(), &mut ctx).unwrap(),
                            value
                        );
                    }
                }
            }
        }
    }
}
