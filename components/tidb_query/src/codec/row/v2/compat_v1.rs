// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! A compatible layer for converting V2 row datum into V1 row datum.

use codec::number::NumberCodec;
use codec::prelude::BufferWriter;
use tidb_query_datatype::{FieldTypeAccessor, FieldTypeTp};

use crate::codec::datum_codec::DatumFlagAndPayloadEncoder;
use crate::codec::{datum, Error, Result};

#[inline]
fn decode_v2_u64(v: &[u8]) -> Result<u64> {
    // See `decodeInt` in TiDB.
    match v.len() {
        1 => Ok(u64::from(v[0])),
        2 => Ok(u64::from(NumberCodec::decode_u16_le(v))),
        4 => Ok(u64::from(NumberCodec::decode_u32_le(v))),
        8 => Ok(u64::from(NumberCodec::decode_u64_le(v))),
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
