// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This `encoder` module is only used for test, so the implementation is very straightforward.
//!
//! According to https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md
//! The row format is:
//!     | version | flag | number_of_non_null_values | number_of_null_values | non_null_value_ids
//!         | null_value_ids | value_offsets | values
//! short spec of each field
//! version: 1 byte
//! flag: 1 byte, when there's id greater than 255 or the total size of the values is greater than 65535 , value is 1, otherwise 0
//! number of non-null values: 2 bytes
//! number of null values: 2 bytes
//! column ids: ids of non-null values + ids of null values, when flag == 1 (big), id is 4 bytes, otherwise 1 byte
//! non-null values offset: when big, offset is 4 bytes, otherwise 2 bytes

use crate::codec::{
    mysql::{decimal::DecimalEncoder, json::JsonEncoder},
    Datum, Error, Result,
};
use codec::prelude::*;
use std::{i16, i32, i8, u16, u32, u8};

const MAX_I8: i64 = i8::MAX as i64;
const MIN_I8: i64 = i8::MIN as i64;
const MAX_I16: i64 = i16::MAX as i64;
const MIN_I16: i64 = i16::MIN as i64;
const MAX_I32: i64 = i32::MAX as i64;
const MIN_I32: i64 = i32::MIN as i64;

const MAX_U8: u64 = u8::MAX as u64;
const MAX_U16: u64 = u16::MAX as u64;
const MAX_U32: u64 = u32::MAX as u64;

pub trait RowEncoder: NumberEncoder {
    fn write_row(&mut self, datums: Vec<Datum>, ids: &[i64]) -> Result<()> {
        let mut is_big = false;
        let mut null_ids = Vec::with_capacity(ids.len());
        let mut non_null_ids = Vec::with_capacity(ids.len());
        let mut non_null_cols = Vec::with_capacity(ids.len());

        for (datum, id) in datums.into_iter().zip(ids) {
            if *id > 255 {
                is_big = true;
            }

            if datum == Datum::Null {
                null_ids.push(*id);
            } else {
                non_null_cols.push((datum, *id));
            }
        }
        non_null_cols.sort_by_key(|(_, id)| *id);
        null_ids.sort();

        let mut offset_wtr = vec![];
        let mut value_wtr = vec![];
        let mut offsets = vec![];

        for (datum, id) in non_null_cols {
            value_wtr.write_datum(datum)?;
            non_null_ids.push(id);
            offsets.push(value_wtr.len());

            if value_wtr.len() > (u16::MAX as usize) {
                is_big = true;
            }
        }

        // encode begins
        self.write_u8(super::CODEC_VERSION)?;
        self.write_flag(is_big)?;
        self.write_u16_le(non_null_ids.len() as u16)?;
        self.write_u16_le(null_ids.len() as u16)?;

        for id in non_null_ids {
            self.write_id(is_big, id)?;
        }
        for id in null_ids {
            self.write_id(is_big, id)?;
        }
        for offset in offsets {
            offset_wtr.write_offset(is_big, offset)?;
        }
        self.write_bytes(&offset_wtr)?;
        self.write_bytes(&value_wtr)?;
        Ok(())
    }

    #[inline]
    fn write_flag(&mut self, is_big: bool) -> codec::Result<()> {
        let flag = if is_big {
            super::Flags::BIG
        } else {
            super::Flags::SMALL
        };
        self.write_u8(flag.bits())
    }

    #[inline]
    fn write_id(&mut self, is_big: bool, id: i64) -> codec::Result<()> {
        if is_big {
            self.write_u32_le(id as u32)
        } else {
            self.write_u8(id as u8)
        }
    }

    #[inline]
    fn write_offset(&mut self, is_big: bool, offset: usize) -> codec::Result<()> {
        if is_big {
            self.write_u32_le(offset as u32)
        } else {
            self.write_u16_le(offset as u16)
        }
    }
}

impl<T: BufferWriter> RowEncoder for T {}

trait DatumEncoder: NumberEncoder + DecimalEncoder + JsonEncoder {
    #[inline]
    fn write_datum(&mut self, datum: Datum) -> Result<()> {
        match datum {
            Datum::I64(i) => self.encode_i64(i).map_err(Error::from),
            Datum::U64(u) => self.encode_u64(u).map_err(Error::from),
            Datum::Bytes(b) => self.write_bytes(&b).map_err(Error::from),
            Datum::Dec(d) => {
                let (prec, frac) = d.prec_and_frac();
                self.write_decimal(&d, prec, frac)?;
                Ok(())
            }
            Datum::Json(j) => self.write_json(&j),

            Datum::F64(f) => self.encode_u64(f.to_bits()).map_err(Error::from),
            Datum::Time(t) => self.encode_u64(t.to_packed_u64()).map_err(Error::from),
            Datum::Dur(d) => self.encode_i64(d.to_nanos()).map_err(Error::from),
            Datum::Min | Datum::Max => unimplemented!("not supported yet"),
            Datum::Null => unreachable!(),
        }
    }

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_i64(&mut self, v: i64) -> codec::Result<()> {
        match v {
            MIN_I8..=MAX_I8 => self.write_u8(v as i8 as u8),
            MIN_I16..=MAX_I16 => self.write_i16_le(v as i16),
            MIN_I32..=MAX_I32 => self.write_i32_le(v as i32),
            _ => self.write_i64_le(v),
        }
    }

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_u64(&mut self, v: u64) -> codec::Result<()> {
        match v {
            0..=MAX_U8 => self.write_u8(v as u8),
            0..=MAX_U16 => self.write_u16_le(v as u16),
            0..=MAX_U32 => self.write_u32_le(v as u32),
            _ => self.write_u64_le(v),
        }
    }
}
impl<T: BufferWriter> DatumEncoder for T {}

#[cfg(test)]
mod tests {
    use crate::codec::row::v2::encoder::RowEncoder;
    use crate::codec::{
        mysql::{duration::NANOS_PER_SEC, Duration, Json, Time},
        Datum,
    };
    use std::str::FromStr;

    #[test]
    fn test_encode() {
        let datums = vec![
            Datum::I64(1000),
            Datum::I64(2),
            Datum::Null,
            Datum::U64(3),
            Datum::I64(32767),
            Datum::Bytes(b"abc".to_vec()),
            Datum::F64(1.8),
            Datum::F64(-1.8),
            Datum::Time(Time::parse_utc_datetime("2018-01-19 03:14:07", 0).unwrap()),
            Datum::Dec(1i64.into()),
            Datum::Json(Json::from_str(r#"{"key":"value"}"#).unwrap()),
            Datum::Dur(Duration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
        ];
        let ids = vec![1, 12, 33, 3, 8, 7, 9, 6, 13, 14, 15, 16];

        let exp = vec![
            128, 0, 11, 0, 1, 0, 1, 3, 6, 7, 8, 9, 12, 13, 14, 15, 16, 33, 2, 0, 3, 0, 11, 0, 14,
            0, 16, 0, 24, 0, 25, 0, 33, 0, 36, 0, 65, 0, 69, 0, 232, 3, 3, 205, 204, 204, 204, 204,
            204, 252, 191, 97, 98, 99, 255, 127, 205, 204, 204, 204, 204, 204, 252, 63, 2, 0, 0, 0,
            135, 51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 3, 0, 12,
            22, 0, 0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 0, 202, 154, 59,
        ];
        let mut buf = vec![];
        buf.write_row(datums, &ids).unwrap();

        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode_big() {
        let datums = vec![
            Datum::I64(1000),
            Datum::I64(2),
            Datum::Null,
            Datum::I64(3),
            Datum::I64(32767),
        ];
        let ids = vec![1, 12, 335, 3, 8];
        let exp = vec![
            128, 1, 4, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 79, 1, 0, 0, 2, 0,
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 232, 3, 3, 255, 127, 2,
        ];
        let mut buf = vec![];
        buf.write_row(datums, &ids).unwrap();

        assert_eq!(exp, buf);
    }
}
