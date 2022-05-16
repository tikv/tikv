// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! This `encoder` module is only used for test, so the implementation is very straightforward.
//!
//! According to <https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md>
//!
//! The row format is:
//!
//! | version | flag | number_of_non_null_columns | number_of_null_columns | non_null_column_ids | null_column_ids | value_offsets | values |
//! |---------| ---- | -------------------------- | ---------------------- | ------------------- | --------------- | ------------- | ------ |
//!
//! length about each field:
//!
//! * version: 1 byte
//! * flag: 1 byte, when there's id greater than 255 or the total size of the values is greater than 65535 , value is 1, otherwise 0
//! * number of non-null values: 2 bytes
//! * number of null values: 2 bytes
//! * non-null column ids: when flag == 1 (big), id is 4 bytes, otherwise 1 byte
//! * null column ids: when flag == 1 (big), id is 4 bytes, otherwise 1 byte
//! * non-null values offset: when big, offset is 4 bytes, otherwise 2 bytes

use std::{i16, i32, i8, u16, u32, u8};

use codec::prelude::*;
use tipb::FieldType;

use crate::{
    codec::{
        data_type::ScalarValue,
        mysql::{decimal::DecimalEncoder, json::JsonEncoder},
        Error, Result,
    },
    expr::EvalContext,
    FieldTypeAccessor, FieldTypeFlag, FieldTypeTp,
};

const MAX_I8: i64 = i8::MAX as i64;
const MIN_I8: i64 = i8::MIN as i64;
const MAX_I16: i64 = i16::MAX as i64;
const MIN_I16: i64 = i16::MIN as i64;
const MAX_I32: i64 = i32::MAX as i64;
const MIN_I32: i64 = i32::MIN as i64;

const MAX_U8: u64 = u8::MAX as u64;
const MAX_U16: u64 = u16::MAX as u64;
const MAX_U32: u64 = u32::MAX as u64;

pub struct Column {
    id: i64,
    value: ScalarValue,
    ft: FieldType,
}

impl Column {
    pub fn new(id: i64, value: impl Into<ScalarValue>) -> Self {
        Column {
            id,
            ft: FieldType::default(),
            value: value.into(),
        }
    }

    pub fn ft(&self) -> &FieldType {
        &self.ft
    }

    #[must_use]
    pub fn with_tp(mut self, tp: FieldTypeTp) -> Self {
        self.ft.as_mut_accessor().set_tp(tp);
        self
    }

    pub fn is_unsigned(&self) -> bool {
        self.ft.is_unsigned()
    }

    #[must_use]
    pub fn with_unsigned(mut self) -> Self {
        self.ft.as_mut_accessor().set_flag(FieldTypeFlag::UNSIGNED);
        self
    }

    #[must_use]
    pub fn with_decimal(mut self, decimal: isize) -> Self {
        self.ft.as_mut_accessor().set_decimal(decimal);
        self
    }
}

pub trait RowEncoder: NumberEncoder {
    fn write_row(&mut self, ctx: &mut EvalContext, columns: Vec<Column>) -> Result<()> {
        let mut is_big = false;
        let mut null_ids = Vec::with_capacity(columns.len());
        let mut non_null_ids = Vec::with_capacity(columns.len());
        let mut non_null_cols = Vec::with_capacity(columns.len());

        for col in columns {
            if col.id > 255 {
                is_big = true;
            }

            if col.value.is_none() {
                null_ids.push(col.id);
            } else {
                non_null_cols.push(col);
            }
        }
        non_null_cols.sort_by_key(|c| c.id);
        null_ids.sort_unstable();

        let mut offset_wtr = vec![];
        let mut value_wtr = vec![];
        let mut offsets = vec![];

        for col in non_null_cols {
            non_null_ids.push(col.id);
            value_wtr.write_value(ctx, &col)?;
            offsets.push(value_wtr.len());
        }
        if value_wtr.len() > (u16::MAX as usize) {
            is_big = true;
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
            super::Flags::default()
        };
        self.write_u8(flag.bits)
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

pub trait ScalarValueEncoder: NumberEncoder + DecimalEncoder + JsonEncoder {
    #[inline]
    fn write_value(&mut self, ctx: &mut EvalContext, col: &Column) -> Result<()> {
        match &col.value {
            ScalarValue::Int(Some(v)) if col.is_unsigned() => {
                self.encode_u64(*v as u64).map_err(Error::from)
            }
            ScalarValue::Int(Some(v)) => self.encode_i64(*v).map_err(Error::from),
            ScalarValue::Decimal(Some(v)) => {
                let (prec, frac) = v.prec_and_frac();
                self.write_decimal(v, prec, frac)?;
                Ok(())
            }
            ScalarValue::Real(Some(v)) => self.write_f64(v.into_inner()).map_err(Error::from),
            ScalarValue::Bytes(Some(v)) => self.write_bytes(v).map_err(Error::from),
            ScalarValue::DateTime(Some(v)) => {
                self.encode_u64(v.to_packed_u64(ctx)?).map_err(Error::from)
            }
            ScalarValue::Duration(Some(v)) => self.encode_i64(v.to_nanos()).map_err(Error::from),
            ScalarValue::Json(Some(v)) => self.write_json(v.as_ref()),
            _ => unreachable!(),
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
impl<T: BufferWriter> ScalarValueEncoder for T {}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::{Column, RowEncoder};
    use crate::{
        codec::{
            data_type::ScalarValue,
            mysql::{duration::NANOS_PER_SEC, Decimal, Duration, Json, Time},
        },
        expr::EvalContext,
    };

    #[test]
    fn test_encode_unsigned() {
        let cols = vec![
            Column::new(1, u64::MAX as i64).with_unsigned(),
            Column::new(2, -1),
        ];
        let exp: Vec<u8> = vec![
            128, 0, 2, 0, 0, 0, 1, 2, 8, 0, 9, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();

        assert_eq!(buf, exp);
    }

    #[test]
    fn test_encode() {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(12, 2),
            Column::new(33, ScalarValue::Int(None)),
            Column::new(3, 3).with_unsigned(),
            Column::new(8, 32767),
            Column::new(7, b"abc".to_vec()),
            Column::new(9, 1.8),
            Column::new(6, -1.8),
            Column::new(
                13,
                Time::parse_datetime(&mut EvalContext::default(), "2018-01-19 03:14:07", 0, false)
                    .unwrap(),
            ),
            Column::new(14, Decimal::from(1i64)),
            Column::new(15, Json::from_str(r#"{"key":"value"}"#).unwrap()),
            Column::new(16, Duration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
        ];

        let exp = vec![
            128, 0, 11, 0, 1, 0, 1, 3, 6, 7, 8, 9, 12, 13, 14, 15, 16, 33, 2, 0, 3, 0, 11, 0, 14,
            0, 16, 0, 24, 0, 25, 0, 33, 0, 36, 0, 65, 0, 69, 0, 232, 3, 3, 64, 3, 51, 51, 51, 51,
            51, 50, 97, 98, 99, 255, 127, 191, 252, 204, 204, 204, 204, 204, 205, 2, 0, 0, 0, 135,
            51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 3, 0, 12, 22, 0,
            0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 0, 202, 154, 59,
        ];

        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();

        assert_eq!(exp, buf);
    }

    #[test]
    fn test_encode_big() {
        let cols = vec![
            Column::new(1, 1000),
            Column::new(12, 2),
            Column::new(335, ScalarValue::Int(None)),
            Column::new(3, 3),
            Column::new(8, 32767),
        ];
        let exp = vec![
            128, 1, 4, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 79, 1, 0, 0, 2, 0,
            0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 232, 3, 3, 255, 127, 2,
        ];
        let mut buf = vec![];
        buf.write_row(&mut EvalContext::default(), cols).unwrap();

        assert_eq!(exp, buf);
    }
}
