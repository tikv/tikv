// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::{
    mysql::{decimal::DecimalEncoder, json::JsonEncoder},
    Datum, Error, Result,
};
use codec::prelude::*;

use super::{BoundedI64, BoundedU64};

const OFFSET_SIZE: usize = 2;
const LARGE_OFFSET_SIZE: usize = 4;
const ID_SIZE: usize = 1;
const LARGE_ID_SIZE: usize = 4;

// the row format is:
// | version | flag | number_of_non_null_values | number_of_null_values | non_null_value_ids | null_value_ids | value_offsets | values
// short spec of each field, check https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md for details
// version: 1 byte
// flag: 1 byte, when there's id greater than 255, value is 1, otherwise 0
// number of non-null values: 2 bytes
// number of null values: 2 bytes
// column ids:  ids of non-null values + ids of null values, when flag == 1 (big), id is 4 bytes, otherwise 1 byte
// non-null values offset: when big, offset is 4 bytes, otherwise 2 bytes
pub fn encode(datums: Vec<Datum>, ids: &[i64]) -> Result<Vec<u8>> {
    let row = Row::new(datums, ids);
    let mut wtr = Vec::with_capacity(row.approx_size());
    wtr.write_row(row)?;
    Ok(wtr)
}

trait RowEncoder: NumberEncoder {
    fn write_row(&mut self, row: Row) -> Result<()> {
        let mut offset_wtr = Vec::with_capacity(row.offsets_len());
        let mut value_wtr = Vec::with_capacity(row.value_approx_size);

        self.write_u8(super::CODEC_VERSION)?;
        self.write_flag(row.is_big)?;
        self.write_u16_le(row.non_null_cols.len() as u16)?;
        self.write_u16_le(row.null_cols.len() as u16)?;

        for (datum, id) in row.non_null_cols {
            self.write_id(row.is_big, id)?;
            value_wtr.write_datum(datum)?;
            offset_wtr.write_offset(row.is_big, value_wtr.len())?;
        }
        for (_, id) in row.null_cols {
            self.write_id(row.is_big, id)?;
        }
        self.write_bytes(&offset_wtr)?;
        self.write_bytes(&value_wtr)?;
        Ok(())
    }

    #[inline]
    fn write_flag(&mut self, is_big: bool) -> codec::Result<()> {
        self.write_u8(if is_big { 1 } else { 0 })
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
impl RowEncoder for Vec<u8> {}

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
            Datum::F64(_) | Datum::Time(_) | Datum::Dur(_) | Datum::Null => unreachable!(),
            Datum::Min | Datum::Max => unimplemented!("not supported yet"),
        }
    }

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_i64(&mut self, v: i64) -> codec::Result<()> {
        match v {
            i8::MIN_I64..=i8::MAX_I64 => self.write_u8(v as i8 as u8),
            i16::MIN_I64..=i16::MAX_I64 => self.write_i16_le(v as i16),
            i32::MIN_I64..=i32::MAX_I64 => self.write_i32_le(v as i32),
            _ => self.write_i64_le(v),
        }
    }

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_u64(&mut self, v: u64) -> codec::Result<()> {
        match v {
            u8::MIN_U64..=u8::MAX_U64 => self.write_u8(v as u8),
            u16::MIN_U64..=u16::MAX_U64 => self.write_u16_le(v as u16),
            u32::MIN_U64..=u32::MAX_U64 => self.write_u32_le(v as u32),
            _ => self.write_u64_le(v),
        }
    }
}
impl DatumEncoder for Vec<u8> {}

struct Row {
    is_big: bool,
    non_null_cols: Vec<(Datum, i64)>,
    null_cols: Vec<(Datum, i64)>,
    value_approx_size: usize,
}

impl Row {
    fn new(datums: Vec<Datum>, ids: &[i64]) -> Row {
        let mut is_big = false;

        let mut non_null_cols = Vec::with_capacity(ids.len());
        let mut null_cols = Vec::with_capacity(ids.len());

        let mut value_approx_size = 0;
        for (datum, id) in datums.into_iter().zip(ids) {
            let datum = flatten(datum);
            if *id > 255 {
                is_big = true;
            }
            value_approx_size += datum_size(&datum);

            if datum == Datum::Null {
                null_cols.push((datum, *id));
            } else {
                non_null_cols.push((datum, *id));
            }
        }
        non_null_cols.sort_by_key(|(_, id)| *id);
        null_cols.sort_by_key(|(_, id)| *id);

        Row {
            is_big,
            non_null_cols,
            null_cols,
            value_approx_size,
        }
    }

    // version(1) + flag(1) + columns_length(4) + ids_length + values_offset_length + values_length
    #[inline]
    fn approx_size(&self) -> usize {
        6 + (self.non_null_cols.len() + self.null_cols.len()) * self.id_size()
            + self.non_null_cols.len() * self.offset_size()
            + self.value_approx_size
    }

    #[inline]
    fn id_size(&self) -> usize {
        if self.is_big {
            LARGE_ID_SIZE
        } else {
            ID_SIZE
        }
    }

    #[inline]
    fn offset_size(&self) -> usize {
        if self.is_big {
            LARGE_OFFSET_SIZE
        } else {
            OFFSET_SIZE
        }
    }

    #[inline]
    fn offsets_len(&self) -> usize {
        self.offset_size() * self.non_null_cols.len()
    }
}

#[inline]
fn datum_size(datum: &Datum) -> usize {
    match *datum {
        Datum::I64(i) => i64_size(i),
        Datum::U64(u) => u64_size(u),
        Datum::Bytes(ref bs) => bs.len(),
        Datum::Dec(ref d) => d.approximate_encoded_size(),
        Datum::Json(ref j) => j.binary_len(),
        Datum::F64(_) | Datum::Time(_) | Datum::Dur(_) => {
            unreachable!("these types should be flattened as numbers before")
        }
        _ => 0,
    }
}

#[allow(clippy::match_overlapping_arm)]
#[inline]
fn i64_size(i: i64) -> usize {
    match i {
        i8::MIN_I64..=i8::MAX_I64 => 1,
        i16::MIN_I64..=i16::MAX_I64 => 2,
        i32::MIN_I64..=i32::MAX_I64 => 4,
        _ => 8,
    }
}

#[allow(clippy::match_overlapping_arm)]
#[inline]
fn u64_size(u: u64) -> usize {
    match u {
        u8::MIN_U64..=u8::MAX_U64 => 1,
        u16::MIN_U64..=u16::MAX_U64 => 2,
        u32::MIN_U64..=u32::MAX_U64 => 4,
        _ => 8,
    }
}

#[inline]
fn flatten(datum: Datum) -> Datum {
    match datum {
        Datum::F64(f) => Datum::U64(f.to_bits()),
        Datum::Time(t) => Datum::U64(t.to_packed_u64()),
        Datum::Dur(d) => Datum::I64(d.to_nanos()),
        _ => datum,
    }
}

#[cfg(test)]
mod tests {
    use super::encode;
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

        assert_eq!(exp, encode(datums, &ids).unwrap());
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

        assert_eq!(exp, encode(datums, &ids).unwrap());
    }
}
