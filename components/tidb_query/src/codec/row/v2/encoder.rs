// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::{
    mysql::{decimal::DecimalEncoder, json::JsonEncoder},
    Datum, Error, Result,
};
use codec::prelude::*;

const OFFSET_SIZE: usize = 2;
const LARGE_OFFSET_SIZE: usize = 4;
const ID_SIZE: usize = 1;
const LARGE_ID_SIZE: usize = 4;

// the row format is:
// | version | flag | number_of_not_null_values | number_of_null_values | not_null_value_ids | null_value_ids | value_offsets | values
// short spec of each field, check https://github.com/pingcap/tidb/blob/master/docs/design/2018-07-19-row-format.md for details
// version: 1 byte
// flag: 1 byte, when there's id greater than 255, value is 1, otherwise 0
// number of not null values: 2 bytes
// number of null values: 2 bytes
// column ids:  ids of not null values + ids of null values, when flag == 1 (large), id is 4 bytes, otherwise 1 byte
// not null values offset: when large, offset is 4 bytes, otherwise 2 bytes
pub fn encode(datums: Vec<Datum>, ids: &[i64]) -> Result<Vec<u8>> {
    let row = Row::new_row(datums, ids);
    let mut wtr = Vec::with_capacity(row.approx_size());
    wtr.encode_row(row)?;
    Ok(wtr)
}

trait DatumEncoder: NumberEncoder {
    // encode_json & encode_decimal does not use codec::BufferWriter yet
    fn encode_datum(&mut self, datum: Datum) -> Result<()>;

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_i64(&mut self, v: i64) -> codec::Result<()> {
        match v {
            -128..=127 => self.write_u8(v as i8 as u8),
            -32768..=32767 => self.write_i16_le(v as i16),
            -2_147_483_648..=2_147_483_647 => self.write_i32_le(v as i32),
            _ => self.write_i64_le(v),
        }
    }

    #[inline]
    fn encode_u64(&mut self, v: u64) -> codec::Result<()> {
        match v {
            0..=255 => self.write_u8(v as u8),
            256..=65535 => self.write_u16_le(v as u16),
            65536..=4_294_967_295 => self.write_u32_le(v as u32),
            _ => self.write_u64_le(v),
        }
    }
}

impl DatumEncoder for Vec<u8> {
    #[inline]
    fn encode_datum(&mut self, datum: Datum) -> Result<()> {
        match datum {
            Datum::I64(i) => self.encode_i64(i).map_err(Error::from),
            Datum::U64(u) => self.encode_u64(u).map_err(Error::from),
            Datum::Bytes(b) => self.write_bytes(&b).map_err(Error::from),
            Datum::Dec(d) => {
                let (prec, frac) = d.prec_and_frac();
                self.encode_decimal(&d, prec, frac)?;
                Ok(())
            }
            Datum::Json(j) => self.encode_json(&j),
            Datum::F64(_) | Datum::Time(_) | Datum::Dur(_) | Datum::Null => unreachable!(),
            Datum::Min | Datum::Max => unimplemented!("not supported yet"),
        }
    }
}

struct Row {
    is_large: bool,
    not_null_cols: Vec<(Datum, i64)>,
    null_cols: Vec<(Datum, i64)>,
    value_approx_size: usize,
}

impl Row {
    fn new_row(datums: Vec<Datum>, ids: &[i64]) -> Row {
        let mut is_large = false;

        let mut not_null_cols = Vec::with_capacity(ids.len());
        let mut null_cols = Vec::with_capacity(ids.len());

        let mut value_approx_size = 0;
        for (datum, id) in datums.into_iter().zip(ids) {
            let datum = flatten(datum);
            if *id > super::LARGE_ID {
                is_large = true;
            }
            value_approx_size += datum_size(&datum);

            if datum == Datum::Null {
                null_cols.push((datum, *id));
            } else {
                not_null_cols.push((datum, *id));
            }
        }
        not_null_cols.sort_by_key(|(_, id)| *id);
        null_cols.sort_by_key(|(_, id)| *id);

        Row {
            is_large,
            not_null_cols,
            null_cols,
            value_approx_size,
        }
    }

    // version(1) + flag(1) + columns_length(4) + ids_length + values_offset_length + values_length
    #[inline]
    fn approx_size(&self) -> usize {
        6 + (self.not_null_cols.len() + self.null_cols.len()) * self.id_size()
            + self.not_null_cols.len() * self.offset_size()
            + self.value_approx_size
    }

    #[inline]
    fn id_size(&self) -> usize {
        if self.is_large {
            LARGE_ID_SIZE
        } else {
            ID_SIZE
        }
    }

    #[inline]
    fn offset_size(&self) -> usize {
        if self.is_large {
            LARGE_OFFSET_SIZE
        } else {
            OFFSET_SIZE
        }
    }

    #[inline]
    fn offsets_len(&self) -> usize {
        self.offset_size() * self.not_null_cols.len()
    }
}

trait RowEncoder: NumberEncoder {
    fn encode_row(&mut self, row: Row) -> Result<()> {
        let mut offset_wtr = Vec::with_capacity(row.offsets_len());
        let mut value_wtr = Vec::with_capacity(row.value_approx_size);

        self.write_u8(super::CODEC_VERSION)?;
        self.encode_flag(row.is_large)?;
        self.write_u16_le(row.not_null_cols.len() as u16)?;
        self.write_u16_le(row.null_cols.len() as u16)?;

        // write ids first, then append offsets and values
        for (datum, id) in row.not_null_cols {
            self.encode_id(row.is_large, id)?;
            value_wtr.encode_datum(datum)?;
            offset_wtr.encode_offset(row.is_large, value_wtr.len())?;
        }
        for (_, id) in row.null_cols {
            self.encode_id(row.is_large, id)?;
        }
        self.write_bytes(&offset_wtr)?;
        self.write_bytes(&value_wtr)?;
        Ok(())
    }

    #[inline]
    fn encode_flag(&mut self, is_large: bool) -> codec::Result<()> {
        self.write_u8(if is_large { 1 } else { 0 })
    }

    #[inline]
    fn encode_id(&mut self, is_large: bool, id: i64) -> codec::Result<()> {
        if is_large {
            self.write_u32_le(id as u32)
        } else {
            self.write_u8(id as u8)
        }
    }

    #[inline]
    fn encode_offset(&mut self, is_large: bool, offset: usize) -> codec::Result<()> {
        if is_large {
            self.write_u32_le(offset as u32)
        } else {
            self.write_u16_le(offset as u16)
        }
    }
}
impl RowEncoder for Vec<u8> {}

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
        -128..=127 => 1,
        -32768..=32767 => 2,
        -2_147_483_648..=2_147_483_647 => 4,
        _ => 8,
    }
}

#[inline]
fn u64_size(u: u64) -> usize {
    match u {
        0..=255 => 1,
        256..=65535 => 2,
        65536..=4_294_967_295 => 4,
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
        mysql::{duration::NANOS_PER_SEC, Duration as MysqlDuration, Json, Time},
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
            Datum::Dur(MysqlDuration::from_nanos(NANOS_PER_SEC, 0).unwrap()),
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
    fn test_encode_large() {
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
