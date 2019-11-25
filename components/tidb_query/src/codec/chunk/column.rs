// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_datatype::prelude::*;
use tidb_query_datatype::{FieldTypeFlag, FieldTypeTp};

use super::{Error, Result};
use crate::codec::mysql::decimal::DECIMAL_STRUCT_SIZE;
use crate::codec::mysql::{
    Decimal, DecimalDecoder, DecimalEncoder, Duration, DurationDecoder, DurationEncoder, Json,
    JsonDecoder, JsonEncoder, Time, TimeDecoder, TimeEncoder,
};
use crate::codec::Datum;
use crate::expr::EvalContext;

use codec::prelude::*;
#[cfg(test)]
use tikv_util::codec::BytesSlice;

/// `Column` stores the same column data of multi rows in one chunk.
#[derive(Default)]
pub struct Column {
    length: usize,
    null_cnt: usize,
    null_bitmap: Vec<u8>,
    var_offsets: Vec<usize>,
    data: Vec<u8>,
    // if the data's length is fixed, fixed_len should be bigger than 0
    fixed_len: usize,
}

impl Column {
    /// Create the column with a specified type and capacity.
    pub fn new(field_type: &dyn FieldTypeAccessor, init_cap: usize) -> Column {
        match field_type.tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year
            | FieldTypeTp::Double
            | FieldTypeTp::Duration => Column::new_fixed_len(8, init_cap),

            FieldTypeTp::Float => Column::new_fixed_len(4, init_cap),

            FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
                Column::new_fixed_len(20, init_cap)
            }
            FieldTypeTp::NewDecimal => Column::new_fixed_len(DECIMAL_STRUCT_SIZE, init_cap),
            _ => Column::new_var_len_column(init_cap),
        }
    }

    /// Get the datum of one row with the specified type.
    pub fn get_datum(
        &self,
        ctx: &mut EvalContext,
        idx: usize,
        field_type: &dyn FieldTypeAccessor,
    ) -> Result<Datum> {
        if self.is_null(idx) {
            return Ok(Datum::Null);
        }
        let d = match field_type.tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year => {
                if field_type.flag().contains(FieldTypeFlag::UNSIGNED) {
                    Datum::U64(self.get_u64(idx)?)
                } else {
                    Datum::I64(self.get_i64(idx)?)
                }
            }
            FieldTypeTp::Double => Datum::F64(self.get_f64(idx)?),
            FieldTypeTp::Float => Datum::F64(f64::from(self.get_f32(idx)?)),
            FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
                Datum::Time(self.get_time(ctx, idx)?)
            }
            FieldTypeTp::Duration => Datum::Dur(self.get_duration(idx, field_type.decimal())?),
            FieldTypeTp::NewDecimal => Datum::Dec(self.get_decimal(idx)?),
            FieldTypeTp::JSON => Datum::Json(self.get_json(idx)?),
            FieldTypeTp::Enum | FieldTypeTp::Bit | FieldTypeTp::Set => {
                return Err(box_err!(
                    "get datum with {} is not supported yet.",
                    field_type.tp()
                ));
            }
            FieldTypeTp::VarChar
            | FieldTypeTp::VarString
            | FieldTypeTp::String
            | FieldTypeTp::Blob
            | FieldTypeTp::TinyBlob
            | FieldTypeTp::MediumBlob
            | FieldTypeTp::LongBlob => Datum::Bytes(self.get_bytes(idx).to_vec()),
            _ => unreachable!(),
        };
        Ok(d)
    }

    /// Append datum to the column.
    pub fn append_datum(&mut self, data: &Datum) -> Result<()> {
        match data {
            Datum::Null => self.append_null(),
            Datum::I64(v) => self.append_i64(*v),
            Datum::U64(v) => self.append_u64(*v),
            Datum::F64(v) => {
                if self.fixed_len == 4 {
                    self.append_f32(*v as f32)
                } else {
                    self.append_f64(*v)
                }
            }
            Datum::Bytes(ref v) => self.append_bytes(v),
            Datum::Dec(ref v) => self.append_decimal(v),
            Datum::Dur(v) => self.append_duration(*v),
            Datum::Time(v) => self.append_time(*v),
            Datum::Json(ref v) => self.append_json(v),
            _ => Err(box_err!("unsupported datum {:?}", data)),
        }
    }

    /// Create a column with a fixed element length.
    pub fn new_fixed_len(element_len: usize, init_cap: usize) -> Column {
        Column {
            fixed_len: element_len,
            data: Vec::with_capacity(element_len * init_cap),
            null_bitmap: Vec::with_capacity(init_cap / 8),
            ..Default::default()
        }
    }

    /// Create a column with variant element length.
    pub fn new_var_len_column(init_cap: usize) -> Column {
        let mut offsets = Vec::with_capacity(init_cap + 1);
        offsets.push(0);
        Column {
            var_offsets: offsets,
            data: Vec::with_capacity(4 * init_cap),
            null_bitmap: Vec::with_capacity(init_cap / 8),
            ..Default::default()
        }
    }

    /// Return whether the column has a fixed length or not.
    #[inline]
    fn is_fixed(&self) -> bool {
        self.fixed_len > 0
    }

    /// Return the column's fixed length.
    #[inline]
    pub fn get_fixed_len(&self) -> usize {
        self.fixed_len
    }

    /// Reset the column
    pub fn reset(&mut self) {
        self.length = 0;
        self.null_cnt = 0;
        self.null_bitmap.clear();
        if !self.var_offsets.is_empty() {
            // The first offset is always 0, it makes slicing the data easier, we need to keep it.
            self.var_offsets.truncate(1);
        }
        self.data.clear();
    }

    /// Return whether the datum for the row is null or not.
    pub fn is_null(&self, row_idx: usize) -> bool {
        if self.null_cnt == 0 {
            return false;
        }

        if let Some(null_byte) = self.null_bitmap.get(row_idx >> 3) {
            null_byte & (1 << ((row_idx) & 7)) == 0
        } else {
            panic!("index out of range");
        }
    }

    /// Update the null bitmap and count when append a datum.
    /// `on` is false means the datum is null.
    fn append_null_bitmap(&mut self, on: bool) {
        let idx = self.length >> 3;
        if idx >= self.null_bitmap.len() {
            self.null_bitmap.push(0);
        }
        if on {
            let pos = self.length & 7;
            self.null_bitmap[idx] |= 1 << pos;
        } else {
            self.null_cnt += 1;
        }
    }

    /// Append null to the column.
    pub fn append_null(&mut self) -> Result<()> {
        self.append_null_bitmap(false);
        if self.is_fixed() {
            let len = self.fixed_len + self.data.len();
            self.data.resize(len, 0);
        } else {
            let offset = self.var_offsets[self.length];
            self.var_offsets.push(offset);
        }
        self.length += 1;
        Ok(())
    }

    /// Called when the fixed datum has been appended.
    fn finish_append_fixed(&mut self) -> Result<()> {
        self.append_null_bitmap(true);
        self.length += 1;
        self.data.resize(self.length * self.fixed_len, 0);
        Ok(())
    }

    /// Append i64 datum to the column.
    pub fn append_i64(&mut self, v: i64) -> Result<()> {
        self.data.write_i64_le(v)?;
        self.finish_append_fixed()
    }

    /// Get the i64 datum of the row in the column.
    pub fn get_i64(&self, idx: usize) -> Result<i64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_i64_le().map_err(Error::from)
    }

    /// Append u64 datum to the column.
    pub fn append_u64(&mut self, v: u64) -> Result<()> {
        self.data.write_u64_le(v)?;
        self.finish_append_fixed()
    }

    /// Get the u64 datum of the row in the column.
    pub fn get_u64(&self, idx: usize) -> Result<u64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_u64_le().map_err(Error::from)
    }

    /// Append a f64 datum to the column.
    pub fn append_f64(&mut self, v: f64) -> Result<()> {
        self.data.write_f64_le(v)?;
        self.finish_append_fixed()
    }

    /// Append a f32 datum to the column.
    pub fn append_f32(&mut self, v: f32) -> Result<()> {
        self.data.write_f32_le(v)?;
        self.finish_append_fixed()
    }

    /// Get the f64 datum of the row in the column.
    pub fn get_f64(&self, idx: usize) -> Result<f64> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_f64_le().map_err(Error::from)
    }

    /// Get the f32 datum of the row in the column.
    pub fn get_f32(&self, idx: usize) -> Result<f32> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_f32_le().map_err(Error::from)
    }

    /// Called when the variant datum has been appended.
    fn finished_append_var(&mut self) -> Result<()> {
        self.append_null_bitmap(true);
        let offset = self.data.len();
        self.var_offsets.push(offset);
        self.length += 1;
        Ok(())
    }

    /// Append a bytes datum to the column.
    pub fn append_bytes(&mut self, byte: &[u8]) -> Result<()> {
        self.data.extend_from_slice(byte);
        self.finished_append_var()
    }

    /// Get the bytes datum of the row in the column.
    pub fn get_bytes(&self, idx: usize) -> &[u8] {
        let start = self.var_offsets[idx];
        let end = self.var_offsets[idx + 1];
        &self.data[start..end]
    }

    /// Append a time datum to the column.
    pub fn append_time(&mut self, t: Time) -> Result<()> {
        self.data.write_time(t)?;
        self.finish_append_fixed()
    }

    /// Get the time datum of the row in the column.
    pub fn get_time(&self, ctx: &mut EvalContext, idx: usize) -> Result<Time> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_time(ctx)
    }

    /// Append a duration datum to the column.
    pub fn append_duration(&mut self, d: Duration) -> Result<()> {
        self.data.write_duration_to_chunk(d)?;
        self.finish_append_fixed()
    }

    /// Get the duration datum of the row in the column.
    pub fn get_duration(&self, idx: usize, fsp: isize) -> Result<Duration> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_duration_from_chunk(fsp)
    }

    /// Append a decimal datum to the column.
    pub fn append_decimal(&mut self, d: &Decimal) -> Result<()> {
        self.data.write_decimal_to_chunk(d)?;
        self.finish_append_fixed()
    }

    /// Get the decimal datum of the row in the column.
    pub fn get_decimal(&self, idx: usize) -> Result<Decimal> {
        let start = idx * self.fixed_len;
        let end = start + self.fixed_len;
        let mut data = &self.data[start..end];
        data.read_decimal_from_chunk()
    }

    /// Append a json datum to the column.
    pub fn append_json(&mut self, j: &Json) -> Result<()> {
        self.data.write_json(j)?;
        self.finished_append_var()
    }

    /// Get the json datum of the row in the column.
    pub fn get_json(&self, idx: usize) -> Result<Json> {
        let start = self.var_offsets[idx];
        let end = self.var_offsets[idx + 1];
        let mut data = &self.data[start..end];
        data.read_json()
    }

    /// Return the total rows in the column.
    pub fn len(&self) -> usize {
        self.length
    }

    #[cfg(test)]
    pub fn decode(buf: &mut BytesSlice<'_>, tp: &dyn FieldTypeAccessor) -> Result<Column> {
        let length = buf.read_u32_le()? as usize;
        let mut col = Column::new(tp, length);
        col.length = length;
        col.null_cnt = buf.read_u32_le()? as usize;
        let null_length = (col.length + 7) / 8 as usize;
        if col.null_cnt > 0 {
            col.null_bitmap = buf.read_bytes(null_length)?.to_vec();
        } else {
            col.null_bitmap = vec![0xFF; null_length];
        }

        let data_length = if col.is_fixed() {
            col.fixed_len * col.length
        } else {
            col.var_offsets.clear();
            for _ in 0..=length {
                col.var_offsets.push(buf.read_i64_le()? as usize);
            }
            col.var_offsets[col.length]
        };
        col.data = buf.read_bytes(data_length)?.to_vec();
        Ok(col)
    }
}

/// `ColumnEncoder` encodes the column.
pub trait ColumnEncoder: NumberEncoder {
    fn write_column(&mut self, col: &Column) -> Result<()> {
        // length
        self.write_u32_le(col.length as u32)?;
        // null_cnt
        self.write_u32_le(col.null_cnt as u32)?;
        // bitmap
        if col.null_cnt > 0 {
            let length = (col.length + 7) / 8;
            self.write_bytes(&col.null_bitmap[0..length])?;
        }
        // offsets
        if !col.is_fixed() {
            //let length = (col.length+1)*4;
            for v in &col.var_offsets {
                self.write_i64_le(*v as i64)?;
            }
        }
        // data
        self.write_bytes(&col.data)?;
        Ok(())
    }
}

impl<T: BufferWriter> ColumnEncoder for T {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::chunk::tests::field_type;
    use crate::codec::datum::Datum;
    use crate::codec::mysql::*;
    use std::{f64, u64};
    use tipb::FieldType;

    #[test]
    fn test_column_i64() {
        let fields = vec![
            field_type(FieldTypeTp::Tiny),
            field_type(FieldTypeTp::Short),
            field_type(FieldTypeTp::Int24),
            field_type(FieldTypeTp::Long),
            field_type(FieldTypeTp::LongLong),
            field_type(FieldTypeTp::Year),
        ];
        let data = vec![
            Datum::Null,
            Datum::I64(-1),
            Datum::I64(12),
            Datum::I64(1024),
        ];
        let mut ctx = EvalContext::default();
        for field in &fields {
            let mut column = Column::new(field, data.len());
            for v in &data {
                column.append_datum(v).unwrap();
            }

            for (id, expect) in data.iter().enumerate() {
                let get = column.get_datum(&mut ctx, id, field).unwrap();
                assert_eq!(&get, expect);
            }
        }
    }

    #[test]
    fn test_column_u64() {
        let mut fields = vec![
            field_type(FieldTypeTp::Tiny),
            field_type(FieldTypeTp::Short),
            field_type(FieldTypeTp::Int24),
            field_type(FieldTypeTp::Long),
            field_type(FieldTypeTp::LongLong),
            field_type(FieldTypeTp::Year),
        ];
        for field in &mut fields {
            field.set_flag(FieldTypeFlag::UNSIGNED.bits());
        }
        let data = vec![
            Datum::Null,
            Datum::U64(1),
            Datum::U64(u64::MIN),
            Datum::U64(u64::MAX),
        ];
        test_colum_datum(fields, data);
    }

    fn test_colum_datum(fields: Vec<FieldType>, data: Vec<Datum>) {
        let mut ctx = EvalContext::default();
        for field in &fields {
            let mut column = Column::new(field, data.len());
            for v in &data {
                column.append_datum(v).unwrap();
            }
            for (id, expect) in data.iter().enumerate() {
                let get = column.get_datum(&mut ctx, id, field).unwrap();
                assert_eq!(&get, expect);
            }
        }
    }

    #[test]
    fn test_column_f64() {
        let fields = vec![field_type(FieldTypeTp::Double)];
        let data = vec![Datum::Null, Datum::F64(f64::MIN), Datum::F64(f64::MAX)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_f32() {
        let fields = vec![field_type(FieldTypeTp::Float)];
        let data = vec![
            Datum::Null,
            Datum::F64(std::f32::MIN.into()),
            Datum::F64(std::f32::MAX.into()),
        ];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_time() {
        let mut ctx = EvalContext::default();
        let fields = vec![
            field_type(FieldTypeTp::Date),
            field_type(FieldTypeTp::DateTime),
            field_type(FieldTypeTp::Timestamp),
        ];
        let time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let data = vec![Datum::Null, Datum::Time(time)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_duration() {
        let fields = vec![field_type(FieldTypeTp::Duration)];
        let duration = Duration::parse(b"10:11:12", 0).unwrap();
        let data = vec![Datum::Null, Datum::Dur(duration)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_decimal() {
        let fields = vec![field_type(FieldTypeTp::NewDecimal)];
        let dec: Decimal = "1234.00".parse().unwrap();
        let data = vec![Datum::Null, Datum::Dec(dec)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_json() {
        let fields = vec![field_type(FieldTypeTp::JSON)];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();

        let data = vec![Datum::Null, Datum::Json(json)];
        test_colum_datum(fields, data);
    }

    #[test]
    fn test_column_bytes() {
        let fields = vec![
            field_type(FieldTypeTp::VarChar),
            field_type(FieldTypeTp::VarString),
            field_type(FieldTypeTp::String),
            field_type(FieldTypeTp::Blob),
            field_type(FieldTypeTp::TinyBlob),
            field_type(FieldTypeTp::MediumBlob),
            field_type(FieldTypeTp::LongBlob),
        ];
        let data = vec![Datum::Null, Datum::Bytes(b"xxx".to_vec())];
        test_colum_datum(fields, data);
    }
}
