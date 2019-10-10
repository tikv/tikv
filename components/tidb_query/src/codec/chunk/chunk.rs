// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use super::column::{Column, ColumnEncoder};
use super::Result;
use crate::codec::data_type::VectorValue;
use crate::codec::Datum;
use codec::buffer::BufferWriter;
use tidb_query_datatype::FieldTypeAccessor;
use tidb_query_datatype::FieldTypeFlag;
#[cfg(test)]
use tikv_util::codec::BytesSlice;
use tipb::FieldType;

/// `Chunk` stores multiple rows of data in Apache Arrow format.
/// See https://arrow.apache.org/docs/memory_layout.html
/// Values are appended in compact format and can be directly accessed without decoding.
/// When the chunk is done processing, we can reuse the allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
}

impl Chunk {
    /// Create a new chunk with field types and capacity.
    pub fn new(tps: &[FieldType], cap: usize) -> Chunk {
        let mut columns = Vec::with_capacity(tps.len());
        for tp in tps {
            columns.push(Column::new(tp, cap));
        }
        Chunk { columns }
    }

    /// Reset the chunk, so the memory it allocated can be reused.
    /// Make sure all the data in the chunk is not used anymore before you reuse this chunk.
    pub fn reset(&mut self) {
        for column in &mut self.columns {
            column.reset();
        }
    }

    /// Get the number of rows in the chunk.
    #[inline]
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows in the chunk.
    #[inline]
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    /// Append a datum to the column
    #[inline]
    pub fn append_datum(&mut self, col_idx: usize, v: &Datum) -> Result<()> {
        self.columns[col_idx].append_datum(v)
    }

    /// Append a datum from vec to the column
    #[inline]
    pub fn append_vec(
        &mut self,
        row_indexes: &[usize],
        field_type: &FieldType,
        vec: &VectorValue,
        column_index: usize,
    ) -> Result<()> {
        let col = &mut self.columns[column_index];
        match vec {
            VectorValue::Int(ref vec) => {
                if field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    for &row_index in row_indexes {
                        match &vec[row_index] {
                            None => {
                                col.append_null().unwrap();
                            }
                            Some(val) => {
                                col.append_u64(*val as u64).unwrap();
                            }
                        }
                    }
                } else {
                    for &row_index in row_indexes {
                        match &vec[row_index] {
                            None => {
                                col.append_null().unwrap();
                            }
                            Some(val) => {
                                col.append_i64(*val).unwrap();
                            }
                        }
                    }
                }
            }
            VectorValue::Real(ref vec) => {
                if col.get_fixed_len() == 4 {
                    for &row_index in row_indexes {
                        match &vec[row_index] {
                            None => {
                                col.append_null().unwrap();
                            }
                            Some(val) => {
                                col.append_f32(f64::from(*val) as f32).unwrap();
                            }
                        }
                    }
                } else {
                    for &row_index in row_indexes {
                        match &vec[row_index] {
                            None => {
                                col.append_null().unwrap();
                            }
                            Some(val) => {
                                col.append_f64(f64::from(*val)).unwrap();
                            }
                        }
                    }
                }
            }
            VectorValue::Decimal(ref vec) => {
                for &row_index in row_indexes {
                    match &vec[row_index] {
                        None => {
                            col.append_null().unwrap();
                        }
                        Some(val) => {
                            col.append_decimal(&val).unwrap();
                        }
                    }
                }
            }
            VectorValue::Bytes(ref vec) => {
                for &row_index in row_indexes {
                    match &vec[row_index] {
                        None => {
                            col.append_null().unwrap();
                        }
                        Some(val) => {
                            col.append_bytes(&val).unwrap();
                        }
                    }
                }
            }
            VectorValue::DateTime(ref vec) => {
                for &row_index in row_indexes {
                    match &vec[row_index] {
                        None => {
                            col.append_null().unwrap();
                        }
                        Some(val) => {
                            col.append_time(&val).unwrap();
                        }
                    }
                }
            }
            VectorValue::Duration(ref vec) => {
                for &row_index in row_indexes {
                    match &vec[row_index] {
                        None => {
                            col.append_null().unwrap();
                        }
                        Some(val) => {
                            col.append_duration(*val).unwrap();
                        }
                    }
                }
            }
            VectorValue::Json(ref vec) => {
                for &row_index in row_indexes {
                    match &vec[row_index] {
                        None => {
                            col.append_null().unwrap();
                        }
                        Some(val) => col.append_json(&val).unwrap(),
                    }
                }
            }
        }
        Ok(())
    }

    /// Get the Row in the chunk with the row index.
    #[inline]
    pub fn get_row(&self, idx: usize) -> Option<Row<'_>> {
        if idx < self.num_rows() {
            Some(Row::new(self, idx))
        } else {
            None
        }
    }

    // Get the Iterator for Row in the Chunk.
    #[inline]
    pub fn iter(&self) -> RowIterator<'_> {
        RowIterator::new(self)
    }

    #[cfg(test)]
    pub fn decode(buf: &mut BytesSlice<'_>, tps: &[FieldType]) -> Result<Chunk> {
        let mut chunk = Chunk {
            columns: Vec::with_capacity(tps.len()),
        };
        for tp in tps {
            chunk.columns.push(Column::decode(buf, tp)?);
        }
        Ok(chunk)
    }
}

/// `ChunkEncoder` encodes the chunk.
pub trait ChunkEncoder: ColumnEncoder {
    fn encode_chunk(&mut self, data: &Chunk) -> Result<()> {
        for col in &data.columns {
            self.write_column(col)?;
        }
        Ok(())
    }
}

impl<T: BufferWriter> ChunkEncoder for T {}

/// `Row` represents one row in the chunk.
pub struct Row<'a> {
    c: &'a Chunk,
    idx: usize,
}

impl<'a> Row<'a> {
    pub fn new(c: &'a Chunk, idx: usize) -> Row<'a> {
        Row { c, idx }
    }

    /// Get the row index of Chunk.
    #[inline]
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Get the number of values in the row.
    #[inline]
    pub fn len(&self) -> usize {
        self.c.num_cols()
    }

    /// Get the datum of the column with the specified type in the row.
    #[inline]
    pub fn get_datum(&self, col_idx: usize, fp: &FieldType) -> Result<Datum> {
        self.c.columns[col_idx].get_datum(self.idx, fp)
    }
}

/// `RowIterator` is an iterator to iterate the row.
pub struct RowIterator<'a> {
    c: &'a Chunk,
    idx: usize,
}

impl<'a> RowIterator<'a> {
    fn new(chunk: &'a Chunk) -> RowIterator<'a> {
        RowIterator { c: chunk, idx: 0 }
    }
}

impl<'a> Iterator for RowIterator<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Row<'a>> {
        if self.idx < self.c.num_rows() {
            let row = Row::new(self.c, self.idx);
            self.idx += 1;
            Some(row)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use tidb_query_datatype::FieldTypeTp;

    use super::*;
    use crate::codec::chunk::tests::*;
    use crate::codec::datum::Datum;
    use crate::codec::mysql::*;

    #[test]
    fn test_append_datum() {
        let fields = vec![
            field_type(FieldTypeTp::LongLong),
            field_type(FieldTypeTp::Float),
            field_type(FieldTypeTp::DateTime),
            field_type(FieldTypeTp::Duration),
            field_type(FieldTypeTp::NewDecimal),
            field_type(FieldTypeTp::JSON),
            field_type(FieldTypeTp::String),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let time: Time = Time::parse_utc_datetime("2012-12-31 11:30:45", -1).unwrap();
        let duration = Duration::parse(b"10:11:12", 0).unwrap();
        let dec: Decimal = "1234.00".parse().unwrap();
        let data = vec![
            Datum::I64(32),
            Datum::F64(32.5),
            Datum::Time(time),
            Datum::Dur(duration),
            Datum::Dec(dec),
            Datum::Json(json),
            Datum::Bytes(b"xxx".to_vec()),
        ];

        let mut chunk = Chunk::new(&fields, 10);
        for (col_id, val) in data.iter().enumerate() {
            chunk.append_datum(col_id, val).unwrap();
        }
        for row in chunk.iter() {
            for col_id in 0..row.len() {
                let got = row.get_datum(col_id, &fields[col_id]).unwrap();
                assert_eq!(got, data[col_id]);
            }

            assert_eq!(row.len(), data.len());
            assert_eq!(row.idx(), 0);
        }
    }

    #[test]
    fn test_codec() {
        let rows = 10;
        let fields = vec![
            field_type(FieldTypeTp::LongLong),
            field_type(FieldTypeTp::LongLong),
            field_type(FieldTypeTp::VarChar),
            field_type(FieldTypeTp::VarChar),
            field_type(FieldTypeTp::NewDecimal),
            field_type(FieldTypeTp::JSON),
        ];
        let mut chunk = Chunk::new(&fields, rows);

        for row_id in 0..rows {
            let s = format!("{}.123435", row_id);
            let bs = Datum::Bytes(s.as_bytes().to_vec());
            let dec = Datum::Dec(s.parse().unwrap());
            let json = Datum::Json(Json::String(s));
            chunk.append_datum(0, &Datum::Null).unwrap();
            chunk.append_datum(1, &Datum::I64(row_id as i64)).unwrap();
            chunk.append_datum(2, &bs).unwrap();
            chunk.append_datum(3, &bs).unwrap();
            chunk.append_datum(4, &dec).unwrap();
            chunk.append_datum(5, &json).unwrap();
        }
        let mut data = vec![];
        data.encode_chunk(&chunk).unwrap();
        let got = Chunk::decode(&mut data.as_slice(), &fields).unwrap();
        assert_eq!(got.num_cols(), fields.len());
        assert_eq!(got.num_rows(), rows);
        for row_id in 0..rows {
            for (col_id, tp) in fields.iter().enumerate() {
                let dt = got.get_row(row_id).unwrap().get_datum(col_id, tp).unwrap();
                let exp = chunk
                    .get_row(row_id)
                    .unwrap()
                    .get_datum(col_id, tp)
                    .unwrap();
                assert_eq!(dt, exp);
            }
        }
    }
}
