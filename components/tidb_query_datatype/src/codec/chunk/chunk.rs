// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use crate::FieldTypeAccessor;
use codec::buffer::BufferWriter;
use tipb::FieldType;

use super::column::{ChunkColumnEncoder, Column};
use super::Result;
use crate::codec::Datum;

/// `Chunk` stores multiple rows of data.
/// Values are appended in compact format and can be directly accessed without decoding.
/// When the chunk is done processing, we can reuse the allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
}

impl Chunk {
    /// Creates a new Chunk from Chunk columns.
    pub fn from_columns(columns: Vec<Column>) -> Chunk {
        Chunk { columns }
    }

    /// Create a new chunk with field types and capacity.
    pub fn new(field_types: &[FieldType], cap: usize) -> Chunk {
        let mut columns = Vec::with_capacity(field_types.len());
        for ft in field_types {
            columns.push(Column::new(ft.as_accessor().tp(), cap));
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
    pub fn decode(
        buf: &mut tikv_util::codec::BytesSlice<'_>,
        field_types: &[FieldType],
    ) -> Result<Chunk> {
        let mut chunk = Chunk {
            columns: Vec::with_capacity(field_types.len()),
        };
        for ft in field_types {
            chunk
                .columns
                .push(Column::decode(buf, ft.as_accessor().tp())?);
        }
        Ok(chunk)
    }
}

/// `ChunkEncoder` encodes the chunk.
pub trait ChunkEncoder: ChunkColumnEncoder {
    fn write_chunk(&mut self, data: &Chunk) -> Result<()> {
        for col in &data.columns {
            self.write_chunk_column(col)?;
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
    use crate::FieldTypeTp;
    use test::{black_box, Bencher};

    use super::*;
    use crate::codec::batch::LazyBatchColumn;
    use crate::codec::datum::{Datum, DatumEncoder};
    use crate::codec::mysql::*;
    use crate::expr::EvalContext;

    #[test]
    fn test_append_datum() {
        let mut ctx = EvalContext::default();
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Float.into(),
            FieldTypeTp::DateTime.into(),
            FieldTypeTp::Duration.into(),
            FieldTypeTp::NewDecimal.into(),
            FieldTypeTp::JSON.into(),
            FieldTypeTp::String.into(),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let time: Time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let duration = Duration::parse(&mut EvalContext::default(), b"10:11:12", 0).unwrap();
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
    fn test_append_lazy_col() {
        let mut ctx = EvalContext::default();
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::Float.into(),
            FieldTypeTp::DateTime.into(),
            FieldTypeTp::Duration.into(),
            FieldTypeTp::NewDecimal.into(),
            FieldTypeTp::JSON.into(),
            FieldTypeTp::String.into(),
        ];
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let time: Time = Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", -1, true).unwrap();
        let duration = Duration::parse(&mut ctx, b"10:11:12", 0).unwrap();
        let dec: Decimal = "1234.00".parse().unwrap();
        let datum_data = vec![
            Datum::I64(32),
            Datum::F64(32.5),
            Datum::Time(time),
            Datum::Dur(duration),
            Datum::Dec(dec),
            Datum::Json(json),
            Datum::Bytes(b"xxx".to_vec()),
        ];

        let raw_vec_data = datum_data
            .iter()
            .map(|datum| {
                let mut col = LazyBatchColumn::raw_with_capacity(1);
                let mut ctx = EvalContext::default();
                let mut datum_raw = Vec::new();
                datum_raw
                    .write_datum(&mut ctx, &[datum.clone()], false)
                    .unwrap();
                col.mut_raw().push(&datum_raw);
                col
            })
            .collect::<Vec<_>>();

        let mut columns = Vec::new();
        for (col_id, raw_col) in raw_vec_data.iter().enumerate() {
            let column =
                Column::from_raw_datums(&fields[col_id], raw_col.raw(), &[0], &mut ctx).unwrap();
            columns.push(column);
        }
        let chunk = Chunk::from_columns(columns);
        for row in chunk.iter() {
            for col_id in 0..row.len() {
                let got = row.get_datum(col_id, &fields[col_id]).unwrap();
                assert_eq!(got, datum_data[col_id]);
            }

            assert_eq!(row.len(), datum_data.len());
            assert_eq!(row.idx(), 0);
        }
    }

    fn bench_encode_from_raw_datum_impl(b: &mut Bencher, datum: Datum, tp: FieldTypeTp) {
        let mut ctx = EvalContext::default();
        let mut raw_col = LazyBatchColumn::raw_with_capacity(1024);
        let mut logical_rows = Vec::new();
        for i in 0..1024 {
            let mut raw_datum = Vec::new();
            raw_datum
                .write_datum(&mut ctx, &[datum.clone()], false)
                .unwrap();
            raw_col.mut_raw().push(&raw_datum);
            logical_rows.push(i);
        }
        let field_type: FieldType = tp.into();

        b.iter(|| {
            let mut ctx = EvalContext::default();
            let mut v = Vec::new();
            let column = Column::from_raw_datums(
                black_box(&field_type),
                black_box(raw_col.raw()),
                black_box(&logical_rows),
                &mut ctx,
            )
            .unwrap();
            v.write_chunk_column(&column).unwrap();
            black_box(v);
        });
    }

    #[bench]
    fn bench_encode_from_raw_int_datum(b: &mut Bencher) {
        bench_encode_from_raw_datum_impl(b, Datum::I64(32), FieldTypeTp::LongLong);
    }

    #[bench]
    fn bench_encode_from_raw_decimal_datum(b: &mut Bencher) {
        let dec: Decimal = "1234.00".parse().unwrap();
        let datum = Datum::Dec(dec);
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::NewDecimal);
    }

    #[bench]
    fn bench_encode_from_raw_bytes_datum(b: &mut Bencher) {
        let datum = Datum::Bytes("v".repeat(100).into_bytes());
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::String);
    }

    #[bench]
    fn bench_encode_from_raw_json_datum(b: &mut Bencher) {
        let json: Json = r#"{"k1":"v1"}"#.parse().unwrap();
        let datum = Datum::Json(json);
        bench_encode_from_raw_datum_impl(b, datum, FieldTypeTp::JSON);
    }

    #[test]
    fn test_codec() {
        let rows = 10;
        let fields: Vec<FieldType> = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::NewDecimal.into(),
            FieldTypeTp::JSON.into(),
        ];
        let mut chunk = Chunk::new(&fields, rows);

        for row_id in 0..rows {
            let s = format!("{}.123435", row_id);
            let bs = Datum::Bytes(s.as_bytes().to_vec());
            let dec = Datum::Dec(s.parse().unwrap());
            let json = Datum::Json(Json::from_string(s).unwrap());
            chunk.append_datum(0, &Datum::Null).unwrap();
            chunk.append_datum(1, &Datum::I64(row_id as i64)).unwrap();
            chunk.append_datum(2, &bs).unwrap();
            chunk.append_datum(3, &bs).unwrap();
            chunk.append_datum(4, &dec).unwrap();
            chunk.append_datum(5, &json).unwrap();
        }
        let mut data = vec![];
        data.write_chunk(&chunk).unwrap();
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
