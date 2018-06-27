// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

// FIXME(shirly): remove following later
#![allow(dead_code)]
use super::column::Column;
use super::Result;
use coprocessor::codec::Datum;
use tipb::expression::FieldType;

/// Chunk stores multiple rows of data in Apache Arrow format.
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
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    /// Get the number of rows in the chunk.
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].len()
        }
    }

    pub fn append_datum(&mut self, col_idx: usize, v: &Datum) -> Result<()> {
        self.columns[col_idx].append_datum(v)
    }

    /// Get the Row in the chunk with the row index.
    pub fn get_row(&self, idx: usize) -> Option<Row> {
        if idx < self.num_rows() {
            Some(Row::new(self, idx))
        } else {
            None
        }
    }

    // Get the Iterator for Row in the Chunk.
    pub fn iter(&self) -> RowIterator {
        RowIterator::new(self)
    }
}

pub struct Row<'a> {
    c: &'a Chunk,
    idx: usize,
}

impl<'a> Row<'a> {
    pub fn new(c: &'a Chunk, idx: usize) -> Row<'a> {
        Row { c, idx }
    }

    /// Get the row index of Chunk.
    pub fn idx(&self) -> usize {
        self.idx
    }

    /// Get the number of values in the row.
    pub fn len(&self) -> usize {
        self.c.num_cols()
    }

    pub fn get_datum(&self, col_idx: usize, fp: &FieldType) -> Result<Datum> {
        self.c.columns[col_idx].get_datum(self.idx, fp)
    }
}

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
mod test {
    use super::*;
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::*;
    use tipb::expression::FieldType;

    pub fn field_type(tp: u8) -> FieldType {
        let mut fp = FieldType::new();
        fp.set_tp(i32::from(tp));
        fp
    }

    #[test]
    fn test_append_datum() {
        let fields = vec![
            field_type(types::LONG_LONG),
            field_type(types::FLOAT),
            field_type(types::DATETIME),
            field_type(types::DURATION),
            field_type(types::NEW_DECIMAL),
            field_type(types::JSON),
            field_type(types::STRING),
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
}
