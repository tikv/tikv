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
use super::Result;
use super::column::Column;
use coprocessor::codec::Datum;
use std::sync::Arc;
use tipb::expression::FieldType;

const CHUNK_INITIAL_CAPACITY: usize = 32;

/// Chunk stores multiple rows of data in Apache Arrow format.
/// See https://arrow.apache.org/docs/memory_layout.html
/// Values are appended in compact format and can be directly accessed without decoding.
/// When the chunk is done processing, we can reuse the allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
    // num_virtual_rows indicates the number of virtual rows, which have zero columns.
    // It is used only when this Chunk doesn't hold any data, i.e. "len(columns)==0".
    num_virtual_rows: usize,
}

impl Chunk {
    ///new_chunk creates a new chunk with field types.
    pub fn new(tps: &[FieldType]) -> Chunk {
        Chunk::new_with_capacity(tps, CHUNK_INITIAL_CAPACITY)
    }

    pub fn new_with_capacity(tps: &[FieldType], cap: usize) -> Chunk {
        let mut columns = Vec::with_capacity(tps.len());
        for tp in tps {
            columns.push(Column::new(tp, cap));
        }
        Chunk {
            columns,
            num_virtual_rows: 0,
        }
    }

    // reset resets the chunk, so the memory it allocated can be reused.
    // Make sure all the data in the chunk is not used anymore before you reuse this chunk.
    pub fn reset(&mut self) {
        for column in &mut self.columns {
            column.reset();
        }
    }

    // num_cols returns the number of rows in the chunk.
    pub fn num_cols(&self) -> usize {
        self.columns.len()
    }

    // num_rows returns the number of rows in the chunk.
    pub fn num_rows(&self) -> usize {
        if self.columns.is_empty() {
            0
        } else {
            self.columns[0].length()
        }
    }

    pub fn append_datum(&mut self, col_idx: usize, v: &Datum) -> Result<()> {
        self.columns[col_idx].append_datum(v)
    }
}

struct ArcChunk {
    chunk: Arc<Chunk>,
}

impl ArcChunk {
    pub fn new(chunk: Chunk) -> ArcChunk {
        ArcChunk {
            chunk: Arc::new(chunk),
        }
    }

    //get_row gets the Row in the chunk with the row index.
    pub fn get_row(&self, idx: usize) -> Row {
        Row::new(self.chunk.clone(), idx)
    }
    // begin returns the first valid Row in the Chunk.
    pub fn begin(&self) -> Row {
        Row::new(self.chunk.clone(), 0)
    }

    //end returns a Row referring to the past-the-end element in the Chunk.
    pub fn end(&self) -> Row {
        let num_rows = self.chunk.num_rows();
        Row::new(self.chunk.clone(), num_rows)
    }
}

pub struct Row {
    c: Arc<Chunk>,
    idx: usize,
}

impl Row {
    pub fn new(c: Arc<Chunk>, idx: usize) -> Row {
        Row { c, idx }
    }

    //idx returns the row index of Chunk.
    pub fn idx(&self) -> usize {
        self.idx
    }

    //len returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.c.num_cols()
    }

    pub fn is_empty(&self) -> bool {
        self.c.num_cols() == 0
    }

    //next returns the next valid Row in the same Chunk.
    pub fn next(&self) -> Row {
        // TODO should we check the idx?
        Row {
            c: self.c.clone(),
            idx: self.idx + 1,
        }
    }

    pub fn get_datum(&self, col_idx: usize, fp: &FieldType) -> Result<Datum> {
        self.c.columns[col_idx].get_datum(self.idx, fp)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use coprocessor::codec::datum::Datum;
    use coprocessor::codec::mysql::*;
    use tipb::expression::FieldType;

    fn field_type(tp: u8) -> FieldType {
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

        let mut chunk = Chunk::new(&fields);
        for (col_id, val) in data.iter().enumerate() {
            chunk.append_datum(col_id, val).unwrap();
        }
        let arc_chunk = ArcChunk::new(chunk);
        let row = arc_chunk.begin();

        for col_id in 0..row.len() {
            let got = row.get_datum(col_id, &fields[col_id]).unwrap();
            assert_eq!(got, data[col_id]);
        }
    }
}
