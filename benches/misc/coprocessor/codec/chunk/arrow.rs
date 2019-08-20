// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::array;
use arrow::datatypes::{self, DataType, Field};
use arrow::record_batch::RecordBatch;

use tidb_query::codec::Datum;
use tidb_query_datatype::prelude::*;
use tidb_query_datatype::{FieldTypeFlag, FieldTypeTp};
use tipb::FieldType;

pub struct Chunk {
    pub data: RecordBatch,
}

impl Chunk {
    pub fn get_datum(&self, col_id: usize, row_id: usize, field_type: &FieldType) -> Datum {
        if let Some(bitmap) = self.data.column(col_id).validity_bitmap() {
            if !bitmap.is_set(row_id) {
                return Datum::Null;
            }
        }

        match field_type.as_accessor().tp() {
            FieldTypeTp::Tiny
            | FieldTypeTp::Short
            | FieldTypeTp::Int24
            | FieldTypeTp::Long
            | FieldTypeTp::LongLong
            | FieldTypeTp::Year => {
                if field_type
                    .as_accessor()
                    .flag()
                    .contains(FieldTypeFlag::UNSIGNED)
                {
                    let data = self
                        .data
                        .column(col_id)
                        .as_any()
                        .downcast_ref::<array::PrimitiveArray<u64>>()
                        .unwrap();

                    Datum::U64(*data.get(row_id))
                } else {
                    let data = self
                        .data
                        .column(col_id)
                        .as_any()
                        .downcast_ref::<array::PrimitiveArray<i64>>()
                        .unwrap();

                    Datum::I64(*data.get(row_id))
                }
            }
            FieldTypeTp::Float | FieldTypeTp::Double => {
                let data = self
                    .data
                    .column(col_id)
                    .as_any()
                    .downcast_ref::<array::PrimitiveArray<f64>>()
                    .unwrap();
                Datum::F64(*data.get(row_id))
            }
            _ => unreachable!(),
        }
    }
}

pub struct ChunkBuilder {
    columns: Vec<ColumnsBuilder>,
}

impl ChunkBuilder {
    pub fn new(cols: usize, rows: usize) -> ChunkBuilder {
        ChunkBuilder {
            columns: vec![ColumnsBuilder::new(rows); cols],
        }
    }

    pub fn build(self, tps: &[FieldType]) -> Chunk {
        let mut fields = Vec::with_capacity(tps.len());
        let mut arrays: Vec<Arc<dyn array::Array>> = Vec::with_capacity(tps.len());
        for (field_type, column) in tps.iter().zip(self.columns.into_iter()) {
            let (field, data) = match field_type.as_accessor().tp() {
                FieldTypeTp::Tiny
                | FieldTypeTp::Short
                | FieldTypeTp::Int24
                | FieldTypeTp::Long
                | FieldTypeTp::LongLong
                | FieldTypeTp::Year => {
                    if field_type
                        .as_accessor()
                        .flag()
                        .contains(FieldTypeFlag::UNSIGNED)
                    {
                        column.into_u64_array()
                    } else {
                        column.into_i64_array()
                    }
                }
                FieldTypeTp::Float | FieldTypeTp::Double => column.into_f64_array(),
                _ => unreachable!(),
            };
            fields.push(field);
            arrays.push(data);
        }
        let schema = datatypes::Schema::new(fields);
        let batch = RecordBatch::new(Arc::new(schema), arrays);
        Chunk { data: batch }
    }

    pub fn append_datum(&mut self, col_id: usize, data: Datum) {
        self.columns[col_id].append_datum(data)
    }
}

#[derive(Clone)]
pub struct ColumnsBuilder {
    data: Vec<Datum>,
}

impl ColumnsBuilder {
    fn new(rows: usize) -> ColumnsBuilder {
        ColumnsBuilder {
            data: Vec::with_capacity(rows),
        }
    }

    fn append_datum(&mut self, data: Datum) {
        self.data.push(data)
    }

    fn into_i64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::Int64, true);
        let mut data: Vec<Option<i64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::I64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }

    fn into_u64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::UInt64, true);
        let mut data: Vec<Option<u64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::U64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }

    fn into_f64_array(self) -> (Field, Arc<dyn array::Array>) {
        let field = Field::new("", DataType::Float64, true);
        let mut data: Vec<Option<f64>> = Vec::with_capacity(self.data.len());
        for v in self.data {
            match v {
                Datum::Null => data.push(None),
                Datum::F64(v) => data.push(Some(v)),
                _ => unreachable!(),
            }
        }
        (field, Arc::new(array::PrimitiveArray::from(data)))
    }
}
