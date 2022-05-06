// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use arrow::{
    array,
    datatypes::{self, DataType, Field},
    record_batch::RecordBatch,
};
use tidb_query_datatype::{codec::Datum, prelude::*, FieldTypeFlag, FieldTypeTp};
use tipb::FieldType;

pub struct Chunk {
    pub data: RecordBatch,
}

impl Chunk {
    pub fn get_datum(&self, col_id: usize, row_id: usize, field_type: &FieldType) -> Datum {
        if self.data.column(col_id).is_null(row_id) {
            return Datum::Null;
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
                        .downcast_ref::<array::UInt64Array>()
                        .unwrap();

                    Datum::U64(data.value(row_id))
                } else {
                    let data = self
                        .data
                        .column(col_id)
                        .as_any()
                        .downcast_ref::<array::Int64Array>()
                        .unwrap();

                    Datum::I64(data.value(row_id))
                }
            }
            FieldTypeTp::Float | FieldTypeTp::Double => {
                let data = self
                    .data
                    .column(col_id)
                    .as_any()
                    .downcast_ref::<array::Float64Array>()
                    .unwrap();
                Datum::F64(data.value(row_id))
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
                        let (f, d) = column.into_u64_array();
                        fields.push(f);
                        arrays.push(d);
                    } else {
                        let (f, d) = column.into_i64_array();
                        fields.push(f);
                        arrays.push(d);
                    }
                }
                FieldTypeTp::Float | FieldTypeTp::Double => {
                    let (f, d) = column.into_f64_array();
                    fields.push(f);
                    arrays.push(d);
                }
                _ => unreachable!(),
            };
        }
        let schema = datatypes::Schema::new(fields);
        let batch = RecordBatch::try_new(Arc::new(schema), arrays).unwrap();
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

    fn into_i64_array(self) -> (Field, Arc<array::Int64Array>) {
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
