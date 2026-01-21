// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, sync::Arc};

use parquet::{
    basic::{Compression, ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::WriterProperties,
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::{Type, TypePtr},
};

use crate::{
    Error, Result,
    schema::{ColumnParquetType, ColumnSchema, TableSchema},
};

#[derive(Debug)]
pub enum CellValue {
    Int64(i64),
    Double(f64),
    Bytes(Vec<u8>),
}

pub struct ParquetWriter {
    writer: SerializedFileWriter<File>,
    columns: Vec<ColumnState>,
    row_group_size: usize,
    rows_in_group: usize,
    pub total_rows: u64,
}

impl ParquetWriter {
    pub fn try_new(
        table: &TableSchema,
        sink: File,
        compression: Compression,
        row_group_size: usize,
    ) -> Result<Self> {
        let schema = build_parquet_schema(table)?;
        let props = Arc::new(
            WriterProperties::builder()
                .set_compression(compression)
                .build(),
        );
        let writer = SerializedFileWriter::new(sink, schema, props)?;
        let columns = table
            .columns
            .iter()
            .map(ColumnState::from_schema)
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            writer,
            columns,
            row_group_size,
            rows_in_group: 0,
            total_rows: 0,
        })
    }

    pub fn write_row(&mut self, values: Vec<Option<CellValue>>) -> Result<()> {
        for (column, value) in self.columns.iter_mut().zip(values.into_iter()) {
            column.push(value)?;
        }
        self.rows_in_group += 1;
        self.total_rows += 1;
        if self.rows_in_group >= self.row_group_size {
            self.flush_row_group()?;
        }
        Ok(())
    }

    pub fn close(mut self) -> Result<()> {
        if self.rows_in_group > 0 {
            self.flush_row_group()?;
        }
        self.writer.close()?;
        Ok(())
    }

    fn flush_row_group(&mut self) -> Result<()> {
        if self.rows_in_group == 0 {
            return Ok(());
        }
        let mut row_group = self.writer.next_row_group()?;
        for column in &mut self.columns {
            column.flush(&mut row_group)?;
        }
        row_group.close()?;
        self.rows_in_group = 0;
        Ok(())
    }
}

fn build_parquet_schema(table: &TableSchema) -> Result<TypePtr> {
    let mut fields = Vec::with_capacity(table.columns.len());
    for column in &table.columns {
        let mut builder = match column.parquet_type {
            ColumnParquetType::Int64 => {
                Type::primitive_type_builder(column.name.as_str(), PhysicalType::INT64)
                    .with_converted_type(ConvertedType::INT_64)
            }
            ColumnParquetType::Double => {
                Type::primitive_type_builder(column.name.as_str(), PhysicalType::DOUBLE)
            }
            ColumnParquetType::Utf8 => {
                Type::primitive_type_builder(column.name.as_str(), PhysicalType::BYTE_ARRAY)
                    .with_converted_type(ConvertedType::UTF8)
            }
            ColumnParquetType::Binary => {
                Type::primitive_type_builder(column.name.as_str(), PhysicalType::BYTE_ARRAY)
            }
        };
        builder = match column.parquet_type {
            ColumnParquetType::Binary | ColumnParquetType::Utf8 => builder,
            _ => builder,
        };
        builder = builder.with_repetition(if column.nullable {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        });
        fields.push(Arc::new(
            builder.build().map_err(|e| Error::Other(Box::new(e)))?,
        ));
    }
    let schema = Type::group_type_builder("tidb_table")
        .with_fields(fields)
        .build()
        .map_err(|e| Error::Other(Box::new(e)))?;
    Ok(schema.into())
}

struct ColumnState {
    parquet_type: ColumnParquetType,
    nullable: bool,
    values_int64: Vec<i64>,
    values_double: Vec<f64>,
    values_binary: Vec<ByteArray>,
    def_levels: Vec<i16>,
}

impl ColumnState {
    fn from_schema(column: &ColumnSchema) -> Result<Self> {
        Ok(Self {
            parquet_type: column.parquet_type.clone(),
            nullable: column.nullable,
            values_int64: Vec::new(),
            values_double: Vec::new(),
            values_binary: Vec::new(),
            def_levels: Vec::new(),
        })
    }

    fn push(&mut self, value: Option<CellValue>) -> Result<()> {
        match (&self.parquet_type, value) {
            (ColumnParquetType::Int64, Some(CellValue::Int64(v))) => {
                self.values_int64.push(v);
                self.push_level(true);
            }
            (ColumnParquetType::Double, Some(CellValue::Double(v))) => {
                self.values_double.push(v);
                self.push_level(true);
            }
            (ColumnParquetType::Utf8 | ColumnParquetType::Binary, Some(CellValue::Bytes(v))) => {
                self.values_binary.push(ByteArray::from(v));
                self.push_level(true);
            }
            (_, None) => {
                if self.nullable {
                    self.def_levels.push(0);
                } else {
                    return Err(Error::Schema("encountered NULL in NOT NULL column".into()));
                }
            }
            (expected, other) => {
                return Err(Error::Schema(format!(
                    "type mismatch: expected {:?}, got {:?}",
                    expected, other
                )));
            }
        }
        Ok(())
    }

    fn flush(&mut self, row_group: &mut SerializedRowGroupWriter<'_, File>) -> Result<()> {
        if let Some(mut writer) = row_group.next_column()? {
            let result = match (writer.untyped(), &self.parquet_type) {
                (ColumnWriter::Int64ColumnWriter(w), ColumnParquetType::Int64) => {
                    let levels = self.levels();
                    w.write_batch(&self.values_int64, levels, None)
                }
                (ColumnWriter::DoubleColumnWriter(w), ColumnParquetType::Double) => {
                    let levels = self.levels();
                    w.write_batch(&self.values_double, levels, None)
                }
                (ColumnWriter::ByteArrayColumnWriter(w), ColumnParquetType::Utf8)
                | (ColumnWriter::ByteArrayColumnWriter(w), ColumnParquetType::Binary) => {
                    let levels = self.levels();
                    w.write_batch(&self.values_binary, levels, None)
                }
                (_, ty) => {
                    return Err(Error::Schema(format!(
                        "column writer mismatch for {:?}",
                        ty
                    )))
                }
            };
            result.map_err(Error::from)?;
            writer.close()?;
        }
        self.clear();
        Ok(())
    }

    fn clear(&mut self) {
        self.values_int64.clear();
        self.values_double.clear();
        self.values_binary.clear();
        self.def_levels.clear();
    }

    fn push_level(&mut self, has_value: bool) {
        if self.nullable {
            self.def_levels.push(if has_value { 1 } else { 0 });
        }
    }

    fn levels(&self) -> Option<&[i16]> {
        if self.nullable {
            Some(self.def_levels.as_slice())
        } else {
            None
        }
    }
}
