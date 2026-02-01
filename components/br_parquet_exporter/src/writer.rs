// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use std::{fs::File, sync::Arc};

use parquet::{
    basic::{Compression, ConvertedType, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    data_type::ByteArray,
    file::{
        properties::{EnabledStatistics, WriterProperties},
        writer::{SerializedFileWriter, SerializedRowGroupWriter},
    },
    schema::types::{ColumnPath, Type, TypePtr},
};

use crate::{
    Error, IcebergFileMetrics, Result,
    schema::{ColumnKind, ColumnParquetType, ColumnSchema, TableSchema},
};

/// A typed value written to Parquet.
#[derive(Debug)]
pub enum CellValue {
    Int64(i64),
    Double(f64),
    Bytes(Vec<u8>),
}

/// Parquet writer for a single TiDB table export.
pub struct ParquetWriter {
    writer: SerializedFileWriter<File>,
    columns: Vec<ColumnState>,
    row_group_size: usize,
    rows_in_group: usize,
    pub total_rows: u64,
}

impl ParquetWriter {
    /// Creates a new writer for `table` and writes Parquet data into `sink`.
    pub fn try_new(
        table: &TableSchema,
        sink: File,
        compression: Compression,
        row_group_size: usize,
        bloom_filter: bool,
    ) -> Result<Self> {
        let schema = build_parquet_schema(table)?;
        let write_batch_size = row_group_size.clamp(1024, 8192);
        let mut builder = WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(true)
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_max_row_group_size(row_group_size)
            .set_write_batch_size(write_batch_size);
        if bloom_filter {
            let ndv = row_group_size as u64;
            for column in &table.columns {
                if should_enable_bloom_filter(column) {
                    builder = builder
                        .set_column_bloom_filter_ndv(ColumnPath::from(column.name.as_str()), ndv);
                }
            }
        }
        let props = Arc::new(builder.build());
        let capacity = row_group_size.min(8192);
        let writer = SerializedFileWriter::new(sink, schema, props)?;
        let columns = table
            .columns
            .iter()
            .map(|column| ColumnState::from_schema(column, capacity))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            writer,
            columns,
            row_group_size,
            rows_in_group: 0,
            total_rows: 0,
        })
    }

    /// Returns Iceberg metrics for all rows written so far.
    pub fn iceberg_metrics(&self) -> IcebergFileMetrics {
        let mut metrics = IcebergFileMetrics::default();
        for column in &self.columns {
            let field_id = column.field_id;
            let null_count = column.metrics.null_count;
            let total_rows = self.total_rows;
            metrics.value_counts.insert(field_id, total_rows);
            metrics.null_value_counts.insert(field_id, null_count);

            if let Some(lower) = column.metrics.lower_bound_bytes() {
                metrics.lower_bounds.insert(field_id, lower);
            }
            if let Some(upper) = column.metrics.upper_bound_bytes() {
                metrics.upper_bounds.insert(field_id, upper);
            }
        }
        metrics
    }

    /// Writes a projected row to the current row group.
    pub fn write_row(&mut self, values: Vec<Option<CellValue>>) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(Error::Invalid(format!(
                "mismatched row length: expected {}, got {}",
                self.columns.len(),
                values.len()
            )));
        }
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

    /// Flushes remaining data and closes the underlying Parquet writer.
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

fn should_enable_bloom_filter(column: &ColumnSchema) -> bool {
    match column.kind {
        ColumnKind::TableId => false,
        _ => matches!(
            column.parquet_type,
            ColumnParquetType::Int64 | ColumnParquetType::Utf8
        ),
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
        builder = builder.with_repetition(if column.nullable {
            Repetition::OPTIONAL
        } else {
            Repetition::REQUIRED
        });
        builder = builder.with_id(Some(column.field_id));
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
    field_id: i32,
    parquet_type: ColumnParquetType,
    nullable: bool,
    metrics: ColumnMetricsState,
    values_int64: Vec<i64>,
    values_double: Vec<f64>,
    values_binary: Vec<ByteArray>,
    def_levels: Vec<i16>,
}

impl ColumnState {
    fn from_schema(column: &ColumnSchema, capacity: usize) -> Result<Self> {
        Ok(Self {
            field_id: column.field_id,
            parquet_type: column.parquet_type,
            nullable: column.nullable,
            metrics: ColumnMetricsState::default(),
            values_int64: Vec::with_capacity(capacity),
            values_double: Vec::with_capacity(capacity),
            values_binary: Vec::with_capacity(capacity),
            def_levels: Vec::with_capacity(capacity),
        })
    }

    fn push(&mut self, value: Option<CellValue>) -> Result<()> {
        match (&self.parquet_type, value) {
            (ColumnParquetType::Int64, Some(CellValue::Int64(v))) => {
                self.metrics.update_int64(v);
                self.values_int64.push(v);
                self.push_level(true);
            }
            (ColumnParquetType::Double, Some(CellValue::Double(v))) => {
                self.metrics.update_double(v);
                self.values_double.push(v);
                self.push_level(true);
            }
            (ColumnParquetType::Utf8 | ColumnParquetType::Binary, Some(CellValue::Bytes(v))) => {
                self.metrics.update_bytes(&v);
                self.values_binary.push(ByteArray::from(v));
                self.push_level(true);
            }
            (_, None) => {
                if self.nullable {
                    self.metrics.null_count = self.metrics.null_count.saturating_add(1);
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
                    )));
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

fn encode_i64_be(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

fn encode_f64_be(value: f64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

#[derive(Debug, Default)]
struct ColumnMetricsState {
    null_count: u64,
    min_int64: Option<i64>,
    max_int64: Option<i64>,
    min_double: Option<f64>,
    max_double: Option<f64>,
    min_bytes: Option<Vec<u8>>,
    max_bytes: Option<Vec<u8>>,
}

impl ColumnMetricsState {
    fn update_int64(&mut self, value: i64) {
        self.min_int64 = Some(self.min_int64.map_or(value, |cur| cur.min(value)));
        self.max_int64 = Some(self.max_int64.map_or(value, |cur| cur.max(value)));
    }

    fn update_double(&mut self, value: f64) {
        if value.is_nan() {
            return;
        }
        self.min_double = Some(self.min_double.map_or(value, |cur| cur.min(value)));
        self.max_double = Some(self.max_double.map_or(value, |cur| cur.max(value)));
    }

    fn update_bytes(&mut self, value: &[u8]) {
        match self.min_bytes.as_mut() {
            None => self.min_bytes = Some(value.to_vec()),
            Some(cur) => {
                if value < cur.as_slice() {
                    *cur = value.to_vec();
                }
            }
        }
        match self.max_bytes.as_mut() {
            None => self.max_bytes = Some(value.to_vec()),
            Some(cur) => {
                if value > cur.as_slice() {
                    *cur = value.to_vec();
                }
            }
        }
    }

    fn lower_bound_bytes(&self) -> Option<Vec<u8>> {
        if let Some(v) = self.min_int64 {
            return Some(encode_i64_be(v));
        }
        if let Some(v) = self.min_double {
            return Some(encode_f64_be(v));
        }
        self.min_bytes.clone()
    }

    fn upper_bound_bytes(&self) -> Option<Vec<u8>> {
        if let Some(v) = self.max_int64 {
            return Some(encode_i64_be(v));
        }
        if let Some(v) = self.max_double {
            return Some(encode_f64_be(v));
        }
        self.max_bytes.clone()
    }
}
