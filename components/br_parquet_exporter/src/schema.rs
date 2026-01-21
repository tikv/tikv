// Copyright 2025 TiKV Project Authors. Licensed under Apache-2.0.

use collections::HashMap as FxHashMap;

use kvproto::brpb::Schema as BackupSchema;
use serde::Deserialize;
use tidb_query_datatype::def::{FieldTypeAccessor, FieldTypeFlag, FieldTypeTp};
use tipb::ColumnInfo;

use crate::{Error, Result};

#[derive(Clone, Debug, PartialEq)]
pub enum ColumnParquetType {
    Int64,
    Double,
    Utf8,
    Binary,
}

#[derive(Clone, Debug)]
pub enum ColumnKind {
    TableId,
    Handle,
    Physical(i64),
}

#[derive(Clone, Debug)]
pub struct ColumnSchema {
    pub name: String,
    pub parquet_type: ColumnParquetType,
    pub nullable: bool,
    pub kind: ColumnKind,
    pub info: Option<ColumnInfo>,
}

#[derive(Clone, Debug)]
pub struct TableSchema {
    pub table_id: i64,
    pub db_name: String,
    pub table_name: String,
    pub is_common_handle: bool,
    pub columns: Vec<ColumnSchema>,
    pub column_map: FxHashMap<i64, ColumnInfo>,
}

impl TableSchema {
    pub fn from_backup_schema(bs: &BackupSchema) -> Result<Self> {
        let db: DbInfo = serde_json::from_slice(bs.get_db())
            .map_err(|e| Error::Schema(format!("decode database info failed: {}", e)))?;
        let table: TableInfo = serde_json::from_slice(bs.get_table())
            .map_err(|e| Error::Schema(format!("decode table info failed: {}", e)))?;
        let mut column_map = FxHashMap::default();
        for col in &table.columns {
            let mut ci = ColumnInfo::default();
            ci.set_column_id(col.id);
            if let Some(tp) = FieldTypeTp::from_i32(col.tp) {
                ci.as_mut_accessor().set_tp(tp);
            }
            let flag = FieldTypeFlag::from_bits_truncate(col.flag);
            ci.as_mut_accessor().set_flag(flag);
            ci.as_mut_accessor()
                .set_flen(col.flen.unwrap_or_default() as isize);
            ci.as_mut_accessor()
                .set_decimal(col.decimal.unwrap_or_default() as isize);
            column_map.insert(col.id, ci);
        }

        let mut columns = Vec::new();
        columns.push(ColumnSchema {
            name: "_tidb_table_id".to_string(),
            parquet_type: ColumnParquetType::Int64,
            nullable: false,
            kind: ColumnKind::TableId,
            info: None,
        });
        columns.push(ColumnSchema {
            name: "_tidb_handle".to_string(),
            parquet_type: ColumnParquetType::Utf8,
            nullable: false,
            kind: ColumnKind::Handle,
            info: None,
        });
        for col in &table.columns {
            if let Some(ci) = column_map.get(&col.id) {
                let parquet_ty = infer_parquet_type(ci);
                let nullable = !ci.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL);
                columns.push(ColumnSchema {
                    name: col.name.display_name(),
                    parquet_type: parquet_ty,
                    nullable,
                    kind: ColumnKind::Physical(col.id),
                    info: Some(ci.clone()),
                });
            }
        }

        Ok(Self {
            table_id: table.id,
            db_name: db.name.display_name(),
            table_name: table.name.display_name(),
            is_common_handle: table.is_common_handle,
            columns,
            column_map,
        })
    }
}

fn infer_parquet_type(ci: &ColumnInfo) -> ColumnParquetType {
    match ci.as_accessor().tp() {
        FieldTypeTp::Tiny
        | FieldTypeTp::Short
        | FieldTypeTp::Long
        | FieldTypeTp::LongLong
        | FieldTypeTp::Int24
        | FieldTypeTp::Year => {
            if ci.as_accessor().is_unsigned() {
                ColumnParquetType::Utf8
            } else {
                ColumnParquetType::Int64
            }
        }
        FieldTypeTp::Float | FieldTypeTp::Double => ColumnParquetType::Double,
        FieldTypeTp::VarChar
        | FieldTypeTp::VarString
        | FieldTypeTp::String
        | FieldTypeTp::Json
        | FieldTypeTp::Enum
        | FieldTypeTp::Set
        | FieldTypeTp::Date
        | FieldTypeTp::DateTime
        | FieldTypeTp::Timestamp
        | FieldTypeTp::Duration
        | FieldTypeTp::NewDecimal => ColumnParquetType::Utf8,
        FieldTypeTp::TinyBlob
        | FieldTypeTp::MediumBlob
        | FieldTypeTp::Blob
        | FieldTypeTp::LongBlob
        | FieldTypeTp::Bit => ColumnParquetType::Binary,
        _ => ColumnParquetType::Binary,
    }
}

#[derive(Deserialize)]
struct DbInfo {
    #[serde(default)]
    pub name: CiString,
}

#[derive(Deserialize)]
struct TableInfo {
    pub id: i64,
    pub name: CiString,
    #[serde(rename = "cols", default)]
    pub columns: Vec<TableColumn>,
    #[serde(rename = "is_common_handle", default)]
    pub is_common_handle: bool,
}

#[derive(Deserialize)]
struct TableColumn {
    pub id: i64,
    pub name: CiString,
    pub tp: i32,
    #[serde(default)]
    pub flag: u32,
    #[serde(default)]
    pub flen: Option<i32>,
    #[serde(default)]
    pub decimal: Option<i32>,
}

#[derive(Deserialize, Default)]
struct CiString {
    #[serde(rename = "O", default)]
    pub original: String,
    #[serde(rename = "L", default)]
    pub lower: String,
}

impl CiString {
    fn display_name(&self) -> String {
        if self.original.is_empty() {
            self.lower.clone()
        } else {
            self.original.clone()
        }
    }
}
