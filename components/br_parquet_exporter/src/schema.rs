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

const PRI_KEY_FLAG: u32 = 1 << 1;

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
    pub pk_is_handle: bool,
    pub primary_key_ids: Vec<i64>,
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
        let mut id_to_offset = FxHashMap::default();
        let mut primary_key_ids_by_flag = Vec::new();
        for col in &table.columns {
            let mut ci = ColumnInfo::default();
            let type_info = col.type_info().ok_or_else(|| {
                Error::Schema(format!(
                    "missing column type info for {}",
                    col.name.display_name()
                ))
            })?;
            ci.set_column_id(col.id);
            if let Some(tp) = FieldTypeTp::from_i32(type_info.tp) {
                ci.as_mut_accessor().set_tp(tp);
            }
            let flag = FieldTypeFlag::from_bits_truncate(type_info.flag);
            ci.as_mut_accessor().set_flag(flag);
            ci.as_mut_accessor()
                .set_flen(clamp_i32(type_info.flen) as isize);
            ci.as_mut_accessor()
                .set_decimal(clamp_i32(type_info.decimal) as isize);
            column_map.insert(col.id, ci);
            id_to_offset.insert(col.id, col.offset);
            if (type_info.flag & PRI_KEY_FLAG) != 0 {
                primary_key_ids_by_flag.push(col.id);
            }
        }
        primary_key_ids_by_flag.sort_by_key(|id| id_to_offset.get(id).copied().unwrap_or_default());

        let primary_key_ids = if table.is_common_handle {
            table
                .index_info
                .as_ref()
                .and_then(|indexes| indexes.iter().find(|idx| idx.primary))
                .map(|primary| {
                    primary
                        .columns
                        .iter()
                        .filter_map(|idx_col| {
                            table
                                .columns
                                .iter()
                                .find(|col| col.offset == idx_col.offset)
                                .map(|col| col.id)
                        })
                        .collect::<Vec<_>>()
                })
                .filter(|ids| !ids.is_empty())
                .unwrap_or(primary_key_ids_by_flag)
        } else {
            primary_key_ids_by_flag
        };

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
            pk_is_handle: table.pk_is_handle,
            primary_key_ids,
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
    #[serde(default, alias = "db_name")]
    pub name: CiString,
}

#[derive(Deserialize)]
struct TableInfo {
    pub id: i64,
    pub name: CiString,
    #[serde(rename = "cols", default)]
    pub columns: Vec<TableColumn>,
    #[serde(rename = "index_info", default)]
    pub index_info: Option<Vec<IndexInfo>>,
    #[serde(rename = "is_common_handle", default)]
    pub is_common_handle: bool,
    #[serde(rename = "pk_is_handle", default)]
    pub pk_is_handle: bool,
}

#[derive(Deserialize)]
struct TableColumn {
    pub id: i64,
    pub name: CiString,
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub tp: Option<i32>,
    #[serde(default)]
    pub flag: Option<u32>,
    #[serde(default)]
    pub flen: Option<i64>,
    #[serde(default)]
    pub decimal: Option<i64>,
    #[serde(rename = "type", default)]
    pub type_info: Option<ColumnTypeInfo>,
}

impl TableColumn {
    fn type_info(&self) -> Option<ColumnTypeInfo> {
        if let Some(tp) = self.tp {
            Some(ColumnTypeInfo {
                tp,
                flag: self.flag.unwrap_or_default(),
                flen: self.flen.unwrap_or_default(),
                decimal: self.decimal.unwrap_or_default(),
            })
        } else {
            self.type_info
        }
    }
}

#[derive(Clone, Copy, Deserialize)]
struct ColumnTypeInfo {
    #[serde(rename = "Tp")]
    pub tp: i32,
    #[serde(rename = "Flag", default)]
    pub flag: u32,
    #[serde(rename = "Flen", default)]
    pub flen: i64,
    #[serde(rename = "Decimal", default)]
    pub decimal: i64,
}

#[derive(Deserialize)]
struct IndexInfo {
    #[serde(rename = "is_primary", default)]
    pub primary: bool,
    #[serde(rename = "idx_cols", default)]
    pub columns: Vec<IndexColumn>,
}

#[derive(Deserialize)]
struct IndexColumn {
    #[serde(default)]
    pub offset: i64,
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

fn clamp_i32(value: i64) -> i32 {
    if value > i32::MAX as i64 {
        i32::MAX
    } else if value < i32::MIN as i64 {
        i32::MIN
    } else {
        value as i32
    }
}
