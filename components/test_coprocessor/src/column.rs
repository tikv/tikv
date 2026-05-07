// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use tidb_query_datatype::{
    codec::{datum, Datum},
    expr::EvalContext,
};
use tipb::{ColumnInfo, FieldType};

use super::*;

pub const TYPE_VAR_CHAR: i32 = 1;
pub const TYPE_LONG: i32 = 2;

#[derive(Clone)]
pub struct Column {
    pub id: i64,
    pub(crate) col_type: i32,
    // negative means not a index key, 0 means primary key, positive means normal index key.
    pub index: i64,
    pub(crate) default_val: Option<Datum>,
}

impl Column {
    pub fn as_column_info(&self) -> ColumnInfo {
        let mut c_info = ColumnInfo::default();
        c_info.set_column_id(self.id);
        c_info.set_tp(self.col_field_type());
        c_info.set_pk_handle(self.index == 0);
        if let Some(ref dv) = self.default_val {
            c_info.set_default_val(
                datum::encode_value(&mut EvalContext::default(), &[dv.clone()]).unwrap(),
            )
        }
        c_info
    }

    pub fn as_field_type(&self) -> FieldType {
        let mut ft = FieldType::default();
        ft.set_tp(self.col_field_type());
        ft
    }

    pub fn col_field_type(&self) -> i32 {
        match self.col_type {
            TYPE_LONG => 8,      // FieldTypeTp::LongLong
            TYPE_VAR_CHAR => 15, // FieldTypeTp::VarChar
            _ => unreachable!("col_type: {}", self.col_type),
        }
    }
}

pub struct ColumnBuilder {
    col_type: i32,
    index: i64,
    default_val: Option<Datum>,
}

impl ColumnBuilder {
    pub fn new() -> ColumnBuilder {
        ColumnBuilder {
            col_type: TYPE_LONG,
            index: -1,
            default_val: None,
        }
    }

    #[must_use]
    pub fn col_type(mut self, t: i32) -> ColumnBuilder {
        self.col_type = t;
        self
    }

    #[must_use]
    pub fn primary_key(mut self, b: bool) -> ColumnBuilder {
        if b {
            self.index = 0;
        } else {
            self.index = -1;
        }
        self
    }

    #[must_use]
    pub fn index_key(mut self, idx_id: i64) -> ColumnBuilder {
        self.index = idx_id;
        self
    }

    #[must_use]
    pub fn default(mut self, val: Datum) -> ColumnBuilder {
        self.default_val = Some(val);
        self
    }

    pub fn build(self) -> Column {
        Column {
            id: next_id(),
            col_type: self.col_type,
            index: self.index,
            default_val: self.default_val,
        }
    }
}

impl Default for ColumnBuilder {
    fn default() -> Self {
        Self::new()
    }
}
