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

use super::*;

use tikv::coprocessor::codec::{datum, Datum};
use tipb::schema::ColumnInfo;

pub const TYPE_VAR_CHAR: i32 = 1;
pub const TYPE_LONG: i32 = 2;

#[derive(Clone)]
pub struct Column {
    pub id: i64,
    pub col_type: i32,
    // negative means not a index key, 0 means primary key, positive means normal index key.
    pub index: i64,
    pub default_val: Option<Datum>,
}

impl Column {
    pub fn get_column_info(&self) -> ColumnInfo {
        let mut c_info = ColumnInfo::new();
        c_info.set_column_id(self.id);
        c_info.set_tp(self.col_type);
        c_info.set_pk_handle(self.index == 0);
        if let Some(ref dv) = self.default_val {
            c_info.set_default_val(datum::encode_value(&[dv.clone()]).unwrap())
        }
        c_info
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

    pub fn col_type(mut self, t: i32) -> ColumnBuilder {
        self.col_type = t;
        self
    }

    pub fn primary_key(mut self, b: bool) -> ColumnBuilder {
        if b {
            self.index = 0;
        } else {
            self.index = -1;
        }
        self
    }

    pub fn index_key(mut self, idx_id: i64) -> ColumnBuilder {
        self.index = idx_id;
        self
    }

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
