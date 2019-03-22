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

use std::collections::BTreeMap;

use kvproto::coprocessor::KeyRange;
use tipb::schema::{self, ColumnInfo};

use protobuf::RepeatedField;

use codec::prelude::BufferNumberEncoder;
use tikv::coprocessor;
use tikv::coprocessor::codec::table;

#[derive(Clone)]
pub struct Table {
    pub id: i64,
    pub(crate) handle_id: i64,
    pub(crate) columns: Vec<(String, Column)>,
    pub(crate) column_index_by_id: BTreeMap<i64, usize>,
    pub(crate) column_index_by_name: BTreeMap<String, usize>,
    pub(crate) idxs: BTreeMap<i64, Vec<i64>>,
}

fn normalize_column_name(name: impl std::borrow::Borrow<str>) -> String {
    name.borrow().to_lowercase()
}

impl Table {
    /// Get a column reference in the table by column id.
    pub fn column_by_id(&self, id: i64) -> Option<&Column> {
        let idx = self.column_index_by_id.get(&id);
        idx.map(|idx| &self.columns[*idx].1)
    }

    /// Get a column reference in the table by column name (case insensitive).
    pub fn column_by_name(&self, name: impl std::borrow::Borrow<str>) -> Option<&Column> {
        let normalized_name = normalize_column_name(name);
        let idx = self.column_index_by_name.get(&normalized_name);
        idx.map(|idx| &self.columns[*idx].1)
    }

    /// Create `schema::TableInfo` from current table.
    pub fn table_info(&self) -> schema::TableInfo {
        let mut info = schema::TableInfo::new();
        info.set_table_id(self.id);
        info.set_columns(RepeatedField::from_vec(self.columns_info()));
        info
    }

    /// Create `Vec<ColumnInfo>` from current table's columns.
    pub fn columns_info(&self) -> Vec<ColumnInfo> {
        self.columns
            .iter()
            .map(|(_, col)| col.as_column_info())
            .collect()
    }

    /// Create `schema::IndexInfo` from current table.
    pub fn index_info(&self, index: i64, store_handle: bool) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.id);
        idx_info.set_index_id(index);
        let mut has_pk = false;
        for col_id in &self.idxs[&index] {
            let col = self.column_by_id(*col_id).unwrap();
            let mut c_info = ColumnInfo::new();
            c_info.set_tp(col.col_type);
            c_info.set_column_id(col.id);
            if col.id == self.handle_id {
                c_info.set_pk_handle(true);
                has_pk = true
            }
            idx_info.mut_columns().push(c_info);
        }
        if !has_pk && store_handle {
            let mut handle_info = ColumnInfo::new();
            handle_info.set_tp(TYPE_LONG);
            handle_info.set_column_id(-1);
            handle_info.set_pk_handle(true);
            idx_info.mut_columns().push(handle_info);
        }
        idx_info
    }

    /// Create a `KeyRange` which select all records in current table.
    pub fn get_record_range_all(&self) -> KeyRange {
        let mut range = KeyRange::new();
        range.set_start(table::encode_row_key(self.id, std::i64::MIN));
        range.set_end(table::encode_row_key(self.id, std::i64::MAX));
        range
    }

    /// Create a `KeyRange` which select one row in current table.
    pub fn get_record_range_one(&self, handle_id: i64) -> KeyRange {
        let start_key = table::encode_row_key(self.id, handle_id);
        let mut end_key = start_key.clone();
        coprocessor::util::convert_to_prefix_next(&mut end_key);
        let mut range = KeyRange::new();
        range.set_start(start_key);
        range.set_end(end_key);
        range
    }

    /// Create a `KeyRange` which select all index records of a specified index in current table.
    pub fn get_index_range_all(&self, idx: i64) -> KeyRange {
        let mut range = KeyRange::new();
        let mut buf = Vec::with_capacity(8);
        buf.write_i64(::std::i64::MIN).unwrap();
        range.set_start(table::encode_index_seek_key(self.id, idx, &buf));
        buf.clear();
        buf.write_i64(::std::i64::MAX).unwrap();
        range.set_end(table::encode_index_seek_key(self.id, idx, &buf));
        range
    }
}

impl<T: std::borrow::Borrow<str>> std::ops::Index<T> for Table {
    type Output = Column;

    fn index(&self, key: T) -> &Column {
        self.column_by_name(key).unwrap()
    }
}

pub struct TableBuilder {
    handle_id: i64,
    columns: Vec<(String, Column)>,
}

impl TableBuilder {
    pub fn new() -> TableBuilder {
        TableBuilder {
            handle_id: -1,
            columns: Vec::new(),
        }
    }

    pub fn add_col(mut self, name: impl std::borrow::Borrow<str>, col: Column) -> TableBuilder {
        if col.index == 0 {
            if self.handle_id > 0 {
                self.handle_id = 0;
            } else if self.handle_id < 0 {
                // maybe need to check type.
                self.handle_id = col.id;
            }
        }
        self.columns.push((normalize_column_name(name), col));
        self
    }

    pub fn build(mut self) -> Table {
        if self.handle_id <= 0 {
            self.handle_id = next_id();
        }

        let mut column_index_by_id = BTreeMap::new();
        let mut column_index_by_name = BTreeMap::new();
        for (index, (some_name, column)) in self.columns.iter().enumerate() {
            column_index_by_id.insert(column.id, index);
            column_index_by_name.insert(some_name.clone(), index);
        }

        let mut idx = BTreeMap::new();
        for (_, col) in &self.columns {
            if col.index < 0 {
                continue;
            }
            let e = idx.entry(col.index).or_insert_with(Vec::new);
            e.push(col.id);
        }
        for (id, val) in &mut idx {
            if *id == 0 {
                continue;
            }
            // TODO: support uniq index.
            val.push(self.handle_id);
        }

        Table {
            id: next_id(),
            handle_id: self.handle_id,
            columns: self.columns,
            column_index_by_id,
            column_index_by_name,
            idxs: idx,
        }
    }
}
