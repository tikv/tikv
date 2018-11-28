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

use tikv::coprocessor;
use tikv::coprocessor::codec::table;
use tikv::util::codec::number::NumberEncoder;

#[derive(Clone)]
pub struct Table {
    pub id: i64,
    pub handle_id: i64,
    pub cols: BTreeMap<i64, Column>,
    pub idxs: BTreeMap<i64, Vec<i64>>,
}

impl Table {
    pub fn get_table_info(&self) -> schema::TableInfo {
        let mut tb_info = schema::TableInfo::new();
        tb_info.set_table_id(self.id);
        tb_info.set_columns(RepeatedField::from_vec(self.get_table_columns()));
        tb_info
    }

    pub fn get_table_columns(&self) -> Vec<ColumnInfo> {
        let mut tb_info = Vec::new();
        for col in self.cols.values() {
            tb_info.push(col.get_column_info());
        }
        tb_info
    }

    pub fn get_index_info(&self, index: i64, store_handle: bool) -> schema::IndexInfo {
        let mut idx_info = schema::IndexInfo::new();
        idx_info.set_table_id(self.id);
        idx_info.set_index_id(index);
        let mut has_pk = false;
        for col_id in &self.idxs[&index] {
            let col = &self.cols[col_id];
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

    pub fn get_select_range(&self) -> KeyRange {
        let mut range = KeyRange::new();
        range.set_start(table::encode_row_key(self.id, ::std::i64::MIN));
        range.set_end(table::encode_row_key(self.id, ::std::i64::MAX));
        range
    }

    pub fn get_point_select_range(&self, handle_id: i64) -> KeyRange {
        let start_key = table::encode_row_key(self.id, handle_id);
        let mut end_key = start_key.clone();
        coprocessor::util::convert_to_prefix_next(&mut end_key);
        let mut range = KeyRange::new();
        range.set_start(start_key);
        range.set_end(end_key);
        range
    }

    pub fn get_index_range(&self, idx: i64) -> KeyRange {
        let mut range = KeyRange::new();
        let mut buf = Vec::with_capacity(8);
        buf.encode_i64(::std::i64::MIN).unwrap();
        range.set_start(table::encode_index_seek_key(self.id, idx, &buf));
        buf.clear();
        buf.encode_i64(::std::i64::MAX).unwrap();
        range.set_end(table::encode_index_seek_key(self.id, idx, &buf));
        range
    }
}

pub struct TableBuilder {
    handle_id: i64,
    cols: BTreeMap<i64, Column>,
}

impl TableBuilder {
    pub fn new() -> TableBuilder {
        TableBuilder {
            handle_id: -1,
            cols: BTreeMap::new(),
        }
    }

    pub fn add_col(mut self, col: Column) -> TableBuilder {
        if col.index == 0 {
            if self.handle_id > 0 {
                self.handle_id = 0;
            } else if self.handle_id < 0 {
                // maybe need to check type.
                self.handle_id = col.id;
            }
        }
        self.cols.insert(col.id, col);
        self
    }

    pub fn build(mut self) -> Table {
        if self.handle_id <= 0 {
            self.handle_id = next_id();
        }
        let mut idx = BTreeMap::new();
        for (&id, col) in &self.cols {
            if col.index < 0 {
                continue;
            }
            let e = idx.entry(col.index).or_insert_with(Vec::new);
            e.push(id);
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
            cols: self.cols,
            idxs: idx,
        }
    }
}
