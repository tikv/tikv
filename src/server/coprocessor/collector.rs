// Copyright 2016 PingCAP, Inc.
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


use std::mem;
use std::collections::HashMap;

use tipb::select::{SelectRequest, Chunk, RowMeta};
use tipb::schema::ColumnInfo;

use util::codec::{datum, Datum, mysql};
use util::xeval::Evaluator;

use super::{Result, util, Collector};

/// A default collector that simply add all qualified row to its inner buffer.
pub struct DefaultCollector {
    cols: Vec<ColumnInfo>,
    chunks: Vec<Chunk>,
}

impl Collector for DefaultCollector {
    fn create(sel: &SelectRequest) -> Result<DefaultCollector> {
        let cols = if sel.has_table_info() {
            sel.get_table_info().get_columns().to_vec()
        } else {
            let mut cols = sel.get_index_info().get_columns();
            if cols.last().map_or(false, |c| c.get_pk_handle()) {
                let len = cols.len();
                cols = &cols[..len - 1];
            }
            cols.to_vec()
        };

        Ok(DefaultCollector {
            cols: cols,
            chunks: vec![],
        })
    }

    fn collect(&mut self,
               _: &mut Evaluator,
               handle: i64,
               values: &HashMap<i64, &[u8]>)
               -> Result<usize> {
        let chunk = util::get_chunk(&mut self.chunks);
        let mut meta = RowMeta::new();
        meta.set_handle(handle);
        let last_len = chunk.get_rows_data().len();
        for col in &self.cols {
            let col_id = col.get_column_id();
            if let Some(v) = values.get(&col_id) {
                chunk.mut_rows_data().extend_from_slice(v);
                continue;
            }
            if col.get_pk_handle() {
                box_try!(datum::encode_to(chunk.mut_rows_data(),
                                          &[util::get_pk(col, handle)],
                                          false));
            } else if mysql::has_not_null_flag(col.get_flag() as u64) {
                return Err(box_err!("column {} of {} is missing", col_id, handle));
            } else {
                box_try!(datum::encode_to(chunk.mut_rows_data(), &[Datum::Null], false));
            }
        }
        meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
        chunk.mut_rows_meta().push(meta);
        Ok(1)
    }

    fn take_collection(&mut self) -> Result<Vec<Chunk>> {
        let mut chunks = vec![];
        mem::swap(&mut chunks, &mut self.chunks);
        Ok(chunks)
    }
}
