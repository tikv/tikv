// Copyright 2017 PingCAP, Inc.
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

use std::usize;
use std::time::Instant;
use std::rc::Rc;
use tipb::select::{Chunk, RowMeta, SelectRequest, SelectResponse};
use tipb::schema::ColumnInfo;
use tipb::expression::{ByItem, Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use kvproto::coprocessor::{KeyRange, Response};

use coprocessor::codec::{datum, mysql, table};
use coprocessor::codec::table::{RowColsDict, TableDecoder};
use coprocessor::codec::datum::Datum;
use coprocessor::metrics::*;
use coprocessor::{Error, Result};
use coprocessor::endpoint::{check_if_outdated, get_chunk, get_pk, is_point, prefix_next,
                            to_pb_error, BATCH_ROW_COUNT, REQ_TYPE_INDEX, REQ_TYPE_SELECT,
                            SINGLE_GROUP};
use util::{escape, Either};
use util::time::duration_to_ms;
use util::collections::{HashMap, HashMapEntry as Entry, HashSet};
use util::codec::number::NumberDecoder;
use storage::{Key, ScanMode, SnapshotStore, Statistics};

use super::xeval::{EvalContext, Evaluator};
use super::aggregate::{self, AggrFunc};
use super::topn_heap::TopNHeap;

const REQUEST_CHECKPOINT: usize = 255;

pub struct SelectContext<'a> {
    snap: SnapshotStore<'a>,
    statistics: &'a mut Statistics,
    core: SelectContextCore,
    deadline: Instant,
}

impl<'a> SelectContext<'a> {
    pub fn new(
        sel: SelectRequest,
        snap: SnapshotStore<'a>,
        deadline: Instant,
        statistics: &'a mut Statistics,
    ) -> Result<SelectContext<'a>> {
        Ok(SelectContext {
            core: try!(SelectContextCore::new(sel)),
            snap: snap,
            deadline: deadline,
            statistics: statistics,
        })
    }

    pub fn handle_request(mut self, tp: i64, mut ranges: Vec<KeyRange>) -> Result<Response> {
        if self.core.desc_scan {
            ranges.reverse();
        }
        let res = match tp {
            REQ_TYPE_SELECT => self.get_rows_from_sel(ranges),
            REQ_TYPE_INDEX => self.get_rows_from_idx(ranges),
            _ => unreachable!(),
        };
        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(()) => {
                sel_resp.set_chunks(RepeatedField::from_vec(self.core.chunks));
                let data = box_try!(sel_resp.write_to_bytes());
                resp.set_data(data);
            }
            Err(e) => if let Error::Other(_) = e {
                sel_resp.set_error(to_pb_error(&e));
                resp.set_data(box_try!(sel_resp.write_to_bytes()));
                resp.set_other_error(format!("{}", e));
                COPR_REQ_ERROR.with_label_values(&["other"]).inc();
            } else {
                return Err(e);
            },
        }
        Ok(resp)
    }

    fn get_rows_from_sel(&mut self, ranges: Vec<KeyRange>) -> Result<()> {
        let mut collected = 0;
        for ran in ranges {
            if collected >= self.core.limit {
                break;
            }
            let timer = Instant::now();
            let row_cnt = try!(self.get_rows_from_range(ran));
            debug!(
                "fetch {} rows takes {} ms",
                row_cnt,
                duration_to_ms(timer.elapsed())
            );
            collected += row_cnt;
            try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
        }
        if self.core.topn {
            self.core.collect_topn_rows()
        } else if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(())
        }
    }

    fn key_only(&self) -> bool {
        match self.core.cols {
            Either::Left(ref cols) => cols.is_empty(),
            Either::Right(_) => false, // TODO: true when index is not uniq index.
        }
    }

    fn get_rows_from_range(&mut self, range: KeyRange) -> Result<usize> {
        let mut row_count = 0;
        if is_point(&range) {
            CORP_GET_OR_SCAN_COUNT.with_label_values(&["point"]).inc();
            let value = match try!(
                self.snap
                    .get(&Key::from_raw(range.get_start()), &mut self.statistics)
            ) {
                None => return Ok(0),
                Some(v) => v,
            };
            let values = {
                let ids = self.core.cols.as_ref().left().unwrap();
                box_try!(table::cut_row(value, ids))
            };
            let h = box_try!(table::decode_handle(range.get_start()));
            row_count += try!(self.core.handle_row(h, values));
        } else {
            CORP_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
            let mut seek_key = if self.core.desc_scan {
                range.get_end().to_vec()
            } else {
                range.get_start().to_vec()
            };
            let upper_bound = if !self.core.desc_scan && !range.get_end().is_empty() {
                Some(Key::from_raw(range.get_end()).encoded().clone())
            } else {
                None
            };
            let mut scanner = try!(self.snap.scanner(
                if self.core.desc_scan {
                    ScanMode::Backward
                } else {
                    ScanMode::Forward
                },
                self.key_only(),
                upper_bound,
                self.statistics
            ));
            while self.core.limit > row_count {
                if row_count & REQUEST_CHECKPOINT == 0 {
                    try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
                }
                let kv = if self.core.desc_scan {
                    try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
                } else {
                    try!(scanner.seek(Key::from_raw(&seek_key)))
                };
                let (key, value) = match kv {
                    Some((key, value)) => (box_try!(key.raw()), value),
                    None => break,
                };
                if range.get_start() > key.as_slice() || range.get_end() <= key.as_slice() {
                    debug!(
                        "key: {} out of range [{}, {})",
                        escape(&key),
                        escape(range.get_start()),
                        escape(range.get_end())
                    );
                    break;
                }
                let h = box_try!(table::decode_handle(&key));
                let row_data = {
                    let ids = self.core.cols.as_ref().left().unwrap();
                    box_try!(table::cut_row(value, ids))
                };
                row_count += try!(self.core.handle_row(h, row_data));
                seek_key = if self.core.desc_scan {
                    box_try!(table::truncate_as_row_key(&key)).to_vec()
                } else {
                    prefix_next(&key)
                };
            }
        }
        Ok(row_count)
    }

    fn get_rows_from_idx(&mut self, ranges: Vec<KeyRange>) -> Result<()> {
        let mut collected = 0;
        for r in ranges {
            if collected >= self.core.limit {
                break;
            }
            collected += try!(self.get_idx_row_from_range(r));
            try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
        }
        if self.core.topn {
            self.core.collect_topn_rows()
        } else if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(())
        }
    }

    fn get_idx_row_from_range(&mut self, r: KeyRange) -> Result<usize> {
        let mut row_cnt = 0;
        let mut seek_key = if self.core.desc_scan {
            r.get_end().to_vec()
        } else {
            r.get_start().to_vec()
        };
        CORP_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
        let upper_bound = if !self.core.desc_scan && !r.get_end().is_empty() {
            Some(Key::from_raw(r.get_end()).encoded().clone())
        } else {
            None
        };
        let mut scanner = try!(self.snap.scanner(
            if self.core.desc_scan {
                ScanMode::Backward
            } else {
                ScanMode::Forward
            },
            self.key_only(),
            upper_bound,
            self.statistics
        ));
        while row_cnt < self.core.limit {
            if row_cnt & REQUEST_CHECKPOINT == 0 {
                try!(check_if_outdated(self.deadline, REQ_TYPE_SELECT));
            }
            let nk = if self.core.desc_scan {
                try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
            } else {
                try!(scanner.seek(Key::from_raw(&seek_key)))
            };
            let (key, val) = match nk {
                Some((key, val)) => (box_try!(key.raw()), val),
                None => break,
            };
            if r.get_start() > key.as_slice() || r.get_end() <= key.as_slice() {
                debug!(
                    "key: {} out of range [{}, {})",
                    escape(&key),
                    escape(r.get_start()),
                    escape(r.get_end())
                );
                break;
            }
            seek_key = if self.core.desc_scan {
                key.clone()
            } else {
                prefix_next(&key)
            };
            {
                let (mut values, handle) = {
                    let mut ids = self.core.cols.as_ref().right().unwrap().clone();
                    if self.core.pk_col.is_some() {
                        ids.pop();
                    }
                    box_try!(table::cut_idx_key(key, &ids))
                };
                let handle = if handle.is_none() {
                    box_try!(val.as_slice().read_i64::<BigEndian>())
                } else {
                    handle.unwrap()
                };
                if let Some(ref pk_col) = self.core.pk_col {
                    let handle_datum = if mysql::has_unsigned_flag(pk_col.get_flag() as u64) {
                        // PK column is unsigned
                        datum::Datum::U64(handle as u64)
                    } else {
                        datum::Datum::I64(handle)
                    };
                    let mut bytes = box_try!(datum::encode_key(&[handle_datum]));
                    values.append(pk_col.get_column_id(), &mut bytes);
                }
                row_cnt += try!(self.core.handle_row(handle, values));
            }
        }
        Ok(row_cnt)
    }
}


struct SelectContextCore {
    ctx: Rc<EvalContext>,
    sel: SelectRequest,
    eval: Evaluator,
    cols: Either<HashSet<i64>, Vec<i64>>,
    pk_col: Option<ColumnInfo>,
    cond_cols: Vec<ColumnInfo>,
    topn_cols: Vec<ColumnInfo>,
    aggr: bool,
    aggr_cols: Vec<ColumnInfo>,
    topn: bool,
    topn_heap: Option<TopNHeap>,
    order_cols: Rc<Vec<ByItem>>,
    limit: usize,
    desc_scan: bool,
    gks: Vec<Rc<Vec<u8>>>,
    gk_aggrs: HashMap<Rc<Vec<u8>>, Vec<Box<AggrFunc>>>,
    chunks: Vec<Chunk>,
}

impl SelectContextCore {
    fn new(sel: SelectRequest) -> Result<SelectContextCore> {
        let cond_cols;
        let topn_cols;
        let mut order_by_cols: Vec<ByItem> = Vec::new();
        let mut aggr_cols = vec![];
        {
            let select_cols = if sel.has_table_info() {
                COPR_EXECUTOR_COUNT.with_label_values(&["tblscan"]).inc();
                sel.get_table_info().get_columns()
            } else {
                COPR_EXECUTOR_COUNT.with_label_values(&["idxscan"]).inc();
                sel.get_index_info().get_columns()
            };
            let mut cond_col_map = HashMap::default();
            if sel.has_field_where() {
                COPR_EXECUTOR_COUNT.with_label_values(&["selection"]).inc();
                try!(collect_col_in_expr(
                    &mut cond_col_map,
                    select_cols,
                    sel.get_field_where()
                ));
            }
            let mut aggr_cols_map = HashMap::default();
            for aggr in sel.get_aggregates() {
                try!(collect_col_in_expr(&mut aggr_cols_map, select_cols, aggr));
            }
            for item in sel.get_group_by() {
                try!(collect_col_in_expr(
                    &mut aggr_cols_map,
                    select_cols,
                    item.get_expr()
                ));
            }
            if !aggr_cols_map.is_empty() {
                for cond_col in cond_col_map.keys() {
                    aggr_cols_map.remove(cond_col);
                }
                aggr_cols = aggr_cols_map.into_iter().map(|(_, v)| v).collect();
            }
            cond_cols = cond_col_map.into_iter().map(|(_, v)| v).collect();

            // get topn cols
            let mut topn_col_map = HashMap::default();
            for item in sel.get_order_by() {
                try!(collect_col_in_expr(
                    &mut topn_col_map,
                    select_cols,
                    item.get_expr()
                ))
            }
            topn_cols = topn_col_map.into_iter().map(|(_, v)| v).collect();
            order_by_cols.extend_from_slice(sel.get_order_by())
        }

        let limit = if sel.has_limit() {
            COPR_EXECUTOR_COUNT.with_label_values(&["limit"]).inc();
            sel.get_limit() as usize
        } else {
            usize::MAX
        };

        // set topn
        let mut topn = false;
        let mut desc_can = false;
        if !sel.get_order_by().is_empty() {
            // order by pk,set desc_scan is enough
            if !sel.get_order_by()[0].has_expr() {
                desc_can = sel.get_order_by().first().map_or(false, |o| o.get_desc());
            } else {
                COPR_EXECUTOR_COUNT.with_label_values(&["topn"]).inc();
                topn = true;
            }
        }

        let mut pk_col = None;
        let cols = if sel.has_table_info() {
            Either::Left(
                sel.get_table_info()
                    .get_columns()
                    .iter()
                    .filter(|c| !c.get_pk_handle())
                    .map(|c| c.get_column_id())
                    .collect(),
            )
        } else {
            let cols = sel.get_index_info().get_columns();
            if cols.last().map_or(false, |c| c.get_pk_handle()) {
                pk_col = Some(cols.last().unwrap().clone());
            }
            Either::Right(cols.iter().map(|c| c.get_column_id()).collect())
        };

        let aggr = if !sel.get_aggregates().is_empty() || !sel.get_group_by().is_empty() {
            COPR_EXECUTOR_COUNT
                .with_label_values(&["aggregation"])
                .inc();
            true
        } else {
            false
        };

        Ok(SelectContextCore {
            ctx: Rc::new(box_try!(EvalContext::new(
                sel.get_time_zone_offset(),
                sel.get_flags()
            ))),
            aggr: aggr,
            aggr_cols: aggr_cols,
            topn_cols: topn_cols,
            sel: sel,
            eval: Default::default(),
            cols: cols,
            pk_col: pk_col,
            cond_cols: cond_cols,
            gks: vec![],
            gk_aggrs: map![],
            chunks: vec![],
            topn: topn,
            topn_heap: {
                if topn {
                    Some(try!(TopNHeap::new(limit)))
                } else {
                    None
                }
            },
            order_cols: Rc::new(order_by_cols),
            limit: limit,
            desc_scan: desc_can,
        })
    }

    fn handle_row(&mut self, h: i64, row_data: RowColsDict) -> Result<usize> {
        // clear all dirty values.
        self.eval.row.clear();
        if try!(self.should_skip(h, &row_data)) {
            return Ok(0);
        }

        // topn & aggr won't appear together
        if self.topn {
            try!(self.collect_topn_row(h, row_data));
            Ok(0)
        } else if self.aggr {
            try!(self.aggregate(h, &row_data));
            Ok(0)
        } else {
            try!(self.get_row(h, row_data));
            Ok(1)
        }
    }

    fn should_skip(&mut self, h: i64, values: &RowColsDict) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        try!(inflate_with_col(
            &mut self.eval,
            &self.ctx,
            values,
            &self.cond_cols,
            h
        ));
        let res = box_try!(self.eval.eval(&self.ctx, self.sel.get_field_where()));
        let b = box_try!(res.into_bool(&self.ctx));
        Ok(b.map_or(true, |v| !v))
    }

    fn collect_topn_row(&mut self, h: i64, values: RowColsDict) -> Result<()> {
        try!(inflate_with_col(
            &mut self.eval,
            &self.ctx,
            &values,
            &self.topn_cols,
            h
        ));
        let mut sort_keys = Vec::with_capacity(self.sel.get_order_by().len());
        // parse order by
        for col in self.sel.get_order_by() {
            let v = box_try!(self.eval.eval(&self.ctx, col.get_expr()));
            sort_keys.push(v);
        }

        self.topn_heap.as_mut().unwrap().try_add_row(
            h,
            values,
            sort_keys,
            self.order_cols.clone(),
            self.ctx.clone(),
        )
    }

    fn get_row(&mut self, h: i64, values: RowColsDict) -> Result<()> {
        let chunk = get_chunk(&mut self.chunks);
        let last_len = chunk.get_rows_data().len();
        let cols = if self.sel.has_table_info() {
            self.sel.get_table_info().get_columns()
        } else {
            self.sel.get_index_info().get_columns()
        };
        for col in cols {
            let col_id = col.get_column_id();
            if let Some(v) = values.get(col_id) {
                chunk.mut_rows_data().extend_from_slice(v);
                continue;
            }
            if col.get_pk_handle() {
                box_try!(datum::encode_to(
                    chunk.mut_rows_data(),
                    &[get_pk(col, h)],
                    false
                ));
            } else if col.has_default_val() {
                chunk
                    .mut_rows_data()
                    .extend_from_slice(col.get_default_val());
            } else if mysql::has_not_null_flag(col.get_flag() as u64) {
                return Err(box_err!("column {} of {} is missing", col_id, h));
            } else {
                box_try!(datum::encode_to(
                    chunk.mut_rows_data(),
                    &[Datum::Null],
                    false
                ));
            }
        }
        let mut meta = RowMeta::new();
        meta.set_handle(h);
        meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
        chunk.mut_rows_meta().push(meta);
        Ok(())
    }

    fn get_group_key(&mut self) -> Result<Vec<u8>> {
        let items = self.sel.get_group_by();
        if items.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(items.len());
        for item in items {
            let v = box_try!(self.eval.eval(&self.ctx, item.get_expr()));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self, h: i64, values: &RowColsDict) -> Result<()> {
        try!(inflate_with_col(
            &mut self.eval,
            &self.ctx,
            values,
            &self.aggr_cols,
            h
        ));
        let gk = Rc::new(try!(self.get_group_key()));
        let aggr_exprs = self.sel.get_aggregates();
        match self.gk_aggrs.entry(gk.clone()) {
            Entry::Occupied(e) => {
                let funcs = e.into_mut();
                for (expr, func) in aggr_exprs.iter().zip(funcs) {
                    // TODO: cache args
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    try!(func.update(&self.ctx, args));
                }
            }
            Entry::Vacant(e) => {
                let mut aggrs = Vec::with_capacity(aggr_exprs.len());
                for expr in aggr_exprs {
                    let mut aggr = try!(aggregate::build_aggr_func(expr));
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    try!(aggr.update(&self.ctx, args));
                    aggrs.push(aggr);
                }
                self.gks.push(gk);
                e.insert(aggrs);
            }
        }
        Ok(())
    }

    fn collect_topn_rows(&mut self) -> Result<()> {
        let sorted_data = try!(self.topn_heap.take().unwrap().into_sorted_vec());
        for row in sorted_data {
            try!(self.get_row(row.handle, row.data));
        }
        Ok(())
    }

    /// Convert aggregate partial result to rows.
    /// Data layout example:
    /// SQL: select count(c1), sum(c2), avg(c3) from t;
    /// Aggs: count(c1), sum(c2), avg(c3)
    /// Rows: groupKey1, count1, value2, count3, value3
    ///       groupKey2, count1, value2, count3, value3
    fn aggr_rows(&mut self) -> Result<()> {
        self.chunks = Vec::with_capacity(
            (self.gk_aggrs.len() + BATCH_ROW_COUNT - 1) / BATCH_ROW_COUNT,
        );
        // Each aggregate partial result will be converted to two datum.
        let mut row_data = Vec::with_capacity(1 + 2 * self.sel.get_aggregates().len());
        for gk in self.gks.drain(..) {
            let aggrs = self.gk_aggrs.remove(&gk).unwrap();

            let chunk = get_chunk(&mut self.chunks);
            // The first column is group key.
            row_data.push(Datum::Bytes(Rc::try_unwrap(gk).unwrap()));
            for mut aggr in aggrs {
                try!(aggr.calc(&mut row_data));
            }
            let last_len = chunk.get_rows_data().len();
            box_try!(datum::encode_to(chunk.mut_rows_data(), &row_data, false));
            let mut meta = RowMeta::new();
            meta.set_length((chunk.get_rows_data().len() - last_len) as i64);
            chunk.mut_rows_meta().push(meta);
            row_data.clear();
        }
        Ok(())
    }
}

fn collect_col_in_expr(
    cols: &mut HashMap<i64, ColumnInfo>,
    col_meta: &[ColumnInfo],
    expr: &Expr,
) -> Result<()> {
    if expr.get_tp() == ExprType::ColumnRef {
        let i = box_try!(expr.get_val().decode_i64());
        if let Entry::Vacant(e) = cols.entry(i) {
            for c in col_meta {
                if c.get_column_id() == i {
                    e.insert(c.clone());
                    return Ok(());
                }
            }
            return Err(box_err!("column meta of {} is missing", i));
        }
    }
    for c in expr.get_children() {
        try!(collect_col_in_expr(cols, col_meta, c));
    }
    Ok(())
}

#[inline]
fn inflate_with_col<'a, T>(
    eval: &mut Evaluator,
    ctx: &EvalContext,
    values: &RowColsDict,
    cols: T,
    h: i64,
) -> Result<()>
where
    T: IntoIterator<Item = &'a ColumnInfo>,
{
    for col in cols {
        let col_id = col.get_column_id();
        if let Entry::Vacant(e) = eval.row.entry(col_id) {
            if col.get_pk_handle() {
                let v = get_pk(col, h);
                e.insert(v);
            } else {
                let value = match values.get(col_id) {
                    None if col.has_default_val() => {
                        // TODO: optimize it to decode default value only once.
                        box_try!(col.get_default_val().decode_col_value(ctx, col))
                    }
                    None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                        return Err(box_err!("column {} of {} is missing", col_id, h));
                    }
                    None => Datum::Null,
                    Some(mut bs) => box_try!(bs.decode_col_value(ctx, col)),
                };
                e.insert(value);
            }
        }
    }
    Ok(())
}
