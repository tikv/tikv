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

use std::{mem, usize};
use std::sync::Arc;
use tipb::select::{Chunk, RowMeta, SelectRequest, SelectResponse};
use tipb::schema::ColumnInfo;
use tipb::expression::{ByItem, Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use kvproto::coprocessor::{KeyRange, Response};

use coprocessor::dag::executor::{ScanOn, Scanner};
use coprocessor::codec::{datum, mysql, table};
use coprocessor::codec::table::{RowColsDict, TableDecoder};
use coprocessor::codec::datum::Datum;
use coprocessor::metrics::*;
use coprocessor::{Error, Result};
use coprocessor::endpoint::{get_pk, is_point, to_pb_error, ReqContext, SINGLE_GROUP};
use util::Either;
use util::collections::{HashMap, HashMapEntry as Entry, HashSet};
use util::codec::number::NumberDecoder;
use storage::{Key, Snapshot, SnapshotStore, Statistics};

use super::xeval::{EvalContext, Evaluator};
use super::aggregate::{self, AggrFunc};
use super::topn_heap::TopNHeap;

const REQUEST_CHECKPOINT: usize = 255;

pub struct SelectContext {
    snap: SnapshotStore,
    statistics: Statistics,
    core: SelectContextCore,
    req_ctx: Arc<ReqContext>,
    scanner: Option<Scanner>,
}

impl SelectContext {
    pub fn new(
        sel: SelectRequest,
        snap: Box<Snapshot>,
        req_ctx: Arc<ReqContext>,
        batch_row_limit: usize,
    ) -> Result<SelectContext> {
        let snap = SnapshotStore::new(
            snap,
            sel.get_start_ts(),
            req_ctx.isolation_level,
            req_ctx.fill_cache,
        );
        Ok(SelectContext {
            core: SelectContextCore::new(sel, batch_row_limit)?,
            snap: snap,
            statistics: Statistics::default(),
            req_ctx: req_ctx,
            scanner: None,
        })
    }

    pub fn handle_request(&mut self, mut ranges: Vec<KeyRange>) -> Result<Response> {
        if self.core.desc_scan {
            ranges.reverse();
        }

        let res = if self.req_ctx.table_scan {
            self.get_rows_from_sel(ranges)
        } else {
            self.get_rows_from_idx(ranges)
        };

        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(()) => {
                let chunks = mem::replace(&mut self.core.chunks, Vec::new());
                sel_resp.set_chunks(RepeatedField::from_vec(chunks));
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

    fn reset_scan_range(&mut self, range: KeyRange) -> Result<()> {
        if self.scanner.is_none() {
            let scan_on = if self.req_ctx.table_scan {
                ScanOn::Table
            } else {
                ScanOn::Index
            };
            let desc = self.core.desc_scan;
            let key_only = self.key_only();
            let s = Scanner::new(&self.snap, scan_on, desc, key_only, range)?;
            self.scanner = Some(s);
            return Ok(());
        }
        let s = self.scanner.as_mut().unwrap();
        s.reset_range(range, &self.snap)?;
        Ok(())
    }

    fn get_rows_from_sel(&mut self, ranges: Vec<KeyRange>) -> Result<()> {
        let mut collected = 0;
        for ran in ranges {
            if collected >= self.core.limit {
                break;
            }
            collected += self.get_rows_from_range(ran)?;
            self.req_ctx.check_if_outdated()?;
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
            COPR_GET_OR_SCAN_COUNT.with_label_values(&["point"]).inc();
            let value = match self.snap
                .get(&Key::from_raw(range.get_start()), &mut self.statistics)?
            {
                None => return Ok(0),
                Some(v) => v,
            };
            let values = {
                let ids = self.core.cols.as_ref().left().unwrap();
                box_try!(table::cut_row(value, ids))
            };
            let h = box_try!(table::decode_handle(range.get_start()));
            row_count += self.core.handle_row(h, values)?;
        } else {
            self.reset_scan_range(range)?;
            let scanner = self.scanner.as_mut().unwrap();
            while self.core.limit > row_count {
                if row_count & REQUEST_CHECKPOINT == 0 {
                    self.req_ctx.check_if_outdated()?;
                }
                COPR_GET_OR_SCAN_COUNT.with_label_values(&["range"]).inc();
                if let Some((key, value)) = scanner.next_row()? {
                    let h = box_try!(table::decode_handle(&key));
                    let row_data = {
                        let ids = self.core.cols.as_ref().left().unwrap();
                        box_try!(table::cut_row(value, ids))
                    };
                    row_count += self.core.handle_row(h, row_data)?;
                    continue;
                }
                break;
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
            collected += self.get_idx_row_from_range(r)?;
            self.req_ctx.check_if_outdated()?;
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
        self.reset_scan_range(r)?;
        let scanner = self.scanner.as_mut().unwrap();
        while row_cnt < self.core.limit {
            if row_cnt & REQUEST_CHECKPOINT == 0 {
                self.req_ctx.check_if_outdated()?;
            }
            if let Some((key, val)) = scanner.next_row()? {
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
                row_cnt += self.core.handle_row(handle, values)?;
                continue;
            }
            break;
        }
        Ok(row_cnt)
    }

    pub fn collect_statistics_into(self, stats: &mut Statistics) {
        stats.add(&self.statistics);
        if let Some(scanner) = self.scanner {
            scanner.collect_statistics_into(stats);
        }
    }
}


struct SelectContextCore {
    ctx: Arc<EvalContext>,
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
    order_cols: Arc<Vec<ByItem>>,
    limit: usize,
    desc_scan: bool,
    gks: Vec<Arc<Vec<u8>>>,
    gk_aggrs: HashMap<Arc<Vec<u8>>, Vec<Box<AggrFunc>>>,
    chunks: Vec<Chunk>,
    batch_row_limit: usize,
}

impl SelectContextCore {
    fn new(sel: SelectRequest, batch_row_limit: usize) -> Result<SelectContextCore> {
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
                collect_col_in_expr(&mut cond_col_map, select_cols, sel.get_field_where())?;
            }
            let mut aggr_cols_map = HashMap::default();
            for aggr in sel.get_aggregates() {
                collect_col_in_expr(&mut aggr_cols_map, select_cols, aggr)?;
            }
            for item in sel.get_group_by() {
                collect_col_in_expr(&mut aggr_cols_map, select_cols, item.get_expr())?;
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
                collect_col_in_expr(&mut topn_col_map, select_cols, item.get_expr())?
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
            ctx: Arc::new(box_try!(EvalContext::new(
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
                    Some(TopNHeap::new(limit)?)
                } else {
                    None
                }
            },
            order_cols: Arc::new(order_by_cols),
            limit: limit,
            desc_scan: desc_can,
            batch_row_limit: batch_row_limit,
        })
    }

    fn handle_row(&mut self, h: i64, row_data: RowColsDict) -> Result<usize> {
        // clear all dirty values.
        self.eval.row.clear();
        if self.should_skip(h, &row_data)? {
            return Ok(0);
        }

        // topn & aggr won't appear together
        if self.topn {
            self.collect_topn_row(h, row_data)?;
            Ok(0)
        } else if self.aggr {
            self.aggregate(h, &row_data)?;
            Ok(0)
        } else {
            self.get_row(h, row_data)?;
            Ok(1)
        }
    }

    fn should_skip(&mut self, h: i64, values: &RowColsDict) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        inflate_with_col(&mut self.eval, &self.ctx, values, &self.cond_cols, h)?;
        let res = box_try!(self.eval.eval(&self.ctx, self.sel.get_field_where()));
        let b = box_try!(res.into_bool(&self.ctx));
        Ok(b.map_or(true, |v| !v))
    }

    fn collect_topn_row(&mut self, h: i64, values: RowColsDict) -> Result<()> {
        inflate_with_col(&mut self.eval, &self.ctx, &values, &self.topn_cols, h)?;
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
        let chunk = get_chunk(&mut self.chunks, self.batch_row_limit);
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
        inflate_with_col(&mut self.eval, &self.ctx, values, &self.aggr_cols, h)?;
        let gk = Arc::new(self.get_group_key()?);
        let aggr_exprs = self.sel.get_aggregates();
        match self.gk_aggrs.entry(gk.clone()) {
            Entry::Occupied(e) => {
                let funcs = e.into_mut();
                for (expr, func) in aggr_exprs.iter().zip(funcs) {
                    // TODO: cache args
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    func.update(&self.ctx, args)?;
                }
            }
            Entry::Vacant(e) => {
                let mut aggrs = Vec::with_capacity(aggr_exprs.len());
                for expr in aggr_exprs {
                    let mut aggr = aggregate::build_aggr_func(expr.get_tp())?;
                    let args = box_try!(self.eval.batch_eval(&self.ctx, expr.get_children()));
                    aggr.update(&self.ctx, args)?;
                    aggrs.push(aggr);
                }
                self.gks.push(gk);
                e.insert(aggrs);
            }
        }
        Ok(())
    }

    fn collect_topn_rows(&mut self) -> Result<()> {
        let sorted_data = self.topn_heap.take().unwrap().into_sorted_vec()?;
        for row in sorted_data {
            self.get_row(row.handle, row.data)?;
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
            (self.gk_aggrs.len() + self.batch_row_limit - 1) / self.batch_row_limit,
        );
        // Each aggregate partial result will be converted to two datum.
        let mut row_data = Vec::with_capacity(1 + 2 * self.sel.get_aggregates().len());
        for gk in self.gks.drain(..) {
            let aggrs = self.gk_aggrs.remove(&gk).unwrap();
            let chunk = get_chunk(&mut self.chunks, self.batch_row_limit);
            // The first column is group key.
            row_data.push(Datum::Bytes(Arc::try_unwrap(gk).unwrap()));
            for mut aggr in aggrs {
                aggr.calc(&mut row_data)?;
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
        collect_col_in_expr(cols, col_meta, c)?;
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

#[inline]
fn get_chunk(chunks: &mut Vec<Chunk>, batch_row_limit: usize) -> &mut Chunk {
    if chunks
        .last()
        .map_or(true, |chunk| chunk.get_rows_meta().len() >= batch_row_limit)
    {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}
