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

use std::sync::Arc;
use std::usize;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::time::Instant;
use std::boxed::FnBox;
use std::rc::Rc;
use std::fmt::{self, Display, Formatter};

use tipb::select::{self, SelectRequest, SelectResponse, Row};
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use threadpool::ThreadPool;

use storage::{Engine, SnapshotStore};
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use storage::{Snapshot, Key};
use util::codec::table::TableDecoder;
use util::codec::number::NumberDecoder;
use util::codec::{Datum, table, datum, mysql};
use util::xeval::Evaluator;
use util::{escape, duration_to_ms};
use util::worker::BatchRunnable;
use util::SlowTimer;
use server::OnResponse;

use super::{Error, Result};
use super::aggregate::{self, AggrFunc};

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;

const DEFAULT_ERROR_CODE: i32 = 1;

// TODO: make this number configurable.
const DEFAULT_POOL_SIZE: usize = 8;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

pub struct Host {
    snap_endpoint: Arc<TiDbEndPoint>,
    pool: ThreadPool,
}

impl Host {
    pub fn new(engine: Arc<Box<Engine>>) -> Host {
        Host {
            snap_endpoint: Arc::new(TiDbEndPoint::new(engine)),
            pool: ThreadPool::new_with_name(thd_name!("endpoint-pool"), DEFAULT_POOL_SIZE),
        }
    }
}

pub struct RequestTask {
    req: Request,
    on_resp: OnResponse,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse) -> RequestTask {
        RequestTask {
            req: req,
            on_resp: on_resp,
        }
    }
}

impl Display for RequestTask {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f,
               "request [context {:?}, tp: {}, ranges: {:?}]",
               self.req.get_context(),
               self.req.get_tp(),
               self.req.get_ranges())
    }
}

impl BatchRunnable<RequestTask> for Host {
    #[allow(for_kv_map)]
    fn run_batch(&mut self, reqs: &mut Vec<RequestTask>) {
        let mut grouped_reqs = map![];
        for req in reqs.drain(..) {
            let key = {
                let ctx = req.req.get_context();
                (ctx.get_region_id(),
                 ctx.get_region_epoch().get_conf_ver(),
                 ctx.get_region_epoch().get_version(),
                 ctx.get_peer().get_id(),
                 ctx.get_peer().get_store_id())
            };
            let mut group = grouped_reqs.entry(key).or_insert_with(|| vec![]);
            group.push(req);
        }
        for (_, reqs) in grouped_reqs {
            let end_point = self.snap_endpoint.clone();
            self.pool.execute(move || end_point.handle_requests(reqs));
        }
    }
}

type ResponseHandler = Box<FnBox(Response) -> ()>;

fn on_error(e: Error, cb: ResponseHandler) {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => resp.set_region_error(e),
        Error::Locked(info) => resp.set_locked(info),
        Error::Other(_) => resp.set_other_error(format!("{}", e)),
    }
    cb(resp)
}

pub struct TiDbEndPoint {
    engine: Arc<Box<Engine>>,
}

impl TiDbEndPoint {
    pub fn new(engine: Arc<Box<Engine>>) -> TiDbEndPoint {
        TiDbEndPoint { engine: engine }
    }
}

impl TiDbEndPoint {
    fn handle_requests(&self, reqs: Vec<RequestTask>) {
        let ts = Instant::now();
        let snap = match self.engine.snapshot(reqs[0].req.get_context()) {
            Ok(s) => s,
            Err(e) => {
                error!("failed to get snapshot: {:?}", e);
                on_error(e.into(),
                         box move |r| {
                    let mut resp_msg = Message::new();
                    resp_msg.set_msg_type(MessageType::CopResp);
                    resp_msg.set_cop_resp(r);
                    for t in reqs {
                        t.on_resp.call_box((resp_msg.clone(),));
                    }
                });
                return;
            }
        };
        metric_time!("copr.snapshot", ts.elapsed());
        for t in reqs {
            let timer = SlowTimer::new();
            let tp = t.req.get_tp();
            self.handle_request(snap.as_ref(), t.req, t.on_resp);
            metric_time!(&format!("copr.request.{}", tp), timer.elapsed());
        }
    }

    fn handle_request(&self, snap: &Snapshot, req: Request, on_resp: OnResponse) {
        let cb = box move |r| {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CopResp);
            resp_msg.set_cop_resp(r);
            on_resp.call_box((resp_msg,));
        };
        match req.get_tp() {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                if let Err(e) = sel.merge_from_bytes(req.get_data()) {
                    on_error(box_err!(e), cb);
                    return;
                }
                match self.handle_select(snap, req, sel) {
                    Ok(r) => cb(r),
                    Err(e) => on_error(e, cb),
                }
            }
            t => on_error(box_err!("unsupported tp {}", t), cb),
        }
    }

    pub fn handle_select(&self,
                         snap: &Snapshot,
                         mut req: Request,
                         sel: SelectRequest)
                         -> Result<Response> {
        let snap = SnapshotStore::new(snap, sel.get_start_ts());
        let mut ctx = try!(SelectContext::new(sel, snap));
        let mut range = req.take_ranges().into_vec();
        let desc = ctx.core.sel.get_order_by().first().map_or(false, |o| o.get_desc());
        debug!("scanning range: {:?}", range);
        if desc {
            range.reverse();
        }
        let limit = if ctx.core.sel.has_limit() {
            ctx.core.sel.get_limit() as usize
        } else {
            usize::MAX
        };
        let sel_ts = Instant::now();
        let res = if req.get_tp() == REQ_TYPE_SELECT {
            ctx.get_rows_from_sel(range, limit, desc)
        } else {
            ctx.get_rows_from_idx(range, limit, desc)
        };
        metric_time!(&format!("copr.select.{}", req.get_tp()), sel_ts.elapsed());
        let resp_ts = Instant::now();
        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(rows) => sel_resp.set_rows(RepeatedField::from_vec(rows)),
            Err(e) => {
                if let Error::Other(_) = e {
                    // should we handle locked here too?
                    sel_resp.set_error(to_pb_error(&e));
                    // TODO add detail error
                    resp.set_other_error(format!("{}", e));
                } else {
                    // other error should be handle by ti client.
                    return Err(e);
                }
            }
        }
        let data = box_try!(sel_resp.write_to_bytes());
        resp.set_data(data);
        metric_time!("copr.compose_resp", resp_ts.elapsed());
        Ok(resp)
    }
}

fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(DEFAULT_ERROR_CODE);
    e.set_msg(format!("{}", err));
    e
}

fn prefix_next(key: &[u8]) -> Vec<u8> {
    let mut nk = key.to_vec();
    if nk.is_empty() {
        nk.push(0);
        return nk;
    }
    let mut i = nk.len() - 1;
    loop {
        if nk[i] == 255 {
            nk[i] = 0;
        } else {
            nk[i] += 1;
            return nk;
        }
        if i == 0 {
            nk = key.to_vec();
            nk.push(0);
            return nk;
        }
        i -= 1;
    }
}

/// `is_point` checks if the key range represents a point.
fn is_point(range: &KeyRange) -> bool {
    range.get_end() == &*prefix_next(range.get_start())
}

#[inline]
fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if mysql::has_unsigned_flag(col.get_flag() as u64) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

#[inline]
fn inflate_with_col<'a, T>(eval: &mut Evaluator,
                           values: &HashMap<i64, &[u8]>,
                           cols: T,
                           h: i64)
                           -> Result<()>
    where T: IntoIterator<Item = &'a ColumnInfo>
{
    for col in cols {
        let col_id = col.get_column_id();
        if let Entry::Vacant(e) = eval.row.entry(col_id) {
            if col.get_pk_handle() {
                let v = get_pk(col, h);
                e.insert(v);
            } else {
                let value = match values.get(&col_id) {
                    None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                        return Err(box_err!("column {} of {} is missing", col_id, h));
                    }
                    None => Datum::Null,
                    Some(bs) => box_try!(bs.clone().decode_col_value(col)),
                };
                e.insert(value);
            }
        }
    }
    Ok(())
}

pub struct SelectContextCore {
    sel: SelectRequest,
    eval: Evaluator,
    cols: HashSet<i64>,
    cond_cols: HashMap<i64, ColumnInfo>,
    aggr: bool,
    gks: Vec<Rc<Vec<u8>>>,
    gk_aggrs: HashMap<Rc<Vec<u8>>, Vec<Box<AggrFunc>>>,
}

impl SelectContextCore {
    fn new(sel: SelectRequest) -> Result<SelectContextCore> {
        let cols;
        let mut cond_cols;

        {
            let select_cols = if sel.has_table_info() {
                sel.get_table_info().get_columns()
            } else {
                sel.get_index_info().get_columns()
            };
            cols = select_cols.iter()
                .filter(|c| !c.get_pk_handle())
                .map(|c| c.get_column_id())
                .collect();
            cond_cols = HashMap::new();
            try!(collect_col_in_expr(&mut cond_cols, select_cols, sel.get_field_where()));
        }


        Ok(SelectContextCore {
            aggr: !sel.get_aggregates().is_empty() || !sel.get_group_by().is_empty(),
            sel: sel,
            eval: Default::default(),
            cols: cols,
            cond_cols: cond_cols,
            gks: vec![],
            gk_aggrs: map![],
        })
    }

    fn handle_row(&mut self, key: &[u8], value: &[u8], dest: &mut Vec<Row>) -> Result<()> {
        let h = box_try!(table::decode_handle(key));

        let row_data = box_try!(table::cut_row(value, &self.cols));
        // clear all dirty values.
        self.eval.row.clear();

        if try!(self.should_skip(h, &row_data)) {
            return Ok(());
        }

        if self.aggr {
            try!(self.aggregate(h, &row_data));
        } else {
            dest.push(try!(self.get_row(h, row_data)))
        }
        Ok(())
    }

    fn should_skip(&mut self, h: i64, values: &HashMap<i64, &[u8]>) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        try!(inflate_with_col(&mut self.eval, values, self.cond_cols.values(), h));
        let res = box_try!(self.eval.eval(self.sel.get_field_where()));
        let b = box_try!(res.into_bool());
        Ok(b.map_or(true, |v| !v))
    }

    fn get_row(&mut self, h: i64, values: HashMap<i64, &[u8]>) -> Result<Row> {
        let mut row = Row::new();
        let handle = box_try!(datum::encode_value(&[Datum::I64(h)]));
        row.set_handle(handle);
        for col in self.sel.get_table_info().get_columns() {
            let col_id = col.get_column_id();
            if let Some(v) = values.get(&col_id) {
                row.mut_data().extend_from_slice(v);
                continue;
            }
            if col.get_pk_handle() {
                box_try!(datum::encode_to(row.mut_data(), &[get_pk(col, h)], false));
            } else if mysql::has_not_null_flag(col.get_flag() as u64) {
                return Err(box_err!("column {} of {} is missing", col_id, h));
            } else {
                box_try!(datum::encode_to(row.mut_data(), &[Datum::Null], false));
            }
        }
        Ok(row)
    }

    fn get_group_key(&mut self) -> Result<Vec<u8>> {
        let items = self.sel.get_group_by();
        if items.is_empty() {
            return Ok(SINGLE_GROUP.to_vec());
        }
        let mut vals = Vec::with_capacity(items.len());
        for item in items {
            let v = box_try!(self.eval.eval(item.get_expr()));
            vals.push(v);
        }
        let res = box_try!(datum::encode_value(&vals));
        Ok(res)
    }

    fn aggregate(&mut self, h: i64, values: &HashMap<i64, &[u8]>) -> Result<()> {
        try!(inflate_with_col(&mut self.eval,
                              values,
                              self.sel.get_table_info().get_columns(),
                              h));
        let gk = Rc::new(try!(self.get_group_key()));
        let aggr_exprs = self.sel.get_aggregates();
        match self.gk_aggrs.entry(gk.clone()) {
            Entry::Occupied(e) => {
                let funcs = e.into_mut();
                for (expr, func) in aggr_exprs.iter().zip(funcs) {
                    // TODO: cache args
                    let args = box_try!(self.eval.batch_eval(expr.get_children()));
                    try!(func.update(args));
                }
            }
            Entry::Vacant(e) => {
                let mut aggrs = Vec::with_capacity(aggr_exprs.len());
                for expr in aggr_exprs {
                    let mut aggr = try!(aggregate::build_aggr_func(expr));
                    let args = box_try!(self.eval.batch_eval(expr.get_children()));
                    try!(aggr.update(args));
                    aggrs.push(aggr);
                }
                self.gks.push(gk);
                e.insert(aggrs);
            }
        }
        Ok(())
    }

    /// Convert aggregate partial result to rows.
    /// Data layout example:
    /// SQL: select count(c1), sum(c2), avg(c3) from t;
    /// Aggs: count(c1), sum(c2), avg(c3)
    /// Rows: groupKey1, count1, value2, count3, value3
    ///       groupKey2, count1, value2, count3, value3
    fn aggr_rows(&mut self) -> Result<Vec<Row>> {
        let mut rows = Vec::with_capacity(self.gk_aggrs.len());
        // Each aggregate partial result will be converted to two datum.
        let mut row_data = Vec::with_capacity(1 + 2 * self.sel.get_aggregates().len());
        for gk in self.gks.drain(..) {
            let aggrs = self.gk_aggrs.remove(&gk).unwrap();

            let mut row = Row::new();
            // The first column is group key.
            row_data.push(Datum::Bytes(Rc::try_unwrap(gk).unwrap()));
            for mut aggr in aggrs {
                try!(aggr.calc(&mut row_data));
            }
            row.set_data(box_try!(datum::encode_value(&row_data)));
            rows.push(row);
            row_data.clear();
        }
        Ok(rows)
    }
}

fn collect_col_in_expr(cols: &mut HashMap<i64, ColumnInfo>,
                       col_meta: &[ColumnInfo],
                       expr: &Expr)
                       -> Result<()> {
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


pub struct SelectContext<'a> {
    snap: SnapshotStore<'a>,
    core: SelectContextCore,
}

impl<'a> SelectContext<'a> {
    fn new(sel: SelectRequest, snap: SnapshotStore<'a>) -> Result<SelectContext<'a>> {
        Ok(SelectContext {
            core: try!(SelectContextCore::new(sel)),
            snap: snap,
        })
    }

    fn get_rows_from_sel(&mut self,
                         ranges: Vec<KeyRange>,
                         limit: usize,
                         desc: bool)
                         -> Result<Vec<Row>> {
        let mut rows = vec![];
        for ran in ranges {
            if rows.len() >= limit {
                break;
            }
            let timer = Instant::now();
            let ran_rows = try!(self.get_rows_from_range(ran, limit - rows.len(), desc));
            debug!("fetch {} rows takes {} ms",
                   ran_rows.len(),
                   duration_to_ms(timer.elapsed()));
            rows.extend(ran_rows);
        }
        Ok(rows)
    }

    fn get_rows_from_range(&mut self,
                           range: KeyRange,
                           limit: usize,
                           desc: bool)
                           -> Result<Vec<Row>> {
        let mut rows = vec![];
        if limit == 0 {
            return Ok(rows);
        }
        if is_point(&range) {
            let value = match try!(self.snap.get(&Key::from_raw(range.get_start()))) {
                None => return Ok(rows),
                Some(v) => v,
            };
            try!(self.core.handle_row(range.get_start(), &value, &mut rows));
        } else {
            let mut seek_key = if desc {
                range.get_end().to_vec()
            } else {
                range.get_start().to_vec()
            };
            let mut scanner = try!(self.snap.scanner());
            while limit > rows.len() {
                let kv = if desc {
                    try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
                } else {
                    try!(scanner.seek(Key::from_raw(&seek_key)))
                };
                let (key, value) = match kv {
                    Some((key, value)) => (box_try!(key.raw()), value),
                    None => break,
                };
                if range.get_start() > &key || range.get_end() <= &key {
                    debug!("key: {} out of range [{}, {})",
                           escape(&key),
                           escape(range.get_start()),
                           escape(range.get_end()));
                    break;
                }
                try!(self.core.handle_row(&key, &value, &mut rows));
                seek_key = if desc {
                    box_try!(table::truncate_as_row_key(&key)).to_vec()
                } else {
                    prefix_next(&key)
                };
            }
        }
        if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(rows)
        }
    }

    fn get_rows_from_idx(&self,
                         ranges: Vec<KeyRange>,
                         limit: usize,
                         desc: bool)
                         -> Result<Vec<Row>> {
        let mut rows = vec![];
        for r in ranges {
            if rows.len() >= limit {
                break;
            }
            let part = try!(self.get_idx_row_from_range(r, limit, desc));
            rows.extend(part);
        }
        Ok(rows)
    }

    fn get_idx_row_from_range(&self, r: KeyRange, limit: usize, desc: bool) -> Result<Vec<Row>> {
        let mut rows = vec![];
        let info = self.core.sel.get_index_info();
        let mut seek_key = if desc {
            r.get_end().to_vec()
        } else {
            r.get_start().to_vec()
        };
        let mut scanner = try!(self.snap.scanner());
        while rows.len() < limit {
            let nk = if desc {
                try!(scanner.reverse_seek(Key::from_raw(&seek_key)))
            } else {
                try!(scanner.seek(Key::from_raw(&seek_key)))
            };
            let (key, val) = match nk {
                Some((key, val)) => (box_try!(key.raw()), val),
                None => break,
            };
            if r.get_start() > &key || r.get_end() <= &key {
                debug!("key: {} out of range [{}, {})",
                       escape(&key),
                       escape(r.get_start()),
                       escape(r.get_end()));
                break;
            }
            let mut datums = box_try!(table::decode_index_key(&key));
            let handle = if datums.len() > info.get_columns().len() {
                datums.pop().unwrap()
            } else {
                let h = box_try!(val.as_slice().read_i64::<BigEndian>());
                Datum::I64(h)
            };
            let data = box_try!(datum::encode_value(&datums));
            let handle_data = box_try!(datum::encode_value(&[handle]));
            let mut row = Row::new();
            row.set_handle(handle_data);
            row.set_data(data);
            rows.push(row);
            seek_key = if desc {
                key
            } else {
                prefix_next(&key)
            };
        }
        Ok(rows)
    }
}
