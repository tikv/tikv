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

use std::usize;
use std::collections::{HashMap, HashSet};
use std::collections::hash_map::Entry;
use std::time::{Instant, Duration};
use std::rc::Rc;
use std::fmt::{self, Display, Formatter, Debug};

use tipb::select::{self, SelectRequest, SelectResponse, Chunk, RowMeta};
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use threadpool::ThreadPool;

use storage::{Engine, SnapshotStore};
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use storage::{engine, Snapshot, Key, ScanMode};
use util::codec::table::TableDecoder;
use util::codec::number::NumberDecoder;
use util::codec::datum::DatumDecoder;
use util::codec::{Datum, table, datum, mysql};
use util::xeval::{Evaluator, EvalContext};
use util::{escape, duration_to_ms, Either};
use util::worker::{BatchRunnable, Scheduler};
use server::OnResponse;

use super::{Error, Result};
use super::aggregate::{self, AggrFunc};
use super::metrics::*;

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;
pub const BATCH_ROW_COUNT: usize = 64;

// If a request has been handled for more than 60 seconds, the client should
// be timeout already, so it can be safely aborted.
const REQUEST_MAX_HANDLE_SECS: u64 = 60;
const REQUEST_CHECKPOINT: usize = 255;

const DEFAULT_ERROR_CODE: i32 = 1;

pub const SINGLE_GROUP: &'static [u8] = b"SingleGroup";

const OUTDATED_ERROR_MSG: &'static str = "request outdated.";

pub struct Host {
    engine: Box<Engine>,
    sched: Scheduler<Task>,
    reqs: HashMap<u64, Vec<RequestTask>>,
    last_req_id: u64,
    pool: ThreadPool,
}

impl Host {
    pub fn new(engine: Box<Engine>, scheduler: Scheduler<Task>, concurrency: usize) -> Host {
        Host {
            engine: engine,
            sched: scheduler,
            reqs: HashMap::new(),
            last_req_id: 0,
            pool: ThreadPool::new_with_name(thd_name!("endpoint-pool"), concurrency),
        }
    }
}

pub enum Task {
    Request(RequestTask),
    SnapRes(u64, engine::Result<Box<Snapshot>>),
}

impl Display for Task {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match *self {
            Task::Request(ref req) => write!(f, "{}", req),
            Task::SnapRes(req_id, _) => write!(f, "snapres [{}]", req_id),
        }
    }
}

pub struct RequestTask {
    req: Request,
    // The deadline before which the task should be responded.
    deadline: Instant,
    on_resp: OnResponse,
}

impl RequestTask {
    pub fn new(req: Request, on_resp: OnResponse) -> RequestTask {
        let deadline = Instant::now() + Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
        RequestTask {
            req: req,
            deadline: deadline,
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

impl BatchRunnable<Task> for Host {
    // TODO: limit pending reqs
    #[allow(for_kv_map)]
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        let now = Instant::now();
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
                    if req.deadline <= now {
                        on_error(Error::Outdated(req.deadline, now, req.req.get_tp()),
                                 req.on_resp);
                        continue;
                    }
                    let key = {
                        let ctx = req.req.get_context();
                        (ctx.get_region_id(),
                         ctx.get_region_epoch().get_conf_ver(),
                         ctx.get_region_epoch().get_version(),
                         ctx.get_peer().get_id(),
                         ctx.get_peer().get_store_id())
                    };
                    let mut group = grouped_reqs.entry(key).or_insert_with(Vec::new);
                    group.push(req);
                }
                Task::SnapRes(q_id, snap_res) => {
                    let reqs = self.reqs.remove(&q_id).unwrap();
                    let snap = match snap_res {
                        Ok(s) => s,
                        Err(e) => {
                            on_snap_failed(e, reqs);
                            continue;
                        }
                    };
                    let end_point = TiDbEndPoint::new(snap);
                    self.pool.execute(move || end_point.handle_requests(reqs));
                }
            }
        }
        for (_, reqs) in grouped_reqs {
            self.last_req_id += 1;
            let id = self.last_req_id;
            let sched = self.sched.clone();
            if let Err(e) = self.engine.async_snapshot(reqs[0].req.get_context(),
                                                       box move |res| {
                                                           sched.schedule(Task::SnapRes(id, res))
                                                               .unwrap()
                                                       }) {
                on_snap_failed(e, reqs);
                continue;
            }
            self.reqs.insert(id, reqs);
        }
    }
}

fn on_error(e: Error, cb: OnResponse) {
    let mut resp = Response::new();
    match e {
        Error::Region(e) => resp.set_region_error(e),
        Error::Locked(info) => resp.set_locked(info),
        Error::Other(_) => resp.set_other_error(format!("{}", e)),
        Error::Outdated(deadline, now, tp) => {
            let t = get_req_type_str(tp);
            let elapsed = now.duration_since(deadline) +
                          Duration::from_secs(REQUEST_MAX_HANDLE_SECS);
            OUTDATED_REQ_WAIT_TIME.with_label_values(&["select", t])
                .observe(elapsed.as_secs() as f64);

            if cfg!(test) {
                resp.set_other_error(OUTDATED_ERROR_MSG.to_owned());
            } else {
                return;
            }
        }
    }
    respond(resp, cb);
}

fn on_snap_failed<E: Into<Error> + Debug>(e: E, reqs: Vec<RequestTask>) {
    error!("failed to get snapshot: {:?}", e);
    on_error(e.into(),
             box move |resp_msg: Message| {
                 for t in reqs {
                     (t.on_resp)(resp_msg.clone());
                 }
             });
}

fn check_if_outdated(deadline: Instant, tp: i64) -> Result<()> {
    let now = Instant::now();
    if deadline <= now {
        return Err(Error::Outdated(deadline, now, tp));
    }
    Ok(())
}

fn respond(resp: Response, on_resp: OnResponse) {
    let mut resp_msg = Message::new();
    resp_msg.set_msg_type(MessageType::CopResp);
    resp_msg.set_cop_resp(resp);
    on_resp.call_box((resp_msg,));
}

pub struct TiDbEndPoint {
    snap: Box<Snapshot>,
}

impl TiDbEndPoint {
    pub fn new(snap: Box<Snapshot>) -> TiDbEndPoint {
        TiDbEndPoint { snap: snap }
    }
}

impl TiDbEndPoint {
    fn handle_requests(&self, reqs: Vec<RequestTask>) {
        for t in reqs {
            let now = Instant::now();
            if t.deadline <= now {
                on_error(Error::Outdated(t.deadline, now, t.req.get_tp()), t.on_resp);
            } else {
                self.handle_request(t.req, t.deadline, t.on_resp);
            }
        }
    }

    fn handle_request(&self, req: Request, deadline: Instant, on_resp: OnResponse) {
        let tp = req.get_tp();
        match tp {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                if let Err(e) = sel.merge_from_bytes(req.get_data()) {
                    on_error(box_err!(e), on_resp);
                    return;
                }
                match self.handle_select(req, deadline, sel) {
                    Ok(r) => respond(r, on_resp),
                    Err(e) => on_error(e, on_resp),
                }
            }
            t => on_error(box_err!("unsupported tp {}", t), on_resp),
        }
    }

    pub fn handle_select(&self,
                         mut req: Request,
                         deadline: Instant,
                         sel: SelectRequest)
                         -> Result<Response> {
        let snap = SnapshotStore::new(self.snap.as_ref(), sel.get_start_ts());
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

        let select_histogram =
            COPR_REQ_HISTOGRAM_VEC.with_label_values(&["select", get_req_type_str(req.get_tp())]);
        let select_timer = select_histogram.start_timer();

        let res = if req.get_tp() == REQ_TYPE_SELECT {
            ctx.get_rows_from_sel(range, limit, desc, deadline)
        } else {
            ctx.get_rows_from_idx(range, limit, desc, deadline)
        };

        select_timer.observe_duration();

        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(()) => sel_resp.set_chunks(RepeatedField::from_vec(ctx.core.chunks)),
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
                           ctx: &EvalContext,
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
                    Some(bs) => box_try!(bs.clone().decode_col_value(ctx, col)),
                };
                e.insert(value);
            }
        }
    }
    Ok(())
}

#[inline]
fn get_chunk(chunks: &mut Vec<Chunk>) -> &mut Chunk {
    if chunks.last().map_or(true, |chunk| chunk.get_rows_meta().len() >= BATCH_ROW_COUNT) {
        let chunk = Chunk::new();
        chunks.push(chunk);
    }
    chunks.last_mut().unwrap()
}

pub struct SelectContextCore {
    ctx: EvalContext,
    sel: SelectRequest,
    eval: Evaluator,
    cols: Either<HashSet<i64>, Vec<i64>>,
    cond_cols: Vec<ColumnInfo>,
    aggr: bool,
    aggr_cols: Vec<ColumnInfo>,
    gks: Vec<Rc<Vec<u8>>>,
    gk_aggrs: HashMap<Rc<Vec<u8>>, Vec<Box<AggrFunc>>>,
    chunks: Vec<Chunk>,
}

impl SelectContextCore {
    fn new(mut sel: SelectRequest) -> Result<SelectContextCore> {
        let cond_cols;
        let mut aggr_cols = vec![];

        {
            let select_cols = if sel.has_table_info() {
                sel.get_table_info().get_columns()
            } else {
                sel.get_index_info().get_columns()
            };
            let mut cond_col_map = HashMap::new();
            try!(collect_col_in_expr(&mut cond_col_map, select_cols, sel.get_field_where()));
            let mut aggr_cols_map = HashMap::new();
            for aggr in sel.get_aggregates() {
                try!(collect_col_in_expr(&mut aggr_cols_map, select_cols, aggr));
            }
            for item in sel.get_group_by() {
                try!(collect_col_in_expr(&mut aggr_cols_map, select_cols, item.get_expr()));
            }
            if !aggr_cols_map.is_empty() {
                for cond_col in cond_col_map.keys() {
                    aggr_cols_map.remove(cond_col);
                }
                aggr_cols = aggr_cols_map.drain().map(|(_, v)| v).collect();
            }
            cond_cols = cond_col_map.drain().map(|(_, v)| v).collect();
        }

        let cols = if sel.has_table_info() {
            Either::Left(sel.get_table_info()
                .get_columns()
                .iter()
                .filter(|c| !c.get_pk_handle())
                .map(|c| c.get_column_id())
                .collect())
        } else {
            let cols = sel.mut_index_info().mut_columns();
            if cols.last().map_or(false, |c| c.get_pk_handle()) {
                cols.pop();
            }
            Either::Right(cols.iter().map(|c| c.get_column_id()).collect())
        };

        Ok(SelectContextCore {
            ctx: box_try!(EvalContext::new(&sel)),
            aggr: !sel.get_aggregates().is_empty() || !sel.get_group_by().is_empty(),
            aggr_cols: aggr_cols,
            sel: sel,
            eval: Default::default(),
            cols: cols,
            cond_cols: cond_cols,
            gks: vec![],
            gk_aggrs: map![],
            chunks: vec![],
        })
    }

    fn handle_row(&mut self, h: i64, row_data: HashMap<i64, &[u8]>) -> Result<usize> {
        // clear all dirty values.
        self.eval.row.clear();

        if try!(self.should_skip(h, &row_data)) {
            return Ok(0);
        }

        if self.aggr {
            try!(self.aggregate(h, &row_data));
            Ok(0)
        } else {
            try!(self.get_row(h, row_data));
            Ok(1)
        }
    }

    fn should_skip(&mut self, h: i64, values: &HashMap<i64, &[u8]>) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        try!(inflate_with_col(&mut self.eval, &self.ctx, values, &self.cond_cols, h));
        let res = box_try!(self.eval.eval(&self.ctx, self.sel.get_field_where()));
        let b = box_try!(res.into_bool());
        Ok(b.map_or(true, |v| !v))
    }

    fn get_row(&mut self, h: i64, values: HashMap<i64, &[u8]>) -> Result<()> {
        let chunk = get_chunk(&mut self.chunks);
        let mut meta = RowMeta::new();
        meta.set_handle(h);
        let cols = if self.sel.has_table_info() {
            self.sel.get_table_info().get_columns()
        } else {
            self.sel.get_index_info().get_columns()
        };
        let last_len = chunk.get_rows_data().len();
        for col in cols {
            let col_id = col.get_column_id();
            if let Some(v) = values.get(&col_id) {
                chunk.mut_rows_data().extend_from_slice(v);
                continue;
            }
            if col.get_pk_handle() {
                box_try!(datum::encode_to(chunk.mut_rows_data(), &[get_pk(col, h)], false));
            } else if mysql::has_not_null_flag(col.get_flag() as u64) {
                return Err(box_err!("column {} of {} is missing", col_id, h));
            } else {
                box_try!(datum::encode_to(chunk.mut_rows_data(), &[Datum::Null], false));
            }
        }
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

    fn aggregate(&mut self, h: i64, values: &HashMap<i64, &[u8]>) -> Result<()> {
        try!(inflate_with_col(&mut self.eval, &self.ctx, values, &self.aggr_cols, h));
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

    /// Convert aggregate partial result to rows.
    /// Data layout example:
    /// SQL: select count(c1), sum(c2), avg(c3) from t;
    /// Aggs: count(c1), sum(c2), avg(c3)
    /// Rows: groupKey1, count1, value2, count3, value3
    ///       groupKey2, count1, value2, count3, value3
    fn aggr_rows(&mut self) -> Result<()> {
        self.chunks = Vec::with_capacity((self.gk_aggrs.len() + BATCH_ROW_COUNT - 1) /
                                         BATCH_ROW_COUNT);
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
                         desc: bool,
                         deadline: Instant)
                         -> Result<()> {
        let mut collected = 0;
        for ran in ranges {
            if collected >= limit {
                break;
            }
            let timer = Instant::now();
            let row_cnt = try!(self.get_rows_from_range(ran, limit, desc, deadline));
            debug!("fetch {} rows takes {} ms",
                   row_cnt,
                   duration_to_ms(timer.elapsed()));
            collected += row_cnt;
            try!(check_if_outdated(deadline, REQ_TYPE_SELECT));
        }
        if self.core.aggr {
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

    fn get_rows_from_range(&mut self,
                           range: KeyRange,
                           limit: usize,
                           desc: bool,
                           deadline: Instant)
                           -> Result<usize> {
        let mut row_count = 0;
        if is_point(&range) {
            let value = match try!(self.snap.get(&Key::from_raw(range.get_start()))) {
                None => return Ok(0),
                Some(v) => v,
            };
            let values = {
                let ids = self.core.cols.as_ref().left().unwrap();
                box_try!(table::cut_row(&value, ids))
            };
            let h = box_try!(table::decode_handle(range.get_start()));
            row_count += try!(self.core.handle_row(h, values));
        } else {
            let mut seek_key = if desc {
                range.get_end().to_vec()
            } else {
                range.get_start().to_vec()
            };
            let mut scanner = try!(self.snap.scanner(if desc {
                                                         ScanMode::Backward
                                                     } else {
                                                         ScanMode::Forward
                                                     },
                                                     self.key_only()));
            while limit > row_count {
                if row_count & REQUEST_CHECKPOINT == 0 {
                    try!(check_if_outdated(deadline, REQ_TYPE_SELECT));
                }
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
                let h = box_try!(table::decode_handle(&key));
                let row_data = {
                    let ids = self.core.cols.as_ref().left().unwrap();
                    box_try!(table::cut_row(&value, ids))
                };
                row_count += try!(self.core.handle_row(h, row_data));
                seek_key = if desc {
                    box_try!(table::truncate_as_row_key(&key)).to_vec()
                } else {
                    prefix_next(&key)
                };
            }
        }
        Ok(row_count)
    }

    fn get_rows_from_idx(&mut self,
                         ranges: Vec<KeyRange>,
                         limit: usize,
                         desc: bool,
                         deadline: Instant)
                         -> Result<()> {
        let mut collected = 0;
        for r in ranges {
            if collected >= limit {
                break;
            }
            collected += try!(self.get_idx_row_from_range(r, limit, desc, deadline));
            try!(check_if_outdated(deadline, REQ_TYPE_SELECT));
        }
        if self.core.aggr {
            self.core.aggr_rows()
        } else {
            Ok(())
        }
    }

    fn get_idx_row_from_range(&mut self,
                              r: KeyRange,
                              limit: usize,
                              desc: bool,
                              deadline: Instant)
                              -> Result<usize> {
        let mut row_cnt = 0;
        let mut seek_key = if desc {
            r.get_end().to_vec()
        } else {
            r.get_start().to_vec()
        };
        let mut scanner = try!(self.snap.scanner(if desc {
                                                     ScanMode::Backward
                                                 } else {
                                                     ScanMode::Forward
                                                 },
                                                 self.key_only()));
        while row_cnt < limit {
            if row_cnt & REQUEST_CHECKPOINT == 0 {
                try!(check_if_outdated(deadline, REQ_TYPE_SELECT));
            }
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
            {
                let (values, mut handle) = {
                    let ids = self.core.cols.as_ref().right().unwrap();
                    box_try!(table::cut_idx_key(&key, ids))
                };
                let handle = if handle.is_empty() {
                    box_try!(val.as_slice().read_i64::<BigEndian>())
                } else {
                    box_try!(handle.decode_datum()).i64()
                };
                row_cnt += try!(self.core.handle_row(handle, values));
            }
            seek_key = if desc { key } else { prefix_next(&key) };
        }
        Ok(row_cnt)
    }
}

pub const STR_REQ_TYPE_SELECT: &'static str = "select";
pub const STR_REQ_TYPE_INDEX: &'static str = "index";
pub const STR_REQ_TYPE_UNKNOWN: &'static str = "unknown";

#[inline]
pub fn get_req_type_str(tp: i64) -> &'static str {
    match tp {
        REQ_TYPE_SELECT => STR_REQ_TYPE_SELECT,
        REQ_TYPE_INDEX => STR_REQ_TYPE_INDEX,
        _ => STR_REQ_TYPE_UNKNOWN,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use util::worker::Worker;
    use storage::engine::{self, TEMP_DIR};

    use kvproto::coprocessor::Request;
    use kvproto::msgpb::MessageType;

    use std::sync::*;
    use std::time::Duration;

    #[test]
    fn test_get_req_type_str() {
        assert_eq!(get_req_type_str(REQ_TYPE_SELECT), STR_REQ_TYPE_SELECT);
        assert_eq!(get_req_type_str(REQ_TYPE_INDEX), STR_REQ_TYPE_INDEX);
        assert_eq!(get_req_type_str(0), STR_REQ_TYPE_UNKNOWN);
    }

    #[test]
    fn test_req_outdated() {
        let mut worker = Worker::new("test-endpoint");
        let engine = engine::new_local_engine(TEMP_DIR, &[]).unwrap();
        let end_point = Host::new(engine, worker.scheduler(), 1);
        worker.start_batch(end_point, 30).unwrap();
        let (tx, rx) = mpsc::channel();
        let mut task = RequestTask::new(Request::new(),
                                        box move |msg| {
                                            tx.send(msg).unwrap();
                                        });
        task.deadline -= Duration::from_secs(super::REQUEST_MAX_HANDLE_SECS);
        worker.schedule(Task::Request(task)).unwrap();
        let resp = rx.recv_timeout(Duration::from_secs(3)).unwrap();
        assert_eq!(resp.get_msg_type(), MessageType::CopResp);
        let copr_resp = resp.get_cop_resp();
        assert!(copr_resp.has_other_error());
        assert_eq!(copr_resp.get_other_error(), super::OUTDATED_ERROR_MSG);
    }
}
