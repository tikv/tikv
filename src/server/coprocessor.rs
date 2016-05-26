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
use std::collections::HashMap;
use std::{result, error};
use std::time::Instant;
use std::boxed::FnBox;
use std::cell::RefCell;
use std::fmt::{self, Display, Formatter};

use tipb::select::{self, SelectRequest, SelectResponse, Row};
use tipb::schema::ColumnInfo;
use tipb::expression::{Expr, ExprType};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use threadpool::ThreadPool;

use storage::{Engine, SnapshotStore, engine, txn, mvcc};
use kvproto::kvrpcpb::LockInfo;
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use kvproto::errorpb;
use storage::{Snapshot, Key};
use util::codec::number::NumberDecoder;
use util::codec::datum::DatumDecoder;
use util::codec::{Datum, table, datum, mysql};
use util::xeval::Evaluator;
use util::{as_slice, escape};
use util::worker::BatchRunnable;
use util::SlowTimer;
use super::OnResponse;

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;

const DEFAULT_ERROR_CODE: i32 = 1;

// TODO: make this number configurable.
const DEFAULT_POOL_SIZE: usize = 8;

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Region(err: errorpb::Error) {
            description("region related failure")
            display("region {:?}", err)
        }
        Locked(l: LockInfo) {
            description("key is locked")
            display("locked {:?}", l)
        }
        Other(err: Box<error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
            display("unknown error {:?}", err)
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

impl From<engine::Error> for Error {
    fn from(e: engine::Error) -> Error {
        match e {
            engine::Error::Request(e) => Error::Region(e),
            _ => Error::Other(box e),
        }
    }
}

impl From<txn::Error> for Error {
    fn from(e: txn::Error) -> Error {
        match e {
            txn::Error::Mvcc(mvcc::Error::KeyIsLocked { primary, ts, key }) => {
                let mut info = LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                info.set_key(key);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

pub struct EndPointHost {
    snap_endpoint: Arc<TiDbEndPoint>,
    pool: ThreadPool,
}

impl EndPointHost {
    pub fn new(engine: Arc<Box<Engine>>) -> EndPointHost {
        EndPointHost {
            snap_endpoint: Arc::new(TiDbEndPoint::new(engine)),
            pool: ThreadPool::new(DEFAULT_POOL_SIZE),
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

impl BatchRunnable<RequestTask> for EndPointHost {
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
        // TODO: Spawn a new thread for handling requests asynchronously.
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
        debug!("[TIME_SNAPSHOT] {:?}", ts.elapsed());
        for t in reqs {
            let timer = SlowTimer::new();
            let tp = t.req.get_tp();
            self.handle_request(snap.as_ref(), t.req, t.on_resp);
            slow_log!(timer, "request type {} takes {:?}", tp, timer.elapsed());
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
        let desc = ctx.sel.get_order_by().first().map_or(false, |o| o.get_desc());
        debug!("scanning range: {:?}", range);
        if desc {
            range.reverse();
        }
        let limit = if ctx.sel.has_limit() {
            ctx.sel.get_limit() as usize
        } else {
            usize::MAX
        };
        let sel_ts = Instant::now();
        let res = if req.get_tp() == REQ_TYPE_SELECT {
            ctx.get_rows_from_sel(range, limit, desc)
        } else {
            ctx.get_rows_from_idx(range, limit, desc)
        };
        debug!("[TIME_SELECT] {:?} {:?}", req.get_tp(), sel_ts.elapsed());
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
        debug!("[TIME_COMPOSE_RESP] {:?}", resp_ts.elapsed());
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

struct SelectContext<'a> {
    sel: SelectRequest,
    snap: SnapshotStore<'a>,
    // TODO: remove refcell.
    eval: RefCell<Evaluator>,
    cond_cols: HashMap<i64, ColumnInfo>,
}

fn collect_col_in_expr(cols: &mut HashMap<i64, ColumnInfo>,
                       col_meta: &[ColumnInfo],
                       expr: &Expr)
                       -> Result<()> {
    if expr.get_tp() == ExprType::ColumnRef {
        let i = box_try!(expr.get_val().decode_i64());
        for c in col_meta {
            if c.get_column_id() == i {
                cols.insert(i, c.clone());
                return Ok(());
            }
        }
        return Err(box_err!("column meta of {} is missing", i));
    }
    for c in expr.get_children() {
        try!(collect_col_in_expr(cols, col_meta, c));
    }
    Ok(())
}

impl<'a> SelectContext<'a> {
    fn new(sel: SelectRequest, snap: SnapshotStore<'a>) -> Result<SelectContext<'a>> {
        let mut ctx = SelectContext {
            sel: sel,
            snap: snap,
            eval: Default::default(),
            cond_cols: Default::default(),
        };
        if !ctx.sel.has_field_where() {
            return Ok(ctx);
        }
        {
            let cols = if ctx.sel.has_table_info() {
                ctx.sel.get_table_info().get_columns()
            } else {
                ctx.sel.get_index_info().get_columns()
            };
            try!(collect_col_in_expr(&mut ctx.cond_cols, cols, ctx.sel.get_field_where()));
        }
        Ok(ctx)
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
            let ran_rows = try!(self.get_rows_from_range(ran, limit - rows.len(), desc));
            rows.extend(ran_rows);
        }
        Ok(rows)
    }

    fn load_row_with_key(&self, key: &[u8], dest: &mut Vec<Row>) -> Result<()> {
        let h = box_try!(table::decode_handle(key));
        if try!(self.should_skip(h)) {
            return Ok(());
        }
        if let Some(r) = try!(self.get_row_by_handle(h)) {
            dest.push(r);
        }
        Ok(())
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
            if let None = try!(self.snap.get(&Key::from_raw(range.get_start()))) {
                return Ok(rows);
            }
            try!(self.load_row_with_key(range.get_start(), &mut rows));
        } else {
            let mut seek_key = if desc {
                range.get_end().to_vec()
            } else {
                range.get_start().to_vec()
            };
            let mut scanner = try!(self.snap.scanner());
            while limit > rows.len() {
                let timer = Instant::now();
                let mut res = if desc {
                    try!(scanner.reverse_scan(Key::from_raw(&seek_key), 1))
                } else {
                    try!(scanner.scan(Key::from_raw(&seek_key), 1))
                };
                trace!("scan takes {:?}", timer.elapsed());
                if res.is_empty() {
                    debug!("no more data to scan.");
                    break;
                }
                let (key, _) = try!(res.pop().unwrap());
                if range.get_start() > &key || range.get_end() <= &key {
                    debug!("key: {} out of range [{}, {})",
                           escape(&key),
                           escape(range.get_start()),
                           escape(range.get_end()));
                    break;
                }
                try!(self.load_row_with_key(&key, &mut rows));
                seek_key = if desc {
                    box_try!(table::truncate_as_row_key(&key)).to_vec()
                } else {
                    prefix_next(&key)
                };
            }
        }
        Ok(rows)
    }

    fn should_skip(&self, h: i64) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        let t_id = self.sel.get_table_info().get_table_id();
        let mut eval = self.eval.borrow_mut();
        for (&col_id, col) in &self.cond_cols {
            if col.get_pk_handle() {
                if mysql::has_unsigned_flag(col.get_flag() as u64) {
                    eval.row.insert(col_id, Datum::U64(h as u64));
                } else {
                    eval.row.insert(col_id, Datum::I64(h));
                }
            } else {
                let key = table::encode_column_key(t_id, h, col_id);
                let data = try!(self.snap.get(&Key::from_raw(&key)));
                let value = match data {
                    None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                        return Err(box_err!("key {} not exists", escape(&key)));
                    }
                    None => Datum::Null,
                    Some(bs) => box_try!(bs.as_slice().decode_col_value(col)),
                };
                eval.row.insert(col_id, value);
            }
        }
        let res = box_try!(eval.eval(self.sel.get_field_where()));
        let b = box_try!(res.into_bool());
        Ok(b.map_or(true, |v| !v))
    }

    fn get_row_by_handle(&self, h: i64) -> Result<Option<Row>> {
        let tid = self.sel.get_table_info().get_table_id();
        let columns = self.sel.get_table_info().get_columns();
        let mut row = Row::new();
        let handle = box_try!(datum::encode_value(&[Datum::I64(h)]));
        for col in columns {
            if col.get_pk_handle() {
                if mysql::has_unsigned_flag(col.get_flag() as u64) {
                    // PK column is unsigned
                    let ud = Datum::U64(h as u64);
                    let handle = box_try!(datum::encode_value(&[ud]));
                    row.mut_data().extend(handle);
                } else {
                    row.mut_data().extend(handle.clone());
                }
            } else {
                let col_id = col.get_column_id();
                if self.cond_cols.contains_key(&col_id) {
                    let d = &self.eval.borrow().row[&col_id];
                    let bytes = box_try!(datum::encode_value(as_slice(d)));
                    row.mut_data().extend(bytes);
                } else {
                    let raw_key = table::encode_column_key(tid, h, col.get_column_id());
                    let key = Key::from_raw(&raw_key);
                    match try!(self.snap.get(&key)) {
                        None if mysql::has_not_null_flag(col.get_flag() as u64) => {
                            return Err(box_err!("key {} not exists", escape(&raw_key)));
                        }
                        None => {
                            let bs = box_try!(datum::encode_value(&[Datum::Null]));
                            row.mut_data().extend(bs);
                        }
                        Some(bs) => row.mut_data().extend(bs),
                    }
                }
            }
        }
        row.set_handle(handle);
        Ok(Some(row))
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
        let info = self.sel.get_index_info();
        let mut seek_key = if desc {
            r.get_end().to_vec()
        } else {
            r.get_start().to_vec()
        };
        let mut scanner = try!(self.snap.scanner());
        while rows.len() < limit {
            trace!("seek {}", escape(&seek_key));
            let timer = Instant::now();
            let mut nk = if desc {
                try!(scanner.reverse_scan(Key::from_raw(&seek_key), 1))
            } else {
                try!(scanner.scan(Key::from_raw(&seek_key), 1))
            };
            trace!("scan takes {:?}", timer.elapsed());
            if nk.is_empty() {
                debug!("no more data to scan");
                break;
            }
            let (key, value) = try!(nk.pop().unwrap());
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
                let h = box_try!((&*value).read_i64::<BigEndian>());
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
