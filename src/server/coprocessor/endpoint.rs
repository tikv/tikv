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
use std::time::Instant;
use std::boxed::FnBox;
use std::fmt::{self, Display, Formatter, Debug};

use tipb::select::{self, SelectRequest, SelectResponse, Chunk};
use tipb::schema::ColumnInfo;
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};
use threadpool::ThreadPool;

use storage::{Engine, SnapshotStore};
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use storage::{engine, Snapshot, Key, ScanMode};
use util::codec::datum::DatumDecoder;
use util::codec::table;
use util::{escape, duration_to_ms, Either};
use util::xeval::Evaluator;
use util::worker::{BatchRunnable, Scheduler};
use server::OnResponse;

use super::{Error, Result, Collector};
use super::aggregate::AggrCollector;
use super::collector::DefaultCollector;
use super::metrics::*;
use super::util;

pub const REQ_TYPE_SELECT: i64 = 101;
pub const REQ_TYPE_INDEX: i64 = 102;

const DEFAULT_ERROR_CODE: i32 = 1;

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

impl BatchRunnable<Task> for Host {
    // TODO: limit pending reqs
    #[allow(for_kv_map)]
    fn run_batch(&mut self, tasks: &mut Vec<Task>) {
        let mut grouped_reqs = map![];
        for task in tasks.drain(..) {
            match task {
                Task::Request(req) => {
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

fn on_snap_failed<E: Into<Error> + Debug>(e: E, reqs: Vec<RequestTask>) {
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
            self.handle_request(t.req, t.on_resp);
        }
    }

    fn handle_request(&self, req: Request, on_resp: OnResponse) {
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
                match self.handle_select(req, sel) {
                    Ok(r) => cb(r),
                    Err(e) => on_error(e, cb),
                }
            }
            t => on_error(box_err!("unsupported tp {}", t), cb),
        }
    }

    pub fn handle_select(&self, mut req: Request, sel: SelectRequest) -> Result<Response> {
        let snap = SnapshotStore::new(self.snap.as_ref(), sel.get_start_ts());
        let mut range = req.take_ranges().into_vec();
        let desc = sel.get_order_by().first().map_or(false, |o| o.get_desc());
        debug!("scanning range: {:?}", range);
        if desc {
            range.reverse();
        }
        let limit = if sel.has_limit() {
            sel.get_limit() as usize
        } else {
            usize::MAX
        };

        let select_histogram =
            COPR_REQ_HISTOGRAM_VEC.with_label_values(&["select", get_req_type_str(req.get_tp())]);
        let select_timer = select_histogram.start_timer();

        let res = if !sel.get_aggregates().is_empty() || !sel.get_group_by().is_empty() {
            let mut ctx: SelectContext<AggrCollector> = try!(SelectContext::new(sel, snap));
            ctx.get_rows(req.get_tp(), range, limit, desc)
        } else {
            let mut ctx: SelectContext<DefaultCollector> = try!(SelectContext::new(sel, snap));
            ctx.get_rows(req.get_tp(), range, limit, desc)
        };

        select_timer.observe_duration();

        let mut resp = Response::new();
        let mut sel_resp = SelectResponse::new();
        match res {
            Ok(chunks) => sel_resp.set_chunks(RepeatedField::from_vec(chunks)),
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

struct SelectContextCore<C: Collector> {
    sel: SelectRequest,
    cols: Either<HashSet<i64>, Vec<i64>>,
    cond_cols: Vec<ColumnInfo>,
    eval: Evaluator,
    collector: C,
}

impl<C: Collector> SelectContextCore<C> {
    fn new(mut sel: SelectRequest) -> Result<SelectContextCore<C>> {
        let collector = try!(C::create(&sel));

        let cond_cols;
        {
            let select_cols = if sel.has_table_info() {
                sel.get_table_info().get_columns()
            } else {
                sel.get_index_info().get_columns()
            };
            let mut cond_col_map = HashMap::new();
            try!(util::collect_col_in_expr(&mut cond_col_map, select_cols, sel.get_field_where()));
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
            eval: box_try!(Evaluator::new(&sel)),
            collector: collector,
            sel: sel,
            cols: cols,
            cond_cols: cond_cols,
        })
    }

    fn handle_row(&mut self, h: i64, row_data: HashMap<i64, &[u8]>) -> Result<usize> {
        // clear all dirty values.
        self.eval.row.clear();

        if try!(self.should_skip(h, &row_data)) {
            return Ok(0);
        }
        self.collector.collect(&mut self.eval, h, &row_data)
    }

    fn should_skip(&mut self, h: i64, values: &HashMap<i64, &[u8]>) -> Result<bool> {
        if !self.sel.has_field_where() {
            return Ok(false);
        }
        try!(util::inflate_with_col(&mut self.eval, values, &self.cond_cols, h));
        let res = box_try!(self.eval.eval(self.sel.get_field_where()));
        let b = box_try!(res.into_bool());
        Ok(b.map_or(true, |v| !v))
    }
}


struct SelectContext<'a, C: Collector> {
    snap: SnapshotStore<'a>,
    core: SelectContextCore<C>,
}

impl<'a, C: Collector> SelectContext<'a, C> {
    fn new(sel: SelectRequest, snap: SnapshotStore<'a>) -> Result<SelectContext<'a, C>> {
        Ok(SelectContext {
            core: try!(SelectContextCore::new(sel)),
            snap: snap,
        })
    }

    fn get_rows(&mut self,
                tp: i64,
                range: Vec<KeyRange>,
                limit: usize,
                desc: bool)
                -> Result<Vec<Chunk>> {
        if tp == REQ_TYPE_SELECT {
            self.get_rows_from_sel(range, limit, desc)
        } else {
            self.get_rows_from_idx(range, limit, desc)
        }
    }

    fn get_rows_from_sel(&mut self,
                         ranges: Vec<KeyRange>,
                         limit: usize,
                         desc: bool)
                         -> Result<Vec<Chunk>> {
        let mut collected = 0;
        for ran in ranges {
            if collected >= limit {
                break;
            }
            let timer = Instant::now();
            let row_cnt = try!(self.get_rows_from_range(ran, limit, desc));
            debug!("fetch {} rows takes {} ms",
                   row_cnt,
                   duration_to_ms(timer.elapsed()));
            collected += row_cnt;
        }
        self.core.collector.take_collection()
    }

    fn key_only(&self) -> bool {
        match self.core.cols {
            Either::Left(ref cols) => cols.is_empty(),
            Either::Right(_) => false, // TODO: true when index is not uniq index.
        }
    }

    fn get_rows_from_range(&mut self, range: KeyRange, limit: usize, desc: bool) -> Result<usize> {
        let mut row_count = 0;
        if util::is_point(&range) {
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
                    util::prefix_next(&key)
                };
            }
        }
        Ok(row_count)
    }

    fn get_rows_from_idx(&mut self,
                         ranges: Vec<KeyRange>,
                         limit: usize,
                         desc: bool)
                         -> Result<Vec<Chunk>> {
        let mut collected = 0;
        for r in ranges {
            if collected >= limit {
                break;
            }
            collected += try!(self.get_idx_row_from_range(r, limit, desc));
        }
        self.core.collector.take_collection()
    }

    fn get_idx_row_from_range(&mut self, r: KeyRange, limit: usize, desc: bool) -> Result<usize> {
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
            seek_key = if desc { key } else { util::prefix_next(&key) };
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

    #[test]
    fn test_get_req_type_str() {
        assert_eq!(get_req_type_str(REQ_TYPE_SELECT), STR_REQ_TYPE_SELECT);
        assert_eq!(get_req_type_str(REQ_TYPE_INDEX), STR_REQ_TYPE_INDEX);
        assert_eq!(get_req_type_str(0), STR_REQ_TYPE_UNKNOWN);
    }
}
