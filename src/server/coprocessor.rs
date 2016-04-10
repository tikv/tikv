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
use std::{result, error};
use mio::Token;
use tipb::select::{self, SelectRequest, SelectResponse, Row};
use tipb::schema::{IndexInfo, TableInfo};
use protobuf::{Message as PbMsg, RepeatedField};
use byteorder::{BigEndian, ReadBytesExt};

use storage::{Engine, SnapshotStore, engine, txn, mvcc};
use kvproto::kvrpcpb::{Context, LockInfo};
use kvproto::msgpb::{MessageType, Message};
use kvproto::coprocessor::{Request, Response, KeyRange};
use kvproto::errorpb;
use storage::Key;
use util::codec::{Datum, table, datum};
use util::xeval::Evaluator;
use server::{self, SendCh, Msg, ConnData};

const REQ_TYPE_SELECT: i64 = 101;
const REQ_TYPE_INDEX: i64 = 102;

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

type Result<T> = result::Result<T, Error>;

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
            txn::Error::Mvcc(mvcc::Error::KeyIsLocked { primary, ts, .. }) => {
                let mut info = LockInfo::new();
                info.set_primary_lock(primary);
                info.set_lock_version(ts);
                Error::Locked(info)
            }
            _ => Error::Other(box e),
        }
    }
}

#[allow(dead_code)]
pub struct SnapshotEndPoint {
    engine: Arc<Box<Engine>>,
    ch: SendCh,
}

impl SnapshotEndPoint {
    pub fn new(engine: Arc<Box<Engine>>, ch: SendCh) -> SnapshotEndPoint {
        // TODO: Spawn a new thread for handling requests asynchronously.
        SnapshotEndPoint {
            engine: engine,
            ch: ch,
        }
    }

    fn new_snapshot<'a>(&'a self, ctx: &Context, start_ts: u64) -> Result<SnapshotStore<'a>> {
        let snapshot = try!(self.engine.snapshot(ctx));
        Ok(SnapshotStore::new(snapshot, start_ts))
    }
}

type ResponseHandler = Box<Fn(Response) -> ()>;

impl SnapshotEndPoint {
    pub fn on_request(&self, req: Request, token: Token, msg_id: u64) -> server::Result<()> {
        let ch = self.ch.clone();
        let cb = box move |r| {
            let mut resp_msg = Message::new();
            resp_msg.set_msg_type(MessageType::CopResp);
            resp_msg.set_cop_resp(r);
            if let Err(e) = ch.send(Msg::WriteData {
                token: token,
                data: ConnData::new(msg_id, resp_msg),
            }) {
                error!("send cop resp failed with token {:?}, msg id {}, err {:?}",
                       token,
                       msg_id,
                       e);
            }
        };
        match req.get_tp() {
            REQ_TYPE_SELECT | REQ_TYPE_INDEX => {
                let mut sel = SelectRequest::new();
                box_try!(sel.merge_from_bytes(req.get_data()));
                match self.handle_select(req, sel) {
                    Ok(r) => cb(r),
                    Err(e) => self.on_error(e, cb),
                }
                Ok(())
            }
            t => Err(box_err!("unsupported tp {}", t)),
        }
    }

    fn on_error(&self, e: Error, cb: ResponseHandler) {
        let mut resp = Response::new();
        match e {
            Error::Region(e) => resp.set_region_error(e),
            Error::Locked(info) => resp.set_locked(info),
            Error::Other(_) => resp.set_other_error(format!("{}", e)),
        }
        cb(resp)
    }

    fn handle_select(&self, req: Request, sel: SelectRequest) -> Result<Response> {
        let store = try!(self.new_snapshot(req.get_context(), sel.get_start_ts()));
        let range = encode_key_range(sel.get_table_info(), sel.get_index_info(), req.get_ranges());
        let res = if req.get_tp() == REQ_TYPE_SELECT {
            get_rows_from_sel(&store, &sel, range)
        } else {
            get_rows_from_idx(&store, &sel, range)
        };
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
        Ok(resp)
    }
}

fn to_pb_error(err: &Error) -> select::Error {
    let mut e = select::Error::new();
    e.set_code(1);
    e.set_msg(format!("{}", err));
    e
}

fn encode_key_range(t_info: &TableInfo,
                    idx_info: &IndexInfo,
                    ranges: &[KeyRange])
                    -> Vec<KeyRange> {
    let t_id = t_info.get_table_id();
    let idx_id = idx_info.get_index_id();
    if idx_id == 0 {
        ranges.iter()
              .map(|r| {
                  let mut new_kr = KeyRange::new();
                  let mut k = table::encode_row_key(t_id, r.get_start());
                  new_kr.set_start(k);
                  k = table::encode_row_key(t_id, r.get_end());
                  new_kr.set_end(k);
                  new_kr
              })
              .collect()
    } else {
        ranges.iter()
              .map(|r| {
                  let mut new_kr = KeyRange::new();
                  let mut k = table::encode_index_seek_key(t_id, idx_id, r.get_start());
                  new_kr.set_start(k);
                  k = table::encode_index_seek_key(t_id, idx_id, r.get_end());
                  new_kr.set_end(k);
                  new_kr
              })
              .collect()
    }
}

fn get_rows_from_sel(store: &SnapshotStore,
                     sel: &SelectRequest,
                     ranges: Vec<KeyRange>)
                     -> Result<Vec<Row>> {
    let mut eval = Evaluator::default();
    let mut rows = vec![];
    for ran in ranges {
        let ran_rows = try!(get_rows_from_range(store, sel, ran, &mut eval));
        rows.extend(ran_rows);
    }
    Ok(rows)
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

fn get_rows_from_range(store: &SnapshotStore,
                       sel: &SelectRequest,
                       mut range: KeyRange,
                       eval: &mut Evaluator)
                       -> Result<Vec<Row>> {
    let mut rows = vec![];
    if is_point(&range) {
        if let None = try!(store.get(&Key::from_raw(range.get_start().to_vec()))) {
            return Ok(rows);
        }
        let h = box_try!(table::decode_handle(range.get_start()));
        if let Some(row) = try!(get_row_by_handle(store, sel, h, eval)) {
            rows.push(row);
        }
    } else {
        let mut seek_key = range.take_start();
        loop {
            let mut res = try!(store.scan(Key::from_raw(seek_key), 1));
            if res.is_empty() {
                break;
            }
            let (key, _) = try!(res.pop().unwrap());
            if range.get_end() < &key {
                break;
            }
            let h = box_try!(table::decode_handle(&key));
            if let Some(row) = try!(get_row_by_handle(store, sel, h, eval)) {
                rows.push(row);
            }
            seek_key = prefix_next(&key);
        }
    }
    Ok(rows)
}

fn get_row_by_handle(store: &SnapshotStore,
                     sel: &SelectRequest,
                     h: i64,
                     eval: &mut Evaluator)
                     -> Result<Option<Row>> {
    let tid = sel.get_table_info().get_table_id();
    let columns = sel.get_table_info().get_columns();
    let mut row = Row::new();
    let handle = box_try!(datum::encode_value(&[Datum::I64(h)]));
    for col in columns {
        if col.get_pk_handle() {
            row.mut_data().extend(handle.clone());
        } else {
            let raw_key = table::encode_column_key(tid, h, col.get_column_id());
            let key = Key::from_raw(raw_key);
            match try!(store.get(&key)) {
                None => return Err(box_err!("key {:?} not exists", key)),
                Some(bs) => row.mut_data().extend(bs),
            }
        }
    }
    row.set_handle(handle);
    if !sel.has_field_where() {
        return Ok(Some(row));
    }
    if !row.get_data().is_empty() {
        let (datums, _) = box_try!(datum::decode(row.get_data()));
        for (c, d) in columns.iter().zip(datums) {
            eval.insert(c.get_column_id(), d);
        }
    }
    let res = box_try!(eval.eval(sel.get_field_where()));
    if let Datum::Null = res {
        return Ok(None);
    }
    if box_try!(res.as_bool()) {
        return Ok(Some(row));
    }
    Ok(None)
}

fn get_rows_from_idx(store: &SnapshotStore,
                     sel: &SelectRequest,
                     ranges: Vec<KeyRange>)
                     -> Result<Vec<Row>> {
    let mut rows = vec![];
    for r in ranges {
        let part = try!(get_idx_row_from_range(store, sel.get_index_info(), r));
        rows.extend(part);
    }
    Ok(rows)
}

fn get_idx_row_from_range(store: &SnapshotStore,
                          info: &IndexInfo,
                          mut r: KeyRange)
                          -> Result<Vec<Row>> {
    let mut rows = vec![];
    let mut seek_key = r.take_start();
    loop {
        let mut nk = try!(store.scan(Key::from_raw(seek_key.clone()), 1));
        if nk.is_empty() {
            return Ok(rows);
        }
        let (key, value) = try!(nk.pop().unwrap());
        if r.get_end() < &key {
            return Ok(rows);
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
        seek_key = prefix_next(&seek_key);
    }
}
