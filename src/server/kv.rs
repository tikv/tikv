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

use std::boxed::Box;

use protobuf::RepeatedField;

use kvproto::kvrpcpb::{CmdGetResponse, CmdScanResponse, CmdPrewriteResponse, CmdCommitResponse,
                       CmdBatchRollbackResponse, CmdCleanupResponse, CmdBatchGetResponse,
                       CmdScanLockResponse, CmdResolveLockResponse, CmdGCResponse, Request,
                       Response, MessageType, KvPair as RpcKvPair, KeyError, LockInfo, Op};
use kvproto::kvrpcpb::{CmdRawGetResponse, CmdRawPutResponse, CmdRawDeleteResponse};
use kvproto::msgpb;
use kvproto::errorpb::{Error as RegionError, ServerIsBusy};
use storage::{Engine, Storage, Key, Value, KvPair, Mutation, Callback, Result as StorageResult,
              Options};
use storage::Error as StorageError;
use storage::txn::Error as TxnError;
use storage::mvcc::Error as MvccError;
use storage::engine::Error as EngineError;
use util::escape;

use super::{Result, Error, OnResponse};

pub struct StoreHandler {
    pub store: Storage,
}

impl StoreHandler {
    pub fn new(store: Storage) -> StoreHandler {
        StoreHandler { store: store }
    }

    fn on_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_get_req() {
            return Err(box_err!("msg doesn't contain a CmdGetRequest"));
        }
        let req = msg.take_cmd_get_req();
        let ctx = msg.take_context();
        let cb = self.make_cb(StoreHandler::cmd_get_done, on_resp);
        self.store
            .async_get(ctx, Key::from_raw(req.get_key()), req.get_version(), cb)
            .map_err(Error::Storage)
    }

    fn on_scan(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_scan_req() {
            return Err(box_err!("msg doesn't contain a CmdScanRequest"));
        }
        let req = msg.take_cmd_scan_req();
        let start_key = req.get_start_key();
        debug!("start_key [{}]", escape(&start_key));
        let cb = self.make_cb(StoreHandler::cmd_scan_done, on_resp);
        let mut options = Options::default();
        options.key_only = req.get_key_only();
        self.store
            .async_scan(msg.take_context(),
                        Key::from_raw(start_key),
                        req.get_limit() as usize,
                        req.get_version(),
                        options,
                        cb)
            .map_err(Error::Storage)
    }

    fn on_prewrite(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            return Err(box_err!("msg doesn't contain a CmdPrewriteRequest"));
        }
        let mut req = msg.take_cmd_prewrite_req();
        let mutations = req.take_mutations()
            .into_iter()
            .map(|mut x| {
                match x.get_op() {
                    Op::Put => Mutation::Put((Key::from_raw(x.get_key()), x.take_value())),
                    Op::Del => Mutation::Delete(Key::from_raw(x.get_key())),
                    Op::Lock => Mutation::Lock(Key::from_raw(x.get_key())),
                }
            })
            .collect();
        let cb = self.make_cb(StoreHandler::cmd_prewrite_done, on_resp);
        let mut options = Options::default();
        options.lock_ttl = req.get_lock_ttl();
        options.skip_constraint_check = req.get_skip_constraint_check();
        self.store
            .async_prewrite(msg.take_context(),
                            mutations,
                            req.get_primary_lock().to_vec(),
                            req.get_start_version(),
                            options,
                            cb)
            .map_err(Error::Storage)
    }

    fn on_commit(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_commit_req() {
            return Err(box_err!("msg doesn't contain a CmdCommitRequest"));
        }
        let req = msg.take_cmd_commit_req();
        let cb = self.make_cb(StoreHandler::cmd_commit_done, on_resp);
        let keys = req.get_keys()
            .iter()
            .map(|x| Key::from_raw(x))
            .collect();
        self.store
            .async_commit(msg.take_context(),
                          keys,
                          req.get_start_version(),
                          req.get_commit_version(),
                          cb)
            .map_err(Error::Storage)
    }

    fn on_batch_rollback(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_batch_rollback_req() {
            return Err(box_err!("msg doesn't contain a CmdRollbackRequest"));
        }
        let req = msg.take_cmd_batch_rollback_req();
        let cb = self.make_cb(StoreHandler::cmd_batch_rollback_done, on_resp);
        let keys = req.get_keys().iter().map(|x| Key::from_raw(x)).collect();
        self.store
            .async_rollback(msg.take_context(), keys, req.get_start_version(), cb)
            .map_err(Error::Storage)
    }

    fn on_cleanup(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_cleanup_req() {
            return Err(box_err!("msg doesn't contain a CmdCleanupRequest"));
        }
        let req = msg.take_cmd_cleanup_req();
        let cb = self.make_cb(StoreHandler::cmd_cleanup_done, on_resp);
        self.store
            .async_cleanup(msg.take_context(),
                           Key::from_raw(req.get_key()),
                           req.get_start_version(),
                           cb)
            .map_err(Error::Storage)
    }

    fn on_batch_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_batch_get_req() {
            return Err(box_err!("msg doesn't contain a CmdBatchGetRequest"));
        }
        let req = msg.take_cmd_batch_get_req();
        let cb = self.make_cb(StoreHandler::cmd_batch_get_done, on_resp);
        self.store
            .async_batch_get(msg.take_context(),
                             req.get_keys().into_iter().map(|x| Key::from_raw(x)).collect(),
                             req.get_version(),
                             cb)
            .map_err(Error::Storage)
    }

    fn on_scan_lock(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_scan_lock_req() {
            return Err(box_err!("msg doesn't contain a CmdScanLockRequest"));
        }
        let req = msg.take_cmd_scan_lock_req();
        let cb = self.make_cb(StoreHandler::cmd_scan_lock_done, on_resp);
        self.store
            .async_scan_lock(msg.take_context(), req.get_max_version(), cb)
            .map_err(Error::Storage)
    }

    fn on_resolve_lock(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_resolve_lock_req() {
            return Err(box_err!("msg doesn't contain a CmdResolveLockRequest"));
        }
        let req = msg.take_cmd_resolve_lock_req();
        let cb = self.make_cb(StoreHandler::cmd_resolve_lock_done, on_resp);
        let commit_ts = if req.get_commit_version() != 0 {
            Some(req.get_commit_version())
        } else {
            None
        };
        self.store
            .async_resolve_lock(msg.take_context(), req.get_start_version(), commit_ts, cb)
            .map_err(Error::Storage)
    }

    fn on_gc(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_gc_req() {
            return Err(box_err!("msg doesn't contain a CmdGcRequest"));
        }
        let req = msg.take_cmd_gc_req();
        let cb = self.make_cb(StoreHandler::cmd_gc_done, on_resp);
        self.store.async_gc(msg.take_context(), req.get_safe_point(), cb).map_err(Error::Storage)
    }

    fn on_raw_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_raw_get_req() {
            return Err(box_err!("msg doesn't contain a CmdRawGetRequest"));
        }
        let mut req = msg.take_cmd_raw_get_req();
        let cb = self.make_cb(StoreHandler::cmd_raw_get_done, on_resp);
        self.store.async_raw_get(msg.take_context(), req.take_key(), cb).map_err(Error::Storage)
    }

    fn on_raw_put(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_raw_put_req() {
            return Err(box_err!("msg doesn't contain a CmdRawPutRequest"));
        }
        let mut req = msg.take_cmd_raw_put_req();
        let cb = self.make_cb(StoreHandler::cmd_raw_put_done, on_resp);
        self.store
            .async_raw_put(msg.take_context(), req.take_key(), req.take_value(), cb)
            .map_err(Error::Storage)
    }

    fn on_raw_delete(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_raw_delete_req() {
            return Err(box_err!("msg doesn't contain a CmdRawDeleteRequest"));
        }
        let mut req = msg.take_cmd_raw_delete_req();
        let cb = self.make_cb(StoreHandler::cmd_raw_delete_done, on_resp);
        self.store.async_raw_delete(msg.take_context(), req.take_key(), cb).map_err(Error::Storage)
    }

    fn make_cb<T: 'static>(&self,
                           f: fn(StorageResult<T>, &mut Response),
                           on_resp: OnResponse)
                           -> Callback<T> {
        Box::new(move |r: StorageResult<T>| {
            let mut resp = Response::new();
            match extract_region_error(&r) {
                Some(e) => resp.set_region_error(e),
                None => f(r, &mut resp),
            }
            let mut resp_msg = msgpb::Message::new();
            resp_msg.set_msg_type(msgpb::MessageType::KvResp);
            resp_msg.set_kv_resp(resp);
            on_resp.call_box((resp_msg,))
        })
    }

    fn cmd_get_done(r: StorageResult<Option<Value>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdGet);
        let mut get_resp = CmdGetResponse::new();
        match r {
            Ok(Some(val)) => get_resp.set_value(val),
            Ok(None) => get_resp.set_value(vec![]),
            Err(e) => get_resp.set_error(extract_key_error(&e)),
        }
        resp.set_cmd_get_resp(get_resp);
    }

    fn cmd_scan_done(kvs: StorageResult<Vec<StorageResult<KvPair>>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdScan);
        let mut scan_resp = CmdScanResponse::new();
        scan_resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(kvs)));
        resp.set_cmd_scan_resp(scan_resp);
    }

    fn cmd_batch_get_done(kvs: StorageResult<Vec<StorageResult<KvPair>>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdBatchGet);
        let mut batch_get_resp = CmdBatchGetResponse::new();
        batch_get_resp.set_pairs(RepeatedField::from_vec(extract_kv_pairs(kvs)));
        resp.set_cmd_batch_get_resp(batch_get_resp);
    }

    fn cmd_prewrite_done(results: StorageResult<Vec<StorageResult<()>>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdPrewrite);
        let mut prewrite_resp = CmdPrewriteResponse::new();
        prewrite_resp.set_errors(RepeatedField::from_vec(extract_key_errors(results)));
        resp.set_cmd_prewrite_resp(prewrite_resp);
    }

    fn cmd_commit_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdCommit);
        let mut cmd_commit_resp = CmdCommitResponse::new();
        if let Err(e) = r {
            cmd_commit_resp.set_error(extract_key_error(&e));
        }
        resp.set_cmd_commit_resp(cmd_commit_resp);
    }

    fn cmd_batch_rollback_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdBatchRollback);
        let mut cmd_batch_rollback_resp = CmdBatchRollbackResponse::new();
        if let Err(e) = r {
            cmd_batch_rollback_resp.set_error(extract_key_error(&e));
        }
        resp.set_cmd_batch_rollback_resp(cmd_batch_rollback_resp)
    }

    fn cmd_cleanup_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdCleanup);
        let mut cmd_cleanup_resp = CmdCleanupResponse::new();
        if let Err(e) = r {
            if let Some(ts) = extract_committed(&e) {
                cmd_cleanup_resp.set_commit_version(ts);
            } else {
                cmd_cleanup_resp.set_error(extract_key_error(&e));
            }
        }
        resp.set_cmd_cleanup_resp(cmd_cleanup_resp);
    }

    fn cmd_scan_lock_done(r: StorageResult<Vec<LockInfo>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdScanLock);
        let mut scan_lock = CmdScanLockResponse::new();
        match r {
            Ok(locks) => scan_lock.set_locks(RepeatedField::from_vec(locks)),
            Err(e) => scan_lock.set_error(extract_key_error(&e)),
        }
        resp.set_cmd_scan_lock_resp(scan_lock);
    }

    fn cmd_resolve_lock_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdResolveLock);
        let mut resolve_lock = CmdResolveLockResponse::new();
        if let Err(e) = r {
            resolve_lock.set_error(extract_key_error(&e));
        }
        resp.set_cmd_resolve_lock_resp(resolve_lock);
    }

    fn cmd_gc_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdGC);
        let mut gc = CmdGCResponse::new();
        if let Err(e) = r {
            gc.set_error(extract_key_error(&e));
        }
        resp.set_cmd_gc_resp(gc);
    }

    fn cmd_raw_get_done(r: StorageResult<Option<Vec<u8>>>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdRawGet);
        let mut raw_get = CmdRawGetResponse::new();
        match r {
            Ok(Some(val)) => raw_get.set_value(val),
            Ok(None) => {}
            Err(e) => raw_get.set_error(format!("{}", e)),
        }
        resp.set_cmd_raw_get_resp(raw_get);
    }

    fn cmd_raw_put_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdRawPut);
        let mut raw_put = CmdRawPutResponse::new();
        if let Err(e) = r {
            raw_put.set_error(format!("{}", e));
        }
        resp.set_cmd_raw_put_resp(raw_put);
    }

    fn cmd_raw_delete_done(r: StorageResult<()>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdRawDelete);
        let mut raw_delete = CmdRawDeleteResponse::new();
        if let Err(e) = r {
            raw_delete.set_error(format!("{}", e));
        }
        resp.set_cmd_raw_delete_resp(raw_delete);
    }

    pub fn on_request(&self, req: Request, on_resp: OnResponse) -> Result<()> {
        if let Err(e) = match req.get_field_type() {
            MessageType::CmdGet => self.on_get(req, on_resp),
            MessageType::CmdScan => self.on_scan(req, on_resp),
            MessageType::CmdPrewrite => self.on_prewrite(req, on_resp),
            MessageType::CmdCommit => self.on_commit(req, on_resp),
            MessageType::CmdCleanup => self.on_cleanup(req, on_resp),
            MessageType::CmdBatchGet => self.on_batch_get(req, on_resp),
            MessageType::CmdBatchRollback => self.on_batch_rollback(req, on_resp),
            MessageType::CmdScanLock => self.on_scan_lock(req, on_resp),
            MessageType::CmdResolveLock => self.on_resolve_lock(req, on_resp),
            MessageType::CmdGC => self.on_gc(req, on_resp),

            MessageType::CmdRawGet => self.on_raw_get(req, on_resp),
            MessageType::CmdRawPut => self.on_raw_put(req, on_resp),
            MessageType::CmdRawDelete => self.on_raw_delete(req, on_resp),
        } {
            // TODO: should we return an error and tell the client later?
            error!("Some error occur err[{:?}]", e);
        }

        Ok(())
    }

    pub fn engine(&self) -> Box<Engine> {
        self.store.get_engine()
    }

    pub fn stop(&mut self) -> Result<()> {
        self.store.stop().map_err(From::from)
    }
}

fn extract_region_error<T>(res: &StorageResult<T>) -> Option<RegionError> {
    match *res {
        // TODO: use `Error::cause` instead.
        Err(StorageError::Engine(EngineError::Request(ref e))) |
        Err(StorageError::Txn(TxnError::Engine(EngineError::Request(ref e)))) |
        Err(StorageError::Txn(TxnError::Mvcc(MvccError::Engine(EngineError::Request(ref e))))) => {
            Some(e.to_owned())
        }
        Err(StorageError::SchedTooBusy) => {
            let mut err = RegionError::new();
            let mut server_is_busy_err = ServerIsBusy::new();
            server_is_busy_err.set_reason(String::from("[scheduler] server is busy"));
            err.set_server_is_busy(server_is_busy_err);
            Some(err)
        }
        _ => None,
    }
}

fn extract_committed(err: &StorageError) -> Option<u64> {
    match *err {
        StorageError::Txn(TxnError::Mvcc(MvccError::Committed { commit_ts })) => Some(commit_ts),
        _ => None,
    }
}

fn extract_key_error(err: &StorageError) -> KeyError {
    let mut key_error = KeyError::new();
    match *err {
        StorageError::Txn(TxnError::Mvcc(MvccError::KeyIsLocked { ref key,
                                                                  ref primary,
                                                                  ts,
                                                                  ttl })) => {
            let mut lock_info = LockInfo::new();
            lock_info.set_key(key.to_owned());
            lock_info.set_primary_lock(primary.to_owned());
            lock_info.set_lock_version(ts);
            lock_info.set_lock_ttl(ttl);
            key_error.set_locked(lock_info);
        }
        StorageError::Txn(TxnError::Mvcc(MvccError::WriteConflict)) |
        StorageError::Txn(TxnError::Mvcc(MvccError::TxnLockNotFound)) => {
            debug!("txn conflicts: {}", err);
            key_error.set_retryable(format!("{:?}", err));
        }
        _ => {
            error!("txn aborts: {}", err);
            key_error.set_abort(format!("{:?}", err));
        }
    }
    key_error
}

fn extract_kv_pairs(res: StorageResult<Vec<StorageResult<KvPair>>>) -> Vec<RpcKvPair> {
    let mut pairs = vec![];
    match res {
        Ok(res) => {
            for r in res {
                let mut pair = RpcKvPair::new();
                match r {
                    Ok((key, value)) => {
                        pair.set_key(key);
                        pair.set_value(value);
                    }
                    Err(e) => {
                        pair.set_error(extract_key_error(&e));
                    }
                }
                pairs.push(pair);
            }
        }
        Err(e) => {
            let mut pair = RpcKvPair::new();
            pair.set_error(extract_key_error(&e));
            pairs.push(pair);
        }
    }
    pairs
}

fn extract_key_errors(res: StorageResult<Vec<StorageResult<()>>>) -> Vec<KeyError> {
    let mut errs = vec![];
    match res {
        Ok(res) => {
            for r in res {
                if let Err(e) = r {
                    errs.push(extract_key_error(&e));
                }
            }
        }
        Err(e) => {
            errs.push(extract_key_error(&e));
        }
    }
    errs
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::*;
    use kvproto::errorpb::NotLeader;
    use storage::{self, txn, mvcc, engine};
    use storage::Result as StorageResult;
    use super::*;

    fn build_resp<T>(r: StorageResult<T>, f: fn(StorageResult<T>, &mut Response)) -> Response {
        let mut resp = Response::new();
        match super::extract_region_error(&r) {
            Some(e) => resp.set_region_error(e),
            None => f(r, &mut resp),
        }
        resp
    }

    #[test]
    fn test_get_done_none() {
        let resp = build_resp(Ok(None), StoreHandler::cmd_get_done);
        let mut cmd = CmdGetResponse::new();
        cmd.set_value(Vec::new());
        let mut expect = Response::new();
        expect.set_field_type(MessageType::CmdGet);
        expect.set_cmd_get_resp(cmd);
        assert_eq!(expect, resp);
    }

    #[test]
    fn test_get_done_some() {
        let val = vec![0x0; 0x8];
        let resp = build_resp(Ok(Some(val.clone())), StoreHandler::cmd_get_done);
        let mut cmd = CmdGetResponse::new();
        cmd.set_value(val);
        let mut expect = Response::new();
        expect.set_field_type(MessageType::CmdGet);
        expect.set_cmd_get_resp(cmd);
        assert_eq!(expect, resp);
    }

    #[test]
    fn test_get_done_error() {
        let resp = build_resp(Err(box_err!("error")), StoreHandler::cmd_get_done);
        let mut cmd = CmdGetResponse::new();
        let mut key_error = KeyError::new();
        key_error.set_abort("Other(StringError(\"error\"))".to_owned());
        cmd.set_error(key_error);
        let mut expect = Response::new();
        expect.set_field_type(MessageType::CmdGet);
        expect.set_cmd_get_resp(cmd);
        assert_eq!(expect, resp);
    }

    #[test]
    fn test_scan_done_empty() {
        let resp = build_resp(Ok(Vec::new()), StoreHandler::cmd_scan_done);
        let cmd = CmdScanResponse::new();
        let mut expect = Response::new();
        expect.set_field_type(MessageType::CmdScan);
        expect.set_cmd_scan_resp(cmd);
        assert_eq!(expect, resp);
    }

    #[test]
    fn test_scan_done_some() {
        let k0 = vec![0x0, 0x0];
        let v0 = vec![0xff, 0xff];
        let k1 = vec![0x0, 0x1];
        let v1 = vec![0xff, 0xfe];
        let kvs = vec![Ok((k0.clone(), v0.clone())), Ok((k1.clone(), v1.clone()))];
        let resp = build_resp(Ok(kvs), StoreHandler::cmd_scan_done);
        assert_eq!(MessageType::CmdScan, resp.get_field_type());
        let cmd = resp.get_cmd_scan_resp();
        let pairs = cmd.get_pairs();
        assert_eq!(2, pairs.len());
        assert_eq!(k0, pairs[0].get_key());
        assert_eq!(v0, pairs[0].get_value());
        assert!(!pairs[0].has_error());
        assert_eq!(k1, pairs[1].get_key());
        assert_eq!(v1, pairs[1].get_value());
        assert!(!pairs[1].has_error());
    }

    #[test]
    fn test_scan_done_lock() {
        use kvproto::kvrpcpb::LockInfo;
        let k0 = vec![0x0, 0x0];
        let v0 = vec![0xff, 0xff];
        let k1 = vec![0x0, 0x1];
        let k1_primary = k0.clone();
        let k1_ts = 10000;
        let kvs = vec![Ok((k0.clone(), v0.clone())),
                       make_lock_error(k1.clone(), k1_primary.clone(), k1_ts, 3000)];
        let resp = build_resp(Ok(kvs), StoreHandler::cmd_scan_done);
        assert_eq!(MessageType::CmdScan, resp.get_field_type());
        let cmd = resp.get_cmd_scan_resp();
        let pairs = cmd.get_pairs();
        assert_eq!(2, pairs.len());
        assert_eq!(k0, pairs[0].get_key());
        assert_eq!(v0, pairs[0].get_value());
        assert!(!pairs[0].has_error());
        let mut lock_info1 = LockInfo::new();
        lock_info1.set_primary_lock(k1_primary.clone());
        lock_info1.set_lock_version(k1_ts);
        lock_info1.set_key(k1.clone());
        lock_info1.set_lock_ttl(3000);
        assert_eq!(lock_info1, *pairs[1].get_error().get_locked());
    }

    #[test]
    fn test_prewrite_done_ok() {
        let resp = build_resp(Ok(Vec::new()), StoreHandler::cmd_prewrite_done);
        assert_eq!(MessageType::CmdPrewrite, resp.get_field_type());
        let cmd = resp.get_cmd_prewrite_resp();
        assert_eq!(cmd.get_errors().len(), 0);
    }

    #[test]
    fn test_prewrite_done_err() {
        let resp = build_resp(Ok(vec![Err(box_err!("error"))]),
                              StoreHandler::cmd_prewrite_done);
        let cmd = resp.get_cmd_prewrite_resp();
        assert_eq!(cmd.get_errors().len(), 1);
    }

    #[test]
    fn test_commit_done_ok() {
        let resp = build_resp(Ok(()), StoreHandler::cmd_commit_done);
        assert_eq!(MessageType::CmdCommit, resp.get_field_type());
        let cmd = resp.get_cmd_commit_resp();
        assert!(!cmd.has_error());
    }

    #[test]
    fn test_commit_done_err() {
        let resp = build_resp(Err(box_err!("commit error")), StoreHandler::cmd_commit_done);
        assert_eq!(MessageType::CmdCommit, resp.get_field_type());
        let cmd = resp.get_cmd_commit_resp();
        assert!(cmd.has_error());
    }

    #[test]
    fn test_extract_error_illegal_tso() {
        let err = StorageError::from(txn::Error::InvalidTxnTso {
            start_ts: 5,
            commit_ts: 4,
        });
        let key_error = extract_key_error(&err);
        assert!(key_error.has_abort());
        assert!(!key_error.has_locked());
        assert!(!key_error.has_retryable());
    }

    #[test]
    fn test_cleanup_done_ok() {
        let resp = build_resp(Ok(()), StoreHandler::cmd_cleanup_done);
        assert_eq!(MessageType::CmdCleanup, resp.get_field_type());
        let cmd = resp.get_cmd_cleanup_resp();
        assert!(!cmd.has_error());
    }

    #[test]
    fn test_cleanup_done_err() {
        let resp = build_resp(Err(box_err!("cleanup error")),
                              StoreHandler::cmd_cleanup_done);
        assert_eq!(MessageType::CmdCleanup, resp.get_field_type());
        let cmd = resp.get_cmd_cleanup_resp();
        assert!(cmd.has_error());
    }

    #[test]
    fn test_rollback_done_ok() {
        let resp = build_resp(Ok(()), StoreHandler::cmd_batch_rollback_done);
        assert_eq!(MessageType::CmdBatchRollback, resp.get_field_type());
        let cmd = resp.get_cmd_batch_rollback_resp();
        assert!(!cmd.has_error());
    }

    #[test]
    fn test_rollback_done_err() {
        let resp = build_resp(Err(box_err!("rollback error")),
                              StoreHandler::cmd_batch_rollback_done);
        assert_eq!(MessageType::CmdBatchRollback, resp.get_field_type());
        let cmd = resp.get_cmd_batch_rollback_resp();
        assert!(cmd.has_error());
    }

    #[test]
    fn test_get_not_leader() {
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res = make_not_leader_error(leader_info.to_owned());
        let resp = build_resp(storage_res, StoreHandler::cmd_get_done);
        assert!(resp.has_region_error());
        let region_err = resp.get_region_error();
        assert!(region_err.has_not_leader());
        assert_eq!(region_err.get_not_leader(), &leader_info);
    }

    #[test]
    fn test_scan_not_leader() {
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res = make_not_leader_error(leader_info.to_owned());
        let resp = build_resp(storage_res, StoreHandler::cmd_scan_done);
        assert!(resp.has_region_error());
        let region_err = resp.get_region_error();
        assert!(region_err.has_not_leader());
        assert_eq!(region_err.get_not_leader(), &leader_info);
    }

    #[test]
    fn test_prewrite_not_leader() {
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res = make_not_leader_error(leader_info.to_owned());
        let resp = build_resp(storage_res, StoreHandler::cmd_prewrite_done);
        assert!(resp.has_region_error());
        let region_err = resp.get_region_error();
        assert!(region_err.has_not_leader());
        assert_eq!(region_err.get_not_leader(), &leader_info);
    }

    fn make_lock_error<T>(key: Vec<u8>, primary: Vec<u8>, ts: u64, ttl: u64) -> StorageResult<T> {
        Err(mvcc::Error::KeyIsLocked {
                key: key,
                primary: primary,
                ts: ts,
                ttl: ttl,
            })
            .map_err(txn::Error::from)
            .map_err(storage::Error::from)
    }

    fn make_not_leader_error<T>(leader_info: NotLeader) -> StorageResult<T> {
        use kvproto::errorpb::Error;
        let mut err = Error::new();
        err.set_not_leader(leader_info);
        Err(engine::Error::Request(err))
            .map_err(storage::txn::Error::from)
            .map_err(storage::Error::from)
    }
}
