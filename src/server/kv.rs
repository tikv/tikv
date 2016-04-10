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

use mio::Token;
use protobuf::RepeatedField;

use kvproto::kvrpcpb::{CmdGetResponse, CmdScanResponse, CmdPrewriteResponse, CmdCommitResponse,
                       CmdCleanupResponse, CmdRollbackThenGetResponse, CmdCommitThenGetResponse,
                       Request, Response, MessageType, Item, ResultType, ResultType_Type,
                       LockInfo, Op};
use kvproto::msgpb;
use kvproto::errorpb::Error as ErrorHeader;
use storage::{Storage, Key, Value, KvPair, Mutation, MaybeLocked, MaybeComitted, MaybeRolledback,
              Callback, Result as StorageResult, engine};

use super::{Result, SendCh, ConnData, Error, Msg};

pub struct StoreHandler {
    pub store: Storage,
    pub ch: SendCh,
}

impl StoreHandler {
    pub fn new(store: Storage, ch: SendCh) -> StoreHandler {
        StoreHandler {
            store: store,
            ch: ch,
        }
    }

    fn on_get(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_get_req() {
            return Err(box_err!("Msg doesn't contain a CmdGetRequest"));
        }
        let mut req = msg.take_cmd_get_req();
        let ctx = msg.take_context();
        let cb = self.make_cb(StoreHandler::cmd_get_done, token, msg_id);
        self.store
            .async_get(ctx, Key::from_raw(req.take_key()), req.get_version(), cb)
            .map_err(Error::Storage)
    }

    fn on_scan(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_scan_req() {
            return Err(box_err!("Msg doesn't contain a CmdScanRequest"));
        }
        let mut req = msg.take_cmd_scan_req();
        let start_key = req.take_start_key();
        debug!("start_key [{:?}]", start_key);
        let cb = self.make_cb(StoreHandler::cmd_scan_done, token, msg_id);
        self.store
            .async_scan(msg.take_context(),
                        Key::from_raw(start_key),
                        req.get_limit() as usize,
                        req.get_version(),
                        cb)
            .map_err(Error::Storage)
    }

    fn on_prewrite(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            return Err(box_err!("Msg doesn't contain a CmdPrewriteRequest"));
        }
        let mut req = msg.take_cmd_prewrite_req();
        let mutations = req.take_mutations()
                           .into_iter()
                           .map(|mut x| {
                               match x.get_op() {
                                   Op::Put => {
                                       Mutation::Put((Key::from_raw(x.take_key()), x.take_value()))
                                   }
                                   Op::Del => Mutation::Delete(Key::from_raw(x.take_key())),
                                   Op::Lock => Mutation::Lock(Key::from_raw(x.take_key())),
                               }
                           })
                           .collect();
        let cb = self.make_cb(StoreHandler::cmd_prewrite_done, token, msg_id);
        self.store
            .async_prewrite(msg.take_context(),
                            mutations,
                            req.get_primary_lock().to_vec(),
                            req.get_start_version(),
                            cb)
            .map_err(Error::Storage)
    }

    fn on_commit(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_commit_req() {
            return Err(box_err!("Msg doesn't contain a CmdCommitRequest"));
        }
        let mut req = msg.take_cmd_commit_req();
        let cb = self.make_cb(StoreHandler::cmd_commit_done, token, msg_id);
        let keys = req.take_keys()
                      .into_iter()
                      .map(Key::from_raw)
                      .collect();
        self.store
            .async_commit(msg.take_context(),
                          keys,
                          req.get_start_version(),
                          req.get_commit_version(),
                          cb)
            .map_err(Error::Storage)
    }

    fn on_cleanup(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_cleanup_req() {
            return Err(box_err!("Msg doesn't contain a CmdCleanupRequest"));
        }
        let mut req = msg.take_cmd_cleanup_req();
        let cb = self.make_cb(StoreHandler::cmd_cleanup_done, token, msg_id);
        self.store
            .async_cleanup(msg.take_context(),
                           Key::from_raw(req.take_key()),
                           req.get_start_version(),
                           cb)
            .map_err(Error::Storage)
    }

    fn on_commit_then_get(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_commit_get_req() {
            return Err(box_err!("Msg doesn't contain a CmdCommitThenGetRequest"));
        }
        let cb = self.make_cb(StoreHandler::cmd_commit_get_done, token, msg_id);
        let mut req = msg.take_cmd_commit_get_req();
        self.store
            .async_commit_then_get(msg.take_context(),
                                   Key::from_raw(req.take_key()),
                                   req.get_lock_version(),
                                   req.get_commit_version(),
                                   req.get_get_version(),
                                   cb)
            .map_err(Error::Storage)
    }

    fn on_rollback_then_get(&self, mut msg: Request, token: Token, msg_id: u64) -> Result<()> {
        if !msg.has_cmd_rb_get_req() {
            return Err(box_err!("Msg doesn't contain a CmdRollbackThenGetRequest"));
        }
        let mut req = msg.take_cmd_rb_get_req();
        let cb = self.make_cb(StoreHandler::cmd_rollback_get_done, token, msg_id);
        self.store
            .async_rollback_then_get(msg.take_context(),
                                     Key::from_raw(req.take_key()),
                                     req.get_lock_version(),
                                     cb)
            .map_err(Error::Storage)
    }

    fn make_cb<T: 'static>(&self,
                           f: fn(StorageResult<T>) -> Response,
                           token: Token,
                           msg_id: u64)
                           -> Callback<T> {
        let ch = self.ch.clone();
        Box::new(move |r: StorageResult<T>| {
            let resp = f(r);

            let mut resp_msg = msgpb::Message::new();
            resp_msg.set_msg_type(msgpb::MessageType::KvResp);
            resp_msg.set_kv_resp(resp);
            if let Err(e) = ch.send(Msg::WriteData {
                token: token,
                data: ConnData::new(msg_id, resp_msg),
            }) {
                error!("send kv cmd resp failed with token {:?}, msg id {}, err {:?}",
                       token,
                       msg_id,
                       e);

            }
        })
    }

    fn cmd_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_get_resp = CmdGetResponse::new();
        let mut res_type = ResultType::new();
        match r {
            Ok(opt) => {
                res_type.set_field_type(ResultType_Type::Ok);
                match opt {
                    Some(val) => cmd_get_resp.set_value(val),
                    None => cmd_get_resp.set_value(Vec::new()),
                }
            }
            Err(ref e) => {
                if let Some(err) = engine::get_request_err(&e) {
                    if err.has_not_leader() {
                        res_type.set_field_type(ResultType_Type::NotLeader);
                        res_type.set_leader_info(err.get_not_leader().to_owned());
                    } else {
                        error!("{:?}", err);
                        res_type.set_field_type(ResultType_Type::Other);
                        res_type.set_msg(format!("engine error: {:?}", err));
                    }
                } else if let Some((_, primary, ts)) = r.get_lock() {
                    res_type.set_field_type(ResultType_Type::Locked);
                    res_type.set_lock_info(make_lock_info(primary, ts));
                } else {
                    let err_str = format!("storage error: {:?}", e);
                    error!("{}", err_str);
                    res_type.set_field_type(ResultType_Type::Retryable);
                    res_type.set_msg(err_str);
                }
            }
        }
        cmd_get_resp.set_res_type(res_type);
        resp.set_field_type(MessageType::CmdGet);
        resp.set_cmd_get_resp(cmd_get_resp);
        resp
    }

    fn cmd_scan_done(kvs: StorageResult<Vec<StorageResult<KvPair>>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_scan_resp = CmdScanResponse::new();
        cmd_scan_resp.set_ok(kvs.is_ok());
        resp.set_field_type(MessageType::CmdScan);

        let mut new_kvs = Vec::new();
        match kvs {
            Ok(kvs) => {
                // convert storage::KvPair to kvrpcpb::Item
                for result in kvs {
                    let mut item = make_item(&result);
                    if let Ok((key, value)) = result {
                        item.set_key(key);
                        item.set_value(value);
                    }
                    new_kvs.push(item);
                }
                cmd_scan_resp.set_results(RepeatedField::from_vec(new_kvs));
            }
            Err(e) => {
                if let Some(err) = engine::get_request_err(&e) {
                    let new_kv = make_not_leader_item(err);
                    new_kvs.push(new_kv);
                    cmd_scan_resp.set_results(RepeatedField::from_vec(new_kvs));
                } else {
                    error!("storage error: {:?}", e);
                }
            }
        }
        resp.set_cmd_scan_resp(cmd_scan_resp);

        resp
    }

    fn cmd_prewrite_done(results: StorageResult<Vec<StorageResult<()>>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_prewrite_resp = CmdPrewriteResponse::new();
        cmd_prewrite_resp.set_ok(results.is_ok());
        resp.set_field_type(MessageType::CmdPrewrite);

        let mut items = vec![];
        match results {
            Ok(results) => {
                for res in results {
                    let item = make_item(&res);
                    items.push(item);
                }
                cmd_prewrite_resp.set_results(RepeatedField::from_vec(items));
            }
            Err(e) => {
                if let Some(err) = engine::get_request_err(&e) {
                    let item = make_not_leader_item(err);
                    items.push(item);
                    cmd_prewrite_resp.set_results(RepeatedField::from_vec(items));
                } else {
                    error!("storage error: {:?}", e);
                }
            }
        }
        resp.set_cmd_prewrite_resp(cmd_prewrite_resp);
        resp
    }

    fn cmd_commit_done(r: StorageResult<()>) -> Response {
        let mut resp = Response::new();
        let mut cmd_commit_resp = CmdCommitResponse::new();
        cmd_commit_resp.set_ok(r.is_ok());
        resp.set_field_type(MessageType::CmdCommit);
        resp.set_cmd_commit_resp(cmd_commit_resp);
        resp
    }

    fn cmd_cleanup_done(r: StorageResult<()>) -> Response {
        let mut resp = Response::new();
        let mut cmd_cleanup_resp = CmdCleanupResponse::new();
        let mut res_type = ResultType::new();
        if r.is_ok() {
            res_type.set_field_type(ResultType_Type::Ok);
        } else if let Some(commit_ts) = r.get_commit() {
            res_type.set_field_type(ResultType_Type::Committed);
            cmd_cleanup_resp.set_commit_version(commit_ts);
        } else if r.is_rolledback() {
            res_type.set_field_type(ResultType_Type::Rolledback);
        } else {
            warn!("Cleanup other error {:?}", r.err());
            res_type.set_field_type(ResultType_Type::Retryable);
        }
        cmd_cleanup_resp.set_res_type(res_type);
        resp.set_field_type(MessageType::CmdCleanup);
        resp.set_cmd_cleanup_resp(cmd_cleanup_resp);
        resp
    }

    fn cmd_commit_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_commit_get_resp = CmdCommitThenGetResponse::new();
        cmd_commit_get_resp.set_ok(r.is_ok());
        if let Err(ref e) = r {
            error!("commit & get error: {}", e);
        }
        if let Ok(Some(val)) = r {
            cmd_commit_get_resp.set_value(val);
        }
        resp.set_field_type(MessageType::CmdCommitThenGet);
        resp.set_cmd_commit_get_resp(cmd_commit_get_resp);
        resp
    }

    fn cmd_rollback_get_done(r: StorageResult<Option<Value>>) -> Response {
        let mut resp = Response::new();
        let mut cmd_rollback_get_resp = CmdRollbackThenGetResponse::new();
        cmd_rollback_get_resp.set_ok(r.is_ok());
        if let Err(ref e) = r {
            error!("rb & get error: {}", e);
        }
        if let Ok(Some(val)) = r {
            cmd_rollback_get_resp.set_value(val);
        }
        resp.set_field_type(MessageType::CmdRollbackThenGet);
        resp.set_cmd_rb_get_resp(cmd_rollback_get_resp);
        resp
    }

    pub fn on_request(&self, req: Request, token: Token, msg_id: u64) -> Result<()> {
        debug!("notify Request token[{:?}] msg_id[{}] type[{:?}]",
               token,
               msg_id,
               req.get_field_type());
        if let Err(e) = match req.get_field_type() {
            MessageType::CmdGet => self.on_get(req, token, msg_id),
            MessageType::CmdScan => self.on_scan(req, token, msg_id),
            MessageType::CmdPrewrite => self.on_prewrite(req, token, msg_id),
            MessageType::CmdCommit => self.on_commit(req, token, msg_id),
            MessageType::CmdCleanup => self.on_cleanup(req, token, msg_id),
            MessageType::CmdCommitThenGet => self.on_commit_then_get(req, token, msg_id),
            MessageType::CmdRollbackThenGet => self.on_rollback_then_get(req, token, msg_id),
        } {
            // TODO: should we return an error and tell the client later?
            error!("Some error occur err[{:?}]", e);
        }

        Ok(())
    }
}

fn make_not_leader_item(err: ErrorHeader) -> Item {
    let mut item = Item::new();
    let mut res_type = ResultType::new();
    // If has_not_leader then fill first item with NotLeader error and return.
    if err.has_not_leader() {
        res_type.set_field_type(ResultType_Type::NotLeader);
        res_type.set_leader_info(err.get_not_leader().to_owned());
    } else {
        error!("{:?}", err);
        res_type.set_field_type(ResultType_Type::Other);
        res_type.set_msg(format!("engine error: {:?}", err));
    }
    item.set_res_type(res_type);
    item
}

fn make_lock_info(primary: Vec<u8>, version: u64) -> LockInfo {
    let mut lock_info = LockInfo::new();
    lock_info.set_primary_lock(primary);
    lock_info.set_lock_version(version);
    lock_info
}

fn make_item<T>(res: &StorageResult<T>) -> Item {
    let mut item = Item::new();
    let mut res_type = ResultType::new();
    if res.is_ok() {
        res_type.set_field_type(ResultType_Type::Ok);
    } else if let Some((key, primary, ts)) = res.get_lock() {
        res_type.set_field_type(ResultType_Type::Locked);
        res_type.set_lock_info(make_lock_info(primary, ts));
        item.set_key(key);
    } else {
        res_type.set_field_type(ResultType_Type::Retryable);
    }
    item.set_res_type(res_type);
    item
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::*;
    use kvproto::errorpb::NotLeader;
    use storage::{self, Value, txn, mvcc, engine};
    use storage::Result as StorageResult;
    use super::*;

    #[test]
    fn test_get_done_none() {
        let actual_resp = StoreHandler::cmd_get_done(Ok(None));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(make_res_type(ResultType_Type::Ok));
        exp_cmd_resp.set_value(Vec::new());
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_get_done_some() {
        let storage_val = vec![0x0; 0x8];
        let actual_resp = StoreHandler::cmd_get_done(Ok(Some(storage_val)));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        exp_cmd_resp.set_res_type(make_res_type(ResultType_Type::Ok));
        exp_cmd_resp.set_value(vec![0x0; 0x8]);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    // #[should_panic]
    fn test_get_done_error() {
        let actual_resp = StoreHandler::cmd_get_done(Err(box_err!("error")));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdGetResponse::new();
        let mut res_type = make_res_type(ResultType_Type::Retryable);
        res_type.set_msg("storage error: Other(StringError(\"error\"))".to_owned());
        exp_cmd_resp.set_res_type(res_type);
        exp_resp.set_field_type(MessageType::CmdGet);
        exp_resp.set_cmd_get_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_empty() {
        let actual_resp = StoreHandler::cmd_scan_done(Ok(Vec::new()));
        let mut exp_resp = Response::new();
        let mut exp_cmd_resp = CmdScanResponse::new();
        exp_cmd_resp.set_ok(true);
        exp_resp.set_field_type(MessageType::CmdScan);
        exp_resp.set_cmd_scan_resp(exp_cmd_resp);
        assert_eq!(exp_resp, actual_resp);
    }

    #[test]
    fn test_scan_done_some() {
        let k0 = vec![0x0, 0x0];
        let v0 = vec![0xff, 0xff];
        let k1 = vec![0x0, 0x1];
        let v1 = vec![0xff, 0xfe];
        let kvs = vec![Ok((k0.clone(), v0.clone())), Ok((k1.clone(), v1.clone()))];
        let actual_resp = StoreHandler::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(v1, actual_kvs[1].get_value());
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
                       make_lock_error(k1.clone(), k1_primary.clone(), k1_ts)];
        let actual_resp = StoreHandler::cmd_scan_done(Ok(kvs));
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let actual_cmd_resp = actual_resp.get_cmd_scan_resp();
        assert_eq!(true, actual_cmd_resp.get_ok());
        let actual_kvs = actual_cmd_resp.get_results();
        assert_eq!(2, actual_kvs.len());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_kvs[0].get_res_type());
        assert_eq!(k0, actual_kvs[0].get_key());
        assert_eq!(v0, actual_kvs[0].get_value());
        let mut exp_res_type1 = make_res_type(ResultType_Type::Locked);
        let mut lock_info1 = LockInfo::new();
        lock_info1.set_primary_lock(k1_primary.clone());
        lock_info1.set_lock_version(k1_ts);
        exp_res_type1.set_lock_info(lock_info1);
        assert_eq!(exp_res_type1, *actual_kvs[1].get_res_type());
        assert_eq!(k1, actual_kvs[1].get_key());
        assert_eq!(k1_primary.clone(),
                   actual_kvs[1].get_res_type().get_lock_info().get_primary_lock());
        assert_eq!(k1_ts,
                   actual_kvs[1].get_res_type().get_lock_info().get_lock_version());
    }

    #[test]
    fn test_prewrite_done_ok() {
        let actual_resp = StoreHandler::cmd_prewrite_done(Ok(Vec::new()));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_prewrite_done_err() {
        let actual_resp = StoreHandler::cmd_prewrite_done(Err(box_err!("prewrite error")));
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_prewrite_resp().get_ok());
    }

    #[test]
    fn test_commit_done_ok() {
        let actual_resp = StoreHandler::cmd_commit_done(Ok(()));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(true, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_commit_done_err() {
        let actual_resp = StoreHandler::cmd_commit_done(Err(box_err!("commit error")));
        assert_eq!(MessageType::CmdCommit, actual_resp.get_field_type());
        assert_eq!(false, actual_resp.get_cmd_commit_resp().get_ok());
    }

    #[test]
    fn test_cleanup_done_ok() {
        let actual_resp = StoreHandler::cmd_cleanup_done(Ok(()));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(make_res_type(ResultType_Type::Ok),
                   *actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    #[test]
    fn test_cleanup_done_err() {
        let actual_resp = StoreHandler::cmd_cleanup_done(Err(box_err!("cleanup error")));
        assert_eq!(MessageType::CmdCleanup, actual_resp.get_field_type());
        assert_eq!(make_res_type(ResultType_Type::Retryable),
                   *actual_resp.get_cmd_cleanup_resp().get_res_type());
    }

    #[test]
    fn test_get_not_leader() {
        use kvproto::errorpb::NotLeader;
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res: StorageResult<Option<Value>> =
            make_not_leader_error(leader_info.to_owned());
        let actual_resp = StoreHandler::cmd_get_done(storage_res);
        assert_eq!(MessageType::CmdGet, actual_resp.get_field_type());
        let mut exp_res_type = make_res_type(ResultType_Type::NotLeader);
        exp_res_type.set_leader_info(leader_info.to_owned());
        assert_eq!(exp_res_type, *actual_resp.get_cmd_get_resp().get_res_type());
    }

    #[test]
    fn test_scan_not_leader() {
        use storage::KvPair;
        use kvproto::errorpb::NotLeader;
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res: StorageResult<Vec<StorageResult<KvPair>>> =
            make_not_leader_error(leader_info.to_owned());
        let actual_resp = StoreHandler::cmd_scan_done(storage_res);
        assert_eq!(MessageType::CmdScan, actual_resp.get_field_type());
        let mut exp_res_type = make_res_type(ResultType_Type::NotLeader);
        exp_res_type.set_leader_info(leader_info.to_owned());
        let results = actual_resp.get_cmd_scan_resp().get_results();
        assert!(results.len() >= 1);
        assert_eq!(exp_res_type, *results[0].get_res_type());
    }

    #[test]
    fn test_prewrite_not_leader() {
        use kvproto::errorpb::NotLeader;
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let storage_res: StorageResult<Vec<StorageResult<()>>> =
            make_not_leader_error(leader_info.to_owned());
        let actual_resp = StoreHandler::cmd_prewrite_done(storage_res);
        assert_eq!(MessageType::CmdPrewrite, actual_resp.get_field_type());
        let mut exp_res_type = make_res_type(ResultType_Type::NotLeader);
        exp_res_type.set_leader_info(leader_info.to_owned());
        let results = actual_resp.get_cmd_prewrite_resp().get_results();
        assert!(results.len() >= 1);
        assert_eq!(exp_res_type, *results[0].get_res_type());
    }

    fn make_lock_error<T>(key: Vec<u8>, primary: Vec<u8>, ts: u64) -> StorageResult<T> {
        Err(mvcc::Error::KeyIsLocked {
            key: key,
            primary: primary,
            ts: ts,
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

    fn make_res_type(tp: ResultType_Type) -> ResultType {
        let mut res_type = ResultType::new();
        res_type.set_field_type(tp);
        res_type
    }
}
