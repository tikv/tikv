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
use std::sync::Arc;

use protobuf::RepeatedField;
use kvproto::kvpb::{Row, KeyError};
use kvproto::kvrpcpb::{CmdGetResponse, CmdScanResponse, CmdPrewriteResponse, CmdCommitResponse,
                       CmdBatchRollbackResponse, CmdCleanupResponse, CmdRollbackThenGetResponse,
                       CmdCommitThenGetResponse, CmdBatchGetResponse, Request, Response,
                       MessageType};
use kvproto::msgpb;
use storage::{Engine, Storage, Callback, CallbackResult};
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
        let mut req = msg.take_cmd_get_req();
        let ctx = msg.take_context();
        let cb = self.make_cb(StoreHandler::cmd_get_done, on_resp);
        self.store
            .async_get(ctx,
                       req.take_row_key(),
                       req.take_columns().into_vec(),
                       req.get_ts(),
                       cb)
            .map_err(Error::Storage)
    }

    fn on_scan(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_scan_req() {
            return Err(box_err!("msg doesn't contain a CmdScanRequest"));
        }
        let mut req = msg.take_cmd_scan_req();
        debug!("start_row [{}]", escape(req.get_start_row_key()));
        let cb = self.make_cb(StoreHandler::cmd_scan_done, on_resp);
        self.store
            .async_scan(msg.take_context(),
                        req.take_start_row_key(),
                        req.take_columns().into_vec(),
                        req.get_limit() as usize,
                        req.get_ts(),
                        cb)
            .map_err(Error::Storage)
    }

    fn on_prewrite(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_prewrite_req() {
            return Err(box_err!("msg doesn't contain a CmdPrewriteRequest"));
        }
        let mut req = msg.take_cmd_prewrite_req();
        let mutations = req.take_mutations().into_vec();
        let cb = self.make_cb(StoreHandler::cmd_prewrite_done, on_resp);
        self.store
            .async_prewrite(msg.take_context(),
                            mutations,
                            req.take_primary(),
                            req.get_ts(),
                            cb)
            .map_err(Error::Storage)
    }

    fn on_commit(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_commit_req() {
            return Err(box_err!("msg doesn't contain a CmdCommitRequest"));
        }
        let mut req = msg.take_cmd_commit_req();
        let cb = self.make_cb(StoreHandler::cmd_commit_done, on_resp);
        let rows = req.take_row_keys().into_vec();
        self.store
            .async_commit(msg.take_context(),
                          rows,
                          req.get_start_ts(),
                          req.get_commit_ts(),
                          cb)
            .map_err(Error::Storage)
    }

    fn on_batch_rollback(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_batch_rollback_req() {
            return Err(box_err!("msg doesn't contain a CmdRollbackRequest"));
        }
        let mut req = msg.take_cmd_batch_rollback_req();
        let cb = self.make_cb(StoreHandler::cmd_batch_rollback_done, on_resp);
        let rows = req.take_row_keys().into_vec();
        self.store
            .async_rollback(msg.take_context(), rows, req.get_ts(), cb)
            .map_err(Error::Storage)
    }

    fn on_cleanup(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_cleanup_req() {
            return Err(box_err!("msg doesn't contain a CmdCleanupRequest"));
        }
        let mut req = msg.take_cmd_cleanup_req();
        let cb = self.make_cb(StoreHandler::cmd_cleanup_done, on_resp);
        self.store
            .async_cleanup(msg.take_context(), req.take_row_key(), req.get_ts(), cb)
            .map_err(Error::Storage)
    }

    fn on_commit_then_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_commit_get_req() {
            return Err(box_err!("msg doesn't contain a CmdCommitThenGetRequest"));
        }
        let cb = self.make_cb(StoreHandler::cmd_commit_get_done, on_resp);
        let mut req = msg.take_cmd_commit_get_req();
        self.store
            .async_commit_then_get(msg.take_context(),
                                   req.take_row_key(),
                                   req.take_columns().into_vec(),
                                   req.get_start_ts(),
                                   req.get_commit_ts(),
                                   req.get_get_ts(),
                                   cb)
            .map_err(Error::Storage)
    }

    fn on_rollback_then_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_rb_get_req() {
            return Err(box_err!("msg doesn't contain a CmdRollbackThenGetRequest"));
        }
        let mut req = msg.take_cmd_rb_get_req();
        let cb = self.make_cb(StoreHandler::cmd_rollback_get_done, on_resp);
        self.store
            .async_rollback_then_get(msg.take_context(),
                                     req.take_row_key(),
                                     req.take_columns().into_vec(),
                                     req.get_ts(),
                                     cb)
            .map_err(Error::Storage)
    }

    fn on_batch_get(&self, mut msg: Request, on_resp: OnResponse) -> Result<()> {
        if !msg.has_cmd_batch_get_req() {
            return Err(box_err!("msg doesn't contain a CmdBatchGetRequest"));
        }
        let mut req = msg.take_cmd_batch_get_req();
        let cb = self.make_cb(StoreHandler::cmd_batch_get_done, on_resp);
        self.store
            .async_batch_get(msg.take_context(),
                             req.take_row_keys().into_vec(),
                             req.take_columns().iter().map(|x| x.get_columns().to_vec()).collect(),
                             req.get_ts(),
                             cb)
            .map_err(Error::Storage)
    }

    fn make_cb<T: 'static>(&self, f: fn(T, &mut Response), on_resp: OnResponse) -> Callback<T> {
        Box::new(move |res| {
            let mut resp = Response::new();
            match res {
                CallbackResult::Ok(x) => f(x, &mut resp),
                CallbackResult::Err(e) => resp.set_region_error(e),
            }
            let mut resp_msg = msgpb::Message::new();
            resp_msg.set_msg_type(msgpb::MessageType::KvResp);
            resp_msg.set_kv_resp(resp);
            on_resp.call_box((resp_msg,))
        })
    }

    fn cmd_get_done(row: Row, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdGet);
        let mut get_resp = CmdGetResponse::new();
        get_resp.set_row(row);
        resp.set_cmd_get_resp(get_resp);
    }

    fn cmd_scan_done(rows: Vec<Row>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdScan);
        let mut scan_resp = CmdScanResponse::new();
        scan_resp.set_rows(RepeatedField::from_vec(rows));
        resp.set_cmd_scan_resp(scan_resp);
    }

    fn cmd_batch_get_done(rows: Vec<Row>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdBatchGet);
        let mut batch_get_resp = CmdBatchGetResponse::new();
        batch_get_resp.set_rows(RepeatedField::from_vec(rows));
        resp.set_cmd_batch_get_resp(batch_get_resp);
    }

    fn cmd_prewrite_done(errs: Vec<KeyError>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdPrewrite);
        let mut prewrite_resp = CmdPrewriteResponse::new();
        prewrite_resp.set_errors(RepeatedField::from_vec(errs));
        resp.set_cmd_prewrite_resp(prewrite_resp);
    }

    fn cmd_commit_done(err: Option<KeyError>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdCommit);
        let mut commit = CmdCommitResponse::new();
        if let Some(e) = err {
            commit.set_error(e);
        }
        resp.set_cmd_commit_resp(commit);
    }

    fn cmd_batch_rollback_done(err: Option<KeyError>, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdBatchRollback);
        let mut rollback = CmdBatchRollbackResponse::new();
        if let Some(e) = err {
            rollback.set_error(e);
        }
        resp.set_cmd_batch_rollback_resp(rollback)
    }

    fn cmd_cleanup_done(r: (Option<KeyError>, Option<u64>), resp: &mut Response) {
        resp.set_field_type(MessageType::CmdCleanup);
        let mut cleanup = CmdCleanupResponse::new();
        match r {
            (Some(e), _) => cleanup.set_error(e),
            (_, Some(ts)) => cleanup.set_commit_ts(ts),
            _ => {}
        }
        resp.set_cmd_cleanup_resp(cleanup);
    }

    fn cmd_commit_get_done(r: Row, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdCommitThenGet);
        let mut commit_get = CmdCommitThenGetResponse::new();
        commit_get.set_row(r);
        resp.set_cmd_commit_get_resp(commit_get);
    }

    fn cmd_rollback_get_done(r: Row, resp: &mut Response) {
        resp.set_field_type(MessageType::CmdRollbackThenGet);
        let mut rollback_get = CmdRollbackThenGetResponse::new();
        rollback_get.set_row(r);
        resp.set_cmd_rb_get_resp(rollback_get);
    }

    pub fn on_request(&self, req: Request, on_resp: OnResponse) -> Result<()> {
        if let Err(e) = match req.get_field_type() {
            MessageType::CmdGet => self.on_get(req, on_resp),
            MessageType::CmdScan => self.on_scan(req, on_resp),
            MessageType::CmdPrewrite => self.on_prewrite(req, on_resp),
            MessageType::CmdCommit => self.on_commit(req, on_resp),
            MessageType::CmdCleanup => self.on_cleanup(req, on_resp),
            MessageType::CmdCommitThenGet => self.on_commit_then_get(req, on_resp),
            MessageType::CmdRollbackThenGet => self.on_rollback_then_get(req, on_resp),
            MessageType::CmdBatchGet => self.on_batch_get(req, on_resp),
            MessageType::CmdBatchRollback => self.on_batch_rollback(req, on_resp),
        } {
            // TODO: should we return an error and tell the client later?
            error!("Some error occur err[{:?}]", e);
        }

        Ok(())
    }

    pub fn engine(&self) -> Arc<Box<Engine>> {
        self.store.get_engine()
    }

    pub fn stop(&mut self) -> Result<()> {
        self.store.stop().map_err(From::from)
    }
}

#[cfg(test)]
mod tests {
    use kvproto::kvrpcpb::*;
    use kvproto::errorpb::{NotLeader, Error as RegionError};
    use kvproto::kvpb::Row;
    use storage::CallbackResult;
    use super::*;

    fn build_resp<T>(res: CallbackResult<T>, f: fn(T, &mut Response)) -> Response {
        let mut resp = Response::new();
        match res {
            CallbackResult::Ok(x) => f(x, &mut resp),
            CallbackResult::Err(e) => resp.set_region_error(e),
        }
        resp
    }

    #[test]
    fn test_get_done() {
        let resp = build_resp(CallbackResult::Ok(Row::new()), StoreHandler::cmd_get_done);
        let mut cmd = CmdGetResponse::new();
        cmd.set_row(Row::new());
        let mut expect = Response::new();
        expect.set_field_type(MessageType::CmdGet);
        expect.set_cmd_get_resp(cmd);
        assert_eq!(expect, resp);
    }

    #[test]
    fn test_get_not_leader() {
        let mut leader_info = NotLeader::new();
        leader_info.set_region_id(1);
        let mut err = RegionError::new();
        err.set_not_leader(leader_info.clone());
        let resp = build_resp(CallbackResult::Err(err), StoreHandler::cmd_get_done);
        assert!(resp.has_region_error());
        let region_err = resp.get_region_error();
        assert!(region_err.has_not_leader());
        assert_eq!(region_err.get_not_leader(), &leader_info);
    }
}
