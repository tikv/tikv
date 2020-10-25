// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::GRPC_MSG_HISTOGRAM_STATIC;
use crate::server::service::kv::{batch_commands_response, poll_future_notify};
use crate::server::tracing::Reporter;
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use futures::Future;
use kvproto::kvrpcpb::*;
use tikv_util::minitrace::*;
use tikv_util::mpsc::batch::Sender;
use tikv_util::time::{duration_to_sec, Instant};

struct ReqContext {
    pub id: u64,
    pub trace_context: TraceContext,
    pub scope: Scope,
    pub collector: Option<Collector>,
}

pub struct ReqBatcher<R> {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ctxs: Vec<ReqContext>,
    raw_get_ctxs: Vec<ReqContext>,
    reporter: R,
}

impl<R: Reporter + Clone + 'static> ReqBatcher<R> {
    pub fn new(reporter: R) -> Self {
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ctxs: vec![],
            raw_get_ctxs: vec![],
            reporter,
        }
    }

    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn add_get_request(
        &mut self,
        mut req: GetRequest,
        scope: Scope,
        collector: Option<Collector>,
        id: u64,
    ) {
        let trace_context = req.mut_context().take_trace_context();
        self.gets.push(req);
        self.get_ctxs.push(ReqContext {
            id,
            trace_context,
            scope,
            collector,
        });
    }

    pub fn add_raw_get_request(
        &mut self,
        mut req: RawGetRequest,
        scope: Scope,
        collector: Option<Collector>,
        id: u64,
    ) {
        let trace_context = req.mut_context().take_trace_context();
        self.raw_gets.push(req);
        self.raw_get_ctxs.push(ReqContext {
            id,
            trace_context,
            scope,
            collector,
        });
    }

    pub fn maybe_commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if self.gets.len() > 10 {
            let gets = std::mem::replace(&mut self.gets, vec![]);
            let get_ctxs = std::mem::replace(&mut self.get_ctxs, vec![]);
            future_batch_get_command(storage, get_ctxs, gets, tx.clone(), self.reporter.clone());
        }
        if self.raw_gets.len() > 16 {
            let gets = std::mem::replace(&mut self.raw_gets, vec![]);
            let get_ctxs = std::mem::replace(&mut self.raw_get_ctxs, vec![]);
            future_batch_raw_get_command(
                storage,
                get_ctxs,
                gets,
                tx.clone(),
                self.reporter.clone(),
            );
        }
    }

    pub fn commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if !self.gets.is_empty() {
            let gets = std::mem::replace(&mut self.gets, vec![]);
            let get_ctxs = std::mem::replace(&mut self.get_ctxs, vec![]);
            future_batch_get_command(storage, get_ctxs, gets, tx.clone(), self.reporter.clone());
        }
        if !self.raw_gets.is_empty() {
            let gets = std::mem::replace(&mut self.raw_gets, vec![]);
            let get_ctxs = std::mem::replace(&mut self.raw_get_ctxs, vec![]);
            future_batch_raw_get_command(
                storage,
                get_ctxs,
                gets,
                tx.clone(),
                self.reporter.clone(),
            );
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager, R: Reporter + 'static>(
    storage: &Storage<E, L>,
    req_ctxs: Vec<ReqContext>,
    gets: Vec<GetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    reporter: R,
) {
    let begin_instant = Instant::now_coarse();
    let _gs = req_ctxs.iter().map(|ctx| ctx.scope.start_scope());
    let f = storage.batch_get_command(gets).then(move |ret| {
        match ret {
            Ok(ret) => {
                for (
                    v,
                    ReqContext {
                        id,
                        trace_context,
                        scope,
                        collector,
                    },
                ) in ret.into_iter().zip(req_ctxs)
                {
                    drop(scope);
                    reporter.report(trace_context, collector);

                    let mut resp = GetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok(Some(val)) => resp.set_value(val),
                            Ok(None) => resp.set_not_found(true),
                            Err(e) => resp.set_error(extract_key_error(&e)),
                        }
                    }
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                    if tx.send_and_notify((id, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = GetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(extract_key_error(&e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::Get(resp));
                for ReqContext {
                    id,
                    trace_context,
                    scope,
                    collector,
                } in req_ctxs
                {
                    drop(scope);
                    reporter.report(trace_context, collector);

                    if tx.send_and_notify((id, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .kv_batch_get_command
            .observe(begin_instant.elapsed_secs());
        Ok(())
    });
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager, R: Reporter + 'static>(
    storage: &Storage<E, L>,
    req_ctxs: Vec<ReqContext>,
    gets: Vec<RawGetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    reporter: R,
) {
    let begin_instant = Instant::now_coarse();
    let f = storage.raw_batch_get_command(gets).then(move |v| {
        match v {
            Ok(v) => {
                if req_ctxs.len() != v.len() {
                    error!("KvService batch response size mismatch");
                }
                for (
                    v,
                    ReqContext {
                        id,
                        trace_context,
                        scope,
                        collector,
                    },
                ) in v.into_iter().zip(req_ctxs)
                {
                    drop(scope);
                    reporter.report(trace_context, collector);

                    let mut resp = RawGetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok(Some(val)) => resp.set_value(val),
                            Ok(None) => resp.set_not_found(true),
                            Err(e) => resp.set_error(format!("{}", e)),
                        }
                    }
                    let mut res = batch_commands_response::Response::default();
                    res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                    if tx.send_and_notify((id, res)).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
            e => {
                let mut resp = RawGetResponse::default();
                if let Some(err) = extract_region_error(&e) {
                    resp.set_region_error(err);
                } else if let Err(e) = e {
                    resp.set_error(format!("{}", e));
                }
                let mut res = batch_commands_response::Response::default();
                res.cmd = Some(batch_commands_response::response::Cmd::RawGet(resp));
                for ReqContext {
                    id,
                    trace_context,
                    scope,
                    collector,
                } in req_ctxs
                {
                    drop(scope);
                    reporter.report(trace_context, collector);

                    if tx.send_and_notify((id, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .raw_batch_get_command
            .observe(duration_to_sec(begin_instant.elapsed()));
        Ok(())
    });
    poll_future_notify(f);
}
