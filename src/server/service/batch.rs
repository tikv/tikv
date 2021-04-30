// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::server::metrics::{GrpcTypeKind, GRPC_MSG_FAIL_COUNTER, GRPC_MSG_HISTOGRAM_STATIC};
use crate::server::service::kv::{
    batch_commands_response, future_get, response_batch_commands_request,
};
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use futures::future::TryFutureExt;
use kvproto::kvrpcpb::*;
use tikv_util::future::poll_future_notify;
use tikv_util::mpsc::batch::Sender;
use tikv_util::time::{duration_to_sec, Instant};

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 16;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;

pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ids: Vec<u64>,
    raw_get_ids: Vec<u64>,
}

impl ReqBatcher {
    pub fn new() -> ReqBatcher {
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ids: vec![],
            raw_get_ids: vec![],
        }
    }

    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn add_get_request(&mut self, req: GetRequest, id: u64) {
        self.gets.push(req);
        self.get_ids.push(id);
    }

    pub fn add_raw_get_request(&mut self, req: RawGetRequest, id: u64) {
        self.raw_gets.push(req);
        self.raw_get_ids.push(id);
    }

    // If the length of queue is very long, we may keep the request to get a larger batch.
    // But if the length of queue is short, we shall schedule the request in parallel at once.
    // Because if we schedule the requests store before, the readpool may become busy and the
    // following requests can change the strategy.
    pub fn maybe_commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
        mut queue_size: usize,
    ) {
        queue_size = std::cmp::min(queue_size, MAX_BATCH_GET_REQUEST_COUNT);
        if queue_size >= MIN_BATCH_GET_REQUEST_COUNT  {
            if self.gets.len() >= queue_size {
                let gets = std::mem::take(&mut self.gets);
                let ids = std::mem::take(&mut self.get_ids);
                future_batch_get_command(storage, ids, gets, tx.clone());
            }
        } else if self.gets.len() >= MIN_BATCH_GET_REQUEST_COUNT {
            self.run_kv_get_in_parallel(storage, tx);
        }
        if (queue_size >= MIN_BATCH_GET_REQUEST_COUNT && self.raw_gets.len() > queue_size)
            || self.raw_gets.len() >= MAX_BATCH_GET_REQUEST_COUNT
        {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            future_batch_raw_get_command(storage, ids, gets, tx.clone());
        }
    }

    pub fn commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
        queue_size: usize,
    ) {
        if !self.gets.is_empty() {
            if queue_size >= MIN_BATCH_GET_REQUEST_COUNT {
                let gets = std::mem::take(&mut self.gets);
                let ids = std::mem::take(&mut self.get_ids);
                future_batch_get_command(storage, ids, gets, tx.clone());
            } else {
                self.run_kv_get_in_parallel(storage, tx);
            }
        }
        if !self.raw_gets.is_empty() {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            future_batch_raw_get_command(storage, ids, gets, tx.clone());
        }
    }

    fn run_kv_get_in_parallel<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        let gets = std::mem::take(&mut self.gets);
        let ids = std::mem::take(&mut self.get_ids);
        let begin_instant = Instant::now();
        for (id, req) in ids.into_iter().zip(gets) {
            let resp = future_get(storage, req)
                .map_ok(|resp| batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp)),
                    ..Default::default()
                })
                .map_err(|_| GRPC_MSG_FAIL_COUNTER.kv_get.inc());
            response_batch_commands_request(
                id,
                resp,
                tx.clone(),
                begin_instant,
                GrpcTypeKind::kv_get,
            );
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
) {
    let begin_instant = Instant::now_coarse();
    let ret = storage.batch_get_command(gets);
    let f = async move {
        match ret.await {
            Ok(ret) => {
                for (v, req) in ret.into_iter().zip(requests) {
                    let mut resp = GetResponse::default();
                    if let Some(err) = extract_region_error(&v) {
                        resp.set_region_error(err);
                    } else {
                        match v {
                            Ok((val, statistics, perf_statistics_delta)) => {
                                let scan_detail_v2 =
                                    resp.mut_exec_details_v2().mut_scan_detail_v2();
                                statistics.write_scan_detail(scan_detail_v2);
                                perf_statistics_delta.write_scan_detail(scan_detail_v2);
                                match val {
                                    Some(val) => resp.set_value(val),
                                    None => resp.set_not_found(true),
                                }
                            }
                            Err(e) => resp.set_error(extract_key_error(&e)),
                        }
                    }
                    let res = batch_commands_response::Response {
                        cmd: Some(batch_commands_response::response::Cmd::Get(resp)),
                        ..Default::default()
                    };
                    if tx.send_and_notify((req, res)).is_err() {
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
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp)),
                    ..Default::default()
                };
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .kv_batch_get_command
            .observe(begin_instant.elapsed_secs());
    };
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
) {
    let begin_instant = Instant::now_coarse();
    let ret = storage.raw_batch_get_command(gets);
    let f = async move {
        match ret.await {
            Ok(v) => {
                if requests.len() != v.len() {
                    error!("KvService batch response size mismatch");
                }
                for (req, v) in requests.into_iter().zip(v.into_iter()) {
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
                    let res = batch_commands_response::Response {
                        cmd: Some(batch_commands_response::response::Cmd::RawGet(resp)),
                        ..Default::default()
                    };
                    if tx.send_and_notify((req, res)).is_err() {
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
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::RawGet(resp)),
                    ..Default::default()
                };
                for req in requests {
                    if tx.send_and_notify((req, res.clone())).is_err() {
                        error!("KvService response batch commands fail");
                    }
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .raw_batch_get_command
            .observe(duration_to_sec(begin_instant.elapsed()));
    };
    poll_future_notify(f);
}
