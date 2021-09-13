// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use crate::config::TraceConfig;
use crate::server::metrics::GRPC_MSG_HISTOGRAM_STATIC;
use crate::server::metrics::REQUEST_BATCH_SIZE_HISTOGRAM_VEC;
use crate::server::service::kv::batch_commands_response;
use crate::storage::kv::{PerfStatisticsDelta, Statistics};
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use crate::storage::{ResponseBatchConsumer, Result};
use crate::trace_and_fill_resp;

use collections::HashMap;
use kvproto::kvrpcpb::*;
use tikv_util::future::poll_future_notify;
use tikv_util::mpsc::batch::Sender;
use tikv_util::time::Instant;
use tikv_util::trace::Span;

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 10;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;
pub const MAX_QUEUE_SIZE_PER_WORKER: usize = 16;

pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ids: Vec<u64>,
    raw_get_ids: Vec<u64>,
    get_trace: HashMap<u64, Trace<GetResponse>>,
    raw_get_trace: HashMap<u64, Trace<RawGetResponse>>,
    begin_instant: Instant,
    batch_size: usize,
    begin_unix_time_ns: u64,
}

type Trace<T> = (Span, Box<dyn FnOnce(&mut T) + Send>);

impl ReqBatcher {
    pub fn new(batch_size: usize, begin_unix_time_ns: u64) -> ReqBatcher {
        let begin_instant = Instant::now_coarse();
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ids: vec![],
            raw_get_ids: vec![],
            get_trace: HashMap::default(),
            raw_get_trace: HashMap::default(),
            begin_instant,
            batch_size: std::cmp::min(batch_size, MAX_BATCH_GET_REQUEST_COUNT),
            begin_unix_time_ns,
        }
    }

    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn add_get_request(&mut self, mut req: GetRequest, id: u64, trace_config: &TraceConfig) {
        let ns = self.begin_unix_time_ns;
        let (span, fill_resp) = trace_and_fill_resp!(get, trace_config, &mut req, GetResponse, ns);

        self.gets.push(req);
        self.get_ids.push(id);
        self.get_trace.insert(id, (span, Box::new(fill_resp)));
    }

    pub fn add_raw_get_request(
        &mut self,
        mut req: RawGetRequest,
        id: u64,
        trace_config: &TraceConfig,
    ) {
        let ns = self.begin_unix_time_ns;
        let (span, fill_resp) =
            trace_and_fill_resp!(raw_get, trace_config, &mut req, RawGetResponse, ns);

        self.raw_gets.push(req);
        self.raw_get_ids.push(id);
        self.raw_get_trace.insert(id, (span, Box::new(fill_resp)));
    }

    pub fn maybe_commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if self.gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.gets);
            let ids = std::mem::take(&mut self.get_ids);
            let get_trace = std::mem::take(&mut self.get_trace);
            let span = Span::from_parents("BatchGetCommand", get_trace.values().map(|t| &t.0));
            let _g = span.enter();
            future_batch_get_command(
                storage,
                ids,
                gets,
                tx.clone(),
                self.begin_instant,
                get_trace,
            );
        }

        if self.raw_gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            let raw_get_trace = std::mem::take(&mut self.raw_get_trace);
            let span =
                Span::from_parents("BatchRawGetCommand", raw_get_trace.values().map(|t| &t.0));
            let _g = span.enter();
            future_batch_raw_get_command(
                storage,
                ids,
                gets,
                tx.clone(),
                self.begin_instant,
                raw_get_trace,
            );
        }
    }

    pub fn commit<E: Engine, L: LockManager>(
        self,
        storage: &Storage<E, L>,
        tx: &Sender<(u64, batch_commands_response::Response)>,
    ) {
        if !self.gets.is_empty() {
            future_batch_get_command(
                storage,
                self.get_ids,
                self.gets,
                tx.clone(),
                self.begin_instant,
                self.get_trace,
            );
        }
        if !self.raw_gets.is_empty() {
            future_batch_raw_get_command(
                storage,
                self.raw_get_ids,
                self.raw_gets,
                tx.clone(),
                self.begin_instant,
                self.raw_get_trace,
            );
        }
    }
}

pub struct BatcherBuilder {
    pool_size: usize,
    enable_batch: bool,
}

impl BatcherBuilder {
    pub fn new(enable_batch: bool, pool_size: usize) -> Self {
        BatcherBuilder {
            enable_batch,
            pool_size,
        }
    }
    pub fn build(
        &self,
        queue_per_worker: usize,
        req_batch_size: usize,
        begin_unix_time_ns: u64,
    ) -> Option<ReqBatcher> {
        if !self.enable_batch {
            return None;
        }
        if req_batch_size > self.pool_size * MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MIN_BATCH_GET_REQUEST_COUNT
        {
            return Some(ReqBatcher::new(
                req_batch_size / self.pool_size,
                begin_unix_time_ns,
            ));
        }
        if req_batch_size >= MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MAX_QUEUE_SIZE_PER_WORKER
        {
            return Some(ReqBatcher::new(req_batch_size, begin_unix_time_ns));
        }
        None
    }
}

pub struct GetCommandResponseConsumer<T> {
    tx: Sender<(u64, batch_commands_response::Response)>,
    trace: HashMap<u64, Trace<T>>,
}

impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>
    for GetCommandResponseConsumer<GetResponse>
{
    fn consume(
        &mut self,
        id: u64,
        res: Result<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
    ) {
        let (span, fill_resp) = self.trace.remove(&id).unwrap();
        drop(span);

        let mut resp = GetResponse::default();
        fill_resp(&mut resp);
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok((val, statistics, perf_statistics_delta)) => {
                    let scan_detail_v2 = resp.mut_exec_details_v2().mut_scan_detail_v2();
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
        if self.tx.send_and_notify((id, res)).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

impl ResponseBatchConsumer<Option<Vec<u8>>> for GetCommandResponseConsumer<RawGetResponse> {
    fn consume(&mut self, id: u64, res: Result<Option<Vec<u8>>>) {
        let (span, fill_resp) = self.trace.remove(&id).unwrap();
        drop(span);
        let mut resp = RawGetResponse::default();
        fill_resp(&mut resp);

        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok(Some(val)) => resp.set_value(val),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        let res = batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::RawGet(resp)),
            ..Default::default()
        };
        if self.tx.send_and_notify((id, res)).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    begin_instant: tikv_util::time::Instant,
    get_trace: HashMap<u64, Trace<GetResponse>>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(gets.len() as f64);
    let ids = requests.clone();
    let res = storage.batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer {
            tx: tx.clone(),
            trace: get_trace,
        },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = GetResponse::default();
            resp.set_region_error(e);
            for id in ids {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp.clone())),
                    ..Default::default()
                };
                if tx.send_and_notify((id, res)).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .kv_batch_get_command
            .observe(begin_instant.saturating_elapsed_secs());
    };
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    requests: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tx: Sender<(u64, batch_commands_response::Response)>,
    begin_instant: tikv_util::time::Instant,
    raw_get_trace: HashMap<u64, Trace<RawGetResponse>>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .raw_get
        .observe(gets.len() as f64);
    let ids = requests.clone();
    let res = storage.raw_batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer {
            tx: tx.clone(),
            trace: raw_get_trace,
        },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = RawGetResponse::default();
            resp.set_region_error(e);
            for id in ids {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::RawGet(resp.clone())),
                    ..Default::default()
                };
                if tx.send_and_notify((id, res)).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
        GRPC_MSG_HISTOGRAM_STATIC
            .raw_batch_get_command
            .observe(begin_instant.saturating_elapsed_secs());
    };
    poll_future_notify(f);
}
