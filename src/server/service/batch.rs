// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::new_request_tracer;
use crate::server::metrics::REQUEST_BATCH_SIZE_HISTOGRAM_VEC;
use crate::server::service::kv::{batch_commands_response, TracedSingleResponse};
use crate::server::service::tracing::{RequestTracer, TracerFactory};
use crate::storage::kv::{PerfStatisticsDelta, Statistics};
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use crate::storage::{ResponseBatchConsumer, Result};
use kvproto::kvrpcpb::*;
use minitrace::prelude::*;
use tikv_util::future::poll_future_notify;
use tikv_util::mpsc::batch::Sender;

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 10;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;
pub const MAX_QUEUE_SIZE_PER_WORKER: usize = 16;

pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ids: Vec<u64>,
    raw_get_ids: Vec<u64>,
    get_tracers: Vec<RequestTracer>,
    raw_get_tracers: Vec<RequestTracer>,
    batch_size: usize,
}

impl ReqBatcher {
    pub fn new(batch_size: usize) -> ReqBatcher {
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ids: vec![],
            raw_get_ids: vec![],
            get_tracers: vec![],
            raw_get_tracers: vec![],
            batch_size: std::cmp::min(batch_size, MAX_BATCH_GET_REQUEST_COUNT),
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
        id: u64,
        tracer_factory: &TracerFactory,
    ) {
        let tracer = new_request_tracer!(kv_get, &mut req, tracer_factory);

        self.gets.push(req);
        self.get_ids.push(id);
        self.get_tracers.push(tracer);
    }

    pub fn add_raw_get_request(
        &mut self,
        mut req: RawGetRequest,
        id: u64,
        tracer_factory: &TracerFactory,
    ) {
        let tracer = new_request_tracer!(raw_get, &mut req, tracer_factory);

        self.raw_gets.push(req);
        self.raw_get_ids.push(id);
        self.raw_get_tracers.push(tracer);
    }

    pub fn maybe_commit<E: Engine, L: LockManager>(
        &mut self,
        storage: &Storage<E, L>,
        tx: &Sender<TracedSingleResponse>,
    ) {
        if self.gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.gets);
            let ids = std::mem::take(&mut self.get_ids);
            let tracers = std::mem::take(&mut self.get_tracers);

            let span = Span::enter_with_parents(
                "BatchGetCommand",
                tracers.iter().map(|tracer| &tracer.root_span),
            );
            let _g = span.set_local_parent();

            future_batch_get_command(storage, ids, gets, tracers, tx.clone());
        }

        if self.raw_gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            let tracers = std::mem::take(&mut self.raw_get_tracers);

            let span = Span::enter_with_parents(
                "BatchRawGetCommand",
                tracers.iter().map(|tracer| &tracer.root_span),
            );
            let _g = span.set_local_parent();

            future_batch_raw_get_command(storage, ids, gets, tracers, tx.clone());
        }
    }

    pub fn commit<E: Engine, L: LockManager>(
        self,
        storage: &Storage<E, L>,
        tx: &Sender<TracedSingleResponse>,
    ) {
        if !self.gets.is_empty() {
            future_batch_get_command(
                storage,
                self.get_ids,
                self.gets,
                self.get_tracers,
                tx.clone(),
            );
        }
        if !self.raw_gets.is_empty() {
            future_batch_raw_get_command(
                storage,
                self.raw_get_ids,
                self.raw_gets,
                self.raw_get_tracers,
                tx.clone(),
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
    pub fn build(&self, queue_per_worker: usize, req_batch_size: usize) -> Option<ReqBatcher> {
        if !self.enable_batch {
            return None;
        }
        if req_batch_size > self.pool_size * MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MIN_BATCH_GET_REQUEST_COUNT
        {
            return Some(ReqBatcher::new(req_batch_size / self.pool_size));
        }
        if req_batch_size >= MIN_BATCH_GET_REQUEST_COUNT
            && queue_per_worker >= MAX_QUEUE_SIZE_PER_WORKER
        {
            return Some(ReqBatcher::new(req_batch_size));
        }
        None
    }
}

pub struct GetCommandResponseConsumer {
    tx: Sender<TracedSingleResponse>,
}

impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>
    for GetCommandResponseConsumer
{
    fn consume(
        &self,
        id: u64,
        res: Result<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
        tracer: RequestTracer,
    ) {
        let mut resp = GetResponse::default();
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
        let task = TracedSingleResponse::new(id, res, tracer);
        if self.tx.send_and_notify(task).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

impl ResponseBatchConsumer<Option<Vec<u8>>> for GetCommandResponseConsumer {
    fn consume(&self, id: u64, res: Result<Option<Vec<u8>>>, tracer: RequestTracer) {
        let mut resp = RawGetResponse::default();
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
        let task = TracedSingleResponse::new(id, res, tracer);
        if self.tx.send_and_notify(task).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    ids: Vec<u64>,
    gets: Vec<GetRequest>,
    tracers: Vec<RequestTracer>,
    tx: Sender<TracedSingleResponse>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(gets.len() as f64);
    let res = storage.batch_get_command(
        gets,
        ids.clone(),
        tracers.clone(),
        GetCommandResponseConsumer { tx: tx.clone() },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = GetResponse::default();
            resp.set_region_error(e);
            for (id, tracer) in ids.into_iter().zip(tracers) {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp.clone())),
                    ..Default::default()
                };
                let task = TracedSingleResponse::new(id, res, tracer);
                if tx.send_and_notify(task).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager>(
    storage: &Storage<E, L>,
    ids: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tracers: Vec<RequestTracer>,
    tx: Sender<TracedSingleResponse>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .raw_get
        .observe(gets.len() as f64);
    let res = storage.raw_batch_get_command(
        gets,
        ids.clone(),
        tracers.clone(),
        GetCommandResponseConsumer { tx: tx.clone() },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = RawGetResponse::default();
            resp.set_region_error(e);
            for (id, tracer) in ids.into_iter().zip(tracers) {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::RawGet(resp.clone())),
                    ..Default::default()
                };
                let task = TracedSingleResponse::new(id, res, tracer);
                if tx.send_and_notify(task).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}
