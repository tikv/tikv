// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use crate::server::metrics::{GrpcTypeKind, REQUEST_BATCH_SIZE_HISTOGRAM_VEC};
use crate::server::service::kv::{
    batch_commands_response, GrpcRequestDuration, MeasuredSingleResponse,
};
use crate::storage::kv::{PerfStatisticsDelta, Statistics};
use crate::storage::{
    errors::{extract_key_error, extract_region_error},
    kv::Engine,
    lock_manager::LockManager,
    Storage,
};
use crate::storage::{ResponseBatchConsumer, Result};
use api_version::KvFormat;
use kvproto::kvrpcpb::*;
use tikv_util::future::poll_future_notify;
use tikv_util::mpsc::batch::Sender;
use tikv_util::time::Instant;

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 10;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;
pub const MAX_QUEUE_SIZE_PER_WORKER: usize = 16;

pub struct ReqBatcher {
    gets: Vec<GetRequest>,
    raw_gets: Vec<RawGetRequest>,
    get_ids: Vec<u64>,
    raw_get_ids: Vec<u64>,
    begin_instant: Instant,
    batch_size: usize,
}

impl ReqBatcher {
    pub fn new(batch_size: usize) -> ReqBatcher {
        let begin_instant = Instant::now_coarse();
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            get_ids: vec![],
            raw_get_ids: vec![],
            begin_instant,
            batch_size: std::cmp::min(batch_size, MAX_BATCH_GET_REQUEST_COUNT),
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

    pub fn maybe_commit<E: Engine, L: LockManager, F: KvFormat>(
        &mut self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
    ) {
        if self.gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.gets);
            let ids = std::mem::take(&mut self.get_ids);
            future_batch_get_command(storage, ids, gets, tx.clone(), self.begin_instant);
        }

        if self.raw_gets.len() >= self.batch_size {
            let gets = std::mem::take(&mut self.raw_gets);
            let ids = std::mem::take(&mut self.raw_get_ids);
            future_batch_raw_get_command(storage, ids, gets, tx.clone(), self.begin_instant);
        }
    }

    pub fn commit<E: Engine, L: LockManager, F: KvFormat>(
        self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
    ) {
        if !self.gets.is_empty() {
            future_batch_get_command(
                storage,
                self.get_ids,
                self.gets,
                tx.clone(),
                self.begin_instant,
            );
        }
        if !self.raw_gets.is_empty() {
            future_batch_raw_get_command(
                storage,
                self.raw_get_ids,
                self.raw_gets,
                tx.clone(),
                self.begin_instant,
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
    tx: Sender<MeasuredSingleResponse>,
}

impl ResponseBatchConsumer<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>
    for GetCommandResponseConsumer
{
    fn consume(
        &self,
        id: u64,
        res: Result<(Option<Vec<u8>>, Statistics, PerfStatisticsDelta)>,
        begin: Instant,
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
        let mesure = GrpcRequestDuration::new(begin, GrpcTypeKind::kv_batch_get_command);
        let task = MeasuredSingleResponse::new(id, res, mesure);
        if self.tx.send_and_notify(task).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

impl ResponseBatchConsumer<Option<Vec<u8>>> for GetCommandResponseConsumer {
    fn consume(&self, id: u64, res: Result<Option<Vec<u8>>>, begin: Instant) {
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
        let mesure = GrpcRequestDuration::new(begin, GrpcTypeKind::raw_batch_get_command);
        let task = MeasuredSingleResponse::new(id, res, mesure);
        if self.tx.send_and_notify(task).is_err() {
            error!("KvService response batch commands fail");
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(gets.len() as f64);
    let ids = requests.clone();
    let res = storage.batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer { tx: tx.clone() },
        begin_instant,
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
                let measure =
                    GrpcRequestDuration::new(begin_instant, GrpcTypeKind::kv_batch_get_command);
                let task = MeasuredSingleResponse::new(id, res, measure);
                if tx.send_and_notify(task).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

fn future_batch_raw_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    gets: Vec<RawGetRequest>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .raw_get
        .observe(gets.len() as f64);
    let ids = requests.clone();
    let res = storage.raw_batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer { tx: tx.clone() },
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
                let measure =
                    GrpcRequestDuration::new(begin_instant, GrpcTypeKind::raw_batch_get_command);
                let task = MeasuredSingleResponse::new(id, res, measure);
                if tx.send_and_notify(task).is_err() {
                    error!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}
