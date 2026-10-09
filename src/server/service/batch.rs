// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

// #[PerformanceCriticalPath]
use std::{collections::HashMap, sync::Arc};

use api_version::KvFormat;
use kvproto::kvrpcpb::*;
use protobuf::Message;
use resource_control::ResourceGroupManager;
use tikv_util::{
    future::poll_future_notify,
    mpsc::future::{Sender, WakePolicy},
    resource_control::DEFAULT_RESOURCE_GROUP_NAME,
    time::Instant,
};
use tracker::{GLOBAL_TRACKERS, RequestInfo, RequestType, Tracker, TrackerToken};
use txn_types::ValueEntry;

use crate::{
    server::{
        metrics::{GrpcTypeKind, REQUEST_BATCH_SIZE_HISTOGRAM_VEC, ResourcePriority},
        service::kv::{GrpcRequestDuration, MeasuredSingleResponse, batch_commands_response},
    },
    storage::{
        ResponseBatchConsumer, Result, Storage,
        errors::{extract_key_error, extract_region_error},
        kv::{Engine, Statistics},
        lock_manager::LockManager,
    },
};

pub const MAX_BATCH_GET_REQUEST_COUNT: usize = 10;
pub const MIN_BATCH_GET_REQUEST_COUNT: usize = 4;
pub const MAX_QUEUE_SIZE_PER_WORKER: usize = 16;

/// The unit a merged point-get batch is spawned, admitted and accounted as.
///
/// A `BatchCommandsRequest` interleaves the point gets of every session on a
/// TiDB, so one message mixes resource groups. Merged gets run as one read
/// pool task whose group, limiter, busy check and noisy verdict come from
/// its first request, so a merge must not cross groups. Background work
/// shares one limiter whatever its group, so it is one bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BatchKey {
    Background,
    /// Bounded to a configured group, as the batch's metric label is.
    Foreground(String),
}

impl BatchKey {
    pub fn of(resource_manager: &Option<Arc<ResourceGroupManager>>, ctx: &Context) -> Self {
        let Some(rm) = resource_manager.as_deref() else {
            return BatchKey::Foreground(DEFAULT_RESOURCE_GROUP_NAME.to_owned());
        };
        let group = ctx.get_resource_control_context().get_resource_group_name();
        if rm.is_background_request(group, ctx.get_request_source()) {
            BatchKey::Background
        } else {
            BatchKey::Foreground(rm.bounded_group_name(group).into_owned())
        }
    }
}

#[derive(Default)]
struct GetBatch {
    gets: Vec<GetRequest>,
    ids: Vec<u64>,
    trackers: Vec<TrackerToken>,
}

#[derive(Default)]
struct RawGetBatch {
    gets: Vec<RawGetRequest>,
    ids: Vec<u64>,
}

/// A message holds a handful of keys at most, so a scan beats hashing.
fn bucket<B: Default>(buckets: &mut Vec<(BatchKey, B)>, key: BatchKey) -> &mut B {
    let index = match buckets.iter().position(|(k, _)| *k == key) {
        Some(index) => index,
        None => {
            buckets.push((key, B::default()));
            buckets.len() - 1
        }
    };
    &mut buckets[index].1
}

pub struct ReqBatcher {
    gets: Vec<(BatchKey, GetBatch)>,
    raw_gets: Vec<(BatchKey, RawGetBatch)>,
    begin_instant: Instant,
    batch_size: usize,
}

impl ReqBatcher {
    pub fn new(batch_size: usize) -> ReqBatcher {
        let begin_instant = Instant::now();
        ReqBatcher {
            gets: vec![],
            raw_gets: vec![],
            begin_instant,
            batch_size: std::cmp::min(batch_size, MAX_BATCH_GET_REQUEST_COUNT),
        }
    }

    #[cfg(test)]
    fn pending_get_batches(&self) -> Vec<(BatchKey, usize)> {
        self.gets
            .iter()
            .map(|(key, batch)| (key.clone(), batch.gets.len()))
            .collect()
    }

    #[cfg(test)]
    fn pending_raw_get_batches(&self) -> Vec<(BatchKey, usize)> {
        self.raw_gets
            .iter()
            .map(|(key, batch)| (key.clone(), batch.gets.len()))
            .collect()
    }

    pub fn can_batch_get(&self, req: &GetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn can_batch_raw_get(&self, req: &RawGetRequest) -> bool {
        req.get_context().get_priority() == CommandPri::Normal
    }

    pub fn add_get_request(&mut self, key: BatchKey, req: GetRequest, id: u64) {
        let tracker = GLOBAL_TRACKERS.insert(Tracker::new(RequestInfo::new(
            req.get_context(),
            RequestType::KvBatchGetCommand,
            req.get_version(),
        )));
        GLOBAL_TRACKERS.with_tracker(tracker, |the_tracker| {
            the_tracker.metrics.grpc_req_size = req.compute_size() as u64;
        });
        let batch = bucket(&mut self.gets, key);
        batch.gets.push(req);
        batch.ids.push(id);
        batch.trackers.push(tracker);
    }

    pub fn add_raw_get_request(&mut self, key: BatchKey, req: RawGetRequest, id: u64) {
        let batch = bucket(&mut self.raw_gets, key);
        batch.gets.push(req);
        batch.ids.push(id);
    }

    pub fn maybe_commit<E: Engine, L: LockManager, F: KvFormat>(
        &mut self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
        resource_manager: &Option<Arc<ResourceGroupManager>>,
    ) {
        for (_, batch) in &mut self.gets {
            if batch.gets.len() >= self.batch_size {
                let GetBatch {
                    gets,
                    ids,
                    trackers,
                } = std::mem::take(batch);
                future_batch_get_command(
                    storage,
                    ids,
                    gets,
                    trackers,
                    tx.clone(),
                    self.begin_instant,
                    resource_manager,
                );
            }
        }

        for (_, batch) in &mut self.raw_gets {
            if batch.gets.len() >= self.batch_size {
                let RawGetBatch { gets, ids } = std::mem::take(batch);
                future_batch_raw_get_command(
                    storage,
                    ids,
                    gets,
                    tx.clone(),
                    self.begin_instant,
                    resource_manager,
                );
            }
        }
    }

    pub fn commit<E: Engine, L: LockManager, F: KvFormat>(
        self,
        storage: &Storage<E, L, F>,
        tx: &Sender<MeasuredSingleResponse>,
        resource_manager: &Option<Arc<ResourceGroupManager>>,
    ) {
        for (
            _,
            GetBatch {
                gets,
                ids,
                trackers,
            },
        ) in self.gets
        {
            if !gets.is_empty() {
                future_batch_get_command(
                    storage,
                    ids,
                    gets,
                    trackers,
                    tx.clone(),
                    self.begin_instant,
                    resource_manager,
                );
            }
        }
        for (_, RawGetBatch { gets, ids }) in self.raw_gets {
            if !gets.is_empty() {
                future_batch_raw_get_command(
                    storage,
                    ids,
                    gets,
                    tx.clone(),
                    self.begin_instant,
                    resource_manager,
                );
            }
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
    trackers: HashMap<u64, TrackerToken>,
}

impl ResponseBatchConsumer<(Option<ValueEntry>, Statistics)> for GetCommandResponseConsumer {
    fn consume(
        &self,
        id: u64,
        res: Result<(Option<ValueEntry>, Statistics)>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
        resource_group: String,
    ) {
        let mut resp = GetResponse::default();
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok((val, statistics)) => {
                    let exec_detail_v2 = resp.mut_exec_details_v2();
                    let tracker = self.trackers.get(&id).copied();
                    {
                        let scan_detail_v2 = exec_detail_v2.mut_scan_detail_v2();
                        statistics.write_scan_detail(scan_detail_v2);
                        if let Some(tracker) = tracker {
                            let _ = GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                                tracker.write_scan_detail(scan_detail_v2);
                            });
                        }
                    }
                    if let Some(tracker) = tracker {
                        let _ = GLOBAL_TRACKERS.with_tracker(tracker, |tracker| {
                            tracker.write_ru_v2(exec_detail_v2.mut_ru_v2());
                        });
                    }
                    match val {
                        Some(val) => {
                            resp.set_value(val.value);
                            if let Some(commit_ts) = val.commit_ts {
                                resp.set_commit_ts(commit_ts.into_inner());
                            }
                        }
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
        let measure = GrpcRequestDuration::new(
            begin,
            GrpcTypeKind::kv_batch_get_command,
            request_source,
            resource_priority,
            resource_group,
        );
        let task = MeasuredSingleResponse::new(id, res, measure, None);
        if self.tx.send_with(task, WakePolicy::Immediately).is_err() {
            warn!("KvService response batch commands fail");
        }
    }
}

impl ResponseBatchConsumer<Option<ValueEntry>> for GetCommandResponseConsumer {
    fn consume(
        &self,
        id: u64,
        res: Result<Option<ValueEntry>>,
        begin: Instant,
        request_source: String,
        resource_priority: ResourcePriority,
        resource_group: String,
    ) {
        let mut resp = RawGetResponse::default();
        if let Some(err) = extract_region_error(&res) {
            resp.set_region_error(err);
        } else {
            match res {
                Ok(Some(val)) => resp.set_value(val.value),
                Ok(None) => resp.set_not_found(true),
                Err(e) => resp.set_error(format!("{}", e)),
            }
        }
        let res = batch_commands_response::Response {
            cmd: Some(batch_commands_response::response::Cmd::RawGet(resp)),
            ..Default::default()
        };
        let measure = GrpcRequestDuration::new(
            begin,
            GrpcTypeKind::raw_batch_get_command,
            request_source,
            resource_priority,
            resource_group,
        );
        let task = MeasuredSingleResponse::new(id, res, measure, None);
        if self.tx.send_with(task, WakePolicy::Immediately).is_err() {
            warn!("KvService response batch commands fail");
        }
    }
}

fn future_batch_get_command<E: Engine, L: LockManager, F: KvFormat>(
    storage: &Storage<E, L, F>,
    requests: Vec<u64>,
    gets: Vec<GetRequest>,
    trackers: Vec<TrackerToken>,
    tx: Sender<MeasuredSingleResponse>,
    begin_instant: tikv_util::time::Instant,
    resource_manager: &Option<Arc<ResourceGroupManager>>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .kv_get
        .observe(gets.len() as f64);
    let id_sources: Vec<_> = requests
        .iter()
        .zip(gets.iter())
        .map(|(id, req)| (*id, req.get_context().get_request_source().to_string()))
        .collect();

    let group_priority = gets
        .first()
        .unwrap()
        .get_context()
        .get_resource_control_context()
        .get_override_priority();
    let resource_priority = ResourcePriority::from(group_priority);
    // The batcher merges one `BatchKey` at a time, so the first request's group
    // stands for the batch; bound it to a configured group.
    let resource_group = match resource_manager.as_deref() {
        Some(rm) => rm
            .bounded_group_name(
                gets.first()
                    .unwrap()
                    .get_context()
                    .get_resource_control_context()
                    .get_resource_group_name(),
            )
            .into_owned(),
        None => DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
    };

    let trackers_by_id: HashMap<u64, TrackerToken> = requests
        .iter()
        .copied()
        .zip(trackers.iter().copied())
        .collect();
    let res = storage.batch_get_command(
        gets,
        requests,
        trackers.clone(),
        GetCommandResponseConsumer {
            tx: tx.clone(),
            trackers: trackers_by_id,
        },
        begin_instant,
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        for tracker in trackers {
            GLOBAL_TRACKERS.remove(tracker);
        }
        if let Some(e) = extract_region_error(&res) {
            let mut resp = GetResponse::default();
            resp.set_region_error(e);
            for (id, source) in id_sources {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::Get(resp.clone())),
                    ..Default::default()
                };
                let measure = GrpcRequestDuration::new(
                    begin_instant,
                    GrpcTypeKind::kv_batch_get_command,
                    source,
                    resource_priority,
                    resource_group.clone(),
                );
                let task = MeasuredSingleResponse::new(id, res, measure, None);
                if tx.send_with(task, WakePolicy::Immediately).is_err() {
                    warn!("KvService response batch commands fail");
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
    resource_manager: &Option<Arc<ResourceGroupManager>>,
) {
    REQUEST_BATCH_SIZE_HISTOGRAM_VEC
        .raw_get
        .observe(gets.len() as f64);
    let id_sources: Vec<_> = requests
        .iter()
        .zip(gets.iter())
        .map(|(id, req)| (*id, req.get_context().get_request_source().to_string()))
        .collect();

    let group_priority = gets
        .first()
        .unwrap()
        .get_context()
        .get_resource_control_context()
        .get_override_priority();
    let resource_priority = ResourcePriority::from(group_priority);
    // The batcher merges one `BatchKey` at a time, so the first request's group
    // stands for the batch; bound it to a configured group.
    let resource_group = match resource_manager.as_deref() {
        Some(rm) => rm
            .bounded_group_name(
                gets.first()
                    .unwrap()
                    .get_context()
                    .get_resource_control_context()
                    .get_resource_group_name(),
            )
            .into_owned(),
        None => DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
    };

    let res = storage.raw_batch_get_command(
        gets,
        requests,
        GetCommandResponseConsumer {
            tx: tx.clone(),
            trackers: HashMap::default(),
        },
    );
    let f = async move {
        // This error can only cause by readpool busy.
        let res = res.await;
        if let Some(e) = extract_region_error(&res) {
            let mut resp = RawGetResponse::default();
            resp.set_region_error(e);
            for (id, source) in id_sources {
                let res = batch_commands_response::Response {
                    cmd: Some(batch_commands_response::response::Cmd::RawGet(resp.clone())),
                    ..Default::default()
                };
                let measure = GrpcRequestDuration::new(
                    begin_instant,
                    GrpcTypeKind::raw_batch_get_command,
                    source,
                    resource_priority,
                    resource_group.clone(),
                );
                let task = MeasuredSingleResponse::new(id, res, measure, None);
                if tx.send_with(task, WakePolicy::Immediately).is_err() {
                    warn!("KvService response batch commands fail");
                }
            }
        }
    };
    poll_future_notify(f);
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tikv_util::mpsc::future::{WakePolicy, unbounded};
    use txn_types::{TimeStamp, ValueEntry};

    use super::*;
    use crate::storage::kv::Statistics;

    fn resource_group_pb(
        name: &str,
        job_types: Vec<String>,
    ) -> kvproto::resource_manager::ResourceGroup {
        use kvproto::resource_manager::{GroupMode, GroupRequestUnitSettings, ResourceGroup};
        let mut group = ResourceGroup::new();
        group.set_name(name.to_owned());
        group.set_mode(GroupMode::RuMode);
        group.set_priority(8);
        let mut ru_setting = GroupRequestUnitSettings::new();
        ru_setting.mut_r_u().mut_settings().set_fill_rate(1000);
        group.set_r_u_settings(ru_setting);
        if !job_types.is_empty() {
            group
                .mut_background_settings()
                .set_job_types(job_types.into());
        }
        group
    }

    fn context(group: &str, source: &str) -> Context {
        let mut ctx = Context::default();
        ctx.mut_resource_control_context()
            .set_resource_group_name(group.to_owned());
        ctx.set_request_source(source.to_owned());
        ctx
    }

    // One message carries every session's point gets, so groups mix; a merged
    // task takes its group from its first request, so the key must separate
    // them. Background shares one limiter across groups, so it is one key.
    #[test]
    fn test_batch_key_separates_groups_and_merges_background() {
        let rm = Arc::new(ResourceGroupManager::default());
        rm.add_resource_group(resource_group_pb("a", vec![]));
        rm.add_resource_group(resource_group_pb("b", vec![]));
        rm.add_resource_group(resource_group_pb("bg", vec!["ddl".into()]));
        let rm = Some(rm);

        assert_eq!(
            BatchKey::of(&rm, &context("a", "query")),
            BatchKey::Foreground("a".to_owned())
        );
        assert_eq!(
            BatchKey::of(&rm, &context("b", "query")),
            BatchKey::Foreground("b".to_owned())
        );
        // Background is decided by group and source together.
        assert_eq!(
            BatchKey::of(&rm, &context("bg", "ddl")),
            BatchKey::Background
        );
        assert_eq!(
            BatchKey::of(&rm, &context("bg", "query")),
            BatchKey::Foreground("bg".to_owned())
        );
        // Unknown groups are bounded to default, as the metric label is.
        assert_eq!(
            BatchKey::of(&rm, &context("nobody", "query")),
            BatchKey::Foreground(DEFAULT_RESOURCE_GROUP_NAME.to_owned())
        );
        // Without resource control everything is one key.
        assert_eq!(
            BatchKey::of(&None, &context("a", "query")),
            BatchKey::Foreground(DEFAULT_RESOURCE_GROUP_NAME.to_owned())
        );
    }

    #[test]
    fn test_req_batcher_buckets_by_key() {
        let mut batcher = ReqBatcher::new(MAX_BATCH_GET_REQUEST_COUNT);
        let a = || BatchKey::Foreground("a".to_owned());
        let b = || BatchKey::Foreground("b".to_owned());

        for (n, key) in [a(), b(), a(), BatchKey::Background, BatchKey::Background]
            .into_iter()
            .enumerate()
        {
            let mut req = GetRequest::default();
            req.set_context(context("x", "query"));
            batcher.add_get_request(key.clone(), req, n as u64);
            batcher.add_raw_get_request(key, RawGetRequest::default(), n as u64);
        }

        let expected = vec![(a(), 2), (b(), 1), (BatchKey::Background, 2)];
        assert_eq!(batcher.pending_get_batches(), expected);
        assert_eq!(batcher.pending_raw_get_batches(), expected);
    }

    #[test]
    fn test_get_command_response_consumer_sets_commit_ts() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = GetCommandResponseConsumer {
            tx,
            trackers: HashMap::default(),
        };

        consumer.consume(
            7,
            Ok((
                Some(ValueEntry::new(b"v".to_vec(), Some(TimeStamp::new(42)))),
                Statistics::default(),
            )),
            Instant::now(),
            "".to_string(),
            ResourcePriority::unknown,
            DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert_eq!(task.id, 7);

        let resp = task.resp.consume();
        let get = resp.get_get();
        assert_eq!(get.get_value(), b"v");
        assert_eq!(get.get_commit_ts(), 42);
    }

    #[test]
    fn test_get_command_response_consumer_commit_ts_default_zero() {
        let (tx, mut rx) = unbounded(WakePolicy::Immediately);
        let consumer = GetCommandResponseConsumer {
            tx,
            trackers: HashMap::default(),
        };

        consumer.consume(
            8,
            Ok((
                Some(ValueEntry::from_value(b"v".to_vec())),
                Statistics::default(),
            )),
            Instant::now(),
            "".to_string(),
            ResourcePriority::unknown,
            DEFAULT_RESOURCE_GROUP_NAME.to_owned(),
        );

        let mut task = rx.recv_timeout(Duration::from_secs(1)).unwrap();
        let resp = task.resp.consume();
        let get = resp.get_get();
        assert_eq!(get.get_value(), b"v");
        assert_eq!(get.get_commit_ts(), 0);
    }
}
