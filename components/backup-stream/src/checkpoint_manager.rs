// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{cell::RefCell, collections::HashMap, sync::Arc, time::Duration};

use futures::{
    channel::mpsc::{self as async_mpsc, Receiver, Sender},
    future::BoxFuture,
    FutureExt, SinkExt, StreamExt,
};
use grpcio::{RpcStatus, RpcStatusCode, WriteFlags};
use kvproto::{
    errorpb::{Error as PbError, *},
    logbackuppb::{FlushEvent, SubscribeFlushEventResponse},
    metapb::Region,
};
use pd_client::PdClient;
use tikv_util::{box_err, defer, info, time::Instant, warn, worker::Scheduler};
use tracing::instrument;
use txn_types::TimeStamp;
use uuid::Uuid;

use crate::{
    annotate,
    errors::{Error, Result},
    future,
    metadata::{store::MetaStore, Checkpoint, CheckpointProvider, MetadataClient},
    metrics,
    subscription_track::ResolveResult,
    try_send, RegionCheckpointOperation, Task,
};

/// A manager for maintaining the last flush ts.
/// This information is provided for the `advancer` in checkpoint V3,
/// which involved a central node (typically TiDB) for collecting all regions'
/// checkpoint then advancing the global checkpoint.
#[derive(Default)]
pub struct CheckpointManager {
    checkpoint_ts: HashMap<u64, LastFlushTsOfRegion>,
    resolved_ts: HashMap<u64, LastFlushTsOfRegion>,
    manager_handle: Option<Sender<SubscriptionOp>>,
}

impl std::fmt::Debug for CheckpointManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointManager")
            .field("checkpoints", &self.checkpoint_ts)
            .field("resolved-ts", &self.resolved_ts)
            .finish()
    }
}

enum SubscriptionOp {
    Add(Subscription),
    Emit(Box<[FlushEvent]>),
    #[cfg(test)]
    Inspect(Box<dyn FnOnce(&SubscriptionManager) + Send>),
}

pub struct SubscriptionManager {
    subscribers: HashMap<Uuid, Subscription>,
    input: Receiver<SubscriptionOp>,
}

impl SubscriptionManager {
    pub async fn main_loop(mut self) {
        info!("subscription manager started!");
        defer! { info!("subscription manager exit.") }
        while let Some(msg) = self.input.next().await {
            match msg {
                SubscriptionOp::Add(sub) => {
                    let uid = Uuid::new_v4();
                    info!("log backup adding new subscriber"; "id" => %uid);
                    self.subscribers.insert(uid, sub);
                }
                SubscriptionOp::Emit(events) => {
                    self.emit_events(events).await;
                }
                #[cfg(test)]
                SubscriptionOp::Inspect(f) => {
                    f(&self);
                }
            }
        }
        // NOTE: Maybe close all subscription streams here.
    }

    #[instrument(skip_all, fields(length = events.len()))]
    async fn emit_events(&mut self, events: Box<[FlushEvent]>) {
        let mut canceled = vec![];
        info!("log backup sending events"; "event_len" => %events.len(), "downstream" => %self.subscribers.len());
        for (id, sub) in &mut self.subscribers {
            let send_all = async {
                for es in events.chunks(1024) {
                    let mut resp = SubscribeFlushEventResponse::new();
                    resp.set_events(es.to_vec().into());
                    sub.feed((resp, WriteFlags::default())).await?;
                }
                sub.flush().await
            };

            if let Err(err) = send_all.await {
                canceled.push(*id);
                Error::from(err).report("sending subscription");
            }
        }

        for c in canceled {
            self.remove_subscription(&c).await;
        }
    }

    #[instrument(skip(self))]
    async fn remove_subscription(&mut self, id: &Uuid) {
        match self.subscribers.remove(id) {
            Some(sub) => {
                info!("client is gone, removing subscription"; "id" => %id);
                // The stream is an endless stream -- we don't need to close it.
                drop(sub);
            }
            None => {
                warn!("BUG: the subscriber has been removed before we are going to remove it."; "id" => %id);
            }
        }
    }
}

// Note: can we make it more generic...?
#[cfg(not(test))]
pub type Subscription =
    grpcio::ServerStreamingSink<kvproto::logbackuppb::SubscribeFlushEventResponse>;

#[cfg(test)]
pub type Subscription = tests::MockSink;

/// The result of getting a checkpoint.
/// The possibility of failed to getting checkpoint is pretty high:
/// because there is a gap between region leader change and flushing.
#[derive(Debug)]
pub enum GetCheckpointResult {
    Ok {
        region: Region,
        checkpoint: TimeStamp,
    },
    NotFound {
        id: RegionIdWithVersion,
        err: PbError,
    },
    EpochNotMatch {
        region: Region,
        err: PbError,
    },
}

impl GetCheckpointResult {
    /// create an "ok" variant with region.
    pub fn ok(region: Region, checkpoint: TimeStamp) -> Self {
        Self::Ok { region, checkpoint }
    }

    fn not_found(id: RegionIdWithVersion) -> Self {
        Self::NotFound {
            id,
            err: not_leader(id.region_id),
        }
    }

    /// create a epoch not match variant with region
    fn epoch_not_match(provided: RegionIdWithVersion, real: &Region) -> Self {
        Self::EpochNotMatch {
            region: real.clone(),
            err: epoch_not_match(
                provided.region_id,
                provided.region_epoch_version,
                real.get_region_epoch().get_version(),
            ),
        }
    }
}

impl CheckpointManager {
    pub fn spawn_subscription_mgr(&mut self) -> future![()] {
        let (tx, rx) = async_mpsc::channel(1024);
        let sub = SubscriptionManager {
            subscribers: Default::default(),
            input: rx,
        };
        self.manager_handle = Some(tx);
        sub.main_loop()
    }

    pub fn resolve_regions(&mut self, region_and_checkpoint: Vec<ResolveResult>) {
        for res in region_and_checkpoint {
            self.do_update(res.region, res.checkpoint);
        }
    }

    pub fn flush(&mut self) {
        info!("log backup checkpoint manager flushing."; "resolved_ts_len" => %self.resolved_ts.len(), "resolved_ts" => ?self.get_resolved_ts());
        self.checkpoint_ts = std::mem::take(&mut self.resolved_ts);
        // Clippy doesn't know this iterator borrows `self.checkpoint_ts` :(
        #[allow(clippy::needless_collect)]
        let items = self
            .checkpoint_ts
            .values()
            .cloned()
            .map(|x| (x.region, x.checkpoint))
            .collect::<Vec<_>>();
        self.notify(items.into_iter());
    }

    /// update a region checkpoint in need.
    #[cfg(test)]
    fn update_region_checkpoint(&mut self, region: &Region, checkpoint: TimeStamp) {
        Self::update_ts(&mut self.checkpoint_ts, region.clone(), checkpoint)
    }

    fn update_ts(
        container: &mut HashMap<u64, LastFlushTsOfRegion>,
        region: Region,
        checkpoint: TimeStamp,
    ) {
        let e = container.entry(region.get_id());
        let ver = region.get_region_epoch().get_version();
        // A hacky way to allow the two closures move out the region.
        // It is safe given the two closures would only be called once.
        let r = RefCell::new(Some(region));
        e.and_modify(|old_cp| {
            let old_ver = old_cp.region.get_region_epoch().get_version();
            let checkpoint_is_newer = old_cp.checkpoint < checkpoint;
            if old_ver < ver || (old_ver == ver && checkpoint_is_newer) {
                *old_cp = LastFlushTsOfRegion {
                    checkpoint,
                    region: r.borrow_mut().take().expect(
                        "unreachable: `and_modify` and `or_insert_with` called at the same time.",
                    ),
                };
            }
        })
        .or_insert_with(|| LastFlushTsOfRegion {
            checkpoint,
            region: r
                .borrow_mut()
                .take()
                .expect("unreachable: `and_modify` and `or_insert_with` called at the same time."),
        });
    }

    pub fn add_subscriber(&mut self, sub: Subscription) -> BoxFuture<'static, Result<()>> {
        let mgr = self.manager_handle.as_ref().cloned();
        let initial_data = self
            .checkpoint_ts
            .values()
            .map(|v| FlushEvent {
                start_key: v.region.start_key.clone(),
                end_key: v.region.end_key.clone(),
                checkpoint: v.checkpoint.into_inner(),
                ..Default::default()
            })
            .collect::<Box<[_]>>();

        // NOTE: we cannot send the real error into the client directly because once
        // we send the subscription into the sink, we cannot fetch it again :(
        async move {
            let mgr = mgr.ok_or(Error::Other(box_err!("subscription manager not get ready")));
            let mut mgr = match mgr {
                Ok(mgr) => mgr,
                Err(err) => {
                    sub.fail(RpcStatus::with_message(
                        RpcStatusCode::UNAVAILABLE,
                        "subscription manager not get ready.".to_owned(),
                    ))
                    .await
                    .map_err(|err| {
                        annotate!(err, "failed to send request to subscriber manager")
                    })?;
                    return Err(err);
                }
            };
            mgr.send(SubscriptionOp::Add(sub))
                .await
                .map_err(|err| annotate!(err, "failed to send request to subscriber manager"))?;
            mgr.send(SubscriptionOp::Emit(initial_data))
                .await
                .map_err(|err| {
                    annotate!(err, "failed to send initial data to subscriber manager")
                })?;
            Ok(())
        }
        .boxed()
    }

    fn notify(&mut self, items: impl Iterator<Item = (Region, TimeStamp)>) {
        if let Some(mgr) = self.manager_handle.as_mut() {
            let r = items
                .map(|(r, ts)| {
                    let mut f = FlushEvent::new();
                    f.set_checkpoint(ts.into_inner());
                    f.set_start_key(r.start_key);
                    f.set_end_key(r.end_key);
                    f
                })
                .collect::<Box<[_]>>();
            let event_size = r.len();
            let res = mgr.try_send(SubscriptionOp::Emit(r));
            // Note: perhaps don't batch in the channel but batch in the receiver side?
            // If so, we can control the memory usage better.
            if let Err(err) = res {
                warn!("the channel is full, dropping some events."; "length" => %event_size, "err" => %err);
            }
        }
    }

    fn do_update(&mut self, region: Region, checkpoint: TimeStamp) {
        Self::update_ts(&mut self.resolved_ts, region, checkpoint)
    }

    /// get checkpoint from a region.
    pub fn get_from_region(&self, region: RegionIdWithVersion) -> GetCheckpointResult {
        let checkpoint = self.checkpoint_ts.get(&region.region_id);
        if checkpoint.is_none() {
            return GetCheckpointResult::not_found(region);
        }
        let checkpoint = checkpoint.unwrap();
        if checkpoint.region.get_region_epoch().get_version() != region.region_epoch_version {
            return GetCheckpointResult::epoch_not_match(region, &checkpoint.region);
        }
        GetCheckpointResult::ok(checkpoint.region.clone(), checkpoint.checkpoint)
    }

    /// get all checkpoints stored.
    pub fn get_all(&self) -> Vec<LastFlushTsOfRegion> {
        self.checkpoint_ts.values().cloned().collect()
    }

    pub fn get_resolved_ts(&self) -> Option<TimeStamp> {
        self.resolved_ts.values().map(|x| x.checkpoint).min()
    }

    #[cfg(test)]
    fn sync_with_subs_mgr<T: Send + 'static>(
        &mut self,
        f: impl FnOnce(&SubscriptionManager) -> T + Send + 'static,
    ) -> T {
        use std::sync::Mutex;

        let (tx, rx) = std::sync::mpsc::sync_channel(1);
        let t = Arc::new(Mutex::new(None));
        let tr = Arc::clone(&t);
        self.manager_handle
            .as_mut()
            .unwrap()
            .try_send(SubscriptionOp::Inspect(Box::new(move |x| {
                *tr.lock().unwrap() = Some(f(x));
                tx.send(()).unwrap();
            })))
            .unwrap();
        rx.recv().unwrap();
        let mut t = t.lock().unwrap();
        t.take().unwrap()
    }
}

fn not_leader(r: u64) -> PbError {
    let mut err = PbError::new();
    let mut nl = NotLeader::new();
    nl.set_region_id(r);
    err.set_not_leader(nl);
    err.set_message(
        format!("the region {} isn't in the region_manager of log backup, maybe not leader or not flushed yet.", r));
    err
}

fn epoch_not_match(id: u64, sent: u64, real: u64) -> PbError {
    let mut err = PbError::new();
    let en = EpochNotMatch::new();
    err.set_epoch_not_match(en);
    err.set_message(format!(
        "the region {} has recorded version {}, but you sent {}",
        id, real, sent,
    ));
    err
}

#[derive(Debug, PartialEq, Hash, Clone, Copy)]
/// A simple region id, but versioned.
pub struct RegionIdWithVersion {
    pub region_id: u64,
    pub region_epoch_version: u64,
}

impl RegionIdWithVersion {
    pub fn new(id: u64, version: u64) -> Self {
        Self {
            region_id: id,
            region_epoch_version: version,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LastFlushTsOfRegion {
    pub region: Region,
    pub checkpoint: TimeStamp,
}

// Allow some type to
#[async_trait::async_trait]
pub trait FlushObserver: Send + 'static {
    /// The callback when the flush has advanced the resolver.
    async fn before(&mut self, checkpoints: Vec<ResolveResult>);
    /// The callback when the flush is done. (Files are fully written to
    /// external storage.)
    async fn after(&mut self, task: &str, rts: u64) -> Result<()>;
    /// The optional callback to rewrite the resolved ts of this flush.
    /// Because the default method (collect all leader resolved ts in the store,
    /// and use the minimal TS.) may lead to resolved ts rolling back, if we
    /// desire a stronger consistency, we can rewrite a safer resolved ts here.
    /// Note the new resolved ts cannot be greater than the old resolved ts.
    async fn rewrite_resolved_ts(
        &mut self,
        #[allow(unused_variables)] _task: &str,
    ) -> Option<TimeStamp> {
        None
    }
}

pub struct BasicFlushObserver<PD> {
    pd_cli: Arc<PD>,
    store_id: u64,
}

impl<PD> BasicFlushObserver<PD> {
    pub fn new(pd_cli: Arc<PD>, store_id: u64) -> Self {
        Self { pd_cli, store_id }
    }
}

#[async_trait::async_trait]
impl<PD: PdClient + 'static> FlushObserver for BasicFlushObserver<PD> {
    async fn before(&mut self, _checkpoints: Vec<ResolveResult>) {}

    async fn after(&mut self, task: &str, rts: u64) -> Result<()> {
        if let Err(err) = self
            .pd_cli
            .update_service_safe_point(
                format!("backup-stream-{}-{}", task, self.store_id),
                TimeStamp::new(rts.saturating_sub(1)),
                // Add a service safe point for 2 hours.
                // We make it the same duration as we meet fatal errors because TiKV may be
                // SIGKILL'ed after it meets fatal error and before it successfully updated the
                // fatal error safepoint.
                // TODO: We'd better make the coordinator, who really
                // calculates the checkpoint to register service safepoint.
                Duration::from_secs(60 * 60 * 2),
            )
            .await
        {
            Error::from(err).report("failed to update service safe point!");
            // don't give up?
        }

        // Currently, we only support one task at the same time,
        // so use the task as label would be ok.
        metrics::STORE_CHECKPOINT_TS
            .with_label_values(&[task])
            .set(rts as _);
        Ok(())
    }
}

pub struct CheckpointV3FlushObserver<S, O> {
    /// We should modify the rts (the local rts isn't right.)
    /// This should be a BasicFlushObserver or something likewise.
    baseline: O,
    sched: Scheduler<Task>,
    meta_cli: MetadataClient<S>,

    checkpoints: Vec<ResolveResult>,
    global_checkpoint_cache: HashMap<String, Checkpoint>,
    start_time: Instant,
}

impl<S, O> CheckpointV3FlushObserver<S, O> {
    pub fn new(sched: Scheduler<Task>, meta_cli: MetadataClient<S>, baseline: O) -> Self {
        Self {
            sched,
            meta_cli,
            checkpoints: vec![],
            // We almost always have only one entry.
            global_checkpoint_cache: HashMap::with_capacity(1),
            baseline,
            start_time: Instant::now(),
        }
    }
}

impl<S, O> CheckpointV3FlushObserver<S, O>
where
    S: MetaStore + 'static,
    O: FlushObserver + Send,
{
    async fn get_checkpoint(&mut self, task: &str) -> Result<Checkpoint> {
        let cp = match self.global_checkpoint_cache.get(task) {
            Some(cp) => *cp,
            None => {
                let global_checkpoint = self.meta_cli.global_checkpoint_of_task(task).await?;
                self.global_checkpoint_cache
                    .insert(task.to_owned(), global_checkpoint);
                global_checkpoint
            }
        };
        Ok(cp)
    }
}

#[async_trait::async_trait]
impl<S, O> FlushObserver for CheckpointV3FlushObserver<S, O>
where
    S: MetaStore + 'static,
    O: FlushObserver + Send,
{
    async fn before(&mut self, checkpoints: Vec<ResolveResult>) {
        self.checkpoints = checkpoints;
    }

    async fn after(&mut self, task: &str, _rts: u64) -> Result<()> {
        let resolve_task = Task::RegionCheckpointsOp(RegionCheckpointOperation::Resolved {
            checkpoints: std::mem::take(&mut self.checkpoints),
            start_time: self.start_time,
        });
        let flush_task = Task::RegionCheckpointsOp(RegionCheckpointOperation::Flush);
        try_send!(self.sched, resolve_task);
        try_send!(self.sched, flush_task);

        let global_checkpoint = self.get_checkpoint(task).await?;
        info!("getting global checkpoint from cache for updating."; "checkpoint" => ?global_checkpoint);
        self.baseline
            .after(task, global_checkpoint.ts.into_inner())
            .await?;
        Ok(())
    }

    async fn rewrite_resolved_ts(&mut self, task: &str) -> Option<TimeStamp> {
        let global_checkpoint = self
            .get_checkpoint(task)
            .await
            .map_err(|err| err.report("failed to get resolved ts for rewriting"))
            .ok()?;
        info!("getting global checkpoint for updating."; "checkpoint" => ?global_checkpoint);
        matches!(global_checkpoint.provider, CheckpointProvider::Global)
            .then(|| global_checkpoint.ts)
    }
}

#[cfg(test)]
pub mod tests {
    use std::{
        assert_matches,
        collections::HashMap,
        sync::{Arc, Mutex, RwLock},
        time::Duration,
    };

    use futures::{future::ok, Sink};
    use grpcio::{RpcStatus, RpcStatusCode};
    use kvproto::{logbackuppb::SubscribeFlushEventResponse, metapb::*};
    use pd_client::{PdClient, PdFuture};
    use txn_types::TimeStamp;

    use super::{BasicFlushObserver, FlushObserver, RegionIdWithVersion};
    use crate::{
        subscription_track::{CheckpointType, ResolveResult},
        GetCheckpointResult,
    };

    fn region(id: u64, version: u64, conf_version: u64) -> Region {
        let mut r = Region::new();
        let mut e = RegionEpoch::new();
        e.set_version(version);
        e.set_conf_ver(conf_version);
        r.set_id(id);
        r.set_region_epoch(e);
        r
    }

    #[derive(Clone)]
    pub struct MockSink(Arc<Mutex<MockSinkInner>>);

    impl MockSink {
        fn with_fail_once(code: RpcStatusCode) -> Self {
            let mut failed = false;
            let inner = MockSinkInner {
                items: Vec::default(),
                closed: false,
                on_error: Box::new(move || {
                    if failed {
                        RpcStatusCode::OK
                    } else {
                        failed = true;
                        code
                    }
                }),
            };
            Self(Arc::new(Mutex::new(inner)))
        }

        fn trivial() -> Self {
            let inner = MockSinkInner {
                items: Vec::default(),
                closed: false,
                on_error: Box::new(|| RpcStatusCode::OK),
            };
            Self(Arc::new(Mutex::new(inner)))
        }

        #[allow(clippy::unused_async)]
        pub async fn fail(&self, status: RpcStatus) -> crate::errors::Result<()> {
            panic!("failed in a case should never fail: {}", status);
        }
    }

    struct MockSinkInner {
        items: Vec<SubscribeFlushEventResponse>,
        closed: bool,
        on_error: Box<dyn FnMut() -> grpcio::RpcStatusCode + Send>,
    }

    impl Sink<(SubscribeFlushEventResponse, grpcio::WriteFlags)> for MockSink {
        type Error = grpcio::Error;

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            Ok(()).into()
        }

        fn start_send(
            self: std::pin::Pin<&mut Self>,
            item: (SubscribeFlushEventResponse, grpcio::WriteFlags),
        ) -> Result<(), Self::Error> {
            let mut guard = self.0.lock().unwrap();
            let code = (guard.on_error)();
            if code != RpcStatusCode::OK {
                return Err(grpcio::Error::RpcFailure(RpcStatus::new(code)));
            }
            guard.items.push(item.0);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            Ok(()).into()
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            let mut guard = self.0.lock().unwrap();
            guard.closed = true;
            Ok(()).into()
        }
    }

    fn simple_resolve_result() -> ResolveResult {
        let mut region = Region::new();
        region.set_id(42);
        ResolveResult {
            region,
            checkpoint: 42.into(),
            checkpoint_type: CheckpointType::MinTs,
        }
    }

    #[test]
    fn test_rpc_sub() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let mut mgr = super::CheckpointManager::default();
        rt.spawn(mgr.spawn_subscription_mgr());

        let trivial_sink = MockSink::trivial();
        rt.block_on(mgr.add_subscriber(trivial_sink.clone()))
            .unwrap();

        mgr.resolve_regions(vec![simple_resolve_result()]);
        mgr.flush();
        mgr.sync_with_subs_mgr(|_| {});
        assert_eq!(trivial_sink.0.lock().unwrap().items.len(), 1);
    }

    #[test]
    fn test_rpc_failure() {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .build()
            .unwrap();
        let mut mgr = super::CheckpointManager::default();
        rt.spawn(mgr.spawn_subscription_mgr());

        let error_sink = MockSink::with_fail_once(RpcStatusCode::INTERNAL);
        rt.block_on(mgr.add_subscriber(error_sink.clone())).unwrap();

        mgr.resolve_regions(vec![simple_resolve_result()]);
        mgr.flush();
        assert_eq!(mgr.sync_with_subs_mgr(|item| { item.subscribers.len() }), 0);
        let sink = error_sink.0.lock().unwrap();
        assert_eq!(sink.items.len(), 0);
        // The stream shouldn't be closed when exit by a failure.
        assert_eq!(sink.closed, false);
    }

    #[test]
    fn test_flush() {
        let mut mgr = super::CheckpointManager::default();
        mgr.do_update(region(1, 32, 8), TimeStamp::new(8));
        mgr.do_update(region(2, 34, 8), TimeStamp::new(15));
        mgr.do_update(region(2, 35, 8), TimeStamp::new(16));
        mgr.do_update(region(2, 35, 8), TimeStamp::new(14));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::NotFound { .. });

        mgr.flush();
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok { checkpoint , .. } if checkpoint.into_inner() == 8);
        let r = mgr.get_from_region(RegionIdWithVersion::new(2, 35));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok { checkpoint , .. } if checkpoint.into_inner() == 16);
        mgr.flush();
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::NotFound { .. });
    }

    #[test]
    fn test_mgr() {
        let mut mgr = super::CheckpointManager::default();
        mgr.update_region_checkpoint(&region(1, 32, 8), TimeStamp::new(8));
        mgr.update_region_checkpoint(&region(2, 34, 8), TimeStamp::new(15));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);
        let r = mgr.get_from_region(RegionIdWithVersion::new(2, 33));
        assert_matches::assert_matches!(r, GetCheckpointResult::EpochNotMatch { .. });
        let r = mgr.get_from_region(RegionIdWithVersion::new(3, 44));
        assert_matches::assert_matches!(r, GetCheckpointResult::NotFound { .. });

        mgr.update_region_checkpoint(&region(1, 30, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);

        mgr.update_region_checkpoint(&region(1, 30, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 8);
        mgr.update_region_checkpoint(&region(1, 32, 8), TimeStamp::new(16));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 32));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 16);
        mgr.update_region_checkpoint(&region(1, 33, 8), TimeStamp::new(24));
        let r = mgr.get_from_region(RegionIdWithVersion::new(1, 33));
        assert_matches::assert_matches!(r, GetCheckpointResult::Ok{checkpoint, ..} if checkpoint.into_inner() == 24);
    }

    pub struct MockPdClient {
        safepoint: RwLock<HashMap<String, TimeStamp>>,
    }

    impl PdClient for MockPdClient {
        fn update_service_safe_point(
            &self,
            name: String,
            safepoint: TimeStamp,
            _ttl: Duration,
        ) -> PdFuture<()> {
            // let _ = self.safepoint.insert(name, safepoint);
            self.safepoint.write().unwrap().insert(name, safepoint);

            Box::pin(ok(()))
        }
    }

    impl MockPdClient {
        fn new() -> Self {
            Self {
                safepoint: RwLock::new(HashMap::default()),
            }
        }

        fn get_service_safe_point(&self, name: String) -> Option<TimeStamp> {
            self.safepoint.read().unwrap().get(&name).copied()
        }
    }

    #[tokio::test]
    async fn test_after() {
        let store_id = 1;
        let pd_cli = Arc::new(MockPdClient::new());
        let mut flush_observer = BasicFlushObserver::new(pd_cli.clone(), store_id);
        let task = String::from("test");
        let rts = 12345;

        let r = flush_observer.after(&task, rts).await;
        assert_eq!(r.is_ok(), true);

        let serivce_id = format!("backup-stream-{}-{}", task, store_id);
        let r = pd_cli.get_service_safe_point(serivce_id).unwrap();
        assert_eq!(r.into_inner(), rts - 1);
    }
}
