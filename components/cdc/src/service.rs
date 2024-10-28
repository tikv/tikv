// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use collections::{HashMap, HashMapEntry};
use crossbeam::atomic::AtomicCell;
use futures::stream::TryStreamExt;
use grpcio::{DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode};
use kvproto::{
    cdcpb::{
        ChangeData, ChangeDataEvent, ChangeDataRequest, ChangeDataRequestKvApi,
        ChangeDataRequest_oneof_request,
    },
    kvrpcpb::ApiVersion,
};
use tikv_util::{error, info, memory::MemoryQuota, warn, worker::*};

use crate::{
    channel::{channel, Sink, CDC_CHANNLE_CAPACITY},
    delegate::{Downstream, DownstreamId, DownstreamState, ObservedRange},
    endpoint::{Deregister, Task},
};
use crate::metrics::CDC_SCHEDULER_PENDING_TASKS;

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

pub fn validate_kv_api(kv_api: ChangeDataRequestKvApi, api_version: ApiVersion) -> bool {
    kv_api == ChangeDataRequestKvApi::TiDb
        || (kv_api == ChangeDataRequestKvApi::RawKv && api_version == ApiVersion::V2)
}

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnId(usize);

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct RequestId(pub u64);

impl ConnId {
    pub fn new() -> ConnId {
        ConnId(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for ConnId {
    fn default() -> Self {
        Self::new()
    }
}

// FeatureGate checks whether a feature is enabled or not on client versions.
//
// NOTE: default features can't be disabled by clients. Clients can only enable
// features by specifying GRPC headers. See `EventFeedHeaders`.
bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        const VALIDATE_CLUSTER_ID = 0b00000010;
        const STREAM_MULTIPLEXING = 0b00000100;
    }
}

impl FeatureGate {
    fn default_features(version: &semver::Version) -> FeatureGate {
        let mut features = FeatureGate::empty();
        if *version >= semver::Version::new(4, 0, 8) {
            features.set(FeatureGate::BATCH_RESOLVED_TS, true);
        }
        if *version >= semver::Version::new(5, 3, 0) {
            features.set(FeatureGate::VALIDATE_CLUSTER_ID, true);
        }
        features
    }

    /// Returns the first version (v4.0.8) that supports batch resolved ts.
    pub fn batch_resolved_ts() -> semver::Version {
        semver::Version::new(4, 0, 8)
    }
}

pub struct Conn {
    id: ConnId,
    sink: Sink,
    downstreams: HashMap<DownstreamKey, DownstreamValue>,
    peer: String,

    // Set when the connection established, or the first request received.
    version: Option<(semver::Version, FeatureGate)>,
}

#[derive(PartialEq, Eq, Hash)]
struct DownstreamKey {
    request_id: RequestId,
    region_id: u64,
}

#[derive(Clone)]
struct DownstreamValue {
    id: DownstreamId,
    state: Arc<AtomicCell<DownstreamState>>,
}

impl Conn {
    pub fn new(conn_id: ConnId, sink: Sink, peer: String) -> Conn {
        Conn {
            id: conn_id,
            sink,
            downstreams: HashMap::default(),
            peer,
            version: None,
        }
    }

    pub fn check_version_and_set_feature(
        &mut self,
        version: semver::Version,
        explicit_features: Vec<&'static str>,
    ) {
        let mut features = FeatureGate::default_features(&version);
        if explicit_features.contains(&EventFeedHeaders::STREAM_MULTIPLEXING) {
            features.set(FeatureGate::STREAM_MULTIPLEXING, true);
        } else {
            // NOTE: we can handle more explicit features here.
        }

        if self.version.replace((version, features)).is_some() {
            panic!("should never be some");
        }
    }

    pub fn features(&self) -> &FeatureGate {
        self.version.as_ref().map(|(_, f)| f).unwrap()
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnId {
        self.id
    }

    pub fn get_sink(&self) -> &Sink {
        &self.sink
    }

    pub fn get_downstream(&self, request_id: RequestId, region_id: u64) -> Option<DownstreamId> {
        let key = DownstreamKey {
            request_id,
            region_id,
        };
        self.downstreams.get(&key).map(|v| v.id)
    }

    pub fn subscribe(
        &mut self,
        request_id: RequestId,
        region_id: u64,
        downstream_id: DownstreamId,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
    ) -> Option<DownstreamId> {
        let key = DownstreamKey {
            request_id,
            region_id,
        };
        match self.downstreams.entry(key) {
            HashMapEntry::Occupied(value) => Some(value.get().id),
            HashMapEntry::Vacant(v) => {
                v.insert(DownstreamValue {
                    id: downstream_id,
                    state: downstream_state,
                });
                None
            }
        }
    }

    pub fn unsubscribe(&mut self, request_id: RequestId, region_id: u64) -> Option<DownstreamId> {
        let key = DownstreamKey {
            request_id,
            region_id,
        };
        self.downstreams.remove(&key).map(|value| value.id)
    }

    pub fn unsubscribe_request(&mut self, request_id: RequestId) -> Vec<(u64, DownstreamId)> {
        let mut downstreams = Vec::new();
        self.downstreams.retain(|key, value| -> bool {
            if key.request_id == request_id {
                downstreams.push((key.region_id, value.id));
                return false;
            }
            true
        });
        downstreams
    }

    pub fn iter_downstreams<F>(&self, mut f: F)
    where
        F: FnMut(RequestId, u64, DownstreamId, &Arc<AtomicCell<DownstreamState>>),
    {
        for (key, value) in &self.downstreams {
            f(key.request_id, key.region_id, value.id, &value.state);
        }
    }

    #[cfg(test)]
    pub fn downstreams_count(&self) -> usize {
        self.downstreams.len()
    }
}

// Examaples for all available headers:
//  * features -> feature_a,feature_b
#[derive(Debug, Default)]
struct EventFeedHeaders {
    features: Vec<&'static str>,
}

impl EventFeedHeaders {
    const FEATURES_KEY: &'static str = "features";
    const STREAM_MULTIPLEXING: &'static str = "stream-multiplexing";
    const FEATURES: &'static [&'static str] = &[Self::STREAM_MULTIPLEXING];

    fn parse_features(value: &[u8]) -> Result<Vec<&'static str>, String> {
        let value = std::str::from_utf8(value).unwrap_or_default();
        let (mut features, mut unknowns) = (Vec::new(), Vec::new());
        for feature in value.split(',').map(|x| x.trim()) {
            if let Some(i) = Self::FEATURES.iter().position(|x| *x == feature) {
                features.push(Self::FEATURES[i]);
            } else {
                unknowns.push(feature);
            }
        }
        if !unknowns.is_empty() {
            return Err(unknowns.join(","));
        }
        Ok(features)
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    memory_quota: Arc<MemoryQuota>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>, memory_quota: Arc<MemoryQuota>) -> Service {
        Service {
            scheduler,
            memory_quota,
        }
    }

    // Parse HTTP/2 headers. Only for `Self::event_feed_v2`.
    fn parse_headers(ctx: &RpcContext<'_>) -> Result<EventFeedHeaders, String> {
        let mut header = EventFeedHeaders::default();
        let metadata = ctx.request_headers();
        for i in 0..metadata.len() {
            let (key, value) = metadata.get(i).unwrap();
            if key == EventFeedHeaders::FEATURES_KEY {
                header.features = EventFeedHeaders::parse_features(value)?;
            }
        }
        Ok(header)
    }

    fn parse_version_from_request_header(
        request: &ChangeDataRequest,
        peer: &str,
    ) -> semver::Version {
        let version_field = request.get_header().get_ticdc_version();
        semver::Version::parse(version_field).unwrap_or_else(|e| {
            warn!(
                "empty or invalid TiCDC version, please upgrading TiCDC";
                "version" => version_field,
                "downstream" => ?peer, "region_id" => request.region_id,
                "error" => ?e,
            );
            semver::Version::new(0, 0, 0)
        })
    }

    fn set_conn_version(
        scheduler: &Scheduler<Task>,
        conn_id: ConnId,
        version: semver::Version,
        explicit_features: Vec<&'static str>,
    ) -> Result<(), String> {
        let task = Task::SetConnVersion {
            conn_id,
            version,
            explicit_features,
        };
        CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["set_conn_version"]).inc();
        scheduler.schedule(task).map_err(|e| format!("{:?}", e))
    }

    // ### Command types:
    // * Register registers a region. 1) both `request_id` and `region_id` must be
    //   specified; 2) `request_id` can be 0 but `region_id` can not.
    // * Deregister deregisters some regions in one same `request_id` or just one
    //   region. 1) if both `request_id` and `region_id` are specified, just
    //   deregister the region; 2) if only `request_id` is specified, all region
    //   subscriptions with the same `request_id` will be deregistered.
    fn handle_request(
        scheduler: &Scheduler<Task>,
        peer: &str,
        request: ChangeDataRequest,
        conn_id: ConnId,
    ) -> Result<(), String> {
        match request.request {
            None | Some(ChangeDataRequest_oneof_request::Register(_)) => {
                Self::handle_register(scheduler, peer, request, conn_id)
            }
            Some(ChangeDataRequest_oneof_request::Deregister(_)) => {
                Self::handle_deregister(scheduler, request, conn_id)
            }
            _ => unreachable!(),
        }
    }

    fn handle_register(
        scheduler: &Scheduler<Task>,
        peer: &str,
        request: ChangeDataRequest,
        conn_id: ConnId,
    ) -> Result<(), String> {
        let observed_range = ObservedRange::new(request.start_key.clone(), request.end_key.clone())
            .unwrap_or_else(|e| {
                warn!(
                    "cdc invalid observed start key or end key version";
                    "downstream" => ?peer,
                    "region_id" => request.region_id,
                    "request_id" => request.region_id,
                    "error" => ?e,
                    "start_key" => log_wrappers::Value::key(&request.start_key),
                    "end_key" => log_wrappers::Value::key(&request.end_key),
                );
                ObservedRange::default()
            });
        let downstream = Downstream::new(
            peer.to_owned(),
            request.get_region_epoch().clone(),
            RequestId(request.request_id),
            conn_id,
            request.kv_api,
            request.filter_loop,
            observed_range,
        );
        let task = Task::Register {
            request,
            downstream,
        };
        CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["register"]).inc();
        scheduler.schedule(task).map_err(|e| format!("{:?}", e))
    }

    fn handle_deregister(
        scheduler: &Scheduler<Task>,
        request: ChangeDataRequest,
        conn_id: ConnId,
    ) -> Result<(), String> {
        let task = if request.region_id != 0 {
            CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["deregister::region"]).inc();
            Task::Deregister(Deregister::Region {
                conn_id,
                request_id: RequestId(request.request_id),
                region_id: request.region_id,
            })
        } else {
            CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["deregister::request"]).inc();
            Task::Deregister(Deregister::Request {
                conn_id,
                request_id: RequestId(request.request_id),
            })
        };
        scheduler.schedule(task).map_err(|e| format!("{:?}", e))
    }

    // Differences between `Self::event_feed` and `Self::event_feed_v2`:
    //
    // ### Why `v2`
    // `v2` is expected to resolve this problem: clients version is higher than
    // server, In which case, `v1` compatibility check mechanism doesn't work.
    //
    // ### How `v2`
    // In `v2`, clients tells requested features to connected servers. If a
    // server finds a client requires unavailable features, it can fail the
    // connection with an UNIMPLEMENTED status code.
    //
    // ### Details about `v2` features
    // * stream-multiplexing: a region can be subscribed multiple times in one
    //   `Conn` with different `request_id`.
    fn handle_event_feed(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
        event_feed_v2: bool,
    ) {
        sink.enhance_batch(true);
        let conn_id = ConnId::new();
        let (event_sink, mut event_drain) =
            channel(conn_id, CDC_CHANNLE_CAPACITY, self.memory_quota.clone());
        let conn = Conn::new(conn_id, event_sink, ctx.peer());
        let mut explicit_features = vec![];

        if event_feed_v2 {
            let headers = match Self::parse_headers(&ctx) {
                Ok(headers) => headers,
                Err(e) => {
                    let peer = ctx.peer();
                    error!("cdc connection with bad headers"; "downstream" => ?peer, "headers" => &e);
                    ctx.spawn(async move {
                        let status = RpcStatus::with_message(RpcStatusCode::UNIMPLEMENTED, e);
                        if let Err(e) = sink.fail(status).await {
                            error!("cdc failed to send error"; "downstream" => ?peer, "error" => ?e);
                        }
                    });
                    return;
                }
            };
            explicit_features = headers.features;
        }
        info!("cdc connection created"; "downstream" => ctx.peer(), "features" => ?explicit_features);

        CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["open_conn"]).inc();
        if let Err(e) = self.scheduler.schedule(Task::OpenConn { conn }) {
            let peer = ctx.peer();
            error!("cdc connection initiate failed"; "downstream" => ?peer, "error" => ?e);
            ctx.spawn(async move {
                let status = RpcStatus::with_message(RpcStatusCode::UNKNOWN, format!("{:?}", e));
                if let Err(e) = sink.fail(status).await {
                    error!("cdc failed to send error"; "downstream" => ?peer, "error" => ?e);
                }
            });
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = async move {
            let mut stream = stream.map_err(|e| format!("{:?}", e));
            if let Some(request) = stream.try_next().await? {
                // Get version from the first request in the stream.
                let version = Self::parse_version_from_request_header(&request, &peer);
                Self::set_conn_version(&scheduler, conn_id, version, explicit_features)?;
                Self::handle_request(&scheduler, &peer, request, conn_id)?;
            }
            while let Some(request) = stream.try_next().await? {
                Self::handle_request(&scheduler, &peer, request, conn_id)?;
            }
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e, "conn_id" => ?conn_id);
            }
            CDC_SCHEDULER_PENDING_TASKS.with_label_values(&["deregister::conn"]).inc();
            Ok::<(), String>(())

        };

        let peer = ctx.peer();
        ctx.spawn(async move {
            if let Err(e) = recv_req.await {
                warn!("cdc receive failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
            } else {
                info!("cdc receive closed"; "downstream" => peer, "conn_id" => ?conn_id);
            }
        });

        let peer = ctx.peer();
        ctx.spawn(async move {
            #[cfg(feature = "failpoints")]
            sleep_before_drain_change_event().await;
            if let Err(e) = event_drain.forward(&mut sink).await {
                warn!("cdc send failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
            } else {
                info!("cdc send closed"; "downstream" => peer, "conn_id" => ?conn_id);
            }
        });
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        self.handle_event_feed(ctx, stream, sink, false);
    }

    fn event_feed_v2(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        sink: DuplexSink<ChangeDataEvent>,
    ) {
        self.handle_event_feed(ctx, stream, sink, true);
    }
}

#[cfg(feature = "failpoints")]
async fn sleep_before_drain_change_event() {
    use std::time::{Duration, Instant};

    use tikv_util::timer::GLOBAL_TIMER_HANDLE;
    let should_sleep = || {
        fail::fail_point!("cdc_sleep_before_drain_change_event", |_| true);
        false
    };
    if should_sleep() {
        let dur = Duration::from_secs(5);
        let timer = GLOBAL_TIMER_HANDLE.delay(Instant::now() + dur);
        let _ = futures::compat::Compat01As03::new(timer).await;
    }
}

#[cfg(test)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use futures::{executor::block_on, SinkExt};
    use grpcio::{self, ChannelBuilder, EnvBuilder, Server, ServerBuilder, WriteFlags};
    use kvproto::cdcpb::{create_change_data, ChangeDataClient, ResolvedTs};
    use tikv_util::future::block_on_timeout;

    use super::*;
    use crate::channel::{recv_timeout, CdcEvent};

    fn new_rpc_suite(capacity: usize) -> (Server, ChangeDataClient, ReceiverWrapper<Task>) {
        let memory_quota = Arc::new(MemoryQuota::new(capacity));
        let (scheduler, rx) = dummy_scheduler();
        let cdc_service = Service::new(scheduler, memory_quota);
        let env = Arc::new(EnvBuilder::new().build());
        let builder =
            ServerBuilder::new(env.clone()).register_service(create_change_data(cdc_service));
        let mut server = builder.bind("127.0.0.1", 0).build().unwrap();
        server.start();
        let (_, port) = server.bind_addrs().next().unwrap();
        let addr = format!("127.0.0.1:{}", port);
        let channel = ChannelBuilder::new(env).connect(&addr);
        let client = ChangeDataClient::new(channel);
        (server, client, rx)
    }

    #[test]
    fn test_flow_control() {
        // Disable CDC sink memory quota.
        let capacity = usize::MAX;
        let (_server, client, mut task_rx) = new_rpc_suite(capacity);
        // Create a event feed stream.
        let (mut tx, mut rx) = client.event_feed().unwrap();
        let mut req = ChangeDataRequest {
            region_id: 1,
            ..Default::default()
        };
        req.mut_header().set_ticdc_version("4.0.7".into());
        block_on(tx.send((req, WriteFlags::default()))).unwrap();
        let task = task_rx.recv_timeout(Duration::from_millis(100)).unwrap();
        let conn = if let Some(Task::OpenConn { conn }) = task {
            conn
        } else {
            panic!("expect to be Task::OpenConn");
        };
        let sink = conn.get_sink().clone();
        // Approximate 1 KB.
        let mut rts = ResolvedTs::default();
        rts.set_regions(vec![u64::MAX; 128]);

        let send = || {
            let rts_ = rts.clone();
            let mut sink_ = sink.clone();
            Box::pin(async move {
                sink_
                    .send_all(
                        vec![CdcEvent::ResolvedTs(rts_)],
                        Arc::new(Default::default()),
                    )
                    .await
            })
        };
        let must_fill_window = || {
            let mut window_size = 0;
            loop {
                if matches!(
                    block_on_timeout(send(), Duration::from_millis(100)),
                    Err(_) | Ok(Err(_))
                ) {
                    // Window is filled and flow control in sink is triggered.
                    break;
                }
                window_size += 1;
                // gRPC window size should not be larger than 1GB.
                assert!(window_size <= 1024 * 1024, "window_size: {}", window_size);
            }
            window_size
        };

        // Fill gRPC window.
        let window_size = must_fill_window();
        assert_ne!(window_size, 0);
        // After receiving a message, sink should be able to send again.
        recv_timeout(&mut rx, Duration::from_millis(100))
            .unwrap()
            .unwrap()
            .unwrap();
        block_on_timeout(send(), Duration::from_millis(100))
            .unwrap()
            .unwrap();
        // gRPC client may update window size after receiving a message,
        // though server should not be able to send messages infinitely.
        let window_size = must_fill_window();
        assert_ne!(window_size, 0);
    }
}
