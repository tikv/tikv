// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{
    atomic::{AtomicU64, AtomicUsize, Ordering},
    Arc,
};

use collections::{HashMap, HashMapEntry};
use futures::stream::{StreamExt, TryStreamExt};
use grpcio::{DuplexSink, RequestStream, RpcContext, RpcStatus, RpcStatusCode, WriteFlags};
use kvproto::{
    cdcpb::{
        ChangeData, ChangeDataEvent, ChangeDataRequest, ChangeDataRequestKvApi,
        ChangeDataRequest_oneof_request,
    },
    kvrpcpb::ApiVersion,
};
use semver::Version;
use tikv_util::{error, info, memory::MemoryQuota, warn, worker::*};
use tokio::runtime::{self, Runtime};

use crate::{
    channel::{channel, DownstreamSink, Sink},
    delegate::{Downstream, DownstreamId, ObservedRange},
    endpoint::{Deregister, Task},
};

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
    fn add_features(&mut self, version: &Version) {
        if *version >= Version::new(4, 0, 8) {
            self.set(FeatureGate::BATCH_RESOLVED_TS, true);
        }
        if *version >= Version::new(5, 3, 0) {
            self.set(FeatureGate::VALIDATE_CLUSTER_ID, true);
        }
    }

    /// Returns the first version (v4.0.8) that supports batch resolved ts.
    pub fn batch_resolved_ts() -> Version {
        Version::new(4, 0, 8)
    }
}

pub struct Conn {
    pub id: ConnId,
    pub sink: Sink,
    pub version: Version,
    pub features: FeatureGate,

    downstreams: HashMap<DownstreamKey, DownstreamValue>,
}

#[derive(PartialEq, Eq, Hash)]
struct DownstreamKey {
    request_id: RequestId,
    region_id: u64,
}

#[derive(Clone)]
struct DownstreamValue {
    id: DownstreamId,
    advanced_to: Arc<AtomicU64>,
}

impl Conn {
    pub fn new(conn_id: ConnId, sink: Sink, version: Version, features: Vec<&'static str>) -> Conn {
        let mut conn = Conn {
            id: conn_id,
            sink,
            version,
            features: FeatureGate::empty(),
            downstreams: HashMap::default(),
        };
        conn.features.add_features(&conn.version);
        if features.contains(&EventFeedHeaders::STREAM_MULTIPLEXING) {
            conn.features.set(FeatureGate::STREAM_MULTIPLEXING, true);
        }
        conn
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
        downstream: &Downstream,
    ) -> Option<DownstreamId> {
        let key = DownstreamKey {
            request_id,
            region_id,
        };
        match self.downstreams.entry(key) {
            HashMapEntry::Occupied(value) => Some(value.get().id),
            HashMapEntry::Vacant(v) => {
                v.insert(DownstreamValue {
                    id: downstream.id,
                    advanced_to: downstream.advanced_to.clone(),
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
        F: FnMut(RequestId, u64, DownstreamId, &Arc<AtomicU64>),
    {
        for (key, value) in &self.downstreams {
            f(key.request_id, key.region_id, value.id, &value.advanced_to);
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
    workers: Arc<Runtime>,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(
        scheduler: Scheduler<Task>,
        memory_quota: Arc<MemoryQuota>,
        workers: usize,
    ) -> Service {
        let workers = runtime::Builder::new_multi_thread()
            .thread_name("cdc-responsers")
            .worker_threads(workers)
            .build()
            .unwrap();
        let workers = Arc::new(workers);
        Service {
            scheduler,
            memory_quota,
            workers,
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

    fn parse_version_from_request_header(request: &ChangeDataRequest, peer: &str) -> Version {
        let version_field = request.get_header().get_ticdc_version();
        Version::parse(version_field).unwrap_or_else(|e| {
            warn!(
                "empty or invalid TiCDC version, please upgrading TiCDC";
                "version" => version_field,
                "downstream" => ?peer, "region_id" => request.region_id,
                "error" => ?e,
            );
            Version::new(0, 0, 0)
        })
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
        sink: &Sink,
    ) -> Result<(), String> {
        match request.request {
            None | Some(ChangeDataRequest_oneof_request::Register(_)) => {
                Self::handle_register(scheduler, peer, request, conn_id, sink.clone())
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
        sink: Sink,
    ) -> Result<(), String> {
        let region_id = request.region_id;
        let req_id = RequestId(request.request_id);

        let observed_range = ObservedRange::new(request.start_key.clone(), request.end_key.clone())
            .unwrap_or_else(|e| {
                warn!(
                    "cdc invalid observed start key or end key version";
                    "downstream" => ?peer,
                    "region_id" => region_id,
                    "request_id" => ?req_id,
                    "error" => ?e,
                    "start_key" => log_wrappers::Value::key(&request.start_key),
                    "end_key" => log_wrappers::Value::key(&request.end_key),
                );
                ObservedRange::default()
            });
        let downstream = Downstream::new(
            RequestId(request.request_id),
            conn_id,
            peer.to_owned(),
            request.get_region_epoch().clone(),
            request.kv_api,
            request.filter_loop,
            observed_range,
            DownstreamSink::new(region_id, req_id, sink),
        );
        info!("creates cdc downstream"; "conn_id" => ?conn_id, "region_id" => region_id, "request_id" => ?req_id);

        let task = Task::Register {
            request,
            downstream,
        };
        scheduler
            .schedule(task)
            .map_err(|e| format!("cdc failed to schedule Register {:?}", e))
    }

    fn handle_deregister(
        scheduler: &Scheduler<Task>,
        request: ChangeDataRequest,
        conn_id: ConnId,
    ) -> Result<(), String> {
        let task = if request.region_id != 0 {
            Task::Deregister(Deregister::Region {
                conn_id,
                request_id: RequestId(request.request_id),
                region_id: request.region_id,
            })
        } else {
            Task::Deregister(Deregister::Request {
                conn_id,
                request_id: RequestId(request.request_id),
            })
        };
        scheduler
            .schedule(task)
            .map_err(|e| format!("cdc failed to schedule Request {:?}", e))
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
        mut rpc_sink: DuplexSink<ChangeDataEvent>,
        event_feed_v2: bool,
    ) {
        let conn_id = ConnId::new();
        let (sink, drain) = channel(conn_id, self.memory_quota.clone());
        let mut features = vec![];

        if event_feed_v2 {
            let headers = match Self::parse_headers(&ctx) {
                Ok(headers) => headers,
                Err(e) => {
                    let peer = ctx.peer();
                    error!("cdc connection with bad headers"; "downstream" => ?peer, "headers" => &e);
                    ctx.spawn(async move {
                        let status = RpcStatus::with_message(RpcStatusCode::UNIMPLEMENTED, e);
                        if let Err(e) = rpc_sink.fail(status).await {
                            error!("cdc failed to send error"; "downstream" => ?peer, "error" => ?e);
                        }
                    });
                    return;
                }
            };
            features = headers.features;
        }
        info!("cdc connection created"; "downstream" => ctx.peer(), "conn_id" => ?conn_id, "features" => ?features);

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = async move {
            let mut stream = stream.map_err(|e| format!("{:?}", e));
            if let Some(request) = stream.try_next().await? {
                // Get version from the first request in the stream.
                let version = Self::parse_version_from_request_header(&request, &peer);
                let conn = Conn::new(conn_id, sink.clone(), version, features);
                scheduler
                    .schedule(Task::OpenConn { conn })
                    .map_err(|e| format!("cdc failed to schedule OpenConn {:?}", e))?;

                Self::handle_request(&scheduler, &peer, request, conn_id, &sink)?;
            }
            while let Some(request) = stream.try_next().await? {
                Self::handle_request(&scheduler, &peer, request, conn_id, &sink)?;
            }
            scheduler
                .schedule(Task::Deregister(Deregister::Conn(conn_id)))
                .map_err(|e| format!("cdc failed to schedule Deregister {:?}", e))?;
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
        self.workers.spawn(async move {
            #[cfg(feature = "failpoints")]
            sleep_before_drain_change_event().await;

            // NOTE: enhance_batch so that we can set `buffer_hint(true)` for all messages.
            rpc_sink.enhance_batch(true);
            let flags = WriteFlags::default().buffer_hint(true);
            if let Err(e) = drain.map(|x| Ok((x, flags))).forward(&mut rpc_sink).await {
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
