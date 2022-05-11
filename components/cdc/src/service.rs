// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::hash_map::Entry,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use collections::HashMap;
use crossbeam::atomic::AtomicCell;
use futures::{
    future::{self, TryFutureExt},
    sink::SinkExt,
    stream::TryStreamExt,
};
use grpcio::{DuplexSink, Error as GrpcError, RequestStream, RpcContext, RpcStatus, RpcStatusCode};
use kvproto::{
    cdcpb::{
        ChangeData, ChangeDataEvent, ChangeDataRequest, ChangeDataRequestKvApi, Compatibility,
    },
    kvrpcpb::ApiVersion,
};
use tikv_util::{error, info, warn, worker::*};

use crate::{
    channel::{channel, MemoryQuota, Sink, CDC_CHANNLE_CAPACITY},
    delegate::{Downstream, DownstreamID, DownstreamState},
    endpoint::{Deregister, Task},
};

static CONNECTION_ID_ALLOC: AtomicUsize = AtomicUsize::new(0);

/// A unique identifier of a Connection.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct ConnID(usize);

impl ConnID {
    pub fn new() -> ConnID {
        ConnID(CONNECTION_ID_ALLOC.fetch_add(1, Ordering::SeqCst))
    }
}

impl Default for ConnID {
    fn default() -> Self {
        Self::new()
    }
}

bitflags::bitflags! {
    pub struct FeatureGate: u8 {
        const BATCH_RESOLVED_TS = 0b00000001;
        // Uncomment when its ready.
        // const LargeTxn       = 0b00000010;
    }
}

impl FeatureGate {
    // Returns the first version (v4.0.8) that supports batch resolved ts.
    pub fn batch_resolved_ts() -> semver::Version {
        semver::Version::new(4, 0, 8)
    }

    // Returns the first version (v5.3.0) that supports validate cluster id.
    pub(crate) fn validate_cluster_id() -> semver::Version {
        semver::Version::new(5, 3, 0)
    }

    pub(crate) fn validate_kv_api(kv_api: ChangeDataRequestKvApi, api_version: ApiVersion) -> bool {
        kv_api == ChangeDataRequestKvApi::TiDb
            || (kv_api == ChangeDataRequestKvApi::RawKv && api_version == ApiVersion::V2)
    }
}

pub struct Conn {
    id: ConnID,
    sink: Sink,
    // region id -> DownstreamID
    downstreams: HashMap<u64, (DownstreamID, Arc<AtomicCell<DownstreamState>>)>,
    peer: String,
    version: Option<(semver::Version, FeatureGate)>,
}

impl Conn {
    pub fn new(sink: Sink, peer: String) -> Conn {
        Conn {
            id: ConnID::new(),
            sink,
            downstreams: HashMap::default(),
            version: None,
            peer,
        }
    }

    // TODO refactor into Error::Version.
    pub fn check_version_and_set_feature(&mut self, ver: semver::Version) -> Option<Compatibility> {
        match &self.version {
            Some((version, _)) => {
                if version == &ver {
                    None
                } else {
                    error!("cdc different version on the same connection";
                        "previous version" => ?version, "version" => ?ver,
                        "downstream" => ?self.peer, "conn_id" => ?self.id);
                    Some(Compatibility {
                        required_version: version.to_string(),
                        ..Default::default()
                    })
                }
            }
            None => {
                let mut features = FeatureGate::empty();
                if FeatureGate::batch_resolved_ts() <= ver {
                    features.toggle(FeatureGate::BATCH_RESOLVED_TS);
                }
                info!("cdc connection version";
                    "version" => ver.to_string(), "features" => ?features, "downstream" => ?self.peer);
                self.version = Some((ver, features));
                None
            }
        }
        // Return Err(Compatibility) when TiKV reaches the next major release,
        // so that we can remove feature gates.
    }

    pub fn get_feature(&self) -> Option<&FeatureGate> {
        self.version.as_ref().map(|(_, f)| f)
    }

    pub fn get_peer(&self) -> &str {
        &self.peer
    }

    pub fn get_id(&self) -> ConnID {
        self.id
    }

    pub fn get_downstreams(
        &self,
    ) -> &HashMap<u64, (DownstreamID, Arc<AtomicCell<DownstreamState>>)> {
        &self.downstreams
    }

    pub fn take_downstreams(
        self,
    ) -> HashMap<u64, (DownstreamID, Arc<AtomicCell<DownstreamState>>)> {
        self.downstreams
    }

    pub fn get_sink(&self) -> &Sink {
        &self.sink
    }

    pub fn subscribe(
        &mut self,
        region_id: u64,
        downstream_id: DownstreamID,
        downstream_state: Arc<AtomicCell<DownstreamState>>,
    ) -> bool {
        match self.downstreams.entry(region_id) {
            Entry::Occupied(_) => false,
            Entry::Vacant(v) => {
                v.insert((downstream_id, downstream_state));
                true
            }
        }
    }

    pub fn unsubscribe(&mut self, region_id: u64) {
        self.downstreams.remove(&region_id);
    }

    pub fn downstream_id(&self, region_id: u64) -> Option<DownstreamID> {
        self.downstreams.get(&region_id).map(|x| x.0)
    }
}

/// Service implements the `ChangeData` service.
///
/// It's a front-end of the CDC service, schedules requests to the `Endpoint`.
#[derive(Clone)]
pub struct Service {
    scheduler: Scheduler<Task>,
    memory_quota: MemoryQuota,
}

impl Service {
    /// Create a ChangeData service.
    ///
    /// It requires a scheduler of an `Endpoint` in order to schedule tasks.
    pub fn new(scheduler: Scheduler<Task>, memory_quota: MemoryQuota) -> Service {
        Service {
            scheduler,
            memory_quota,
        }
    }
}

impl ChangeData for Service {
    fn event_feed(
        &mut self,
        ctx: RpcContext<'_>,
        stream: RequestStream<ChangeDataRequest>,
        mut sink: DuplexSink<ChangeDataEvent>,
    ) {
        let (event_sink, mut event_drain) =
            channel(CDC_CHANNLE_CAPACITY, self.memory_quota.clone());
        let peer = ctx.peer();
        let conn = Conn::new(event_sink, peer);
        let conn_id = conn.get_id();

        if let Err(status) = self
            .scheduler
            .schedule(Task::OpenConn { conn })
            .map_err(|e| {
                RpcStatus::with_message(RpcStatusCode::INVALID_ARGUMENT, format!("{:?}", e))
            })
        {
            error!("cdc connection initiate failed"; "error" => ?status);
            ctx.spawn(
                sink.fail(status)
                    .unwrap_or_else(|e| error!("cdc failed to send error"; "error" => ?e)),
            );
            return;
        }

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        let recv_req = stream.try_for_each(move |request| {
            let region_epoch = request.get_region_epoch().clone();
            let req_id = request.get_request_id();
            let req_kvapi = request.get_kv_api();
            let version = match semver::Version::parse(request.get_header().get_ticdc_version()) {
                Ok(v) => v,
                Err(e) => {
                    warn!("empty or invalid TiCDC version, please upgrading TiCDC";
                        "version" => request.get_header().get_ticdc_version(),
                        "error" => ?e);
                    semver::Version::new(0, 0, 0)
                }
            };
            let downstream =
                Downstream::new(peer.clone(), region_epoch, req_id, conn_id, req_kvapi);
            let ret = scheduler
                .schedule(Task::Register {
                    request,
                    downstream,
                    conn_id,
                    version,
                })
                .map_err(|e| {
                    GrpcError::RpcFailure(RpcStatus::with_message(
                        RpcStatusCode::INVALID_ARGUMENT,
                        format!("{:?}", e),
                    ))
                });
            future::ready(ret)
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();
        ctx.spawn(async move {
            let res = recv_req.await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e, "conn_id" => ?conn_id);
            }
            match res {
                Ok(()) => {
                    info!("cdc receive closed"; "downstream" => peer, "conn_id" => ?conn_id);
                }
                Err(e) => {
                    warn!("cdc receive failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });

        let peer = ctx.peer();
        let scheduler = self.scheduler.clone();

        ctx.spawn(async move {
            #[cfg(feature = "failpoints")]
            sleep_before_drain_change_event().await;

            let res = event_drain.forward(&mut sink).await;
            // Unregister this downstream only.
            let deregister = Deregister::Conn(conn_id);
            if let Err(e) = scheduler.schedule(Task::Deregister(deregister)) {
                error!("cdc deregister failed"; "error" => ?e);
            }
            match res {
                Ok(_s) => {
                    info!("cdc send closed"; "downstream" => peer, "conn_id" => ?conn_id);
                    let _ = sink.close().await;
                }
                Err(e) => {
                    warn!("cdc send failed"; "error" => ?e, "downstream" => peer, "conn_id" => ?conn_id);
                }
            }
        });
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

    use futures::executor::block_on;
    use grpcio::{self, ChannelBuilder, EnvBuilder, Server, ServerBuilder, WriteFlags};
    use kvproto::cdcpb::{create_change_data, ChangeDataClient, ResolvedTs};

    use super::*;
    use crate::channel::{poll_timeout, recv_timeout, CdcEvent};

    fn new_rpc_suite(capacity: usize) -> (Server, ChangeDataClient, ReceiverWrapper<Task>) {
        let memory_quota = MemoryQuota::new(capacity);
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
            Box::pin(async move { sink_.send_all(vec![CdcEvent::ResolvedTs(rts_)]).await })
        };
        let must_fill_window = || {
            let mut window_size = 0;
            loop {
                if matches!(
                    poll_timeout(&mut send(), Duration::from_millis(100)),
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
        poll_timeout(&mut send(), Duration::from_millis(100))
            .unwrap()
            .unwrap();
        // gRPC client may update window size after receiving a message,
        // though server should not be able to send messages infinitely.
        let window_size = must_fill_window();
        assert_ne!(window_size, 0);
    }
}
