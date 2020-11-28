// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::server::{Error, Result};
use futures::{Future, Sink, Stream};
use futures_cpupool::CpuPool;
use grpcio::{RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink, WriteFlags};
use kvproto::diagnosticspb::{
    Diagnostics, SearchLogRequest, SearchLogResponse, ServerInfoRequest, ServerInfoResponse,
    ServerInfoType,
};

#[cfg(feature = "prost-codec")]
use kvproto::diagnosticspb::search_log_request::Target as SearchLogRequestTarget;
#[cfg(not(feature = "prost-codec"))]
use kvproto::diagnosticspb::SearchLogRequestTarget;

use security::{check_common_name, SecurityManager};
use tikv_util::{
    sys::{SystemExt, SYS_INFO},
    timer::GLOBAL_TIMER_HANDLE,
};

mod ioload;
mod log;
mod sys;

/// Service handles the RPC messages for the `Diagnostics` service.
#[derive(Clone)]
pub struct Service {
    pool: CpuPool,
    log_file: String,
    slow_log_file: String,
    security_mgr: Arc<SecurityManager>,
}

impl Service {
    pub fn new(
        pool: CpuPool,
        log_file: String,
        slow_log_file: String,
        security_mgr: Arc<SecurityManager>,
    ) -> Self {
        Service {
            pool,
            log_file,
            slow_log_file,
            security_mgr,
        }
    }
}

impl Diagnostics for Service {
    fn search_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: SearchLogRequest,
        sink: ServerStreamingSink<SearchLogResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let log_file = if req.get_target() == SearchLogRequestTarget::Normal {
            self.log_file.to_owned()
        } else {
            self.slow_log_file.to_owned()
        };
        let stream = self
            .pool
            .spawn_fn(move || log::search(log_file, req))
            .map(|stream| {
                stream
                    .map(|resp| (resp, WriteFlags::default().buffer_hint(true)))
                    .map_err(|e| {
                        grpcio::Error::RpcFailure(RpcStatus::new(
                            RpcStatusCode::UNKNOWN,
                            Some(format!("{:?}", e)),
                        ))
                    })
            })
            .map_err(|e| {
                grpcio::Error::RpcFailure(RpcStatus::new(
                    RpcStatusCode::UNKNOWN,
                    Some(format!("{:?}", e)),
                ))
            });
        let future = self.pool.spawn(
            stream
                .and_then(|stream| sink.send_all(stream))
                .map(|_| ())
                .map_err(|e| {
                    error!("search log RPC error"; "error" => ?e);
                }),
        );
        ctx.spawn(future);
    }

    fn server_info(
        &mut self,
        ctx: RpcContext<'_>,
        req: ServerInfoRequest,
        sink: UnarySink<ServerInfoResponse>,
    ) {
        if !check_common_name(self.security_mgr.cert_allowed_cn(), &ctx) {
            return;
        }
        let tp = req.get_tp();
        let collect = self
            .pool
            .spawn_fn(move || {
                let s = match tp {
                    ServerInfoType::LoadInfo | ServerInfoType::All => {
                        let mut system = SYS_INFO.lock().unwrap();
                        system.refresh_networks_list();
                        system.refresh_all();
                        let load = (
                            sys::cpu_time_snapshot(),
                            system
                                .get_networks()
                                .into_iter()
                                .map(|(n, d)| {
                                    (n.to_owned(), sys::NicSnapshot::from_network_data(d))
                                })
                                .collect(),
                            ioload::IoLoad::snapshot(),
                        );
                        let when = Instant::now() + Duration::from_millis(1000);
                        (Some(load), when)
                    }
                    _ => (None, Instant::now()),
                };
                Ok(s)
            })
            .and_then(|(load, when)| {
                let timer = GLOBAL_TIMER_HANDLE.clone();
                timer.delay(when).then(|_| Ok(load))
            })
            .and_then(move |load| -> Result<ServerInfoResponse> {
                let mut server_infos = Vec::new();
                match req.get_tp() {
                    ServerInfoType::HardwareInfo => sys::hardware_info(&mut server_infos),
                    ServerInfoType::LoadInfo => sys::load_info(load.unwrap(), &mut server_infos),
                    ServerInfoType::SystemInfo => sys::system_info(&mut server_infos),
                    ServerInfoType::All => {
                        sys::hardware_info(&mut server_infos);
                        sys::load_info(load.unwrap(), &mut server_infos);
                        sys::system_info(&mut server_infos);
                    }
                };
                // Sort pairs by key to make result stable
                server_infos
                    .sort_by(|a, b| (a.get_tp(), a.get_name()).cmp(&(b.get_tp(), b.get_name())));
                let mut resp = ServerInfoResponse::default();
                resp.set_items(server_infos.into());
                Ok(resp)
            });
        let f = self
            .pool
            .spawn(collect)
            .and_then(|res| sink.success(res).map_err(Error::from))
            .map_err(|e| debug!("Diagnostics rpc failed"; "err" => ?e));
        ctx.spawn(f);
    }
}
