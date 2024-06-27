// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Mutex,
    time::{Duration, Instant},
};

use futures::{
    compat::Future01CompatExt,
    future::{FutureExt, TryFutureExt},
    sink::SinkExt,
    stream::StreamExt,
};
use kvproto::{
    diagnosticspb::{
        SearchLogRequest, SearchLogRequestTarget, SearchLogResponse, ServerInfoRequest,
        ServerInfoResponse, ServerInfoType,
    },
    diagnosticspb_grpc::diagnostics_server::Diagnostics,
};
use tikv_util::{
    sys::{ioload, SystemExt},
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::runtime::Handle;

use crate::server::Error;

mod log;
/// Information about the current hardware and operating system.
pub mod sys;

lazy_static! {
    pub static ref SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
}

/// Service handles the RPC messages for the `Diagnostics` service.
#[derive(Clone)]
pub struct Service {
    pool: Handle,
    log_file: String,
    slow_log_file: String,
}

impl Service {
    pub fn new(pool: Handle, log_file: String, slow_log_file: String) -> Self {
        Service {
            pool,
            log_file,
            slow_log_file,
        }
    }
}

#[tonic::async_trait]
impl Diagnostics for Service {
    type search_logStream = tonic::codegen::BoxStream<SearchLogResponse>;
    async fn search_log(
        &self,
        request: tonic::Request<SearchLogRequest>,
    ) -> std::result::Result<
        tonic::Response<tonic::codegen::BoxStream<SearchLogResponse>>,
        tonic::Status,
    > {
        let req = request.into_inner();
        let log_file = if req.get_target() == SearchLogRequestTarget::Normal {
            self.log_file.to_owned()
        } else {
            self.slow_log_file.to_owned()
        };

        let join_handle = self.pool.spawn(async move {
            log::search(log_file, req)
                .map(|stream| {
                    stream.map(|resp| {
                        let res: tonic::Result<_> = Ok(resp);
                        res
                    })
                })
                .map_err(|e| tonic::Status::unknown(format!("{:?}", e)))
        });

        let stream = join_handle.await.unwrap()?;

        Ok(tonic::Response::new(Box::pin(stream) as _))
    }

    async fn server_info(
        &self,
        request: tonic::Request<ServerInfoRequest>,
    ) -> tonic::Result<tonic::Response<ServerInfoResponse>> {
        let req = request.into_inner();
        let tp = req.get_tp();

        let collect = async move {
            let (load, when) = match tp {
                ServerInfoType::LoadInfo | ServerInfoType::All => {
                    let mut system = SYS_INFO.lock().unwrap();
                    system.refresh_networks_list();
                    system.refresh_all();
                    let load = (
                        sys::cpu_time_snapshot(),
                        system
                            .networks()
                            .into_iter()
                            .map(|(n, d)| (n.to_owned(), sys::NicSnapshot::from_network_data(d)))
                            .collect(),
                        ioload::IoLoad::snapshot(),
                    );
                    let when = Instant::now() + Duration::from_millis(1000);
                    (Some(load), when)
                }
                _ => (None, Instant::now()),
            };

            let timer = GLOBAL_TIMER_HANDLE.clone();
            let _ = timer.delay(when).compat().await;

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
            resp
        };

        let resp = self.pool.spawn(collect).await.unwrap();
        Ok(tonic::Response::new(resp))
    }
}
