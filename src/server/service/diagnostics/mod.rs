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
use grpcio::{
    Result as GrpcResult, RpcContext, RpcStatus, RpcStatusCode, ServerStreamingSink, UnarySink,
    WriteFlags,
};
use kvproto::diagnosticspb::{
    Diagnostics, SearchLogRequest, SearchLogRequestTarget, SearchLogResponse, ServerInfoRequest,
    ServerInfoResponse, ServerInfoType,
};
use tikv_util::{sys::SystemExt, timer::GLOBAL_TIMER_HANDLE};
use tokio::runtime::Handle;

use crate::server::Error;

mod ioload;
mod log;
mod sys;

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

impl Diagnostics for Service {
    fn search_log(
        &mut self,
        ctx: RpcContext<'_>,
        req: SearchLogRequest,
        mut sink: ServerStreamingSink<SearchLogResponse>,
    ) {
        let log_file = if req.get_target() == SearchLogRequestTarget::Normal {
            self.log_file.to_owned()
        } else {
            self.slow_log_file.to_owned()
        };

        let stream = self.pool.spawn(async move {
            log::search(log_file, req)
                .map(|stream| stream.map(|resp| (resp, WriteFlags::default().buffer_hint(true))))
                .map_err(|e| {
                    grpcio::Error::RpcFailure(RpcStatus::with_message(
                        RpcStatusCode::UNKNOWN,
                        format!("{:?}", e),
                    ))
                })
        });

        let f = self
            .pool
            .spawn(async move {
                match stream.await.unwrap() {
                    Ok(s) => {
                        let res = async move {
                            sink.send_all(&mut s.map(Ok)).await?;
                            sink.close().await?;
                            GrpcResult::Ok(())
                        }
                        .await;
                        if let Err(e) = res {
                            error!("search log RPC error"; "error" => ?e);
                        }
                    }
                    Err(e) => {
                        error!("search log RPC error"; "error" => ?e);
                    }
                }
            })
            .map(|res| res.unwrap());

        ctx.spawn(f);
    }

    fn server_info(
        &mut self,
        ctx: RpcContext<'_>,
        req: ServerInfoRequest,
        sink: UnarySink<ServerInfoResponse>,
    ) {
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
                            .get_networks()
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

        let f = self.pool.spawn(collect).then(|res| async move {
            let res = sink.success(res.unwrap()).map_err(Error::from).await;
            if let Err(e) = res {
                debug!("Diagnostics rpc failed"; "err" => ?e);
            }
        });

        ctx.spawn(f);
    }
}
