// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
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
use lazy_static::lazy_static;
use tikv_util::{
    sys::{SystemExt, ioload},
    timer::GLOBAL_TIMER_HANDLE,
};
use tokio::runtime::Handle;

use crate::server::Error;

mod log;
/// Information about the current hardware and operating system.
pub mod sys;

lazy_static! {
    // Process CPU usage is computed across successive scans of the same System.
    static ref PROCESS_SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
    static ref LOAD_SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
    static ref HW_SYS_INFO: Mutex<sysinfo::System> = Mutex::new(sysinfo::System::new());
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

/// Captures the initial CPU, network, and block I/O samples for a load request.
fn load_info_snapshot() -> (
    sys::CpuTimeSnapshot,
    HashMap<String, sys::NicSnapshot>,
    HashMap<String, ioload::IoLoad>,
) {
    let prev_nic = {
        let mut system = LOAD_SYS_INFO.lock().unwrap();
        fail::fail_point!("diagnostics_load_info_snapshot_holds_lock");
        system.refresh_networks_list();
        system.refresh_networks();
        system
            .networks()
            .into_iter()
            .map(|(n, d)| (n.to_owned(), sys::NicSnapshot::from_network_data(d)))
            .collect()
    };
    (
        sys::cpu_time_snapshot(),
        prev_nic,
        ioload::IoLoad::snapshot(),
    )
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
                    let load = load_info_snapshot();
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

#[cfg(all(test, feature = "failpoints"))]
mod lock_split_regression_tests {
    use std::{
        sync::{Arc, OnceLock, mpsc},
        thread::{self, ThreadId},
    };

    use super::{
        sys::{hardware_info, process_info},
        *,
    };

    const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

    /// Checks that `hardware_info` finishes while the lock is held.
    fn assert_probe_finishes_while_holder_holds_lock(
        fail_point: &'static str,
        holder_body: impl FnOnce() + Send + 'static,
    ) {
        let _scenario = fail::FailScenario::setup();

        let (entered_tx, entered_rx) = mpsc::channel::<()>();
        let (release_tx, release_rx) = mpsc::channel::<()>();
        let (done_tx, done_rx) = mpsc::channel::<()>();

        // Other tests may call the same collector. Pause only the designated holder.
        let holder_id = Arc::new(OnceLock::<ThreadId>::new());

        let callback_holder_id = Arc::clone(&holder_id);
        let release_rx = Mutex::new(release_rx);
        let guard = fail::FailGuard::with_callback(fail_point, move || {
            if callback_holder_id.get().copied() == Some(thread::current().id()) {
                let _ = entered_tx.send(());
                let _ = release_rx.lock().unwrap().recv();
            }
        })
        .unwrap();

        let spawn_holder_id = Arc::clone(&holder_id);
        let holder = thread::spawn(move || {
            let _ = spawn_holder_id.set(thread::current().id());
            holder_body();
        });

        let entered = entered_rx.recv_timeout(HANDSHAKE_TIMEOUT);

        let mut probe_thread: Option<thread::JoinHandle<()>> = None;
        let done = if entered.is_ok() {
            probe_thread = Some(thread::spawn(move || {
                let mut items = Vec::new();
                hardware_info(&mut items);
                let _ = done_tx.send(());
            }));
            Some(done_rx.recv_timeout(HANDSHAKE_TIMEOUT))
        } else {
            None
        };

        drop(release_tx);
        drop(guard);
        let holder_result = holder.join();
        let probe_result = probe_thread.take().map(|probe| {
            // Join the probe even if the holder panicked.
            probe.join()
        });

        if let Err(panic) = holder_result {
            std::panic::resume_unwind(panic);
        }
        if let Some(Err(panic)) = probe_result {
            std::panic::resume_unwind(panic);
        }

        assert!(
            entered.is_ok(),
            "the holder never reached the fail point {:?} within {:?}; it may not have been \
             scheduled, or the fail-point registry was cleared concurrently",
            fail_point,
            HANDSHAKE_TIMEOUT
        );
        assert!(
            done.expect("entered was received, so the probe must have been started")
                .is_ok(),
            "hardware_info never finished while the holder held the lock behind {:?}: the two \
             paths share one lock (tikv#20372 regression)",
            fail_point
        );
    }

    #[test]
    fn test_process_scan_does_not_block_hardware_info() {
        assert_probe_finishes_while_holder_holds_lock(
            "diagnostics_process_info_holds_lock",
            || {
                let mut items = Vec::new();
                process_info(&mut items);
            },
        );
    }

    #[test]
    fn test_load_snapshot_does_not_block_hardware_info() {
        assert_probe_finishes_while_holder_holds_lock(
            "diagnostics_load_info_snapshot_holds_lock",
            || {
                let _snapshot = load_info_snapshot();
            },
        );
    }
}
