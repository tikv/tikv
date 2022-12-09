// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use anyhow::Context;
use external_storage::request::anyhow_to_io_log_error;
use grpcio::{self};
use kvproto::brpb as proto;
use slog_global::{error, info};
use tokio::runtime::{Builder, Runtime};

use crate::request::{restore_receiver, write_receiver};

#[derive(Debug)]
pub struct SocketService {
    server: grpcio::Server,
    listener: std::os::unix::net::UnixListener,
}

pub fn new_service() -> io::Result<SocketService> {
    (|| -> anyhow::Result<SocketService> {
        let env = Arc::new(grpcio::EnvBuilder::new().build());
        let storage_service = Service::new().context("new storage service")?;
        let builder = grpcio::ServerBuilder::new(env)
            .register_service(proto::create_external_storage(storage_service));
        let grpc_socket_path = "/tmp/grpc-external-storage.sock";
        let socket_addr = format!("unix:{}", grpc_socket_path);
        let socket_path = std::path::PathBuf::from(grpc_socket_path);
        // Keep the listener in scope: otherwise the socket is destroyed
        let listener = bind_socket(&socket_path).context("GRPC new service create socket")?;
        let mut server = builder
            .bind(socket_addr, 0)
            .build()
            .context("GRPC build server")?;
        server.start();
        let (..) = server.bind_addrs().next().context("GRPC bind server")?;
        Ok(SocketService { server, listener })
    })()
    .context("new service")
    .map_err(anyhow_to_io_log_error)
}

/// Service handles the RPC messages for the `ExternalStorage` service.
#[derive(Clone)]
pub struct Service {
    runtime: Arc<Runtime>,
}

impl Service {
    /// Create a new backup service.
    pub fn new() -> io::Result<Service> {
        let runtime = Arc::new(
            Builder::new()
                .basic_scheduler()
                .thread_name("external-storage-grpc-service")
                .core_threads(1)
                .enable_all()
                .build()?,
        );
        Ok(Service { runtime })
    }
}

impl proto::ExternalStorage for Service {
    fn save(
        &mut self,
        _ctx: grpcio::RpcContext,
        req: proto::ExternalStorageWriteRequest,
        sink: grpcio::UnarySink<proto::ExternalStorageWriteResponse>,
    ) {
        info!("write request {:?}", req.get_object_name());
        let result = write_receiver(&self.runtime, req);
        match result {
            Ok(_) => {
                let rsp = proto::ExternalStorageWriteResponse::default();
                info!("success write");
                sink.success(rsp);
            }
            Err(e) => {
                error!("write {}", e);
                sink.fail(make_rpc_error(anyhow_to_io_log_error(e)));
            }
        }
    }

    fn restore(
        &mut self,
        _ctx: grpcio::RpcContext,
        req: proto::ExternalStorageRestoreRequest,
        sink: grpcio::UnarySink<proto::ExternalStorageRestoreResponse>,
    ) {
        info!(
            "restore request {:?} {:?}",
            req.get_object_name(),
            req.get_restore_name()
        );
        let result = restore_receiver(&self.runtime, req);
        match result {
            Ok(_) => {
                let rsp = proto::ExternalStorageRestoreResponse::default();
                info!("success restore");
                sink.success(rsp);
            }
            Err(e) => {
                error!("restore {}", e);
                sink.fail(make_rpc_error(e));
            }
        }
    }
}

pub fn make_rpc_error(err: io::Error) -> grpcio::RpcStatus {
    grpcio::RpcStatus::new(
        match err.kind() {
            ErrorKind::NotFound => grpcio::RpcStatusCode::NOT_FOUND,
            ErrorKind::InvalidInput => grpcio::RpcStatusCode::INVALID_ARGUMENT,
            ErrorKind::PermissionDenied => grpcio::RpcStatusCode::UNAUTHENTICATED,
            _ => grpcio::RpcStatusCode::UNKNOWN,
        },
        Some(format!("{:?}", err)),
    )
}

fn bind_socket(socket_path: &std::path::Path) -> anyhow::Result<std::os::unix::net::UnixListener> {
    let msg = format!("bind socket {:?}", &socket_path);
    info!("{}", msg);
    std::os::unix::net::UnixListener::bind(&socket_path).context(msg)
}
