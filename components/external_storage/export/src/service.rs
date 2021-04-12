// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use external_storage::ExternalStorage;

use std::io::{self, ErrorKind};
use std::sync::Arc;

use crate::request::{
    anyhow_to_io_log_error, file_name_for_write, restore_receiver, restore_sender, write_receiver,
    write_sender, DropPath,
};
use anyhow::Context;
use futures_io::AsyncRead;
use grpcio::{self};
use kvproto::backup as proto;
#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;
use slog_global::{error, info};
use tikv_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};

struct ExternalStorageClient {
    backend: Backend,
    runtime: Arc<Runtime>,
    rpc: proto::ExternalStorageClient,
    name: &'static str,
    url: url::Url,
}

pub fn new_client(
    backend: Backend,
    name: &'static str,
    url: url::Url,
) -> io::Result<Box<dyn ExternalStorage>> {
    let runtime = Builder::new()
        .basic_scheduler()
        .thread_name("external-storage-grpc-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    Ok(Box::new(ExternalStorageClient {
        backend,
        runtime: Arc::new(runtime),
        rpc: new_rpc_client()?,
        name,
        url,
    }))
}

impl ExternalStorage for ExternalStorageClient {
    fn name(&self) -> &'static str {
        self.name
    }

    fn url(&self) -> io::Result<url::Url> {
        Ok(self.url.clone())
    }

    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        info!("external storage writing");
        (|| -> anyhow::Result<()> {
            let file_path = file_name_for_write(&self.name, &name);
            let req = write_sender(
                &self.runtime,
                self.backend.clone(),
                file_path.clone(),
                name,
                reader,
                content_length,
            )?;
            info!("grpc write request");
            self.rpc
                .write(&req)
                .map_err(rpc_error_to_io)
                .context("rpc write")?;
            info!("grpc write request finished");
            DropPath(file_path);
            Ok(())
        })()
        .context("external storage write")
        .map_err(anyhow_to_io_log_error)
    }

    fn read(&self, _name: &str) -> Box<dyn AsyncRead + Unpin> {
        unimplemented!("use restore instead of read")
    }

    fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
    ) -> io::Result<()> {
        info!("external storage restore");
        let req = restore_sender(
            self.backend.clone(),
            storage_name,
            restore_name,
            expected_length,
            speed_limiter,
        )?;
        self.rpc.restore(&req).map_err(rpc_error_to_io).map(|_| ())
    }
}

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

fn new_rpc_client() -> io::Result<proto::ExternalStorageClient> {
    let env = Arc::new(grpcio::EnvBuilder::new().build());
    let grpc_socket_path = "/tmp/grpc-external-storage.sock";
    let socket_addr = format!("unix:{}", grpc_socket_path);
    let channel = grpcio::ChannelBuilder::new(env).connect(&socket_addr);
    Ok(proto::ExternalStorageClient::new(channel))
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
    fn write(
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

pub fn rpc_error_to_io(err: grpcio::Error) -> io::Error {
    let msg = format!("{}", err);
    match err {
        grpcio::Error::RpcFailure(status) => match status.status {
            grpcio::RpcStatusCode::NOT_FOUND => io::Error::new(ErrorKind::NotFound, msg),
            grpcio::RpcStatusCode::INVALID_ARGUMENT => io::Error::new(ErrorKind::InvalidInput, msg),
            grpcio::RpcStatusCode::UNAUTHENTICATED => {
                io::Error::new(ErrorKind::PermissionDenied, msg)
            }
            _ => io::Error::new(ErrorKind::Other, msg),
        },
        _ => io::Error::new(ErrorKind::Other, msg),
    }
}

fn bind_socket(
    socket_path: &std::path::PathBuf,
) -> anyhow::Result<std::os::unix::net::UnixListener> {
    let msg = format!("bind socket {:?}", &socket_path);
    info!("{}", msg);
    std::os::unix::net::UnixListener::bind(&socket_path).context(msg)
}
