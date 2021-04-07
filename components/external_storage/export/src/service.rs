// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use crate::{create_storage, create_storage_no_client};
use external_storage::{read_external_storage_into_file, ExternalStorage};

use std::f64::INFINITY;
use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};

use anyhow::Context;
use file_system::File;
use futures_io::{AsyncRead, AsyncWrite};
use grpcio::{self};
use kvproto::backup as proto;
#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;
use slog_global::{error, info};
use tikv_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};
use tokio_util::compat::Tokio02AsyncReadCompatExt;

pub struct ExternalStorageClient {
    backend: Backend,
    runtime: Arc<Mutex<Runtime>>,
    rpc: proto::ExternalStorageClient,
    blob_storage: Arc<Box<dyn ExternalStorage>>,
}

pub fn new_client(
    backend: Backend,
    blob_storage: Box<dyn ExternalStorage>,
) -> io::Result<ExternalStorageClient> {
    let runtime = Builder::new()
        .basic_scheduler()
        .thread_name("external-storage-grpc-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    Ok(ExternalStorageClient {
        backend,
        runtime: Arc::new(Mutex::new(runtime)),
        blob_storage: Arc::new(blob_storage),
        rpc: new_rpc_client()?,
    })
}

impl ExternalStorage for ExternalStorageClient {
    fn name(&self) -> &'static str {
        self.blob_storage.name()
    }

    fn url(&self) -> io::Result<url::Url> {
        self.blob_storage.url()
    }

    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        info!("external storage writing");
        (|| -> anyhow::Result<()> {
            let object_name = name.to_string();
            // TODO: the reader should write direct to this file
            // currently it is copying into an intermediate buffer
            // Writing to a file here uses up disk space
            // But as a positive it gets the backup data out of the DB the fastest
            // Currently this waits for the file to be completely written before sending to storage
            let file_path = socket_name(&*self.blob_storage, &object_name);
            self.runtime.lock().unwrap().block_on(async {
                let msg = |action: &str| format!("{} file {:?}", action, &file_path);
                info!("{}", msg("create"));
                let f = tokio::fs::File::create(file_path.clone())
                    .await
                    .context(msg("create"))?;
                let mut writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(Box::pin(f.compat()));
                info!("{}", msg("copy"));
                futures_util::io::copy(reader, &mut writer)
                    .await
                    .context(msg("copy"))
            })?;
            let mut req = proto::ExternalStorageWriteRequest::default();
            req.set_object_name(name.to_string());
            req.set_content_length(content_length);
            let mut sb = proto::StorageBackend::default();
            sb.backend = Some(self.backend.clone());
            req.set_storage_backend(sb);
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
        _speed_limiter: &Limiter,
    ) -> io::Result<()> {
        info!("external storage restore");
        // TODO: send speed_limiter
        let mut req = proto::ExternalStorageRestoreRequest::default();
        req.set_object_name(storage_name.to_string().clone());
        let restore_str = restore_name.to_str().ok_or(io::Error::new(
            ErrorKind::InvalidData,
            format!("could not convert to str {:?}", &restore_name),
        ))?;
        req.set_restore_name(restore_str.to_string());
        req.set_content_length(expected_length);
        let mut sb = proto::StorageBackend::default();
        sb.backend = Some(self.backend.clone());
        req.set_storage_backend(sb);
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
    runtime: Arc<Mutex<Runtime>>,
}

impl Service {
    /// Create a new backup service.
    pub fn new() -> io::Result<Service> {
        let runtime = Arc::new(Mutex::new(
            Builder::new()
                .basic_scheduler()
                .thread_name("external-storage-grpc-service")
                .core_threads(1)
                .enable_all()
                .build()?,
        ));
        Ok(Service { runtime })
    }
}

fn socket_name(storage: &dyn ExternalStorage, object_name: &str) -> std::path::PathBuf {
    let full_name = format!("{}-{}", storage.name(), object_name);
    std::env::temp_dir().join(full_name)
}

fn write_inner(
    runtime: &mut Runtime,
    storage_backend: &proto::StorageBackend,
    object_name: &str,
    content_length: u64,
) -> anyhow::Result<()> {
    let storage = create_storage_no_client(storage_backend).context("create storage")?;
    let file_path = socket_name(&storage, object_name);
    let reader = runtime
        .block_on(open_file_as_async_read(file_path))
        .context("open file")?;
    storage
        .write(object_name, reader, content_length)
        .context("storage write")
}

impl proto::ExternalStorage for Service {
    fn write(
        &mut self,
        _ctx: grpcio::RpcContext,
        req: proto::ExternalStorageWriteRequest,
        sink: grpcio::UnarySink<proto::ExternalStorageWriteResponse>,
    ) {
        info!("write request {:?}", req.get_object_name());
        let result = write_inner(
            &mut self.runtime.lock().unwrap(),
            req.get_storage_backend(),
            req.get_object_name(),
            req.get_content_length(),
        );
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
        let object_name = req.get_object_name();
        let storage_backend = req.get_storage_backend();
        let file_name = std::path::PathBuf::from(req.get_restore_name());
        let expected_length = req.get_content_length();
        let result = self.runtime.lock().unwrap().block_on(restore_inner(
            storage_backend,
            object_name,
            file_name,
            expected_length,
        ));
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

async fn restore_inner(
    storage_backend: &proto::StorageBackend,
    object_name: &str,
    file_name: std::path::PathBuf,
    expected_length: u64,
) -> io::Result<()> {
    let storage = create_storage(&storage_backend)?;
    // TODO: support encryption. The service must be launched with or sent a DataKeyManager
    let output: &mut dyn io::Write = &mut File::create(file_name)?;
    // the minimum speed of reading data, in bytes/second.
    // if reading speed is slower than this rate, we will stop with
    // a "TimedOut" error.
    // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
    const MINIMUM_READ_SPEED: usize = 8192;
    let limiter = Limiter::new(INFINITY);
    let x = read_external_storage_into_file(
        &mut storage.read(object_name),
        output,
        &limiter,
        expected_length,
        MINIMUM_READ_SPEED,
    )
    .await;
    x
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

fn anyhow_to_io_log_error(err: anyhow::Error) -> io::Error {
    let string = format!("{:#}", &err);
    match err.downcast::<io::Error>() {
        Ok(e) => {
            // It will be difficult to propagate the context
            // without changing the error type to anyhow or a custom TiKV error
            error!("{}", string);
            e
        }
        Err(_) => io::Error::new(ErrorKind::Other, string),
    }
}

async fn open_file_as_async_read(
    file_path: std::path::PathBuf,
) -> anyhow::Result<Box<dyn AsyncRead + Unpin + Send>> {
    info!("open file {:?}", &file_path);
    let f = tokio::fs::File::open(file_path)
        .await
        .context("open file")?;
    let reader: Box<dyn AsyncRead + Unpin + Send> = Box::new(Box::pin(f.compat()));
    Ok(reader)
}

fn bind_socket(
    socket_path: &std::path::PathBuf,
) -> anyhow::Result<std::os::unix::net::UnixListener> {
    let msg = format!("bind socket {:?}", &socket_path);
    info!("{}", msg);
    std::os::unix::net::UnixListener::bind(&socket_path).context(msg)
}

struct DropPath(std::path::PathBuf);

impl Drop for DropPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
