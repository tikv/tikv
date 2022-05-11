// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use anyhow::Context;
use futures_io::AsyncRead;
use grpcio::{self};
use kvproto::brpb as proto;
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
use tikv_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};

use crate::{
    request::{
        anyhow_to_io_log_error, file_name_for_write, restore_sender, write_sender, DropPath,
    },
    ExternalStorage,
};

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

fn new_rpc_client() -> io::Result<proto::ExternalStorageClient> {
    let env = Arc::new(grpcio::EnvBuilder::new().build());
    let grpc_socket_path = "/tmp/grpc-external-storage.sock";
    let socket_addr = format!("unix:{}", grpc_socket_path);
    let channel = grpcio::ChannelBuilder::new(env).connect(&socket_addr);
    Ok(proto::ExternalStorageClient::new(channel))
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
                .save(&req)
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
