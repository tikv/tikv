// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::export::{create_storage_no_client, read_external_storage_into_file, ExternalStorage};
use anyhow::Context;
use file_system::File;
use futures::executor::block_on;
use futures_io::{AsyncRead, AsyncWrite};
use kvproto::backup as proto;
#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;
use slog_global::{error, info};
use std::f64::INFINITY;
use std::io::{self, ErrorKind};
use tikv_util::time::Limiter;
use tokio::runtime::Runtime;
use tokio_util::compat::Tokio02AsyncReadCompatExt;

pub fn write_sender(
    runtime: &Runtime,
    backend: Backend,
    file_path: std::path::PathBuf,
    name: &str,
    reader: Box<dyn AsyncRead + Send + Unpin>,
    content_length: u64,
) -> io::Result<proto::ExternalStorageWriteRequest> {
    (|| -> anyhow::Result<proto::ExternalStorageWriteRequest> {
        // TODO: the reader should write direct to the file_path
        // currently it is copying into an intermediate buffer
        // Writing to a file here uses up disk space
        // But as a positive it gets the backup data out of the DB the fastest
        // Currently this waits for the file to be completely written before sending to storage
        runtime.enter(|| {
            block_on(async {
                let msg = |action: &str| format!("{} file {:?}", action, &file_path);
                let f = tokio::fs::File::create(file_path.clone())
                    .await
                    .context(msg("create"))?;
                let mut writer: Box<dyn AsyncWrite + Unpin + Send> = Box::new(Box::pin(f.compat()));
                futures_util::io::copy(reader, &mut writer)
                    .await
                    .context(msg("copy"))
            })
        })?;
        let mut req = proto::ExternalStorageWriteRequest::default();
        req.set_object_name(name.to_string());
        req.set_content_length(content_length);
        let mut sb = proto::StorageBackend::default();
        sb.backend = Some(backend);
        req.set_storage_backend(sb);
        Ok(req)
    })()
    .context("write_sender")
    .map_err(anyhow_to_io_log_error)
}

pub fn restore_sender(
    backend: Backend,
    storage_name: &str,
    restore_name: std::path::PathBuf,
    expected_length: u64,
    _speed_limiter: &Limiter,
) -> io::Result<proto::ExternalStorageRestoreRequest> {
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
    sb.backend = Some(backend);
    req.set_storage_backend(sb);
    Ok(req)
}

pub fn write_receiver(
    runtime: &Runtime,
    req: proto::ExternalStorageWriteRequest,
) -> anyhow::Result<()> {
    let storage_backend = req.get_storage_backend();
    let object_name = req.get_object_name();
    let content_length = req.get_content_length();
    let storage = create_storage_no_client(storage_backend).context("create storage")?;
    let file_path = file_name_for_write(storage.name(), object_name);
    let reader = runtime
        .enter(|| block_on(open_file_as_async_read(file_path)))
        .context("open file")?;
    storage
        .write(object_name, reader, content_length)
        .context("storage write")
}

pub fn restore_receiver(
    runtime: &Runtime,
    req: proto::ExternalStorageRestoreRequest,
) -> io::Result<()> {
    let object_name = req.get_object_name();
    let storage_backend = req.get_storage_backend();
    let file_name = std::path::PathBuf::from(req.get_restore_name());
    let expected_length = req.get_content_length();
    runtime.enter(|| {
        block_on(restore_inner(
            storage_backend,
            object_name,
            file_name,
            expected_length,
        ))
    })
}

pub async fn restore_inner(
    storage_backend: &proto::StorageBackend,
    object_name: &str,
    file_name: std::path::PathBuf,
    expected_length: u64,
) -> io::Result<()> {
    let storage = create_storage_no_client(&storage_backend)?;
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

pub fn anyhow_to_io_log_error(err: anyhow::Error) -> io::Error {
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

pub struct DropPath(pub std::path::PathBuf);

impl Drop for DropPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

pub fn file_name_for_write(storage_name: &str, object_name: &str) -> std::path::PathBuf {
    let full_name = format!("{}-{}", storage_name, object_name);
    std::env::temp_dir().join(full_name)
}
