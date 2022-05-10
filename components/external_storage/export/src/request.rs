// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self};

use anyhow::Context;
use external_storage::request::file_name_for_write;
use file_system::File;
use futures::executor::block_on;
use futures_io::AsyncRead;
use kvproto::brpb as proto;
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
use slog_global::info;
use tikv_util::time::Limiter;
use tokio::runtime::Runtime;
use tokio_util::compat::Tokio02AsyncReadCompatExt;

use crate::export::{create_storage_no_client, read_external_storage_into_file, ExternalStorage};

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
    let limiter = Limiter::new(f64::INFINITY);
    let x = read_external_storage_into_file(
        &mut storage.read(object_name),
        output,
        &limiter,
        expected_length,
        None,
        MINIMUM_READ_SPEED,
    )
    .await;
    x
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
