// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self, ErrorKind};

use anyhow::Context;
use futures::executor::block_on;
use futures_io::{AsyncRead, AsyncWrite};
use kvproto::brpb as proto;
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
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
    req.set_object_name(storage_name.to_string());
    let restore_str = restore_name.to_str().ok_or_else(|| {
        io::Error::new(
            ErrorKind::InvalidData,
            format!("could not convert to str {:?}", &restore_name),
        )
    })?;
    req.set_restore_name(restore_str.to_string());
    req.set_content_length(expected_length);
    let mut sb = proto::StorageBackend::default();
    sb.backend = Some(backend);
    req.set_storage_backend(sb);
    Ok(req)
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

pub fn file_name_for_write(storage_name: &str, object_name: &str) -> std::path::PathBuf {
    let full_name = format!("{}-{}", storage_name, object_name);
    std::env::temp_dir().join(full_name)
}

pub struct DropPath(pub std::path::PathBuf);

impl Drop for DropPath {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
