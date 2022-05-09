// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    io::{self, ErrorKind},
    sync::Arc,
};

use anyhow::Context;
use futures_io::AsyncRead;
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
use protobuf::{self, Message};
use slog_global::info;
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
    library: libloading::Library,
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
        .thread_name("external-storage-dylib-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    let library = unsafe {
        libloading::Library::new(
            std::path::Path::new("./")
                .join(libloading::library_filename("external_storage_export")),
        )
        .map_err(libloading_err_to_io)?
    };
    external_storage_init_ffi_dynamic(&library)?;
    Ok(Box::new(ExternalStorageClient {
        runtime: Arc::new(runtime),
        backend,
        library,
        name,
        url,
    }) as _)
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
            let bytes = req.write_to_bytes()?;
            info!("write request");
            call_ffi_dynamic(&self.library, b"external_storage_write", bytes)?;
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
        let bytes = req.write_to_bytes()?;
        call_ffi_dynamic(&self.library, b"external_storage_restore", bytes)
    }
}

pub fn extern_to_io_err(e: ffi_support::ExternError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
}

type FfiInitFn<'a> =
    libloading::Symbol<'a, unsafe extern "C" fn(error: &mut ffi_support::ExternError) -> ()>;
type FfiFn<'a> = libloading::Symbol<
    'a,
    unsafe extern "C" fn(error: &mut ffi_support::ExternError, bytes: Vec<u8>) -> (),
>;

fn external_storage_init_ffi_dynamic(library: &libloading::Library) -> io::Result<()> {
    let mut e = ffi_support::ExternError::default();
    unsafe {
        let func: FfiInitFn = library
            .get(b"external_storage_init")
            .map_err(libloading_err_to_io)?;
        func(&mut e);
    }
    if e.get_code() != ffi_support::ErrorCode::SUCCESS {
        return Err(extern_to_io_err(e));
    }
    Ok(())
}

fn call_ffi_dynamic(
    library: &libloading::Library,
    fn_name: &[u8],
    bytes: Vec<u8>,
) -> io::Result<()> {
    let mut e = ffi_support::ExternError::default();
    unsafe {
        let func: FfiFn = library.get(fn_name).map_err(libloading_err_to_io)?;
        func(&mut e, bytes);
    }
    if e.get_code() != ffi_support::ErrorCode::SUCCESS {
        return Err(extern_to_io_err(e));
    }
    Ok(())
}

fn libloading_err_to_io(e: libloading::Error) -> io::Error {
    // TODO: custom error type
    let kind = match e {
        libloading::Error::DlOpen { .. } | libloading::Error::DlOpenUnknown => {
            ErrorKind::AddrNotAvailable
        }
        _ => ErrorKind::Other,
    };
    io::Error::new(kind, format!("{}", e))
}
