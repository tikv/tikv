// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::request::{
    anyhow_to_io_log_error, file_name_for_write, restore_receiver, restore_sender, write_receiver,
    write_sender, DropPath,
};
use anyhow::Context;
use external_storage::ExternalStorage;
use futures_io::AsyncRead;
use kvproto::backup as proto;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use protobuf::{self, Message};
use slog_global::{error, info, warn};
use std::io::{self, ErrorKind};
use std::sync::{Arc, Mutex};
use tikv_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;

struct ExternalStorageClient {
    backend: Backend,
    runtime: Arc<Runtime>,
    blob_storage: Arc<Box<dyn ExternalStorage>>,
    library: Option<libloading::Library>,
}

pub fn new_client(
    backend: Backend,
    blob_storage: Box<dyn ExternalStorage>,
) -> io::Result<Box<dyn ExternalStorage>> {
    let runtime = Builder::new()
        .basic_scheduler()
        .thread_name("external-storage-dylib-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    let library = unsafe {
        match libloading::Library::new(
            std::path::Path::new("./")
                .join(libloading::library_filename("external_storage_export")),
        ) {
            Ok(lib) => Some(lib),
            Err(libloading::Error::DlOpen { .. }) | Err(libloading::Error::DlOpenUnknown) => {
                warn!("could not open external_storage_export");
                None
            }
            Err(e) => return Err(libloading_err_to_io(e)),
        }
    };
    match &library {
        None => external_storage_init_ffi()?,
        Some(lib) => external_storage_init_ffi_dynamic(lib)?,
    }
    Ok(Box::new(ExternalStorageClient {
        runtime: Arc::new(runtime),
        backend,
        blob_storage: Arc::new(blob_storage),
        library,
    }) as _)
}

fn external_storage_init_ffi() -> io::Result<()> {
    let mut e = ffi_support::ExternError::default();
    external_storage_init(&mut e);
    if e.get_code() != ffi_support::ErrorCode::SUCCESS {
        Err(extern_to_io_err(e))?;
    }
    Ok(())
}

impl ExternalStorageClient {
    fn external_storage_write(&self, bytes: Vec<u8>) -> io::Result<()> {
        match &self.library {
            Some(library) => call_ffi_dynamic(&library, b"external_storage_write", bytes),
            None => {
                let mut e = ffi_support::ExternError::default();
                unsafe {
                    external_storage_write(bytes.as_ptr(), bytes.len() as i32, &mut e);
                }
                if e.get_code() != ffi_support::ErrorCode::SUCCESS {
                    Err(extern_to_io_err(e))
                } else {
                    Ok(())
                }
            }
        }
    }

    fn external_storage_restore(&self, bytes: Vec<u8>) -> io::Result<()> {
        match &self.library {
            Some(library) => call_ffi_dynamic(&library, b"external_storage_restore", bytes),
            None => {
                let mut e = ffi_support::ExternError::default();
                unsafe {
                    external_storage_restore(bytes.as_ptr(), bytes.len() as i32, &mut e);
                }
                if e.get_code() != ffi_support::ErrorCode::SUCCESS {
                    Err(extern_to_io_err(e))
                } else {
                    Ok(())
                }
            }
        }
    }
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
            let file_path = file_name_for_write(&*self.blob_storage, &name);
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
            self.external_storage_write(bytes)?;
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
        self.external_storage_restore(bytes)
    }
}

fn extern_to_io_err(e: ffi_support::ExternError) -> io::Error {
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
        Err(extern_to_io_err(e))?;
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
        Err(extern_to_io_err(e))?;
    }
    Ok(())
}

static RUNTIME: OnceCell<Runtime> = OnceCell::new();
lazy_static! {
    static ref RUNTIME_INIT: Mutex<()> = Mutex::new(());
}

/// # Safety
/// Deref data pointer, thus unsafe
#[no_mangle]
pub extern "C" fn external_storage_init(error: &mut ffi_support::ExternError) {
    ffi_support::call_with_result(error, || {
        (|| -> anyhow::Result<()> {
            let guarded = RUNTIME_INIT.lock().unwrap();
            if RUNTIME.get().is_some() {
                return Ok(());
            }
            let runtime = Builder::new()
                .basic_scheduler()
                .thread_name("external-storage-dylib")
                .core_threads(1)
                .enable_all()
                .build()
                .context("build runtime")?;
            match RUNTIME.set(runtime) {
                Err(_) => error!("runtime already set"),
                _ => (),
            }
            Ok(*guarded)
        })()
        .context("external_storage_init")
        .map_err(anyhow_to_extern_err)
    })
}

/// # Safety
/// Deref data pointer, thus unsafe
#[no_mangle]
pub unsafe extern "C" fn external_storage_write(
    data: *const u8,
    len: i32,
    error: &mut ffi_support::ExternError,
) {
    ffi_support::call_with_result(error, || {
        (|| -> anyhow::Result<()> {
            let runtime = RUNTIME
                .get()
                .context("must first call external_storage_init")?;
            let buffer = get_buffer(data, len);
            let req: proto::ExternalStorageWriteRequest = protobuf::parse_from_bytes(buffer)?;
            info!("write request {:?}", req.get_object_name());
            Ok(write_receiver(&runtime, req)?)
        })()
        .context("external_storage_write")
        .map_err(anyhow_to_extern_err)
    })
}

/// # Safety
/// Deref data pointer, thus unsafe
pub unsafe extern "C" fn external_storage_restore(
    data: *const u8,
    len: i32,
    error: &mut ffi_support::ExternError,
) {
    ffi_support::call_with_result(error, || {
        (|| -> anyhow::Result<()> {
            let runtime = RUNTIME
                .get()
                .context("must first call external_storage_init")?;
            let buffer = get_buffer(data, len);
            let req: proto::ExternalStorageRestoreRequest = protobuf::parse_from_bytes(buffer)?;
            info!("restore request {:?}", req.get_object_name());
            Ok(restore_receiver(runtime, req)?)
        })()
        .context("external_storage_restore")
        .map_err(anyhow_to_extern_err)
    })
}

unsafe fn get_buffer<'a>(data: *const u8, len: i32) -> &'a [u8] {
    assert!(len >= 0, "Bad buffer len: {}", len);
    if len == 0 {
        // This will still fail, but as a bad protobuf format.
        &[]
    } else {
        assert!(!data.is_null(), "Unexpected null data pointer");
        std::slice::from_raw_parts(data, len as usize)
    }
}

fn anyhow_to_extern_err(e: anyhow::Error) -> ffi_support::ExternError {
    ffi_support::ExternError::new_error(ffi_support::ErrorCode::new(1), format!("{:?}", e))
}

fn libloading_err_to_io(e: libloading::Error) -> io::Error {
    io::Error::new(ErrorKind::Other, format!("{}", e))
}
