// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;

use anyhow::Context;
use kvproto::brpb as proto;
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
use lazy_static::lazy_static;
use once_cell::sync::OnceCell;
use protobuf::{self};
use slog_global::{error, info};
use tokio::runtime::{Builder, Runtime};

use crate::request::{restore_receiver, write_receiver};

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
            if RUNTIME.set(runtime).is_err() {
                error!("runtime already set")
            }
            #[allow(clippy::unit_arg)]
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
            write_receiver(&runtime, req)
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

pub mod staticlib {
    use std::{
        io::{self},
        sync::Arc,
    };

    use external_storage::{
        dylib_client::extern_to_io_err,
        request::{
            anyhow_to_io_log_error, file_name_for_write, restore_sender, write_sender, DropPath,
        },
        ExternalStorage,
    };
    use futures_io::AsyncRead;
    use protobuf::Message;
    use tikv_util::time::Limiter;

    use super::*;

    struct ExternalStorageClient {
        backend: Backend,
        runtime: Arc<Runtime>,
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
        external_storage_init_ffi()?;
        Ok(Box::new(ExternalStorageClient {
            runtime: Arc::new(runtime),
            backend,
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
                external_storage_write_ffi(bytes)?;
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
            external_storage_restore_ffi(bytes)
        }
    }

    fn external_storage_write_ffi(bytes: Vec<u8>) -> io::Result<()> {
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

    fn external_storage_restore_ffi(bytes: Vec<u8>) -> io::Result<()> {
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

    fn external_storage_init_ffi() -> io::Result<()> {
        let mut e = ffi_support::ExternError::default();
        external_storage_init(&mut e);
        if e.get_code() != ffi_support::ErrorCode::SUCCESS {
            return Err(extern_to_io_err(e));
        }
        Ok(())
    }
}
