// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//!
//! This crate define an abstraction of external storage. Currently, it
//! supports local storage.

#[macro_use(
    slog_error,
    slog_info,
    slog_debug,
    slog_warn,
    slog_log,
    slog_record,
    slog_b,
    slog_kv,
    slog_record_static
)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::io;
use std::marker::Unpin;
use std::path::Path;
use std::sync::Arc;

use futures_io::AsyncRead;
use kvproto::backup::StorageBackend_oneof_backend as Backend;
use kvproto::backup::{Noop, StorageBackend, S3};

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod s3;
pub use s3::S3Storage;

/// Create a new storage from the given storage backend description.
pub fn create_storage(backend: &StorageBackend) -> io::Result<Arc<dyn ExternalStorage>> {
    match &backend.backend {
        Some(Backend::local(local)) => {
            let p = Path::new(&local.path);
            LocalStorage::new(p).map(|s| Arc::new(s) as _)
        }
        Some(Backend::noop(_)) => Ok(Arc::new(NoopStorage::default()) as _),
        Some(Backend::s3(config)) => S3Storage::new(config).map(|s| Arc::new(s) as _),
        _ => {
            let u = url_of_backend(backend);
            error!("unknown storage"; "scheme" => u.scheme());
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown storage {}", u),
            ))
        }
    }
}

/// Formats the storage backend as a URL.
pub fn url_of_backend(backend: &StorageBackend) -> url::Url {
    let mut u = url::Url::parse("unknown:///").unwrap();
    match &backend.backend {
        Some(Backend::local(local)) => {
            u.set_scheme("local").unwrap();
            u.set_path(&local.path);
        }
        Some(Backend::noop(_)) => {
            u.set_scheme("noop").unwrap();
        }
        Some(Backend::s3(s3)) => {
            u.set_scheme("s3").unwrap();
            if let Err(e) = u.set_host(Some(&s3.bucket)) {
                warn!("ignoring invalid S3 bucket name"; "bucket" => &s3.bucket, "error" => %e);
            }
            u.set_path(s3.get_prefix());
        }
        Some(_) => unimplemented!(),
        None => {}
    }
    u
}

/// Creates a local `StorageBackend` to the given path.
pub fn make_local_backend(path: &Path) -> StorageBackend {
    let path = path.display().to_string();
    let mut backend = StorageBackend::default();
    backend.mut_local().set_path(path);
    backend
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    let mut backend = StorageBackend::default();
    backend.set_noop(noop);
    backend
}

// Creates a S3 `StorageBackend`
pub fn make_s3_backend(config: S3) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_s3(config);
    backend
}

/// An abstraction of an external storage.
// TODO: these should all be returning a future (i.e. async fn).
pub trait ExternalStorage: Sync + Send + 'static {
    /// Write all contents of the read to the given path.
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> io::Result<Box<dyn AsyncRead + Unpin>>;
}

impl ExternalStorage for Arc<dyn ExternalStorage> {
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        (**self).write(name, reader, content_length)
    }
    fn read(&self, name: &str) -> io::Result<Box<dyn AsyncRead + Unpin>> {
        (**self).read(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_storage() {
        let backend = make_local_backend(Path::new("/tmp/a"));
        create_storage(&backend).unwrap();

        let backend = make_noop_backend();
        create_storage(&backend).unwrap();

        let backend = StorageBackend::default();
        assert!(create_storage(&backend).is_err());
    }

    #[test]
    fn test_url_of_backend() {
        let backend = make_local_backend(Path::new("/tmp/a"));
        assert_eq!(url_of_backend(&backend).to_string(), "local:///tmp/a");

        let backend = make_noop_backend();
        assert_eq!(url_of_backend(&backend).to_string(), "noop:///");

        let mut backend = StorageBackend::default();
        let s3 = backend.mut_s3();
        s3.set_bucket("bucket".to_owned());
        s3.set_prefix("/backup 01/prefix/".to_owned());
        s3.set_endpoint("http://endpoint.com".to_owned());
        // ^ only 'bucket' and 'prefix' should be visible in url_of_backend()
        assert_eq!(
            url_of_backend(&backend).to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
    }
}
