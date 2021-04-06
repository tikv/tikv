// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//!
//! This crate define an abstraction of external storage. Currently, it
//! supports local storage.

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::io;
use std::marker::Unpin;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use futures_io::AsyncRead;
#[cfg(feature = "protobuf-codec")]
use kvproto::backup::StorageBackend_oneof_backend as Backend;
#[cfg(feature = "prost-codec")]
use kvproto::backup::{storage_backend::Backend, Local};
use kvproto::backup::{Gcs, Noop, StorageBackend, S3};

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod s3;
pub use s3::S3Storage;
mod gcs;
pub use gcs::GCSStorage;
mod metrics;
use metrics::*;

/// Create a new storage from the given storage backend description.
pub fn create_storage(backend: &StorageBackend) -> io::Result<Arc<dyn ExternalStorage>> {
    let start = Instant::now();
    let (label, storage) = match &backend.backend {
        Some(Backend::Local(local)) => {
            let p = Path::new(&local.path);
            ("local", LocalStorage::new(p).map(|s| Arc::new(s) as _))
        }
        Some(Backend::Noop(_)) => ("noop", Ok(Arc::new(NoopStorage::default()) as _)),
        Some(Backend::S3(config)) => ("s3", S3Storage::new(config).map(|s| Arc::new(s) as _)),
        Some(Backend::Gcs(config)) => ("gcs", GCSStorage::new(config).map(|s| Arc::new(s) as _)),
        _ => {
            let u = url_of_backend(backend);
            error!("unknown storage"; "scheme" => u.scheme());
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown storage {}", u),
            ));
        }
    };
    EXT_STORAGE_CREATE_HISTOGRAM
        .with_label_values(&[label])
        .observe(start.elapsed().as_secs_f64());
    storage
}

/// Formats the storage backend as a URL.
pub fn url_of_backend(backend: &StorageBackend) -> url::Url {
    let mut u = url::Url::parse("unknown:///").unwrap();
    match &backend.backend {
        Some(Backend::Local(local)) => {
            u.set_scheme("local").unwrap();
            u.set_path(&local.path);
        }
        Some(Backend::Noop(_)) => {
            u.set_scheme("noop").unwrap();
        }
        Some(Backend::S3(s3)) => {
            u.set_scheme("s3").unwrap();
            if let Err(e) = u.set_host(Some(&s3.bucket)) {
                warn!("ignoring invalid S3 bucket name"; "bucket" => &s3.bucket, "error" => %e);
            }
            u.set_path(s3.get_prefix());
        }
        Some(Backend::Gcs(gcs)) => {
            u.set_scheme("gcs").unwrap();
            if let Err(e) = u.set_host(Some(&gcs.bucket)) {
                warn!("ignoring invalid GCS bucket name"; "bucket" => &gcs.bucket, "error" => %e);
            }
            u.set_path(gcs.get_prefix());
        }
        None => {}
    }
    u
}

/// Creates a local `StorageBackend` to the given path.
pub fn make_local_backend(path: &Path) -> StorageBackend {
    let path = path.display().to_string();
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::Local(Local { path })),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.mut_local().set_path(path);
        backend
    }
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::Noop(noop)),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.set_noop(noop);
        backend
    }
}

// Creates a S3 `StorageBackend`
pub fn make_s3_backend(config: S3) -> StorageBackend {
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::S3(config)),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.set_s3(config);
        backend
    }
}

// Creates a GCS `StorageBackend`
pub fn make_gcs_backend(config: Gcs) -> StorageBackend {
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::Gcs(config)),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.set_gcs(config);
        backend
    }
}

/// An abstraction of an external storage.
// TODO: these should all be returning a future (i.e. async fn).
pub trait ExternalStorage: 'static {
    /// Write all contents of the read to the given path.
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_>;
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
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).read(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_create_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let backend = make_local_backend(&path.join("not_exist"));
        match create_storage(&backend) {
            Ok(_) => panic!("must be NotFound error"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
        }

        let backend = make_local_backend(path);
        create_storage(&backend).unwrap();

        let backend = make_noop_backend();
        create_storage(&backend).unwrap();

        let backend = StorageBackend::default();
        assert!(create_storage(&backend).is_err());
    }

    #[test]
    fn test_url_of_backend() {
        use kvproto::backup::S3;

        let backend = make_local_backend(Path::new("/tmp/a"));
        assert_eq!(url_of_backend(&backend).to_string(), "local:///tmp/a");

        let backend = make_noop_backend();
        assert_eq!(url_of_backend(&backend).to_string(), "noop:///");

        let mut backend = StorageBackend {
            backend: Some(Backend::S3(S3 {
                bucket: "bucket".to_owned(),
                prefix: "/backup 01/prefix/".to_owned(),
                endpoint: "http://endpoint.com".to_owned(),
                // ^ only 'bucket' and 'prefix' should be visible in url_of_backend()
                ..S3::default()
            })),
            ..Default::default()
        };
        assert_eq!(
            url_of_backend(&backend).to_string(),
            "s3://bucket/backup%2001/prefix/"
        );

        backend.backend = Some(Backend::Gcs(Gcs {
            bucket: "bucket".to_owned(),
            prefix: "/backup 02/prefix/".to_owned(),
            endpoint: "http://endpoint.com".to_owned(),
            // ^ only 'bucket' and 'prefix' should be visible in url_of_backend()
            ..Gcs::default()
        }));
        assert_eq!(
            url_of_backend(&backend).to_string(),
            "gcs://bucket/backup%2002/prefix/"
        );
    }
}
