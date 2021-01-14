// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//! Cloud provider backends can be found under components/cloud

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::io;
use std::marker::Unpin;
use std::sync::Arc;
use std::time::Instant;

use futures_io::AsyncRead;

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
<<<<<<< HEAD
mod s3;
pub use s3::S3Storage;
mod gcs;
pub use gcs::GCSStorage;
=======
mod util;
pub use util::{
    block_on_external_io, error_stream, retry, AsyncReadAsSyncStreamOfBytes, RetryError,
};
>>>>>>> reorg for cloud interface
mod metrics;
use metrics::EXT_STORAGE_CREATE_HISTOGRAM;

<<<<<<< HEAD
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
=======
pub const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

pub fn record_storage_create(start: Instant, storage: &dyn ExternalStorage) {
>>>>>>> reorg for cloud interface
    EXT_STORAGE_CREATE_HISTOGRAM
        .with_label_values(&[storage.name()])
        .observe(start.elapsed().as_secs_f64());
}

/// An abstraction of an external storage.
// TODO: these should all be returning a future (i.e. async fn).
pub trait ExternalStorage: 'static {
    fn name(&self) -> &'static str;

    fn url(&self) -> url::Url;

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
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn url(&self) -> url::Url {
        (**self).url()
    }

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

impl ExternalStorage for Box<dyn ExternalStorage> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn url(&self) -> url::Url {
        (**self).url()
    }

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

#[derive(Clone, Debug, Default)]
pub struct BucketConf {
    pub endpoint: Option<String>,
    pub region: Option<String>,
    pub bucket: String,
    pub prefix: Option<String>,
    pub storage_class: Option<String>,
}

impl BucketConf {
    pub fn url(&self, host: &str) -> url::Url {
        let mut u = url::Url::parse(host).unwrap();
        if let Err(e) = u.set_host(Some(&self.bucket)) {
            warn!("ignoring invalid GCS bucket name"; "bucket" => &self.bucket, "error" => %e);
        }
        u.set_path(&self.prefix.as_ref().unwrap_or(&String::new()));
        u
    }

    pub fn validate(&self) -> io::Result<()> {
        if self.bucket.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "Missing field bucket for bucket name",
            ));
        }
        Ok(())
    }
}

pub fn empty_to_none(s: String) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s)
    }
}

pub fn none_to_empty(opt: Option<String>) -> String {
    if let Some(s) = opt {
        s
    } else {
        "".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_validate() {
        assert!(BucketConf::default().validate().is_err());
        let bucket = BucketConf {
            bucket: "bucket".to_owned(),
            ..BucketConf::default()
        };
        assert!(bucket.validate().is_ok());
    }

    #[test]
    fn test_url_of_bucket() {
        let bucket = BucketConf {
            bucket: "bucket".to_owned(),
            prefix: Some("/backup 01/prefix/".to_owned()),
            endpoint: Some("http://endpoint.com".to_owned()),
            ..BucketConf::default()
        };
        assert_eq!(
            bucket.url("s3://").to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
    }
}
