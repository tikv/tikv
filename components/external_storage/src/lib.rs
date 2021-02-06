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

use cloud::blob::BlobStorage;
use futures_io::AsyncRead;

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod metrics;
use metrics::EXT_STORAGE_CREATE_HISTOGRAM;

pub fn record_storage_create(start: Instant, storage: &dyn ExternalStorage) {
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

pub struct BlobStore(Box<dyn BlobStorage>);

impl BlobStore {
    pub fn new(inner: Box<dyn BlobStorage>) -> Self {
        BlobStore(inner)
    }
}

impl std::ops::Deref for BlobStore {
    type Target = Box<dyn BlobStorage>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ExternalStorage for BlobStore {
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
