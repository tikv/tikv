// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//!
//! This crate define an abstraction of external storage. Currently, it
//! supports local storage.

#[macro_use(slog_error, slog_info, slog_debug)]
extern crate slog;
#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::io::{self, Read};
use std::path::Path;
use std::sync::Arc;

use url::Url;

mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;

/// Create a new storage from the given url.
pub fn create_storage(url: &str) -> io::Result<Arc<dyn ExternalStorage>> {
    let url = Url::parse(url).map_err(|e| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("failed to create storage {} {}", e, url),
        )
    })?;

    match url.scheme() {
        LocalStorage::SCHEME => {
            let p = Path::new(url.path());
            LocalStorage::new(p).map(|s| Arc::new(s) as _)
        }
        NoopStorage::SCHEME => Ok(Arc::new(NoopStorage::new()) as _),
        other => {
            error!("unknown storage"; "scheme" => other);
            Err(io::Error::new(
                io::ErrorKind::Other,
                format!("unknown storage {}", url),
            ))
        }
    }
}

/// An abstraction of an external storage.
pub trait ExternalStorage: Sync + Send + 'static {
    /// Write all contents of the read to the given path.
    // TODO: should it return a writer?
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()>;
    /// Read all contents of the given path.
    fn read(&self, name: &str) -> io::Result<Box<dyn Read>>;
}

impl ExternalStorage for Arc<dyn ExternalStorage> {
    fn write(&self, name: &str, reader: &mut dyn Read) -> io::Result<()> {
        (**self).write(name, reader)
    }
    fn read(&self, name: &str) -> io::Result<Box<dyn Read>> {
        (**self).read(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_storage() {
        create_storage("local:///tmp/a").unwrap();
        create_storage("noop:///foo").unwrap();
        assert!(create_storage("invalid").is_err());
    }
}
