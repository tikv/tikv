// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! To use External storage with protobufs as an application, import this module.
//! external_storage contains the actual library code
//! Cloud provider backends are under components/cloud
use std::io;
use std::path::Path;
use std::time::Instant;

#[cfg(feature = "aws")]
pub use aws::S3Storage;
#[cfg(feature = "aws")]
use kvproto::backup::S3;

#[cfg(feature = "gcp")]
pub use gcp::GCSStorage;
#[cfg(feature = "gcp")]
use kvproto::backup::Gcs;

#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;
#[cfg(feature = "libloading")]
use libloading;

pub use external_storage::{
    record_storage_create, BlobStore, ExternalStorage, LocalStorage, NoopStorage,
};
use kvproto::backup::{Noop, StorageBackend};

/// Create a new storage from the given storage backend description.
pub fn create_storage(storage_backend: &StorageBackend) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        let start = Instant::now();
        let storage: Box<dyn ExternalStorage> = match backend {
            Backend::Local(local) => {
                let p = Path::new(&local.path);
                Box::new(LocalStorage::new(p)?) as Box<dyn ExternalStorage>
            }
            Backend::Noop(_) => Box::new(NoopStorage::default()) as Box<dyn ExternalStorage>,
            #[cfg(feature = "aws")]
            Backend::S3(config) => {
                let store = Box::new(S3Storage::from_input(config.clone())?);
                Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
            }
            #[cfg(feature = "gcp")]
            Backend::Gcs(config) => {
                let store = Box::new(GCSStorage::from_input(config.clone())?);
                Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
            }
            Backend::CloudDynamic(dyn_backend) => match dyn_backend.name.as_str() {
                #[cfg(feature = "aws")]
                "aws" | "s3" => {
                    let store = Box::new(S3Storage::from_cloud_dynamic(&dyn_backend)?);
                    Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
                }
                #[cfg(feature = "gcp")]
                "gcp" | "gcs" => {
                    let store = Box::new(GCSStorage::from_cloud_dynamic(&dyn_backend)?);
                    Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
                }
                #[cfg(feature = "libloading")]
                _ => load_external_storage(dyn_backend)?,
                #[cfg(not(feature = "libloading"))]
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("cloud dynamic backend not found for {:?}", storage_backend),
                    ))
                }
            },
            #[cfg(not(any(feature = "gcp", feature = "aws")))]
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "external storage backend not enabled for {:?}",
                        storage_backend
                    ),
                ))
            }
        };
        record_storage_create(start, &*storage);
        Ok(storage)
    } else {
        Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("no storage backend {:?}", storage_backend),
        ))
    }
}

#[cfg(feature = "libloading")]
type BackendToStorage = fn(Backend) -> io::Result<Box<dyn ExternalStorage>>;

#[cfg(feature = "libloading")]
fn load_external_storage(backend: CloudDynamic) -> io::Result<Box<dyn ExternalStorage>> {
    let backend_name = backend.name;
    let lib = libloading::Library::new(format!("target/debug/libtikv_cloud{}.so", backend_name))?;
    // .expect(&format!("load library for {}", backend_name));
    let new_storage: libloading::Symbol<BackendToStorage> = unsafe { lib.get(b"new_storage") }?;
    // .expect(&format!("load new_storage symbol for {}", backend_name));
    Ok(new_storage(backend)?)
}

#[cfg(feature = "aws")]
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

#[cfg(feature = "gcp")]
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
}
