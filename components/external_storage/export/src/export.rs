// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! To use External storage with protobufs as an application, import this module.
//! external_storage contains the actual library code
//! Cloud provider backends are under components/cloud
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

#[cfg(feature = "cloud-aws")]
pub use aws::S3Storage;
#[cfg(feature = "cloud-gcp")]
pub use gcp::GCSStorage;

#[cfg(feature = "prost-codec")]
pub use kvproto::backup::storage_backend::Backend;
use kvproto::backup::CloudDynamic;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::backup::StorageBackend_oneof_backend as Backend;
#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws"))]
use kvproto::backup::{Gcs, S3};

#[cfg(feature = "cloud-storage-dylib")]
use super::dylib;
#[cfg(feature = "cloud-storage-grpc")]
use super::service;
use cloud::blob::BlobStorage;
use encryption::DataKeyManager;
use external_storage::record_storage_create;
pub use external_storage::{
    read_external_storage_into_file, ExternalStorage, LocalStorage, NoopStorage,
};
use futures_io::AsyncRead;
use kvproto::backup::{Noop, StorageBackend};
use tikv_util::stream::block_on_external_io;
use tikv_util::time::Limiter;

pub fn create_storage(storage_backend: &StorageBackend) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend(backend)
    } else {
        Err(no_storage_backend(storage_backend))
    }
}

// when the flag cloud-storage-grpc is set create_storage is automatically wrapped with a client
// This function can be used by the server to avoid any wrapping
pub fn create_storage_no_client(
    storage_backend: &StorageBackend,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend_inner(backend)
    } else {
        Err(no_storage_backend(storage_backend))
    }
}

fn no_storage_backend(storage_backend: &StorageBackend) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("no storage backend {:?}", storage_backend),
    )
}

#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws"))]
fn blob_store(store: Box<dyn BlobStorage>) -> Box<dyn ExternalStorage> {
    Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
}

#[cfg(all(feature = "cloud-storage-grpc", not(feature = "cloud-storage-dylib")))]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    let client = service::new_client(backend.clone(), create_backend_inner(backend)?)?;
    return Ok(Box::new(client) as Box<dyn ExternalStorage>);
}

#[cfg(all(feature = "cloud-storage-dylib", not(feature = "cloud-storage-grpc")))]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    dylib::new_client(backend.clone(), create_backend_inner(backend)?)
}

#[cfg(all(feature = "cloud-storage-dylib", feature = "cloud-storage-grpc"))]
panic!("cannot set both cloud-storage-dylib and cloud-storage-grpc");

#[cfg(all(
    not(feature = "cloud-storage-grpc"),
    not(feature = "cloud-storage-dylib")
))]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    create_backend_inner(backend)
}

/// Create a new storage from the given storage backend description.
fn create_backend_inner(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    let start = Instant::now();
    let storage: Box<dyn ExternalStorage> = match backend {
        Backend::Local(local) => {
            let p = Path::new(&local.path);
            Box::new(LocalStorage::new(p)?) as Box<dyn ExternalStorage>
        }
        Backend::Noop(_) => Box::new(NoopStorage::default()) as Box<dyn ExternalStorage>,
        #[cfg(feature = "cloud-aws")]
        Backend::S3(config) => blob_store(Box::new(S3Storage::from_input(config.clone())?)),
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => blob_store(Box::new(GCSStorage::from_input(config.clone())?)),
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => blob_store(Box::new(S3Storage::from_cloud_dynamic(&dyn_backend)?)),
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => blob_store(Box::new(GCSStorage::from_cloud_dynamic(&dyn_backend)?)),
            _ => {
                let storage_backend = make_cloud_backend(dyn_backend.clone());
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("cloud dynamic backend not found for {:?}", storage_backend),
                ));
            }
        },
        #[cfg(not(any(feature = "cloud-gcp", feature = "cloud-aws")))]
        _ => {
            let mut storage_backend = StorageBackend::default();
            storage_backend.backend = Some(backend.clone());
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!(
                    "external storage backend not enabled for {:?}",
                    storage_backend
                ),
            ));
        }
    };
    record_storage_create(start, &*storage);
    Ok(storage)
}

#[cfg(feature = "cloud-aws")]
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

#[cfg(feature = "cloud-gcp")]
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

pub fn make_cloud_backend(config: CloudDynamic) -> StorageBackend {
    #[cfg(feature = "prost-codec")]
    {
        StorageBackend {
            backend: Some(Backend::CloudDynamic(config)),
        }
    }
    #[cfg(feature = "protobuf-codec")]
    {
        let mut backend = StorageBackend::default();
        backend.set_cloud_dynamic(config);
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

pub struct EncryptedExternalStorage {
    pub key_manager: Arc<DataKeyManager>,
    pub storage: Box<dyn ExternalStorage>,
}

impl ExternalStorage for EncryptedExternalStorage {
    fn name(&self) -> &'static str {
        self.storage.name()
    }
    fn url(&self) -> io::Result<url::Url> {
        self.storage.url()
    }
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        self.storage.write(name, reader, content_length)
    }
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        self.storage.read(name)
    }
    fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
    ) -> io::Result<()> {
        let mut input = self.read(storage_name);
        let file_writer: &mut dyn Write = &mut self.key_manager.create_file(&restore_name)?;
        let min_read_speed: usize = 8192;
        block_on_external_io(read_external_storage_into_file(
            &mut input,
            file_writer,
            &speed_limiter,
            expected_length,
            min_read_speed,
        ))
    }
}

impl ExternalStorage for BlobStore {
    fn name(&self) -> &'static str {
        (**self).name()
    }
    fn url(&self) -> io::Result<url::Url> {
        (**self).url()
    }
    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        (**self).put(name, reader, content_length)
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).get(name)
    }
}
