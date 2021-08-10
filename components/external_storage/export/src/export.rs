// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! To use External storage with protobufs as an application, import this module.
//! external_storage contains the actual library code
//! Cloud provider backends are under components/cloud
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;

#[cfg(feature = "cloud-aws")]
pub use aws::{Config as S3Config, S3Storage};
#[cfg(feature = "cloud-gcp")]
pub use gcp::{Config as GCSConfig, GCSStorage};

#[cfg(feature = "prost-codec")]
pub use kvproto::brpb::storage_backend::Backend;
use kvproto::brpb::CloudDynamic;
#[cfg(feature = "protobuf-codec")]
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws"))]
use kvproto::brpb::{Gcs, S3};

#[cfg(feature = "cloud-storage-dylib")]
use crate::dylib;
#[cfg(any(feature = "cloud-storage-dylib", feature = "cloud-storage-grpc"))]
use cloud::blob::BlobConfig;
use cloud::blob::BlobStorage;
use encryption::DataKeyManager;
#[cfg(feature = "cloud-storage-dylib")]
use external_storage::dylib_client;
#[cfg(feature = "cloud-storage-grpc")]
use external_storage::grpc_client;
use external_storage::record_storage_create;
pub use external_storage::{
    read_external_storage_into_file, ExternalStorage, LocalStorage, NoopStorage,
};
use futures_io::AsyncRead;
use kvproto::brpb::{Noop, StorageBackend};
use tikv_util::stream::block_on_external_io;
use tikv_util::time::{Instant, Limiter};
#[cfg(feature = "cloud-storage-dylib")]
use tikv_util::warn;

pub fn create_storage(storage_backend: &StorageBackend) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend(backend)
    } else {
        Err(bad_storage_backend(storage_backend))
    }
}

// when the flag cloud-storage-dylib or cloud-storage-grpc is set create_storage is automatically wrapped with a client
// This function is used by the library/server to avoid any wrapping
pub fn create_storage_no_client(
    storage_backend: &StorageBackend,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend_inner(backend)
    } else {
        Err(bad_storage_backend(storage_backend))
    }
}

fn bad_storage_backend(storage_backend: &StorageBackend) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("bad storage backend {:?}", storage_backend),
    )
}

fn bad_backend(backend: Backend) -> io::Error {
    let storage_backend = StorageBackend {
        backend: Some(backend),
        ..Default::default()
    };
    bad_storage_backend(&storage_backend)
}

#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws"))]
fn blob_store<Blob: BlobStorage>(store: Blob) -> Box<dyn ExternalStorage> {
    Box::new(BlobStore::new(store)) as Box<dyn ExternalStorage>
}

#[cfg(feature = "cloud-storage-grpc")]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    match create_config(backend) {
        Some(config) => {
            let conf = config?;
            grpc_client::new_client(backend.clone(), conf.name(), conf.url()?)
        }
        None => Err(bad_backend(backend.clone())),
    }
}

#[cfg(feature = "cloud-storage-dylib")]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    match create_config(backend) {
        Some(config) => {
            let conf = config?;
            let r = dylib_client::new_client(backend.clone(), conf.name(), conf.url()?);
            match r {
                Err(e) if e.kind() == io::ErrorKind::AddrNotAvailable => {
                    warn!("could not open dll for external_storage_export");
                    dylib::staticlib::new_client(backend.clone(), conf.name(), conf.url()?)
                }
                _ => r,
            }
        }
        None => Err(bad_backend(backend.clone())),
    }
}

#[cfg(all(
    not(feature = "cloud-storage-grpc"),
    not(feature = "cloud-storage-dylib")
))]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn ExternalStorage>> {
    create_backend_inner(backend)
}

#[cfg(any(feature = "cloud-storage-dylib", feature = "cloud-storage-grpc"))]
fn create_config(backend: &Backend) -> Option<io::Result<Box<dyn BlobConfig>>> {
    match backend {
        #[cfg(feature = "cloud-aws")]
        Backend::S3(config) => {
            let conf = S3Config::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => {
            let conf = GCSConfig::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => {
                let conf = S3Config::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => {
                let conf = GCSConfig::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            _ => None,
        },
        _ => None,
    }
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
        Backend::S3(config) => blob_store(S3Storage::from_input(config.clone())?),
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => blob_store(GCSStorage::from_input(config.clone())?),
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => blob_store(S3Storage::from_cloud_dynamic(dyn_backend)?),
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => blob_store(GCSStorage::from_cloud_dynamic(dyn_backend)?),
            _ => {
                return Err(bad_backend(Backend::CloudDynamic(dyn_backend.clone())));
            }
        },
        #[cfg(not(any(feature = "cloud-gcp", feature = "cloud-aws")))]
        _ => return Err(bad_backend(backend.clone())),
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

pub struct BlobStore<Blob: BlobStorage>(Blob);

impl<Blob: BlobStorage> BlobStore<Blob> {
    pub fn new(inner: Blob) -> Self {
        BlobStore(inner)
    }
}

impl<Blob: BlobStorage> std::ops::Deref for BlobStore<Blob> {
    type Target = Blob;
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
            speed_limiter,
            expected_length,
            min_read_speed,
        ))
    }
}

impl<Blob: BlobStorage> ExternalStorage for BlobStore<Blob> {
    fn name(&self) -> &'static str {
        (**self).config().name()
    }
    fn url(&self) -> io::Result<url::Url> {
        (**self).config().url()
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
