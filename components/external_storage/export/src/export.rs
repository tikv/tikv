// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

//! To use External storage with protobufs as an application, import this
//! module. external_storage contains the actual library code
//! Cloud provider backends are under components/cloud
use std::{io, path::Path, sync::Arc};

use async_trait::async_trait;
#[cfg(feature = "cloud-aws")]
pub use aws::{Config as S3Config, S3Storage};
#[cfg(feature = "cloud-azure")]
pub use azure::{AzureStorage, Config as AzureConfig};
#[cfg(any(feature = "cloud-storage-dylib", feature = "cloud-storage-grpc"))]
use cloud::blob::BlobConfig;
use cloud::blob::{BlobStorage, PutResource};
use encryption::DataKeyManager;
#[cfg(feature = "cloud-storage-dylib")]
use external_storage::dylib_client;
#[cfg(feature = "cloud-storage-grpc")]
use external_storage::grpc_client;
pub use external_storage::{
    compression_reader_dispatcher, encrypt_wrap_reader, read_external_storage_info_buff,
    read_external_storage_into_file, record_storage_create, BackendConfig, ExternalData,
    ExternalStorage, HdfsStorage, LocalStorage, NoopStorage, RestoreConfig, UnpinReader,
    MIN_READ_SPEED,
};
#[cfg(feature = "cloud-gcp")]
pub use gcp::{Config as GcsConfig, GcsStorage};
pub use kvproto::brpb::StorageBackend_oneof_backend as Backend;
#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws", feature = "cloud-azure"))]
use kvproto::brpb::{AzureBlobStorage, Gcs, S3};
use kvproto::brpb::{CloudDynamic, Noop, StorageBackend};
use tikv_util::time::{Instant, Limiter};
#[cfg(feature = "cloud-storage-dylib")]
use tikv_util::warn;

#[cfg(feature = "cloud-storage-dylib")]
use crate::dylib;

pub fn create_storage(
    storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend(backend, config)
    } else {
        Err(bad_storage_backend(storage_backend))
    }
}

// when the flag cloud-storage-dylib or cloud-storage-grpc is set create_storage
// is automatically wrapped with a client This function is used by the
// library/server to avoid any wrapping
pub fn create_storage_no_client(
    storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend_inner(backend, config)
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

#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws", feature = "cloud-azure"))]
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
pub fn create_backend(
    backend: &Backend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    create_backend_inner(backend, config)
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
            let conf = GcsConfig::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        #[cfg(feature = "cloud-azure")]
        Backend::AzureBlobStorage(config) => {
            let conf = AzureConfig::from_input(config.clone());
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
                let conf = GcsConfig::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            #[cfg(feature = "cloud-azure")]
            "azure" | "azblob" => {
                let conf = AzureConfig::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Create a new storage from the given storage backend description.
fn create_backend_inner(
    backend: &Backend,
    backend_config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    let start = Instant::now();
    let storage: Box<dyn ExternalStorage> = match backend {
        Backend::Local(local) => {
            let p = Path::new(&local.path);
            Box::new(LocalStorage::new(p)?) as Box<dyn ExternalStorage>
        }
        Backend::Hdfs(hdfs) => {
            Box::new(HdfsStorage::new(&hdfs.remote, backend_config.hdfs_config)?)
        }
        Backend::Noop(_) => {
            Box::<external_storage::NoopStorage>::default() as Box<dyn ExternalStorage>
        }
        #[cfg(feature = "cloud-aws")]
        Backend::S3(config) => {
            let mut s = S3Storage::from_input(config.clone())?;
            s.set_multi_part_size(backend_config.s3_multi_part_size);
            blob_store(s)
        }
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => blob_store(GcsStorage::from_input(config.clone())?),
        #[cfg(feature = "cloud-azure")]
        Backend::AzureBlobStorage(config) => blob_store(AzureStorage::from_input(config.clone())?),
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => blob_store(S3Storage::from_cloud_dynamic(dyn_backend)?),
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => blob_store(GcsStorage::from_cloud_dynamic(dyn_backend)?),
            #[cfg(feature = "cloud-azure")]
            "azure" | "azblob" => blob_store(AzureStorage::from_cloud_dynamic(dyn_backend)?),
            _ => {
                return Err(bad_backend(Backend::CloudDynamic(dyn_backend.clone())));
            }
        },
        #[allow(unreachable_patterns)]
        _ => return Err(bad_backend(backend.clone())),
    };
    record_storage_create(start, &*storage);
    Ok(storage)
}

#[cfg(feature = "cloud-aws")]
// Creates a S3 `StorageBackend`
pub fn make_s3_backend(config: S3) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_s3(config);
    backend
}

pub fn make_local_backend(path: &Path) -> StorageBackend {
    let path = path.display().to_string();
    let mut backend = StorageBackend::default();
    backend.mut_local().set_path(path);
    backend
}

pub fn make_hdfs_backend(remote: String) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.mut_hdfs().set_remote(remote);
    backend
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    let mut backend = StorageBackend::default();
    backend.set_noop(noop);
    backend
}

#[cfg(feature = "cloud-gcp")]
pub fn make_gcs_backend(config: Gcs) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_gcs(config);
    backend
}

#[cfg(feature = "cloud-azure")]
pub fn make_azblob_backend(config: AzureBlobStorage) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_azure_blob_storage(config);
    backend
}

pub fn make_cloud_backend(config: CloudDynamic) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_cloud_dynamic(config);
    backend
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_create_storage() {
        let temp_dir = Builder::new().tempdir().unwrap();
        let path = temp_dir.path();
        let backend = make_local_backend(&path.join("not_exist"));
        match create_storage(&backend, Default::default()) {
            Ok(_) => panic!("must be NotFound error"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
        }

        let backend = make_local_backend(path);
        create_storage(&backend, Default::default()).unwrap();

        let backend = make_noop_backend();
        create_storage(&backend, Default::default()).unwrap();

        let backend = StorageBackend::default();
        assert!(create_storage(&backend, Default::default()).is_err());
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

pub struct EncryptedExternalStorage<S> {
    pub key_manager: Arc<DataKeyManager>,
    pub storage: S,
}

#[async_trait]
impl<S: ExternalStorage> ExternalStorage for EncryptedExternalStorage<S> {
    fn name(&self) -> &'static str {
        self.storage.name()
    }
    fn url(&self) -> io::Result<url::Url> {
        self.storage.url()
    }
    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        self.storage.write(name, reader, content_length).await
    }
    fn read(&self, name: &str) -> ExternalData<'_> {
        self.storage.read(name)
    }
    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
        self.storage.read_part(name, off, len)
    }
    async fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> io::Result<()> {
        let RestoreConfig {
            range,
            compression_type,
            expected_sha256,
            file_crypter,
        } = restore_config;

        let reader = {
            let inner = if let Some((off, len)) = range {
                self.read_part(storage_name, off, len)
            } else {
                self.read(storage_name)
            };

            compression_reader_dispatcher(compression_type, inner)?
        };
        let file_writer = self.key_manager.create_file_for_write(&restore_name)?;
        let min_read_speed: usize = 8192;
        let mut input = encrypt_wrap_reader(file_crypter, reader)?;

        read_external_storage_into_file(
            &mut input,
            file_writer,
            speed_limiter,
            expected_length,
            expected_sha256,
            min_read_speed,
        )
        .await
    }
}

#[async_trait]
impl<Blob: BlobStorage> ExternalStorage for BlobStore<Blob> {
    fn name(&self) -> &'static str {
        (**self).config().name()
    }
    fn url(&self) -> io::Result<url::Url> {
        (**self).config().url()
    }
    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        (**self)
            .put(name, PutResource(reader.0), content_length)
            .await
    }

    fn read(&self, name: &str) -> ExternalData<'_> {
        (**self).get(name)
    }

    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
        (**self).get_part(name, off, len)
    }
}
