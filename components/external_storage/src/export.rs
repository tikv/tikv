// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, path::Path, sync::Arc};

use async_trait::async_trait;
pub use aws::{Config as S3Config, S3Storage};
pub use azure::{AzureStorage, Config as AzureConfig};
pub use cloud::blob::BlobObject;
use cloud::blob::{BlobStorage, DeletableStorage, IterableStorage, PutResource};
use encryption::DataKeyManager;
use futures_util::{future::LocalBoxFuture, stream::LocalBoxStream};
use gcp::GcsStorage;
use kvproto::brpb::{
    AzureBlobStorage, Gcs, Noop, StorageBackend, StorageBackend_oneof_backend as Backend, S3,
};
use tikv_util::{
    stream::block_on_external_io,
    time::{Instant, Limiter},
};

use crate::{
    compression_reader_dispatcher, encrypt_wrap_reader, read_external_storage_into_file,
    record_storage_create, wrap_with_checksum_reader_if_needed, BackendConfig, ExternalData,
    ExternalStorage, HdfsStorage, LocalStorage, NoopStorage, RestoreConfig, UnpinReader,
};

pub async fn create_storage_async(
    storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    if let Some(backend) = &storage_backend.backend {
        create_backend_async(backend, config).await
    } else {
        Err(bad_storage_backend(storage_backend))
    }
}

pub fn create_storage(
    storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn ExternalStorage>> {
    block_on_external_io(create_storage_async(storage_backend, config))
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

fn blob_store<Blob: BlobStorage + IterableStorage + DeletableStorage>(
    store: Blob,
) -> Box<dyn ExternalStorage> {
    Box::new(Compat::new(store)) as Box<dyn ExternalStorage>
}

async fn create_backend_async(
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
        Backend::Noop(_) => Box::<NoopStorage>::default() as Box<dyn ExternalStorage>,
        Backend::S3(config) => {
            let mut s = S3Storage::from_input_async(config.clone()).await?;
            s.set_multi_part_size(backend_config.s3_multi_part_size);
            blob_store(s)
        }
        Backend::Gcs(config) => blob_store(GcsStorage::from_input(config.clone())?),
        Backend::AzureBlobStorage(config) => blob_store(AzureStorage::from_input(config.clone())?),
        Backend::CloudDynamic(dyn_backend) => {
            // CloudDynamic backend is no longer supported.
            return Err(bad_backend(Backend::CloudDynamic(dyn_backend.clone())));
        }
        #[allow(unreachable_patterns)]
        _ => return Err(bad_backend(backend.clone())),
    };
    record_storage_create(start, &*storage);
    Ok(storage)
}

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

pub fn make_gcs_backend(config: Gcs) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_gcs(config);
    backend
}

pub fn make_azblob_backend(config: AzureBlobStorage) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_azure_blob_storage(config);
    backend
}

pub struct Compat<Blob>(Blob);

impl<Blob> Compat<Blob> {
    pub fn new(inner: Blob) -> Self {
        Compat(inner)
    }

    pub fn into_inner(self) -> Blob {
        self.0
    }
}

impl<Blob> std::ops::Deref for Compat<Blob> {
    type Target = Blob;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct AutoEncryptLocalRestoredFileExternalStorage<S> {
    pub key_manager: Arc<DataKeyManager>,
    pub storage: S,
}

#[async_trait]
impl<S: ExternalStorage> ExternalStorage for AutoEncryptLocalRestoredFileExternalStorage<S> {
    fn name(&self) -> &'static str {
        self.storage.name()
    }
    fn url(&self) -> io::Result<url::Url> {
        self.storage.url()
    }
    async fn write(
        &self,
        name: &str,
        reader: UnpinReader<'_>,
        content_length: u64,
    ) -> io::Result<()> {
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
            expected_plaintext_file_checksum: expected_sha256,
            file_crypter,
            opt_encrypted_file_checksum,
        } = restore_config;

        let (mut reader, opt_hasher) = {
            let inner = if let Some((off, len)) = range {
                self.read_part(storage_name, off, len)
            } else {
                self.read(storage_name)
            };

            // wrap with checksum reader if needed
            //
            let (checksum_reader, opt_hasher) =
                wrap_with_checksum_reader_if_needed(opt_encrypted_file_checksum.is_some(), inner)?;

            // wrap with decrypter if needed
            //
            let encrypted_reader = encrypt_wrap_reader(file_crypter, checksum_reader)?;

            (
                compression_reader_dispatcher(compression_type, encrypted_reader)?,
                opt_hasher,
            )
        };
        let file_writer = self.key_manager.create_file_for_write(&restore_name)?;
        let min_read_speed: usize = 8192;
        read_external_storage_into_file(
            &mut reader,
            file_writer,
            speed_limiter,
            expected_length,
            expected_sha256,
            min_read_speed,
            opt_encrypted_file_checksum,
            opt_hasher,
        )
        .await
    }

    fn iter_prefix(&self, prefix: &str) -> LocalBoxStream<'_, io::Result<BlobObject>> {
        self.storage.iter_prefix(prefix)
    }

    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        self.storage.delete(name)
    }
}

#[async_trait]
impl<Blob: BlobStorage + IterableStorage + DeletableStorage> ExternalStorage for Compat<Blob> {
    fn name(&self) -> &'static str {
        (**self).config().name()
    }
    fn url(&self) -> io::Result<url::Url> {
        (**self).config().url()
    }
    async fn write(
        &self,
        name: &str,
        reader: UnpinReader<'_>,
        content_length: u64,
    ) -> io::Result<()> {
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

    /// Walk the prefix of the blob storage.
    /// It returns the stream of items.
    fn iter_prefix(
        &self,
        prefix: &str,
    ) -> LocalBoxStream<'_, std::result::Result<BlobObject, io::Error>> {
        (**self).iter_prefix(prefix)
    }

    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        (**self).delete(name)
    }
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
