// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! External storage support.
//! Cloud provider backends can be found under components/cloud

#[macro_use]
extern crate slog_global;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

use std::{
    io::{self, Write},
    marker::Unpin,
    sync::Arc,
    time::Duration,
};

use async_trait::async_trait;
use encryption::{encryption_method_from_db_encryption_method, DecrypterReader, Iv};
use engine_traits::FileEncryptionInfo;
use file_system::File;
use futures_io::AsyncRead;
use futures_util::AsyncReadExt;
use openssl::hash::{Hasher, MessageDigest};
use tikv_util::{
    stream::{block_on_external_io, READ_BUF_SIZE},
    time::{Instant, Limiter},
};
use tokio::time::timeout;

mod hdfs;
pub use hdfs::{HdfsConfig, HdfsStorage};
mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod metrics;
use metrics::EXT_STORAGE_CREATE_HISTOGRAM;
#[cfg(feature = "cloud-storage-dylib")]
pub mod dylib_client;
#[cfg(feature = "cloud-storage-grpc")]
pub mod grpc_client;
#[cfg(any(feature = "cloud-storage-dylib", feature = "cloud-storage-grpc"))]
pub mod request;

pub fn record_storage_create(start: Instant, storage: &dyn ExternalStorage) {
    EXT_STORAGE_CREATE_HISTOGRAM
        .with_label_values(&[storage.name()])
        .observe(start.saturating_elapsed().as_secs_f64());
}

/// UnpinReader is a simple wrapper for AsyncRead + Unpin + Send.
/// This wrapper would remove the lifetime at the argument of the generted async function
/// in order to make rustc happy. (And reduce the length of signture of write.)
/// see https://github.com/rust-lang/rust/issues/63033
pub struct UnpinReader(pub Box<dyn AsyncRead + Unpin + Send>);

#[derive(Debug, Default)]
pub struct BackendConfig {
    pub s3_multi_part_size: usize,
    pub hdfs_config: HdfsConfig,
}

/// An abstraction of an external storage.
// TODO: these should all be returning a future (i.e. async fn).
#[async_trait]
pub trait ExternalStorage: 'static + Send + Sync {
    fn name(&self) -> &'static str;

    fn url(&self) -> io::Result<url::Url>;

    /// Write all contents of the read to the given path.
    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()>;

    /// Read all contents of the given path.
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_>;

    /// Read from external storage and restore to the given path
    fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        expected_sha256: Option<Vec<u8>>,
        speed_limiter: &Limiter,
        file_crypter: Option<FileEncryptionInfo>,
    ) -> io::Result<()> {
        let reader = self.read(storage_name);
        let output: &mut dyn Write = &mut File::create(restore_name)?;
        // the minimum speed of reading data, in bytes/second.
        // if reading speed is slower than this rate, we will stop with
        // a "TimedOut" error.
        // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
        let min_read_speed: usize = 8192;
        let mut input = encrypt_wrap_reader(file_crypter, reader)?;

        block_on_external_io(read_external_storage_into_file(
            &mut input,
            output,
            speed_limiter,
            expected_length,
            expected_sha256,
            min_read_speed,
        ))
    }
}

#[async_trait]
impl ExternalStorage for Arc<dyn ExternalStorage> {
    fn name(&self) -> &'static str {
        (**self).name()
    }

    fn url(&self) -> io::Result<url::Url> {
        (**self).url()
    }

    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        (**self).write(name, reader, content_length).await
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).read(name)
    }
}

#[async_trait]
impl ExternalStorage for Box<dyn ExternalStorage> {
    fn name(&self) -> &'static str {
        self.as_ref().name()
    }

    fn url(&self) -> io::Result<url::Url> {
        self.as_ref().url()
    }

    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        self.as_ref().write(name, reader, content_length).await
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        self.as_ref().read(name)
    }
}

/// Wrap the reader with file_crypter.
/// Return the reader directly if file_crypter is None.
pub fn encrypt_wrap_reader<'a>(
    file_crypter: Option<FileEncryptionInfo>,
    reader: Box<dyn AsyncRead + Unpin + 'a>,
) -> io::Result<Box<dyn AsyncRead + Unpin + 'a>> {
    let input = match file_crypter {
        Some(x) => Box::new(DecrypterReader::new(
            reader,
            encryption_method_from_db_encryption_method(x.method),
            &x.key,
            Iv::from_slice(&x.iv)?,
        )?),
        None => reader,
    };

    Ok(input)
}

pub async fn read_external_storage_into_file(
    input: &mut (dyn AsyncRead + Unpin),
    output: &mut dyn Write,
    speed_limiter: &Limiter,
    expected_length: u64,
    expected_sha256: Option<Vec<u8>>,
    min_read_speed: usize,
) -> io::Result<()> {
    let dur = Duration::from_secs((READ_BUF_SIZE / min_read_speed) as u64);

    // do the I/O copy from external_storage to the local file.
    let mut buffer = vec![0u8; READ_BUF_SIZE];
    let mut file_length = 0;
    let mut hasher = Hasher::new(MessageDigest::sha256()).map_err(|err| {
        io::Error::new(
            io::ErrorKind::Other,
            format!("openssl hasher failed to init: {}", err),
        )
    })?;

    loop {
        // separate the speed limiting from actual reading so it won't
        // affect the timeout calculation.
        let bytes_read = timeout(dur, input.read(&mut buffer))
            .await
            .map_err(|_| io::ErrorKind::TimedOut)??;
        if bytes_read == 0 {
            break;
        }
        speed_limiter.consume(bytes_read).await;
        output.write_all(&buffer[..bytes_read])?;
        if expected_sha256.is_some() {
            hasher.update(&buffer[..bytes_read]).map_err(|err| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("openssl hasher udpate failed: {}", err),
                )
            })?;
        }
        file_length += bytes_read as u64;
    }

    if expected_length != 0 && expected_length != file_length {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "downloaded size {}, expected {}",
                file_length, expected_length
            ),
        ));
    }

    if let Some(expected_s) = expected_sha256 {
        let cal_sha256 = hasher.finish().map_or_else(
            |err| {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("openssl hasher finish failed: {}", err),
                ))
            },
            |bytes| Ok(bytes.to_vec()),
        )?;
        if !expected_s.eq(&cal_sha256) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sha256 not match, expect: {:?}, calculate: {:?}",
                    expected_s, cal_sha256,
                ),
            ));
        }
    }

    Ok(())
}
