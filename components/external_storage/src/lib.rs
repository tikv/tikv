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

use async_compression::futures::bufread::ZstdDecoder;
use async_trait::async_trait;
use encryption::{DecrypterReader, FileEncryptionInfo, Iv};
use file_system::File;
use futures::io::BufReader;
use futures_io::AsyncRead;
use futures_util::AsyncReadExt;
use kvproto::brpb::CompressionType;
use openssl::hash::{Hasher, MessageDigest};
use tikv_util::{
    future::RescheduleChecker,
    stream::READ_BUF_SIZE,
    time::{Instant, Limiter},
};
use tokio::time::timeout;

mod hdfs;
pub use hdfs::{HdfsConfig, HdfsStorage};
pub mod local;
pub use local::LocalStorage;
mod noop;
pub use noop::NoopStorage;
mod metrics;
use metrics::EXT_STORAGE_CREATE_HISTOGRAM;
mod export;
pub use export::*;

pub fn record_storage_create(start: Instant, storage: &dyn ExternalStorage) {
    EXT_STORAGE_CREATE_HISTOGRAM
        .with_label_values(&[storage.name()])
        .observe(start.saturating_elapsed().as_secs_f64());
}

/// UnpinReader is a simple wrapper for AsyncRead + Unpin + Send.
/// This wrapper would remove the lifetime at the argument of the generated
/// async function in order to make rustc happy. (And reduce the length of
/// signature of write.) see https://github.com/rust-lang/rust/issues/63033
pub struct UnpinReader(pub Box<dyn AsyncRead + Unpin + Send>);

pub type ExternalData<'a> = Box<dyn AsyncRead + Unpin + Send + 'a>;

#[derive(Debug, Default)]
pub struct BackendConfig {
    pub s3_multi_part_size: usize,
    pub hdfs_config: HdfsConfig,
}

#[derive(Debug, Default)]
pub struct RestoreConfig {
    pub range: Option<(u64, u64)>,
    pub compression_type: Option<CompressionType>,
    pub expected_sha256: Option<Vec<u8>>,
    pub file_crypter: Option<FileEncryptionInfo>,
}

/// a reader dispatcher for different compression type.
pub fn compression_reader_dispatcher(
    compression_type: Option<CompressionType>,
    inner: ExternalData<'_>,
) -> io::Result<ExternalData<'_>> {
    match compression_type {
        Some(c) => match c {
            // The log files generated from TiKV v6.2.0 use the default value (0).
            // So here regard Unkown(0) as uncompressed type.
            CompressionType::Unknown => Ok(inner),
            CompressionType::Zstd => Ok(Box::new(ZstdDecoder::new(BufReader::new(inner)))),
            _ => Err(io::Error::new(
                io::ErrorKind::Other,
                format!(
                    "the compression type is unimplemented, compression type id {:?}",
                    c
                ),
            )),
        },
        None => Ok(inner),
    }
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
    fn read(&self, name: &str) -> ExternalData<'_>;

    /// Read part of contents of the given path.
    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_>;

    /// Read from external storage and restore to the given path
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
        let output = File::create(restore_name)?;
        // the minimum speed of reading data, in bytes/second.
        // if reading speed is slower than this rate, we will stop with
        // a "TimedOut" error.
        // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
        let min_read_speed: usize = 8192;
        let input = encrypt_wrap_reader(file_crypter, reader)?;

        read_external_storage_into_file(
            input,
            output,
            speed_limiter,
            expected_length,
            expected_sha256,
            min_read_speed,
        )
        .await
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

    fn read(&self, name: &str) -> ExternalData<'_> {
        (**self).read(name)
    }

    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
        (**self).read_part(name, off, len)
    }

    async fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> io::Result<()> {
        self.as_ref()
            .restore(
                storage_name,
                restore_name,
                expected_length,
                speed_limiter,
                restore_config,
            )
            .await
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

    fn read(&self, name: &str) -> ExternalData<'_> {
        self.as_ref().read(name)
    }

    fn read_part(&self, name: &str, off: u64, len: u64) -> ExternalData<'_> {
        self.as_ref().read_part(name, off, len)
    }

    async fn restore(
        &self,
        storage_name: &str,
        restore_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        restore_config: RestoreConfig,
    ) -> io::Result<()> {
        self.as_ref()
            .restore(
                storage_name,
                restore_name,
                expected_length,
                speed_limiter,
                restore_config,
            )
            .await
    }
}

/// Wrap the reader with file_crypter.
/// Return the reader directly if file_crypter is None.
pub fn encrypt_wrap_reader(
    file_crypter: Option<FileEncryptionInfo>,
    reader: ExternalData<'_>,
) -> io::Result<ExternalData<'_>> {
    let input = match file_crypter {
        Some(x) => Box::new(DecrypterReader::new(
            reader,
            x.method,
            &x.key,
            Iv::from_slice(&x.iv)?,
        )?),
        None => reader,
    };

    Ok(input)
}

pub async fn read_external_storage_into_file<In, Out>(
    mut input: In,
    mut output: Out,
    speed_limiter: &Limiter,
    expected_length: u64,
    expected_sha256: Option<Vec<u8>>,
    min_read_speed: usize,
) -> io::Result<()>
where
    In: AsyncRead + Unpin,
    Out: Write,
{
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
    let mut yield_checker =
        RescheduleChecker::new(tokio::task::yield_now, Duration::from_millis(10));
    loop {
        // separate the speed limiting from actual reading so it won't
        // affect the timeout calculation.
        let start = Instant::now();
        let bytes_read = timeout(dur, input.read(&mut buffer))
            .await
            .map_err(|_| io::ErrorKind::TimedOut)??;
        if bytes_read == 0 {
            break;
        }
        let elapsed = start.saturating_elapsed().as_millis();
        if bytes_read < 64 * elapsed {
            warn!("the speed is too slow"; "elapsed" => elapsed, "bytes_read" => bytes_read);
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
        yield_checker.check().await;
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

pub const MIN_READ_SPEED: usize = 8192;

pub async fn read_external_storage_info_buff(
    reader: &mut (dyn AsyncRead + Unpin + Send),
    speed_limiter: &Limiter,
    expected_length: u64,
    expected_sha256: Option<Vec<u8>>,
    min_read_speed: usize,
) -> io::Result<Vec<u8>> {
    // the minimum speed of reading data, in bytes/second.
    // if reading speed is slower than this rate, we will stop with
    // a "TimedOut" error.
    // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
    let read_speed = if min_read_speed > 0 {
        min_read_speed
    } else {
        MIN_READ_SPEED
    };
    let dur = Duration::from_secs((READ_BUF_SIZE / read_speed) as u64);
    let mut output = Vec::new();
    let mut buffer = vec![0u8; READ_BUF_SIZE];

    loop {
        // separate the speed limiting from actual reading so it won't
        // affect the timeout calculation.
        let bytes_read = timeout(dur, reader.read(&mut buffer))
            .await
            .map_err(|_| io::ErrorKind::TimedOut)??;
        if bytes_read == 0 {
            break;
        }

        speed_limiter.consume(bytes_read).await;
        output.append(&mut buffer[..bytes_read].to_vec());
    }

    // check length of file
    if expected_length > 0 && output.len() != expected_length as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "length not match, downloaded size {}, expected {}",
                output.len(),
                expected_length
            ),
        ));
    }
    // check sha256 of file
    if let Some(sha256) = expected_sha256 {
        let mut hasher = Hasher::new(MessageDigest::sha256()).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("openssl hasher failed to init: {}", err),
            )
        })?;
        hasher.update(&output).map_err(|err| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("openssl hasher udpate failed: {}", err),
            )
        })?;

        let cal_sha256 = hasher.finish().map_or_else(
            |err| {
                Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("openssl hasher finish failed: {}", err),
                ))
            },
            |bytes| Ok(bytes.to_vec()),
        )?;
        if !sha256.eq(&cal_sha256) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "sha256 not match, expect: {:?}, calculate: {:?}",
                    sha256, cal_sha256,
                ),
            ));
        }
    }

    Ok(output)
}
