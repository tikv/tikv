// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::dfs::{new_filename, new_tmp_filename, File, Options, DFS};
use async_trait::async_trait;
use aws_sdk_s3::model::{Tag, Tagging};
use aws_sdk_s3::{ByteStream, Client, Credentials, Endpoint, Region};
use aws_smithy_http::result::SdkError;
use aws_smithy_types::retry::ErrorKind;
use aws_types::credentials::SharedCredentialsProvider;
use bytes::Bytes;
use file_system::{IOOp, IORateLimiter, IOType};
use http::Uri;
use std::io::Write;
use std::ops::Deref;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tikv_util::time::Instant;
use tokio::runtime::Runtime;

const MAX_RETRY_COUNT: u32 = 5;

#[derive(Clone)]
pub struct S3FS {
    core: Arc<S3FSCore>,
}

impl S3FS {
    pub fn new(
        tenant_id: u32,
        local_dir: PathBuf,
        end_point: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
        rate_limiter: Arc<IORateLimiter>,
    ) -> Self {
        let core = Arc::new(S3FSCore::new(
            tenant_id,
            local_dir,
            end_point,
            key_id,
            secret_key,
            region,
            bucket,
            rate_limiter,
        ));
        Self { core }
    }
}

impl Deref for S3FS {
    type Target = S3FSCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct S3FSCore {
    tenant_id: u32,
    local_dir: PathBuf,
    s3c: aws_sdk_s3::Client,
    bucket: String,
    runtime: tokio::runtime::Runtime,
    rate_limiter: Arc<file_system::IORateLimiter>,
    tmp_id: AtomicU64,
}

impl S3FSCore {
    pub fn new(
        tenant_id: u32,
        local_dir: PathBuf,
        end_point: String,
        key_id: String,
        secret_key: String,
        region: String,
        bucket: String,
        rate_limiter: Arc<file_system::IORateLimiter>,
    ) -> Self {
        if !local_dir.exists() {
            std::fs::create_dir_all(&local_dir).unwrap();
        }
        if !local_dir.is_dir() {
            panic!("path {:?} is not dir", &local_dir);
        }
        let credential_provider = Credentials::new(key_id, secret_key, None, None, "config");
        let shared_provider = SharedCredentialsProvider::new(credential_provider);
        let mut s3_conf_builder = aws_sdk_s3::Config::builder();
        s3_conf_builder.set_credentials_provider(Some(shared_provider));
        s3_conf_builder = s3_conf_builder.region(Region::new(region));
        if end_point.len() > 0 {
            let endpoint = Endpoint::immutable(Uri::from_str(end_point.as_str()).unwrap());
            s3_conf_builder = s3_conf_builder.endpoint_resolver(endpoint);
        }
        let cfg = s3_conf_builder.build();
        let s3c = Client::from_conf(cfg);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .thread_name("s3")
            .build()
            .unwrap();
        Self {
            tenant_id,
            local_dir,
            s3c,
            bucket,
            runtime,
            rate_limiter,
            tmp_id: AtomicU64::new(0),
        }
    }

    fn local_file_path(&self, file_id: u64) -> PathBuf {
        self.local_dir.join(new_filename(file_id))
    }

    fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self.tmp_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.local_dir.join(new_tmp_filename(file_id, tmp_id))
    }

    fn file_key(&self, file_id: u64) -> String {
        format!("{:08x}/{:016x}.t1", self.tenant_id, file_id)
    }

    fn is_err_retryable<T>(&self, sdk_err: &SdkError<T>) -> bool {
        match sdk_err {
            SdkError::ConstructionFailure(_) => false,
            SdkError::TimeoutError(_) => true,
            SdkError::DispatchFailure(conn_err) => {
                if conn_err.is_io() || conn_err.is_timeout() {
                    return true;
                }
                match conn_err.is_other() {
                    None => false,
                    Some(other_kind) => match other_kind {
                        ErrorKind::TransientError => true,
                        ErrorKind::ThrottlingError => true,
                        ErrorKind::ServerError => true,
                        ErrorKind::ClientError => false,
                        _ => false,
                    },
                }
            }
            SdkError::ResponseError { .. } => false,
            SdkError::ServiceError { .. } => true,
        }
    }

    fn write_local_file(&self, file_name: &PathBuf, data: Bytes) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .custom_flags(libc::O_DSYNC)
            .open(file_name)?;
        let mut start_off = 0;
        let write_batch_size = 256 * 1024;
        while start_off < data.len() {
            self.rate_limiter
                .request(IOType::Compaction, IOOp::Write, write_batch_size);
            let end_off = std::cmp::min(start_off + write_batch_size, data.len());
            file.write(&data[start_off..end_off])?;
            start_off = end_off;
        }
        Ok(())
    }
}

#[async_trait]
impl DFS for S3FS {
    fn open(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<Arc<dyn File>> {
        let path = self.local_file_path(file_id);
        match std::fs::File::open(&path) {
            Ok(fd) => {
                let meta = fd.metadata()?;
                let local_file = LocalFile {
                    id: file_id,
                    fd: Arc::new(fd),
                    size: meta.size(),
                };
                Ok(Arc::new(local_file))
            }
            Err(err) => {
                error!("failed to open file {}, error {:?}", file_id, &err);
                Err(err.into())
            }
        }
    }

    async fn prefetch(&self, file_id: u64, opts: Options) -> crate::dfs::Result<()> {
        let local_file_path = self.local_file_path(file_id);
        if local_file_path.exists() {
            return Ok(());
        }
        let data = self.read_file(file_id, opts).await?;
        let tmp_file_path = self.tmp_file_path(file_id);
        self.write_local_file(&tmp_file_path, data)?;
        std::fs::rename(tmp_file_path, &local_file_path)?;
        Ok(())
    }

    async fn read_file(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<Bytes> {
        let key = self.file_key(file_id);
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let result = self
                .s3c
                .get_object()
                .bucket(&self.bucket)
                .key(&key)
                .send()
                .await;
            if result.is_ok() {
                return match result.unwrap().body.collect().await {
                    Ok(agg_data) => {
                        let data = agg_data.into_bytes();
                        info!(
                            "read file {}, size {}, takes {:?}, retry {}",
                            file_id,
                            data.len(),
                            start_time.saturating_elapsed(),
                            retry_cnt
                        );
                        Ok(data)
                    }
                    Err(err) => Err(crate::dfs::Error::S3(err.to_string())),
                };
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * 100;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    continue;
                } else {
                    error!(
                        "read file {}, takes {:?}, reach max retry count {}",
                        file_id,
                        start_time.saturating_elapsed(),
                        MAX_RETRY_COUNT
                    );
                }
            }
            return Err(err.into());
        }
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> crate::dfs::Result<()> {
        let key = self.file_key(file_id);
        let mut retry_cnt = 0;
        let start_time = Instant::now();
        loop {
            let hyper_body = hyper::Body::from(data.clone());
            let sdk_body = aws_smithy_http::body::SdkBody::from(hyper_body);
            let result = self
                .s3c
                .put_object()
                .bucket(&self.bucket)
                .key(&key)
                .body(ByteStream::new(sdk_body))
                .send()
                .await;
            if result.is_ok() {
                info!(
                    "create file {}, size {}, takes {:?}, retry {}",
                    file_id,
                    data.len(),
                    start_time.saturating_elapsed(),
                    retry_cnt
                );
                return Ok(());
            }
            let err = result.unwrap_err();
            if self.is_err_retryable(&err) {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt) * 100;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    continue;
                } else {
                    error!(
                        "create file {}, takes {:?}, reach max retry count {}",
                        file_id,
                        start_time.saturating_elapsed(),
                        MAX_RETRY_COUNT
                    );
                }
            }
            return Err(err.into());
        }
    }

    async fn remove(&self, file_id: u64, _opts: Options) {
        let local_file_path = self.local_file_path(file_id);
        if let Err(err) = std::fs::remove_file(&local_file_path) {
            error!("failed to remove local file {:?}", err);
        }
        let mut retry_cnt = 0;
        loop {
            let key = self.file_key(file_id);
            let bucket = self.bucket.clone();
            let tagging = Tagging::builder()
                .tag_set(Tag::builder().key("deleted").value("true").build())
                .build();
            if let Err(err) = self
                .s3c
                .put_object_tagging()
                .bucket(bucket)
                .key(key)
                .tagging(tagging)
                .send()
                .await
            {
                if retry_cnt < MAX_RETRY_COUNT {
                    retry_cnt += 1;
                    let retry_sleep = 2u64.pow(retry_cnt as u32) * 100;
                    tokio::time::sleep(Duration::from_millis(retry_sleep)).await;
                    continue;
                } else {
                    error!(
                        "failed to remove file {}, reach max retry count {}, err {:?}",
                        file_id, MAX_RETRY_COUNT, err,
                    );
                }
            }
            return;
        }
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn local_dir(&self) -> &Path {
        &self.local_dir
    }

    fn tenant_id(&self) -> u32 {
        self.tenant_id
    }
}

#[derive(Clone)]
pub struct LocalFile {
    pub id: u64,
    fd: Arc<std::fs::File>,
    pub size: u64,
}

impl File for LocalFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> crate::dfs::Result<Bytes> {
        let mut buf = vec![0; length];
        self.fd.read_at(&mut buf, off)?;
        Ok(Bytes::from(buf))
    }
}

#[cfg(test)]
mod tests {
    use crate::dfs::s3::S3FS;
    use crate::dfs::{Options, DFS};
    use file_system::{IORateLimitMode, IORateLimiter};
    use std::path::PathBuf;
    use std::str::FromStr;
    use std::sync::Arc;

    #[test]
    fn test_s3() {
        crate::tests::init_logger();

        let end_point = "http://127.0.0.1:9000";
        let local_dir = PathBuf::from_str("/tmp/s3test").unwrap();
        if !local_dir.exists() {
            std::fs::create_dir(&local_dir).unwrap();
        }
        let rate_limiter = Arc::new(IORateLimiter::new(IORateLimitMode::WriteOnly, true, false));
        rate_limiter.set_io_rate_limit(0);
        let s3fs = S3FS::new(
            123,
            local_dir,
            end_point.into(),
            "minioadmin".into(),
            "minioadmin".into(),
            "local".into(),
            "shard-db".into(),
            rate_limiter,
        );
        let file_data = "abcdefgh".to_string().into_bytes();
        let (tx, rx) = tikv_util::mpsc::bounded(1);

        let fs = s3fs.clone();
        let file_data2 = file_data.clone();
        let f = async move {
            match fs
                .create(321, bytes::Bytes::from(file_data2), Options::new(1, 1))
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let local_file = s3fs.local_file_path(321);
        let data = std::fs::read(&local_file).unwrap();
        assert_eq!(&data, &file_data);
        std::fs::remove_file(&local_file).unwrap();
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            let opts = Options::new(1, 1);
            match fs.prefetch(321, opts).await {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let data = std::fs::read(&local_file).unwrap();
        assert_eq!(&data, &file_data);
        let file = s3fs.open(321, Options::new(1, 1)).unwrap();
        assert_eq!(file.size(), 8u64);
        assert_eq!(file.id(), 321u64);
        let data = file.read(0, 8).unwrap();
        assert_eq!(&data, &file_data);
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(321, Options::new(1, 1)).await;
            tx.send(true).unwrap();
        };
        s3fs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        assert!(!local_file.exists());
    }
}
