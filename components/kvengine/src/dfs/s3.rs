// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use crate::dfs::{new_filename, File, Options, DFS};
use async_trait::async_trait;
use bytes::Bytes;
use rusoto_core::{Region, RusotoError};
use rusoto_s3::util::{AddressingStyle, S3Config};
use rusoto_s3::S3;
use std::ops::Deref;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tikv_util::time::Instant;
use tokio::io::AsyncReadExt;
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
    ) -> Self {
        let core = Arc::new(S3FSCore::new(
            tenant_id, local_dir, end_point, key_id, secret_key, region, bucket,
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
    s3c: rusoto_s3::S3Client,
    bucket: String,
    runtime: tokio::runtime::Runtime,
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
    ) -> Self {
        let credential = rusoto_credential::StaticProvider::new(key_id, secret_key, None, None);
        let http_connector = hyper::client::connect::HttpConnector::new();
        let mut config = rusoto_core::HttpConfig::new();
        config.read_buf_size(256 * 1024);
        let http_client =
            rusoto_core::HttpClient::from_connector_with_config(http_connector, config);
        let end_point = if end_point.is_empty() {
            format!("http://s3.{}.amazonaws.com", region.as_str())
        } else {
            end_point
        };
        let region = Region::Custom {
            name: region,
            endpoint: end_point,
        };
        let mut s3c = rusoto_s3::S3Client::new_with(http_client, credential, region);
        s3c.set_config(S3Config {
            addressing_style: AddressingStyle::Path,
        });
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
        }
    }

    fn local_file_path(&self, file_id: u64) -> PathBuf {
        self.local_dir.join(new_filename(file_id))
    }

    fn file_key(&self, file_id: u64) -> String {
        format!("{:08x}/{:016x}.sst", self.tenant_id, file_id)
    }

    fn is_err_retryable<T>(&self, rustoto_err: &RusotoError<T>) -> bool {
        match rustoto_err {
            RusotoError::Service(_) => true,
            RusotoError::HttpDispatch(_) => true,
            RusotoError::InvalidDnsName(_) => false,
            RusotoError::Credentials(_) => false,
            RusotoError::Validation(_) => false,
            RusotoError::ParseError(_) => false,
            RusotoError::Unknown(resp) => resp.status.is_server_error(),
            RusotoError::Blocking => false,
        }
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

    async fn read_file(&self, file_id: u64, _opts: Options) -> crate::dfs::Result<Bytes> {
        let mut retry_cnt = 0;
        let start_time = Instant::now_coarse();
        loop {
            let mut req = rusoto_s3::GetObjectRequest::default();
            req.bucket = self.bucket.clone();
            req.key = self.file_key(file_id);
            let result = self.s3c.get_object(req).await;
            if result.is_ok() {
                let output = result.unwrap();
                let body = output.body.unwrap();
                let length = output.content_length.unwrap_or(0) as usize;
                let mut buf = Vec::with_capacity(length);
                return match body.into_async_read().read_to_end(&mut buf).await {
                    Ok(read_size) => {
                        info!(
                            "read file {}, size {}, takes {:?}, retry {}",
                            file_id,
                            read_size,
                            start_time.saturating_elapsed(),
                            retry_cnt
                        );
                        Ok(Bytes::from(buf))
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
        let mut retry_cnt = 0;
        let start_time = Instant::now();
        let data_len = data.len();
        loop {
            let mut req = rusoto_s3::PutObjectRequest::default();
            req.key = self.file_key(file_id);
            req.bucket = self.bucket.clone();
            req.content_length = Some(data_len as i64);
            let data = data.clone();
            let stream = futures::stream::once(async move { Ok(data) });
            req.body = Some(rusoto_core::ByteStream::new(stream));
            let result = self.s3c.put_object(req).await;
            if result.is_ok() {
                info!(
                    "create file {}, size {}, takes {:?}, retry {}",
                    file_id,
                    data_len,
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
        let mut retry_cnt = 0;
        loop {
            let bucket = self.bucket.clone();
            let key = self.file_key(file_id);
            let mut req = rusoto_s3::CopyObjectRequest::default();
            req.copy_source = format!("{}/{}", &bucket, &key);
            req.key = key;
            req.bucket = bucket;
            req.tagging = Some("deleted=true".into());
            req.tagging_directive = Some("REPLACE".into());
            req.metadata_directive = Some("REPLACE".into());
            if let Err(err) = self.s3c.copy_object(req).await {
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

    fn read_at(&self, buf: &mut [u8], offset: u64) -> crate::dfs::Result<()> {
        self.fd.read_at(buf, offset)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::dfs::s3::S3FS;
    use crate::dfs::{Options, DFS};
    use bytes::Buf;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use std::str::FromStr;

    #[test]
    fn test_s3() {
        crate::tests::init_logger();

        let end_point = "http://localhost:9000";
        let local_dir = PathBuf::from_str("/tmp/s3test").unwrap();
        if !local_dir.exists() {
            std::fs::create_dir(&local_dir).unwrap();
        }
        let s3fs = S3FS::new(
            123,
            local_dir,
            end_point.into(),
            "minioadmin".into(),
            "minioadmin".into(),
            "local".into(),
            "shard-db".into(),
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
        let fs = s3fs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let local_file = s3fs.local_file_path(321);
        let move_local_file = local_file.clone();
        let f = async move {
            let opts = Options::new(1, 1);
            match fs.read_file(321, opts).await {
                Ok(data) => {
                    let mut file = std::fs::File::create(&move_local_file).unwrap();
                    file.write_all(data.chunk()).unwrap();
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
        let _ = fs::remove_file(local_file);
    }
}
