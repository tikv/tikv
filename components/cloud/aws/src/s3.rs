// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error as StdError, io, time::Duration};

use async_trait::async_trait;
use cloud::{
    blob::{none_to_empty, BlobConfig, BlobStorage, BucketConf, PutResource, StringNonEmpty},
    metrics::CLOUD_REQUEST_HISTOGRAM_VEC,
};
use fail::fail_point;
use futures_util::{
    future::FutureExt,
    io::{AsyncRead, AsyncReadExt},
    stream::TryStreamExt,
};
pub use kvproto::brpb::{Bucket as InputBucket, CloudDynamic, S3 as InputConfig};
use rusoto_core::{request::DispatchSignedRequest, ByteStream, RusotoError};
use rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use rusoto_s3::{util::AddressingStyle, *};
use thiserror::Error;
use tikv_util::{
    debug,
    stream::{error_stream, retry},
    time::Instant,
};
use tokio::time::{sleep, timeout};

use crate::util;

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(900);
pub const STORAGE_VENDOR_NAME_AWS: &str = "aws";

#[derive(Clone)]
pub struct AccessKeyPair {
    pub access_key: StringNonEmpty,
    pub secret_access_key: StringNonEmpty,
}

impl std::fmt::Debug for AccessKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessKeyPair")
            .field("access_key", &self.access_key)
            .field("secret_access_key", &"?")
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    bucket: BucketConf,
    sse: Option<StringNonEmpty>,
    acl: Option<StringNonEmpty>,
    access_key_pair: Option<AccessKeyPair>,
    force_path_style: bool,
    sse_kms_key_id: Option<StringNonEmpty>,
    storage_class: Option<StringNonEmpty>,
    multi_part_size: usize,
}

impl Config {
    #[cfg(test)]
    pub fn default(bucket: BucketConf) -> Self {
        Self {
            bucket,
            sse: None,
            acl: None,
            access_key_pair: None,
            force_path_style: false,
            sse_kms_key_id: None,
            storage_class: None,
            multi_part_size: MINIMUM_PART_SIZE,
        }
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Config> {
        let bucket = BucketConf::from_cloud_dynamic(cloud_dynamic)?;
        let attrs = &cloud_dynamic.attrs;
        let def = &String::new();
        let force_path_style_str = attrs.get("force_path_style").unwrap_or(def).clone();
        let force_path_style = force_path_style_str == "true" || force_path_style_str == "True";
        let access_key_opt = attrs.get("access_key");
        let access_key_pair = if let Some(access_key) = access_key_opt {
            let secret_access_key = attrs.get("secret_access_key").unwrap_or(def).clone();
            Some(AccessKeyPair {
                access_key: StringNonEmpty::required_field(access_key.clone(), "access_key")?,
                secret_access_key: StringNonEmpty::required_field(
                    secret_access_key,
                    "secret_access_key",
                )?,
            })
        } else {
            None
        };
        let storage_class = bucket.storage_class.clone();
        Ok(Config {
            bucket,
            storage_class,
            sse: StringNonEmpty::opt(attrs.get("sse").unwrap_or(def).clone()),
            acl: StringNonEmpty::opt(attrs.get("acl").unwrap_or(def).clone()),
            access_key_pair,
            force_path_style,
            sse_kms_key_id: StringNonEmpty::opt(attrs.get("sse_kms_key_id").unwrap_or(def).clone()),
            multi_part_size: MINIMUM_PART_SIZE,
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let storage_class = StringNonEmpty::opt(input.storage_class);
        let endpoint = StringNonEmpty::opt(input.endpoint);
        let config_bucket = BucketConf {
            endpoint,
            bucket: StringNonEmpty::required_field(input.bucket, "bucket")?,
            prefix: StringNonEmpty::opt(input.prefix),
            storage_class: storage_class.clone(),
            region: StringNonEmpty::opt(input.region),
        };
        let access_key_pair = match StringNonEmpty::opt(input.access_key) {
            None => None,
            Some(ak) => Some(AccessKeyPair {
                access_key: ak,
                secret_access_key: StringNonEmpty::required_field(
                    input.secret_access_key,
                    "secret_access_key",
                )?,
            }),
        };
        Ok(Config {
            storage_class,
            bucket: config_bucket,
            sse: StringNonEmpty::opt(input.sse),
            acl: StringNonEmpty::opt(input.acl),
            access_key_pair,
            force_path_style: input.force_path_style,
            sse_kms_key_id: StringNonEmpty::opt(input.sse_kms_key_id),
            multi_part_size: MINIMUM_PART_SIZE,
        })
    }
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        self.bucket.url("s3").map_err(|s| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("error creating bucket url: {}", s),
            )
        })
    }
}

#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: S3Client,
}

impl S3Storage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        Self::new(Config::from_cloud_dynamic(cloud_dynamic)?)
    }

    pub fn set_multi_part_size(&mut self, mut size: usize) {
        if size < MINIMUM_PART_SIZE {
            // default multi_part_size is 5MB, S3 cannot allow a smaller size.
            size = MINIMUM_PART_SIZE
        }
        self.config.multi_part_size = size;
    }

    /// Create a new S3 storage for the given config.
    pub fn new(config: Config) -> io::Result<S3Storage> {
        Self::with_request_dispatcher(config, util::new_http_client()?)
    }

    fn new_creds_dispatcher<Creds, Dispatcher>(
        config: Config,
        dispatcher: Dispatcher,
        credentials_provider: Creds,
    ) -> io::Result<S3Storage>
    where
        Creds: ProvideAwsCredentials + Send + Sync + 'static,
        Dispatcher: DispatchSignedRequest + Send + Sync + 'static,
    {
        let bucket_region = none_to_empty(config.bucket.region.clone());
        let bucket_endpoint = config.bucket.endpoint.clone();
        let region = util::get_region(&bucket_region, &none_to_empty(bucket_endpoint))?;
        let mut client = S3Client::new_with(dispatcher, credentials_provider, region);
        if config.force_path_style {
            client.config_mut().addressing_style = AddressingStyle::Path;
        }
        Ok(S3Storage { config, client })
    }

    pub fn with_request_dispatcher<D>(config: Config, dispatcher: D) -> io::Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        // static credentials are used with minio
        if let Some(access_key_pair) = &config.access_key_pair {
            let cred_provider = StaticProvider::new_minimal(
                (*access_key_pair.access_key).to_owned(),
                (*access_key_pair.secret_access_key).to_owned(),
            );
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        } else {
            let cred_provider = util::CredentialsProvider::new()?;
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        }
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", *prefix, key);
        }
        key.to_owned()
    }
}

/// A helper for uploading a large files to S3 storage.
///
/// Note: this uploader does not support uploading files larger than 19.5 GiB.
struct S3Uploader<'client> {
    client: &'client S3Client,

    bucket: String,
    key: String,
    acl: Option<StringNonEmpty>,
    server_side_encryption: Option<StringNonEmpty>,
    sse_kms_key_id: Option<StringNonEmpty>,
    storage_class: Option<StringNonEmpty>,
    multi_part_size: usize,

    upload_id: String,
    parts: Vec<CompletedPart>,
}

/// The errors a uploader can meet.
/// This was made for make the result of [S3Uploader::run] get [Send].
#[derive(Debug, Error)]
enum UploadError {
    #[error("io error {0}")]
    Io(#[from] io::Error),
    #[error("rusoto error {0}")]
    // Maybe make it a trait if needed?
    Rusoto(String),
}

impl<T: 'static + StdError> From<RusotoError<T>> for UploadError {
    fn from(r: RusotoError<T>) -> Self {
        Self::Rusoto(format!("{}", r))
    }
}

/// Specifies the minimum size to use multi-part upload.
/// AWS S3 requires each part to be at least 5 MiB.
const MINIMUM_PART_SIZE: usize = 5 * 1024 * 1024;

impl<'client> S3Uploader<'client> {
    /// Creates a new uploader with a given target location and upload configuration.
    fn new(client: &'client S3Client, config: &Config, key: String) -> Self {
        Self {
            client,
            key,
            bucket: config.bucket.bucket.to_string(),
            acl: config.acl.as_ref().cloned(),
            server_side_encryption: config.sse.as_ref().cloned(),
            sse_kms_key_id: config.sse_kms_key_id.as_ref().cloned(),
            storage_class: config.storage_class.as_ref().cloned(),
            multi_part_size: config.multi_part_size,
            upload_id: "".to_owned(),
            parts: Vec::new(),
        }
    }

    /// Executes the upload process.
    async fn run(
        mut self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> Result<(), UploadError> {
        if est_len <= self.multi_part_size as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry(|| self.upload(&data)).await?;
            Ok(())
        } else {
            // Otherwise, use multipart upload to improve robustness.
            self.upload_id = retry(|| self.begin()).await?;
            let upload_res = async {
                let mut buf = vec![0; self.multi_part_size];
                let mut part_number = 1;
                loop {
                    let data_size = reader.read(&mut buf).await?;
                    if data_size == 0 {
                        break;
                    }
                    let part = retry(|| self.upload_part(part_number, &buf[..data_size])).await?;
                    self.parts.push(part);
                    part_number += 1;
                }
                Ok(())
            }
            .await;

            if upload_res.is_ok() {
                retry(|| self.complete()).await?;
            } else {
                let _ = retry(|| self.abort()).await;
            }
            upload_res
        }
    }

    /// Starts a multipart upload process.
    async fn begin(&self) -> Result<String, RusotoError<CreateMultipartUploadError>> {
        match timeout(
            Self::get_timeout(),
            self.client
                .create_multipart_upload(CreateMultipartUploadRequest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    acl: self.acl.as_ref().map(|s| s.to_string()),
                    server_side_encryption: self
                        .server_side_encryption
                        .as_ref()
                        .map(|s| s.to_string()),
                    ssekms_key_id: self.sse_kms_key_id.as_ref().map(|s| s.to_string()),
                    storage_class: self.storage_class.as_ref().map(|s| s.to_string()),
                    ..Default::default()
                }),
        )
        .await
        {
            Ok(output) => output?.upload_id.ok_or_else(|| {
                RusotoError::ParseError(
                    "missing upload-id from create_multipart_upload()".to_owned(),
                )
            }),
            Err(_) => Err(RusotoError::ParseError(
                "timeout after 15mins for begin in s3 storage".to_owned(),
            )),
        }
    }

    /// Completes a multipart upload process, asking S3 to join all parts into a single file.
    async fn complete(&self) -> Result<(), RusotoError<CompleteMultipartUploadError>> {
        let res = timeout(
            Self::get_timeout(),
            self.client
                .complete_multipart_upload(CompleteMultipartUploadRequest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    upload_id: self.upload_id.clone(),
                    multipart_upload: Some(CompletedMultipartUpload {
                        parts: Some(self.parts.clone()),
                    }),
                    ..Default::default()
                }),
        )
        .await
        .map_err(|_| {
            RusotoError::ParseError("timeout after 15mins for complete in s3 storage".to_owned())
        })?;
        res.map(|_| ())
    }

    /// Aborts the multipart upload process, deletes all uploaded parts.
    async fn abort(&self) -> Result<(), RusotoError<AbortMultipartUploadError>> {
        let res = timeout(
            Self::get_timeout(),
            self.client
                .abort_multipart_upload(AbortMultipartUploadRequest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    upload_id: self.upload_id.clone(),
                    ..Default::default()
                }),
        )
        .await
        .map_err(|_| {
            RusotoError::ParseError("timeout after 15mins for abort in s3 storage".to_owned())
        })?;
        res.map(|_| ())
    }

    /// Uploads a part of the file.
    ///
    /// The `part_number` must be between 1 to 10000.
    async fn upload_part(
        &self,
        part_number: i64,
        data: &[u8],
    ) -> Result<CompletedPart, RusotoError<UploadPartError>> {
        match timeout(Self::get_timeout(), async {
            let start = Instant::now();
            let r = self
                .client
                .upload_part(UploadPartRequest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    upload_id: self.upload_id.clone(),
                    part_number,
                    content_length: Some(data.len() as i64),
                    body: Some(data.to_vec().into()),
                    ..Default::default()
                })
                .await;
            CLOUD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["s3", "upload_part"])
                .observe(start.saturating_elapsed().as_secs_f64());
            r
        })
        .await
        {
            Ok(part) => Ok(CompletedPart {
                e_tag: part?.e_tag,
                part_number: Some(part_number),
            }),
            Err(_) => Err(RusotoError::ParseError(
                "timeout after 15mins for upload part in s3 storage".to_owned(),
            )),
        }
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus relatively cheap to
    /// retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RusotoError<PutObjectError>> {
        let res = timeout(Self::get_timeout(), async {
            #[cfg(feature = "failpoints")]
            let delay_duration = (|| {
                fail_point!("s3_sleep_injected", |t| {
                    let t = t.unwrap().parse::<u64>().unwrap();
                    Duration::from_millis(t)
                });
                Duration::from_millis(0)
            })();
            #[cfg(not(feature = "failpoints"))]
            let delay_duration = Duration::from_millis(0);

            if delay_duration > Duration::from_millis(0) {
                sleep(delay_duration).await;
            }

            #[cfg(feature = "failpoints")]
            fail_point!("s3_put_obj_err", |_| {
                Err(RusotoError::ParseError("failed to put object".to_owned()))
            });

            let start = Instant::now();
            let r = self
                .client
                .put_object(PutObjectRequest {
                    bucket: self.bucket.clone(),
                    key: self.key.clone(),
                    acl: self.acl.as_ref().map(|s| s.to_string()),
                    server_side_encryption: self
                        .server_side_encryption
                        .as_ref()
                        .map(|s| s.to_string()),
                    ssekms_key_id: self.sse_kms_key_id.as_ref().map(|s| s.to_string()),
                    storage_class: self.storage_class.as_ref().map(|s| s.to_string()),
                    content_length: Some(data.len() as i64),
                    body: Some(data.to_vec().into()),
                    ..Default::default()
                })
                .await;
            CLOUD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["s3", "put_object"])
                .observe(start.saturating_elapsed().as_secs_f64());
            r
        })
        .await
        .map_err(|_| {
            RusotoError::ParseError("timeout after 15mins for upload in s3 storage".to_owned())
        })?;
        res.map(|_| ())
    }

    fn get_timeout() -> Duration {
        fail_point!("s3_timeout_injected", |t| -> Duration {
            let t = t.unwrap().parse::<u64>();
            Duration::from_millis(t.unwrap())
        });
        CONNECTION_TIMEOUT
    }
}

const STORAGE_NAME: &str = "s3";

#[async_trait]
impl BlobStorage for S3Storage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.config.clone()) as Box<dyn BlobConfig>
    }

    async fn put(
        &self,
        name: &str,
        mut reader: PutResource,
        content_length: u64,
    ) -> io::Result<()> {
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);

        let uploader = S3Uploader::new(&self.client, &self.config, key);
        let result = uploader.run(&mut reader, content_length).await;
        result.map_err(|e| {
            let error_code = if let UploadError::Io(ref io_error) = e {
                io_error.kind()
            } else {
                io::ErrorKind::Other
            };
            // Even we can check whether there is an `io::Error` internal and extract it directly,
            // We still need to keep the message 'failed to put object' here for adapting the string-matching based
            // retry logic in BR :(
            io::Error::new(error_code, format!("failed to put object {}", e))
        })
    }

    fn get(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let key = self.maybe_prefix_key(name);
        let bucket = self.config.bucket.bucket.clone();
        debug!("read file from s3 storage"; "key" => %key);
        let req = GetObjectRequest {
            key,
            bucket: (*bucket).clone(),
            ..Default::default()
        };
        Box::new(
            self.client
                .get_object(req)
                .map(move |future| match future {
                    Ok(out) => out.body.unwrap(),
                    Err(RusotoError::Service(GetObjectError::NoSuchKey(key))) => {
                        ByteStream::new(error_stream(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("no key {} at bucket {}", key, *bucket),
                        )))
                    }
                    Err(e) => ByteStream::new(error_stream(io::Error::new(
                        io::ErrorKind::Other,
                        format!("failed to get object {}", e),
                    ))),
                })
                .flatten_stream()
                .into_async_read(),
        )
    }
}

#[cfg(test)]
mod tests {
    use rusoto_core::signature::SignedRequest;
    use rusoto_mock::{MockRequestDispatcher, MultipleMockRequestDispatcher};
    use tikv_util::stream::block_on_external_io;

    use super::*;

    #[test]
    fn test_s3_config() {
        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-2".to_string());
        bucket.prefix = StringNonEmpty::opt("myprefix".to_string());
        let mut config = Config::default(bucket);
        config.access_key_pair = Some(AccessKeyPair {
            access_key: StringNonEmpty::required("abc".to_string()).unwrap(),
            secret_access_key: StringNonEmpty::required("xyz".to_string()).unwrap(),
        });
        let mut s = S3Storage::new(config.clone()).unwrap();
        // set a less than 5M value not work
        s.set_multi_part_size(1024);
        assert_eq!(s.config.multi_part_size, 5 * 1024 * 1024);
        // set 8M
        s.set_multi_part_size(8 * 1024 * 1024);
        assert_eq!(s.config.multi_part_size, 8 * 1024 * 1024);
        // set 6M
        s.set_multi_part_size(6 * 1024 * 1024);
        assert_eq!(s.config.multi_part_size, 6 * 1024 * 1024);
        // set a less than 5M value will fallback to 5M
        s.set_multi_part_size(1024);
        assert_eq!(s.config.multi_part_size, 5 * 1024 * 1024);

        config.bucket.region = StringNonEmpty::opt("foo".to_string());
        assert!(S3Storage::new(config).is_err());
    }

    #[tokio::test]
    async fn test_s3_storage_multi_part() {
        let magic_contents = "567890";

        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let bucket = BucketConf::default(bucket_name);
        let mut config = Config::default(bucket);
        let multi_part_size = 2;
        // set multi_part_size to use upload_part function
        config.multi_part_size = multi_part_size;

        // split magic_contents into 3 parts, so we mock 5 requests here(1 begin + 3 part + 1 complete)
        let dispatcher = MultipleMockRequestDispatcher::new(vec![
            MockRequestDispatcher::with_status(200).with_body(
                r#"<?xml version="1.0" encoding="UTF-8"?>
               <root>
                 <UploadId>1</UploadId>
               </root>"#,
            ),
            MockRequestDispatcher::with_status(200),
            MockRequestDispatcher::with_status(200),
            MockRequestDispatcher::with_status(200),
            MockRequestDispatcher::with_status(200),
        ]);

        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());

        let s = S3Storage::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();

        let resp = s
            .put(
                "mykey",
                PutResource(Box::new(magic_contents.as_bytes())),
                magic_contents.len() as u64,
            )
            .await;
        assert!(resp.is_ok());
        assert_eq!(
            CLOUD_REQUEST_HISTOGRAM_VEC
                .get_metric_with_label_values(&["s3", "upload_part"])
                .unwrap()
                .get_sample_count(),
            // length of magic_contents
            (magic_contents.len() / multi_part_size) as u64,
        );
    }

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_s3_storage() {
        let magic_contents = "5678";
        let bucket_name = StringNonEmpty::required("mybucket".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-2".to_string());
        bucket.prefix = StringNonEmpty::opt("myprefix".to_string());
        let mut config = Config::default(bucket);
        config.force_path_style = true;
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-2");
                assert_eq!(req.hostname(), "s3.ap-southeast-2.amazonaws.com");
                assert_eq!(req.path(), "/mybucket/myprefix/mykey");
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let s = S3Storage::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        s.put(
            "mykey",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        )
        .await
        .unwrap();

        let mut reader = s.get("mykey");
        let mut buf = Vec::new();
        let ret = reader.read_to_end(&mut buf).await;
        assert!(ret.unwrap() == 0);
        assert!(buf.is_empty());

        // inject put error
        let s3_put_obj_err_fp = "s3_put_obj_err";
        fail::cfg(s3_put_obj_err_fp, "return").unwrap();
        let resp = s
            .put(
                "mykey",
                PutResource(Box::new(magic_contents.as_bytes())),
                magic_contents.len() as u64,
            )
            .await;
        fail::remove(s3_put_obj_err_fp);
        assert!(resp.is_err());

        // test timeout
        let s3_timeout_injected_fp = "s3_timeout_injected";
        let s3_sleep_injected_fp = "s3_sleep_injected";

        // inject 100ms timeout
        fail::cfg(s3_timeout_injected_fp, "return(100)").unwrap();
        // inject 200ms delay
        fail::cfg(s3_sleep_injected_fp, "return(200)").unwrap();
        let resp = s
            .put(
                "mykey",
                PutResource(Box::new(magic_contents.as_bytes())),
                magic_contents.len() as u64,
            )
            .await;
        fail::remove(s3_sleep_injected_fp);
        // timeout occur due to delay 200ms
        assert!(resp.is_err());

        // inject 50ms delay
        fail::cfg(s3_sleep_injected_fp, "return(50)").unwrap();
        let resp = s
            .put(
                "mykey",
                PutResource(Box::new(magic_contents.as_bytes())),
                magic_contents.len() as u64,
            )
            .await;
        fail::remove(s3_sleep_injected_fp);
        fail::remove(s3_timeout_injected_fp);
        // no timeout
        assert!(resp.is_ok());
    }

    #[test]
    fn test_s3_storage_with_virtual_host() {
        let magic_contents = "abcd";
        let bucket_name = StringNonEmpty::required("bucket2".to_string()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.region = StringNonEmpty::opt("ap-southeast-1".to_string());
        bucket.prefix = StringNonEmpty::opt("prefix2".to_string());
        let mut config = Config::default(bucket);
        config.force_path_style = false;
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-1");
                assert_eq!(req.hostname(), "bucket2.s3.ap-southeast-1.amazonaws.com");
                assert_eq!(req.path(), "/prefix2/key2");
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let s = S3Storage::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        block_on_external_io(s.put(
            "key2",
            PutResource(Box::new(magic_contents.as_bytes())),
            magic_contents.len() as u64,
        ))
        .unwrap();
    }

    #[test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test) if we've got a
    // reliable way to test s3 (rusoto_mock requires custom logic to verify the
    // body stream which itself can have bug)
    fn test_real_s3_storage() {
        use tikv_util::time::Limiter;

        let bucket = BucketConf {
            endpoint: "http://127.0.0.1:9000".to_owned(),
            bucket: "bucket".to_owned(),
            prefix: "prefix".to_owned(),
            ..BucketConf::default()
        };
        let s3 = Config {
            access_key: "93QZ01QRBYQQXC37XHZV".to_owned(),
            secret_access_key: "N2VcI4Emg0Nm7fDzGBMJvguHHUxLGpjfwt2y4+vJ".to_owned(),
            force_path_style: true,
            ..Config::default()
        };

        let limiter = Limiter::new(f64::INFINITY);

        let storage = S3Storage::new(&s3).unwrap();
        const LEN: usize = 1024 * 1024 * 4;
        static CONTENT: [u8; LEN] = [50_u8; LEN];
        storage
            .write(
                "huge_file",
                Box::new(limiter.limit(&CONTENT[..])),
                LEN as u64,
            )
            .unwrap();

        let mut reader = storage.get("huge_file");
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf).await.unwrap();
        assert_eq!(buf.len(), LEN);
        assert_eq!(buf.iter().position(|b| *b != 50_u8), None);
    }

    #[test]
    fn test_url_of_backend() {
        let bucket_name = StringNonEmpty::required("bucket".to_owned()).unwrap();
        let mut bucket = BucketConf::default(bucket_name);
        bucket.prefix = Some(StringNonEmpty::static_str("/backup 01/prefix/"));
        let s3 = Config::default(bucket.clone());
        assert_eq!(
            s3.url().unwrap().to_string(),
            "s3://bucket/backup%2001/prefix/"
        );
        bucket.endpoint = Some(StringNonEmpty::static_str("http://endpoint.com"));
        let s3 = Config::default(bucket);
        assert_eq!(
            s3.url().unwrap().to_string(),
            "http://endpoint.com/bucket/backup%2001/prefix/"
        );
    }

    #[test]
    fn test_config_round_trip() {
        let mut input = InputConfig::default();
        input.set_bucket("bucket".to_owned());
        input.set_prefix("backup 02/prefix/".to_owned());
        input.set_region("us-west-2".to_owned());
        let c1 = Config::from_input(input.clone()).unwrap();
        let c2 = Config::from_cloud_dynamic(&cloud_dynamic_from_input(input)).unwrap();
        assert_eq!(c1.bucket.bucket, c2.bucket.bucket);
        assert_eq!(c1.bucket.prefix, c2.bucket.prefix);
        assert_eq!(c1.bucket.region, c2.bucket.region);
        assert_eq!(
            c1.bucket.region,
            StringNonEmpty::opt("us-west-2".to_owned())
        );
    }

    fn cloud_dynamic_from_input(mut s3: InputConfig) -> CloudDynamic {
        let mut bucket = InputBucket::default();
        if !s3.endpoint.is_empty() {
            bucket.endpoint = s3.take_endpoint();
        }
        if !s3.region.is_empty() {
            bucket.region = s3.take_region();
        }
        if !s3.prefix.is_empty() {
            bucket.prefix = s3.take_prefix();
        }
        if !s3.storage_class.is_empty() {
            bucket.storage_class = s3.take_storage_class();
        }
        if !s3.bucket.is_empty() {
            bucket.bucket = s3.take_bucket();
        }
        let mut attrs = std::collections::HashMap::new();
        if !s3.sse.is_empty() {
            attrs.insert("sse".to_owned(), s3.take_sse());
        }
        if !s3.acl.is_empty() {
            attrs.insert("acl".to_owned(), s3.take_acl());
        }
        if !s3.access_key.is_empty() {
            attrs.insert("access_key".to_owned(), s3.take_access_key());
        }
        if !s3.secret_access_key.is_empty() {
            attrs.insert("secret_access_key".to_owned(), s3.take_secret_access_key());
        }
        if !s3.sse_kms_key_id.is_empty() {
            attrs.insert("sse_kms_key_id".to_owned(), s3.take_sse_kms_key_id());
        }
        if s3.force_path_style {
            attrs.insert("force_path_style".to_owned(), "true".to_owned());
        }
        let mut cd = CloudDynamic::default();
        cd.set_provider_name("aws".to_owned());
        cd.set_attrs(attrs);
        cd.set_bucket(bucket);
        cd
    }
}
