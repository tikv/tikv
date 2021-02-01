// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use rusoto_core::request::HttpClient;
use rusoto_credential::{ProvideAwsCredentials, StaticProvider};
use std::io;
use std::marker::PhantomData;

use futures_util::{
    future::FutureExt,
    io::{AsyncRead, AsyncReadExt},
    stream::TryStreamExt,
};

use rusoto_core::{
    request::DispatchSignedRequest,
    {ByteStream, RusotoError},
};
use rusoto_s3::*;

use tikv_util::stream::{block_on_external_io, error_stream, retry};
use external_storage::{
    empty_to_none, none_to_empty, BucketConf, ExternalStorage,
pub use kvproto::backup::{Bucket as InputBucket, CloudDynamic, S3 as InputConfig};

#[derive(Clone)]
pub struct AccessKeyPair {
    pub access_key: String,
    pub secret_access_key: String,
}

impl std::fmt::Debug for AccessKeyPair {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccessKeyPair")
            .field("access_key", &self.access_key)
            .field("secret_access_key", &"REDACTED".to_string())
            .finish()
    }
}

#[derive(Clone, Debug, Default)]
pub struct Config {
    pub bucket: BucketConf,
    pub sse: Option<String>,
    pub acl: Option<String>,
    pub access_key_pair: Option<AccessKeyPair>,
    pub force_path_style: bool,
    pub sse_kms_key_id: Option<String>,
}

impl Config {
    pub fn validate(&self) -> io::Result<()> {
        self.bucket.validate()
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Config> {
        let bucket = cloud_dynamic.bucket.clone().into_option().ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, "Required field bucket is missing")
        })?;
        let attrs = &cloud_dynamic.attrs;
        let def = &String::new();
        let force_path_style_str = attrs.get("force_path_style").unwrap_or(def).clone();
        let force_path_style = force_path_style_str == "true" || force_path_style_str == "True";
        let config_bucket = BucketConf {
            bucket: bucket.bucket,
            endpoint: empty_to_none(bucket.endpoint),
            prefix: empty_to_none(bucket.prefix),
            storage_class: empty_to_none(bucket.storage_class),
            region: None,
        };
        let access_key_opt = attrs.get("access_key");
        let access_key_pair = if let Some(access_key) = access_key_opt {
            let secret_access_key = attrs.get("secret_access_key").unwrap_or(def).clone();
            Some(AccessKeyPair {
                access_key: access_key.clone(),
                secret_access_key,
            })
        } else {
            None
        };
        Ok(Config {
            bucket: config_bucket,
            sse: empty_to_none(attrs.get("sse").unwrap_or(def).clone()),
            acl: empty_to_none(attrs.get("acl").unwrap_or(def).clone()),
            access_key_pair,
            force_path_style,
            sse_kms_key_id: empty_to_none(attrs.get("sse_kms_key_id").unwrap_or(def).clone()),
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let bucket = input
            .bucket_info
            .into_option()
            .unwrap_or_else(InputBucket::default);
        let config_bucket = BucketConf {
            bucket: bucket.bucket,
            endpoint: empty_to_none(bucket.endpoint),
            prefix: empty_to_none(bucket.prefix),
            storage_class: empty_to_none(bucket.storage_class),
            region: None,
        };
        let access_key_pair = if !input.access_key.is_empty() {
            Some(AccessKeyPair {
                access_key: input.access_key,
                secret_access_key: input.secret_access_key,
            })
        } else {
            None
        };
        Ok(Config {
            bucket: config_bucket,
            sse: empty_to_none(input.sse),
            acl: empty_to_none(input.acl),
            access_key_pair,
            force_path_style: input.force_path_style,
            sse_kms_key_id: empty_to_none(input.sse_kms_key_id),
        })
    }
}

/// S3 compatible storage
#[derive(Clone)]
pub struct S3Storage {
    config: Config,
    client: S3Client,
    // This should be safe to remove now that we don't use the global client/dispatcher
    // See https://github.com/tikv/tikv/issues/7236.
    _not_send: PhantomData<*const ()>,
}

impl S3Storage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        Self::new(Config::from_cloud_dynamic(cloud_dynamic)?)
    }

    /// Create a new S3 storage for the given config.
    pub fn new(config: Config) -> io::Result<S3Storage> {
        config.validate()?;
        // Need to explicitly create a dispatcher
        // See https://github.com/tikv/tikv/issues/7236.
        let dispatcher = HttpClient::new()
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("{}", e)))?;
        Self::with_request_dispatcher(config, dispatcher)
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
        config.validate()?;
        let bucket_region = none_to_empty(config.bucket.region.clone());
        let bucket_endpoint = none_to_empty(config.bucket.endpoint.clone());
        let region = rusoto_util::get_region(&bucket_region, &bucket_endpoint)?;
        let client = S3Client::new_with(dispatcher, credentials_provider, region);
        Ok(S3Storage {
            config,
            client,
            _not_send: PhantomData::default(),
        })
    }

    pub fn with_request_dispatcher<D>(config: Config, dispatcher: D) -> io::Result<S3Storage>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        config.validate()?;
        // TODO: this should not be supported.
        // It implies static AWS credentials.
        if let Some(access_key_pair) = &config.access_key_pair {
            let cred_provider = StaticProvider::new_minimal(
                access_key_pair.access_key.to_owned(),
                access_key_pair.secret_access_key.to_owned(),
            );
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        } else {
            let cred_provider = rusoto_util::CredentialsProvider::new()?;
            Self::new_creds_dispatcher(config, dispatcher, cred_provider)
        }
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", prefix, key);
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
    acl: Option<String>,
    server_side_encryption: Option<String>,
    ssekms_key_id: Option<String>,
    storage_class: Option<String>,

    upload_id: String,
    parts: Vec<CompletedPart>,
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
            bucket: config.bucket.bucket.clone(),
            acl: config.acl.as_ref().cloned(),
            server_side_encryption: config.sse.as_ref().cloned(),
            ssekms_key_id: config.sse_kms_key_id.as_ref().cloned(),
            storage_class: config.bucket.storage_class.as_ref().cloned(),
            upload_id: "".to_owned(),
            parts: Vec::new(),
        }
    }

    /// Executes the upload process.
    async fn run(
        mut self,
        reader: &mut (dyn AsyncRead + Unpin),
        est_len: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if est_len <= MINIMUM_PART_SIZE as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry(|| self.upload(&data)).await?;
            Ok(())
        } else {
            // Otherwise, use multipart upload to improve robustness.
            self.upload_id = retry(|| self.begin()).await?;
            let upload_res = async {
                let mut buf = vec![0; MINIMUM_PART_SIZE];
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
        let output = self
            .client
            .create_multipart_upload(CreateMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                acl: self.acl.clone(),
                server_side_encryption: self.server_side_encryption.clone(),
                ssekms_key_id: self.ssekms_key_id.clone(),
                storage_class: self.storage_class.clone(),
                ..Default::default()
            })
            .await?;
        output.upload_id.ok_or_else(|| {
            RusotoError::ParseError("missing upload-id from create_multipart_upload()".to_owned())
        })
    }

    /// Completes a multipart upload process, asking S3 to join all parts into a single file.
    async fn complete(&self) -> Result<(), RusotoError<CompleteMultipartUploadError>> {
        self.client
            .complete_multipart_upload(CompleteMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.upload_id.clone(),
                multipart_upload: Some(CompletedMultipartUpload {
                    parts: Some(self.parts.clone()),
                }),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Aborts the multipart upload process, deletes all uploaded parts.
    async fn abort(&self) -> Result<(), RusotoError<AbortMultipartUploadError>> {
        self.client
            .abort_multipart_upload(AbortMultipartUploadRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                upload_id: self.upload_id.clone(),
                ..Default::default()
            })
            .await?;
        Ok(())
    }

    /// Uploads a part of the file.
    ///
    /// The `part_number` must be between 1 to 10000.
    async fn upload_part(
        &self,
        part_number: i64,
        data: &[u8],
    ) -> Result<CompletedPart, RusotoError<UploadPartError>> {
        let part = self
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
            .await?;
        Ok(CompletedPart {
            e_tag: part.e_tag,
            part_number: Some(part_number),
        })
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus relatively cheap to
    /// retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RusotoError<PutObjectError>> {
        self.client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: self.key.clone(),
                acl: self.acl.clone(),
                server_side_encryption: self.server_side_encryption.clone(),
                ssekms_key_id: self.ssekms_key_id.clone(),
                storage_class: self.storage_class.clone(),
                content_length: Some(data.len() as i64),
                body: Some(data.to_vec().into()),
                ..Default::default()
            })
            .await?;
        Ok(())
    }
}

fn url_for(config: &Config) -> url::Url {
    config.bucket.url("s3://")
}

const STORAGE_NAME: &str = "s3";

impl ExternalStorage for S3Storage {
    fn name(&self) -> &'static str {
        &STORAGE_NAME
    }

    fn url(&self) -> url::Url {
        url_for(&self.config)
    }

    fn write(
        &self,
        name: &str,
        mut reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        let key = self.maybe_prefix_key(name);
        debug!("save file to s3 storage"; "key" => %key);

        let uploader = S3Uploader::new(&self.client, &self.config, key);
        block_on_external_io(uploader.run(&mut *reader, content_length)).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("failed to put object {}", e))
        })
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let key = self.maybe_prefix_key(name);
        let bucket = self.config.bucket.bucket.clone();
        debug!("read file from s3 storage"; "key" => %key);
        let req = GetObjectRequest {
            key,
            bucket: bucket.clone(),
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
                            format!("no key {} at bucket {}", key, bucket),
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
    use super::*;
    use futures::io::AsyncReadExt;
    use rusoto_core::signature::SignedRequest;
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_s3_config() {
        let mut config = Config {
            bucket: BucketConf {
                bucket: "mybucket".to_string(),
                region: Some("ap-southeast-2".to_string()),
                prefix: Some("myprefix".to_string()),
                ..BucketConf::default()
            },
            access_key_pair: Some(AccessKeyPair {
                access_key: "abc".to_string(),
                secret_access_key: "xyz".to_string(),
            }),
            ..Default::default()
        };
        assert!(S3Storage::new(config.clone()).is_ok());
        config.bucket.bucket = "".to_owned();
        assert!(S3Storage::new(config).is_err());
    }

    #[test]
    fn test_s3_storage() {
        let magic_contents = "5678";
        let config = Config {
            bucket: BucketConf {
                region: empty_to_none("ap-southeast-2".to_string()),
                bucket: "mybucket".to_string(),
                prefix: empty_to_none("myprefix".to_string()),
                ..BucketConf::default()
            },
            ..Config::default()
        };
        let dispatcher = MockRequestDispatcher::with_status(200).with_request_checker(
            move |req: &SignedRequest| {
                assert_eq!(req.region.name(), "ap-southeast-2");
                assert_eq!(req.path(), "/mybucket/myprefix/mykey");
                // PutObject is translated to HTTP PUT.
                assert_eq!(req.payload.is_some(), req.method() == "PUT");
            },
        );
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let s = S3Storage::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        s.write(
            "mykey",
            Box::new(magic_contents.as_bytes()),
            magic_contents.len() as u64,
        )
        .unwrap();
        let mut reader = s.read("mykey");
        let mut buf = Vec::new();
        let ret = block_on_external_io(reader.read_to_end(&mut buf));
        assert!(ret.unwrap() == 0);
        assert!(buf.is_empty());
    }

    #[test]
    #[cfg(FALSE)]
    // FIXME: enable this (or move this to an integration test) if we've got a
    // reliable way to test s3 (rusoto_mock requires custom logic to verify the
    // body stream which itself can have bug)
    fn test_real_s3_storage() {
        use std::f64::INFINITY;
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

        let limiter = Limiter::new(INFINITY);

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

        let mut reader = storage.read("huge_file");
        let mut buf = Vec::new();
        block_on_external_io(reader.read_to_end(&mut buf)).unwrap();
        assert_eq!(buf.len(), LEN);
        assert_eq!(buf.iter().position(|b| *b != 50_u8), None);
    }

    #[test]
    fn test_url_of_backend() {
        let s3 = Config {
            bucket: BucketConf {
                bucket: "bucket".to_owned(),
                prefix: empty_to_none("/backup 01/prefix/".to_owned()),
                endpoint: empty_to_none("http://endpoint.com".to_owned()),
                ..BucketConf::default()
            },
            // ^ only 'bucket' and 'prefix' should be visible in url_of_backend()
            ..Config::default()
        };
        assert_eq!(url_for(&s3).to_string(), "s3://bucket/backup%2001/prefix/");
    }
}
