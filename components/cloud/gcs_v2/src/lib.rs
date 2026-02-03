// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use std::io;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use cloud::blob::{
    BlobConfig, BlobObject, BlobStorage, BlobStream, DeletableStorage, IterableStorage, PutResource,
};
use cloud::metrics;
use futures::future::LocalBoxFuture;
use futures::stream::Stream;
use futures_util::io::AsyncReadExt as _;
use futures_util::stream::StreamExt;
use futures_util::TryStreamExt;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use kvproto::brpb::Gcs as InputConfig;
use tikv_util::{error, info, time::Instant};
use tokio::sync::OnceCell;
use url::Url;

mod kms;
pub use kms::GcpKms;

#[derive(Debug)]
struct GcsApiError {
    op: &'static str,
    source: google_cloud_storage::Error,
}

impl GcsApiError {
    fn new(op: &'static str, source: google_cloud_storage::Error) -> Self {
        Self { op, source }
    }

    fn http_status_code(&self) -> Option<u16> {
        self.source.http_status_code()
    }

    fn grpc_code(&self) -> Option<google_cloud_gax::error::rpc::Code> {
        self.source.status().map(|s| s.code)
    }

    fn into_io_error(self) -> io::Error {
        let kind = match (self.http_status_code(), self.grpc_code()) {
            (Some(401 | 403), _) => io::ErrorKind::PermissionDenied,
            (Some(404), _) => io::ErrorKind::NotFound,
            (Some(408), _) => io::ErrorKind::TimedOut,
            (Some(429), _) => io::ErrorKind::Interrupted,
            (Some(code), _) if (500..=599).contains(&code) => io::ErrorKind::Interrupted,
            (Some(_), _) => io::ErrorKind::InvalidInput,

            (None, Some(google_cloud_gax::error::rpc::Code::NotFound)) => io::ErrorKind::NotFound,
            (None, Some(google_cloud_gax::error::rpc::Code::Unauthenticated))
            | (None, Some(google_cloud_gax::error::rpc::Code::PermissionDenied)) => {
                io::ErrorKind::PermissionDenied
            }
            (None, Some(google_cloud_gax::error::rpc::Code::DeadlineExceeded)) => {
                io::ErrorKind::TimedOut
            }
            (None, Some(google_cloud_gax::error::rpc::Code::Unavailable))
            | (None, Some(google_cloud_gax::error::rpc::Code::ResourceExhausted))
            | (None, Some(google_cloud_gax::error::rpc::Code::Aborted))
            | (None, Some(google_cloud_gax::error::rpc::Code::Internal)) => io::ErrorKind::Interrupted,

            (None, None) if self.source.is_timeout() => io::ErrorKind::TimedOut,
            (None, None) if self.source.is_transport() => io::ErrorKind::Interrupted,
            _ => io::ErrorKind::Other,
        };

        io::Error::new(kind, self)
    }
}

impl tikv_util::stream::RetryError for GcsApiError {
    fn is_retryable(&self) -> bool {
        if self.source.is_timeout() {
            return true;
        }
        if self.source.is_transient_and_before_rpc() {
            return true;
        }

        if let Some(code) = self.http_status_code() {
            return code == 408 || code == 429 || (500..=599).contains(&code);
        }

        if self.source.is_transport() {
            return true;
        }

        match self.grpc_code() {
            Some(google_cloud_gax::error::rpc::Code::Unavailable)
            | Some(google_cloud_gax::error::rpc::Code::DeadlineExceeded)
            | Some(google_cloud_gax::error::rpc::Code::ResourceExhausted)
            | Some(google_cloud_gax::error::rpc::Code::Aborted)
            | Some(google_cloud_gax::error::rpc::Code::Internal) => true,
            _ => false,
        }
    }
}

impl std::fmt::Display for GcsApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = self.http_status_code() {
            return write!(
                f,
                "gcs_v2 {op} failed (http={code}): {err}",
                op = self.op,
                err = self.source
            );
        }
        if let Some(code) = self.grpc_code() {
            return write!(
                f,
                "gcs_v2 {op} failed (grpc={code:?}): {err}",
                op = self.op,
                err = self.source
            );
        }
        write!(f, "gcs_v2 {op} failed: {err}", op = self.op, err = self.source)
    }
}

impl std::error::Error for GcsApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

#[derive(Clone, Debug)]
pub struct Config {
    bucket: String,
    prefix: String,
    url_prefix: String,
    endpoint: Option<String>,
    storage_class: Option<String>,
    predefined_acl: Option<String>,
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        "gcs"
    }
    fn url(&self) -> io::Result<Url> {
        if let Some(ep) = &self.endpoint {
            let mut u =
                Url::parse(ep).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            u.set_path(&format!(
                "{}/{}",
                &self.bucket.trim_end_matches(DEFAULT_SEP),
                &self.url_prefix.trim_start_matches(DEFAULT_SEP)
            ));
            Ok(u)
        } else {
            let mut url = Url::parse("gcs://").unwrap();
            url.set_host(Some(&self.bucket)).unwrap();
            if !self.url_prefix.is_empty() {
                url.set_path(&self.url_prefix);
            }
            Ok(url)
        }
    }
}

#[derive(Clone, Debug)]
pub struct GcsStorage {
    inner: std::sync::Arc<GcsStorageInner>,
}

#[derive(Debug)]
struct GcsStorageInner {
    data_client: OnceCell<Storage>,
    data_endpoint: Option<String>,
    // Store credentials as JSON to defer google_cloud_auth calls to async context
    data_credentials_json: Option<String>,
    control_client: OnceCell<StorageControl>,
    control_endpoint: Option<String>,
    // Store credentials as JSON to defer google_cloud_auth calls to async context
    control_credentials_json: Option<String>,
    config: Config,
}

impl GcsStorageInner {
    async fn get_data_client(&self) -> io::Result<Storage> {
        let client = self
            .data_client
            .get_or_try_init(|| async {
                info!("initializing GCS data client");
                let start = Instant::now();
                
                let mut builder = Storage::builder();
                if let Some(ep) = &self.data_endpoint {
                    builder = builder.with_endpoint(ep);
                }
                
                // Build credentials in async context to avoid tokio::spawn in sync context
                if let Some(creds_json) = &self.data_credentials_json {
                    if creds_json == "anonymous" {
                        // Anonymous credentials
                        let creds = google_cloud_auth::credentials::anonymous::Builder::new().build();
                        builder = builder.with_credentials(creds);
                    } else {
                        // Service account credentials
                        let creds_value: serde_json::Value = serde_json::from_str(creds_json)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                        let creds = google_cloud_auth::credentials::service_account::Builder::new(creds_value)
                            .build()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                        builder = builder.with_credentials(creds);
                    }
                }

                let result = builder
                    .build()
                    .await
                    .map_err(|e: google_cloud_gax::client_builder::Error| {
                        let kind = if e.is_transport() {
                            io::ErrorKind::Interrupted
                        } else if e.is_default_credentials() {
                            io::ErrorKind::PermissionDenied
                        } else {
                            io::ErrorKind::Other
                        };
                        io::Error::new(kind, e)
                    });
                
                if let Ok(_) = &result {
                    info!("GCS data client initialized successfully"; "duration" => ?start.saturating_elapsed());
                } else if let Err(e) = &result {
                    error!("GCS data client initialization failed"; "err" => ?e, "duration" => ?start.saturating_elapsed());
                }
                
                result
            })
            .await?;
        Ok(client.clone())
    }

    async fn get_control_client(&self) -> io::Result<StorageControl> {
        let client = self
            .control_client
            .get_or_try_init(|| async {
                info!("initializing GCS control client");
                let start = Instant::now();
                
                let mut builder = StorageControl::builder();
                if let Some(ep) = &self.control_endpoint {
                    builder = builder.with_endpoint(ep);
                }
                
                // Build credentials in async context to avoid tokio::spawn in sync context
                if let Some(creds_json) = &self.control_credentials_json {
                    if creds_json == "anonymous" {
                        // Anonymous credentials
                        let creds = google_cloud_auth::credentials::anonymous::Builder::new().build();
                        builder = builder.with_credentials(creds);
                    } else {
                        // Service account credentials
                        let creds_value: serde_json::Value = serde_json::from_str(creds_json)
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                        let creds = google_cloud_auth::credentials::service_account::Builder::new(creds_value)
                            .build()
                            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
                        builder = builder.with_credentials(creds);
                    }
                }
                
                let result = builder
                    .build()
                    .await
                    .map_err(|e: google_cloud_gax::client_builder::Error| {
                        let kind = if e.is_transport() {
                            io::ErrorKind::Interrupted
                        } else if e.is_default_credentials() {
                            io::ErrorKind::PermissionDenied
                        } else {
                            io::ErrorKind::Other
                        };
                        io::Error::new(kind, e)
                    });
                
                if let Ok(_) = &result {
                    info!("GCS control client initialized successfully"; "duration" => ?start.saturating_elapsed());
                } else if let Err(e) = &result {
                    error!("GCS control client initialization failed"; "err" => ?e, "duration" => ?start.saturating_elapsed());
                }
                
                result
            })
            .await?;
        Ok(client.clone())
    }
}

const DEFAULT_SEP: char = '/';

impl GcsStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        let bucket = input.bucket.clone();
        let url_prefix = input.prefix.clone();
        let prefix = url_prefix.trim_end_matches(DEFAULT_SEP).to_owned();
        let endpoint = if input.endpoint.is_empty() { None } else { Some(input.endpoint.clone()) };
        let storage_class = Self::parse_storage_class(&input.storage_class)?;
        let predefined_acl = Self::parse_predefined_acl(&input.predefined_acl)?;

        // CRITICAL FIX: Store credentials as JSON string instead of building Credentials object
        // google_cloud_auth::credentials::Builder::build() may call tokio::spawn internally,
        // which will panic if called from a non-Tokio thread (like Backup-Worker thread).
        // We defer the credential building to async context in get_data_client/get_control_client.
        let creds_json = if !input.credentials_blob.is_empty() {
            // Validate JSON format but don't build credentials yet
            let _: serde_json::Value = serde_json::from_str(&input.credentials_blob)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            Some(input.credentials_blob.clone())
        } else if endpoint.is_some() {
            // Use special marker for anonymous credentials
            Some("anonymous".to_string())
        } else {
            None
        };

        Ok(GcsStorage {
            inner: std::sync::Arc::new(GcsStorageInner {
                data_client: OnceCell::new(),
                data_endpoint: endpoint.clone(),
                data_credentials_json: creds_json.clone(),
                control_client: OnceCell::new(),
                control_endpoint: endpoint.clone(),
                control_credentials_json: creds_json,
                config: Config {
                    bucket,
                    prefix,
                    url_prefix,
                    endpoint: endpoint.clone(),
                    storage_class,
                    predefined_acl,
                },
            }),
        })
    }

    fn parse_storage_class(storage_class: &str) -> io::Result<Option<String>> {
        if storage_class.is_empty() {
            return Ok(None);
        }
        match storage_class {
            "STANDARD"
            | "NEARLINE"
            | "COLDLINE"
            | "ARCHIVE"
            | "DURABLE_REDUCED_AVAILABILITY"
            | "REGIONAL"
            | "MULTI_REGIONAL" => Ok(Some(storage_class.to_string())),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid storage_class: {storage_class}"),
            )),
        }
    }

    fn parse_predefined_acl(predefined_acl: &str) -> io::Result<Option<String>> {
        if predefined_acl.is_empty() {
            return Ok(None);
        }
        match predefined_acl {
            "authenticatedRead"
            | "bucketOwnerFullControl"
            | "bucketOwnerRead"
            | "private"
            | "projectPrivate"
            | "publicRead" => Ok(Some(predefined_acl.to_string())),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("invalid predefined_acl: {predefined_acl}"),
            )),
        }
    }

    async fn get_data_client(&self) -> io::Result<Storage> {
        self.inner.get_data_client().await
    }

    async fn get_control_client(&self) -> io::Result<StorageControl> {
        self.inner.get_control_client().await
    }
    
    /// Warm up the storage by initializing clients eagerly.
    /// This should be called right after from_input() to avoid lazy initialization delays
    /// during critical operations like backup.
    pub async fn warmup(&self) -> io::Result<()> {
        info!("warming up GCS storage clients");
        let start = Instant::now();
        
        // Initialize both clients
        self.get_data_client().await?;
        self.get_control_client().await?;
        
        info!("GCS storage clients warmed up"; "duration" => ?start.saturating_elapsed());
        Ok(())
    }
    
    fn full_path(&self, name: &str) -> String {
        if self.inner.config.prefix.is_empty() {
            name.to_owned()
        } else {
            let name = name.trim_start_matches(DEFAULT_SEP);
            format!("{}{}{}", self.inner.config.prefix, DEFAULT_SEP, name)
        }
    }

    fn strip_prefix_if_needed(&self, key: String) -> String {
        if self.inner.config.prefix.is_empty() {
            return key;
        }
        if key.starts_with(self.inner.config.prefix.as_str()) {
            return key[self.inner.config.prefix.len()..]
                .trim_start_matches(DEFAULT_SEP)
                .to_owned();
        }
        key
    }
    
    fn bucket_resource_name(&self) -> String {
        format!("projects/_/buckets/{}", self.inner.config.bucket)
    }
}

#[async_trait]
impl BlobStorage for GcsStorage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.inner.config.clone())
    }

    async fn put(&self, name: &str, mut reader: PutResource<'_>, content_length: u64) -> io::Result<()> {
        let full_name = self.full_path(name);

        let begin = Instant::now_coarse();
        let mut data = Vec::new();
        reader
            .0
            .read_to_end(&mut data)
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", "read_local"])
            .observe(begin.saturating_elapsed_secs());

        let bucket = self.bucket_resource_name();
        let client = self.get_data_client().await?;
        let data = Bytes::from(data);
        let storage_class = self.inner.config.storage_class.clone();
        let predefined_acl = self.inner.config.predefined_acl.clone();
        let begin = Instant::now_coarse();
        let req = if content_length == 0 {
            "insert_simple"
        } else {
            "insert_multipart"
        };
        tikv_util::stream::retry_ext(
            move || {
                let client = client.clone();
                let bucket = bucket.clone();
                let full_name = full_name.clone();
                let data = data.clone();
                let storage_class = storage_class.clone();
                let predefined_acl = predefined_acl.clone();
                async move {
                    let mut builder = client.write_object(bucket, full_name, data);
                    if let Some(storage_class) = storage_class.as_ref() {
                        builder = builder.set_storage_class(storage_class.clone());
                    }
                    if let Some(predefined_acl) = predefined_acl.as_ref() {
                        builder = builder.set_predefined_acl(predefined_acl.clone());
                    }
                    builder
                        .send_buffered()
                        .await
                        .map(|_| ())
                        .map_err(|e| GcsApiError::new("write_object", e))
                }
            },
            tikv_util::stream::RetryExt::default(),
        )
        .await
        .map_err(|e| e.into_io_error())?;
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .with_label_values(&["gcp", req])
            .observe(begin.saturating_elapsed_secs());

        Ok(())
    }

    fn get(&self, name: &str) -> BlobStream<'_> {
        self.get_range(name, None)
    }

    fn get_part(&self, name: &str, off: u64, len: u64) -> BlobStream<'_> {
        self.get_range(name, Some((off, len)))
    }
}

impl GcsStorage {
    fn get_range(&self, name: &str, range: Option<(u64, u64)>) -> BlobStream<'_> {
        let bucket = self.bucket_resource_name();
        let object = self.full_path(name);
        let storage = self.inner.clone();

        let stream = async_stream::try_stream! {
            let client = storage.get_data_client().await?;
            let mut response = tikv_util::stream::retry_ext(
                || {
                    let client = client.clone();
                    let bucket = bucket.clone();
                    let object = object.clone();
                    async move {
                        let mut builder = client.read_object(&bucket, &object);
                        if let Some((off, len)) = range {
                            builder = builder.set_read_range(ReadRange::segment(off, len));
                        }
                        builder.send().await.map_err(|e| GcsApiError::new("read_object", e))
                    }
                },
                tikv_util::stream::RetryExt::default(),
            )
            .await
            .map_err(|e| e.into_io_error())?;

            while let Some(chunk) = response.next().await {
                let chunk = chunk.map_err(|e| GcsApiError::new("read_object stream", e).into_io_error())?;
                yield chunk;
            }
        };
        Box::new(stream.boxed().into_async_read())
    }
}

impl IterableStorage for GcsStorage {
    fn iter_prefix(
        &self,
        prefix: &str,
    ) -> Pin<Box<dyn Stream<Item = std::result::Result<BlobObject, io::Error>> + '_>> {
        let full_prefix = self.full_path(prefix);
        let bucket_path = self.bucket_resource_name(); // Control API usually needs resource name

        let stream = async_stream::try_stream! {
            let client = self.get_control_client().await?;

            let mut page_token = String::new();
            loop {
                let begin = Instant::now_coarse();
                let resp = client
                    .list_objects()
                    .set_parent(bucket_path.clone())
                    .set_prefix(full_prefix.clone())
                    .set_page_token(page_token)
                    .send()
                    .await
                    .map_err(|e| GcsApiError::new("list_objects", e).into_io_error())?;
                metrics::CLOUD_REQUEST_HISTOGRAM_VEC
                    .with_label_values(&["gcp", "list"])
                    .observe(begin.saturating_elapsed_secs());

                for object in resp.objects {
                    yield BlobObject {
                        key: self.strip_prefix_if_needed(object.name),
                    };
                }

                if resp.next_page_token.is_empty() {
                    break;
                }
                page_token = resp.next_page_token;
            }
        };
        
        Box::pin(stream)
    }
}

impl DeletableStorage for GcsStorage {
    fn delete(&self, name: &str) -> LocalBoxFuture<'_, io::Result<()>> {
        let full_name = self.full_path(name);
        let bucket_resource = self.bucket_resource_name();

        Box::pin(async move {
            let client = self.get_control_client().await?;
            let begin = Instant::now_coarse();
            tikv_util::stream::retry_ext(
                || {
                    let client = client.clone();
                    let bucket_resource = bucket_resource.clone();
                    let full_name = full_name.clone();
                    async move {
                        client
                            .delete_object()
                            .set_bucket(bucket_resource)
                            .set_object(full_name)
                            .send()
                            .await
                            .map(|_| ())
                            .map_err(|e| GcsApiError::new("delete_object", e))
                    }
                },
                tikv_util::stream::RetryExt::default(),
            )
            .await
            .map_err(|e| e.into_io_error())?;
            metrics::CLOUD_REQUEST_HISTOGRAM_VEC
                .with_label_values(&["gcp", "delete"])
                .observe(begin.saturating_elapsed_secs());
            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn gax_grpc_code_maps_to_io_kind() {
        let status = google_cloud_gax::error::rpc::Status::default()
            .set_code(google_cloud_gax::error::rpc::Code::NotFound);
        let e = google_cloud_storage::Error::service(status);
        let io = GcsApiError::new("unit-test", e).into_io_error();
        assert_eq!(io.kind(), io::ErrorKind::NotFound);
        let msg = io.to_string();
        assert!(msg.contains("grpc=NotFound"), "msg={msg}");
    }

    #[test]
    fn from_input_trims_prefix_slashes() -> io::Result<()> {
        let mut input = InputConfig::default();
        input.bucket = "b".to_string();
        input.prefix = "/pfx/".to_string();
        input.endpoint = "http://127.0.0.1:1".to_string();
        let s = GcsStorage::from_input(input)?;
        assert_eq!(s.inner.config.prefix, "/pfx");
        Ok(())
    }

    #[test]
    fn url_with_custom_endpoint() -> io::Result<()> {
        let mut input = InputConfig::default();
        input.bucket = "bucket".to_string();
        input.prefix = "/backup 01/prefix/".to_string();
        input.endpoint = "http://endpoint.com".to_string();
        let s = GcsStorage::from_input(input)?;
        assert_eq!(
            s.config().url()?.to_string(),
            "http://endpoint.com/bucket/backup%2001/prefix/"
        );
        Ok(())
    }
}
