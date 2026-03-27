// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::{future::Future, io, pin::Pin};

use async_trait::async_trait;
use cloud::{
    blob::{
        BlobConfig, BlobObject, BlobStorage, BlobStream, DeletableStorage, IterableStorage,
        PutResource,
    },
    metrics,
};
use futures::{future::LocalBoxFuture, stream::Stream};
use futures_util::{TryStreamExt, io::AsyncReadExt as _, stream::StreamExt};
use google_cloud_storage::{
    client::{Storage, StorageControl},
    model_ext::ReadRange,
    streaming_source::{SizeHint, StreamingSource},
    stub::Storage as StorageStub,
};
use kvproto::brpb::Gcs as InputConfig;
use tikv_util::{error, info, time::Instant};
use tokio::sync::{Mutex, OnceCell};
use url::Url;

mod credentials;
mod kms;
use credentials::{
    CredentialsMode, build_credentials, ensure_rustls_fips_provider, validate_credentials_json,
};
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
            | (None, Some(google_cloud_gax::error::rpc::Code::Internal)) => {
                io::ErrorKind::Interrupted
            }

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

        matches!(
            self.grpc_code(),
            Some(google_cloud_gax::error::rpc::Code::Unavailable)
                | Some(google_cloud_gax::error::rpc::Code::DeadlineExceeded)
                | Some(google_cloud_gax::error::rpc::Code::ResourceExhausted)
                | Some(google_cloud_gax::error::rpc::Code::Aborted)
                | Some(google_cloud_gax::error::rpc::Code::Internal)
        )
    }
}

impl std::fmt::Display for GcsApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(code) = self.http_status_code() {
            return write!(
                f,
                "gcp_v2 {op} failed (http={code}): {err}",
                op = self.op,
                err = self.source
            );
        }
        if let Some(code) = self.grpc_code() {
            return write!(
                f,
                "gcp_v2 {op} failed (grpc={code:?}): {err}",
                op = self.op,
                err = self.source
            );
        }
        write!(
            f,
            "gcp_v2 {op} failed: {err}",
            op = self.op,
            err = self.source
        )
    }
}

impl std::error::Error for GcsApiError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

const PUT_READ_CHUNK_SIZE: usize = 256 * 1024;

struct PutResourceSource {
    reader: Mutex<PutResource<'static>>,
    exact_size: u64,
}

impl PutResourceSource {
    fn new(reader: PutResource<'static>, exact_size: u64) -> Self {
        Self {
            reader: Mutex::new(reader),
            exact_size,
        }
    }
}

impl StreamingSource for PutResourceSource {
    type Error = io::Error;

    async fn next(&mut self) -> Option<Result<bytes::Bytes, Self::Error>> {
        let mut buf = vec![0; (self.exact_size as usize).clamp(1, PUT_READ_CHUNK_SIZE)];
        match self.reader.lock().await.read(&mut buf).await {
            Ok(0) => None,
            Ok(n) => {
                buf.truncate(n);
                Some(Ok(bytes::Bytes::from(buf)))
            }
            Err(e) => Some(Err(e)),
        }
    }

    fn size_hint(&self) -> impl Future<Output = Result<SizeHint, Self::Error>> + Send {
        std::future::ready(Ok(SizeHint::with_exact(self.exact_size)))
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
            let mut url =
                Url::parse("gcs://").map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            url.set_host(Some(&self.bucket))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
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
    // Defer credential building to async context to avoid tokio::spawn in sync context.
    data_credentials_mode: CredentialsMode,
    control_client: OnceCell<StorageControl>,
    control_endpoint: Option<String>,
    // Defer credential building to async context to avoid tokio::spawn in sync context.
    control_credentials_mode: CredentialsMode,
    config: Config,
}

impl GcsStorageInner {
    async fn get_data_client(&self) -> io::Result<Storage> {
        let client = self
            .data_client
            .get_or_try_init(|| async {
                let endpoint = self.data_endpoint.as_deref();
                // Build credentials in async context to avoid tokio::spawn in sync context.
                let creds = build_credentials(&self.data_credentials_mode)?;
                Self::init_client(
                    "data",
                    Storage::builder(),
                    move |mut b| {
                        if let Some(ep) = endpoint {
                            b = b.with_endpoint(ep);
                        }
                        if let Some(creds) = creds {
                            b = b.with_credentials(creds);
                        }
                        b
                    },
                    |b| async move { b.build().await },
                )
                .await
            })
            .await?;
        Ok(client.clone())
    }

    async fn get_control_client(&self) -> io::Result<StorageControl> {
        let client = self
            .control_client
            .get_or_try_init(|| async {
                let endpoint = self.control_endpoint.as_deref();
                // Build credentials in async context to avoid tokio::spawn in sync context.
                let creds = build_credentials(&self.control_credentials_mode)?;
                Self::init_client(
                    "control",
                    StorageControl::builder(),
                    move |mut b| {
                        if let Some(ep) = endpoint {
                            b = b.with_endpoint(ep);
                        }
                        if let Some(creds) = creds {
                            b = b.with_credentials(creds);
                        }
                        b
                    },
                    |b| async move { b.build().await },
                )
                .await
            })
            .await?;
        Ok(client.clone())
    }

    async fn init_client<T, B, OB, F, Fut>(
        kind: &'static str,
        builder: B,
        on_builder: OB,
        build: F,
    ) -> io::Result<T>
    where
        OB: FnOnce(B) -> B,
        F: FnOnce(B) -> Fut,
        Fut: Future<Output = Result<T, google_cloud_gax::client_builder::Error>>,
    {
        let builder = on_builder(builder);
        Self::build_client(kind, builder, build).await
    }

    async fn build_client<T, B, F, Fut>(kind: &'static str, builder: B, build: F) -> io::Result<T>
    where
        F: FnOnce(B) -> Fut,
        Fut: Future<Output = Result<T, google_cloud_gax::client_builder::Error>>,
    {
        info!("initializing GCS client"; "kind" => kind);
        let start = Instant::now();
        let result = build(builder).await.map_err(Self::map_client_builder_error);
        if result.is_ok() {
            info!("GCS client initialized successfully"; "kind" => kind, "duration" => ?start.saturating_elapsed());
        } else if let Err(e) = &result {
            error!("GCS client initialization failed"; "kind" => kind, "err" => ?e, "duration" => ?start.saturating_elapsed());
        }
        result
    }

    fn map_client_builder_error(e: google_cloud_gax::client_builder::Error) -> io::Error {
        let kind = if e.is_transport() {
            io::ErrorKind::Interrupted
        } else if e.is_default_credentials() {
            io::ErrorKind::PermissionDenied
        } else {
            io::ErrorKind::Other
        };
        io::Error::new(kind, e)
    }
}

const DEFAULT_SEP: char = '/';

impl GcsStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        ensure_rustls_fips_provider()?;
        let bucket = input.bucket.clone();
        let url_prefix = input.prefix.clone();
        let prefix = url_prefix.trim_end_matches(DEFAULT_SEP).to_owned();
        let endpoint = if input.endpoint.is_empty() {
            None
        } else {
            Some(input.endpoint.clone())
        };
        let storage_class = Self::parse_storage_class(&input.storage_class)?;
        let predefined_acl = Self::parse_predefined_acl(&input.predefined_acl)?;

        // Store credential mode instead of building a Credentials object here.
        // google_cloud_auth::credentials::Builder::build() may call tokio::spawn
        // internally, which will panic if called from a non-Tokio thread (like
        // Backup-Worker thread). We defer the credential building to async
        // context in get_data_client/get_control_client.
        let creds_mode = if !input.credentials_blob.is_empty() {
            // Validate JSON format but don't build credentials yet
            validate_credentials_json(&input.credentials_blob)?;
            CredentialsMode::Json(input.credentials_blob.clone())
        } else {
            CredentialsMode::Default
        };

        Ok(GcsStorage {
            inner: std::sync::Arc::new(GcsStorageInner {
                data_client: OnceCell::new(),
                data_endpoint: endpoint.clone(),
                data_credentials_mode: creds_mode.clone(),
                control_client: OnceCell::new(),
                control_endpoint: endpoint.clone(),
                control_credentials_mode: creds_mode,
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
        let bucket = self.inner.config.bucket.as_str();
        if bucket.starts_with("projects/") {
            bucket.to_owned()
        } else {
            format!("projects/_/buckets/{bucket}")
        }
    }
}

async fn put_with_client<S>(
    client: &google_cloud_storage::client::Storage<S>,
    bucket: String,
    full_name: String,
    reader: PutResource<'_>,
    content_length: u64,
    storage_class: Option<String>,
    predefined_acl: Option<String>,
) -> io::Result<()>
where
    S: StorageStub + 'static,
{
    // SAFETY: `put()` awaits `write_object(...).send_buffered()` to
    // completion before returning, so the widened lifetime never outlives
    // the original `reader`. In google-cloud-storage 1.0.0, the buffered
    // upload path consumes the payload inside that future and does not
    // detach it into a background task.
    let reader = unsafe { std::mem::transmute::<PutResource<'_>, PutResource<'static>>(reader) };
    let payload = PutResourceSource::new(reader, content_length);
    let begin = Instant::now_coarse();
    let mut builder = client
        .write_object(bucket, full_name, payload)
        // TiKV's upload source is single-pass. In
        // `google-cloud-storage` 1.0.0, the doc comments for
        // `with_resumable_upload_threshold()` in
        // `src/storage/client.rs` and `src/storage/write_object.rs` say
        // smaller uploads use single-shot uploads for better performance.
        //
        // We still force resumable uploads here because backup writes favor
        // retry/continue behavior over the extra RPCs for small objects.
        .with_resumable_upload_threshold(0_usize);
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
        .map_err(|e| GcsApiError::new("write_object", e).into_io_error())?;
    metrics::CLOUD_REQUEST_HISTOGRAM_VEC
        // Keep metric labels aligned with the actual upload API used above.
        .with_label_values(&["gcp", "insert_multipart"])
        .observe(begin.saturating_elapsed_secs());

    Ok(())
}

#[async_trait]
impl BlobStorage for GcsStorage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.inner.config.clone())
    }

    async fn put(
        &self,
        name: &str,
        reader: PutResource<'_>,
        content_length: u64,
    ) -> io::Result<()> {
        let full_name = self.full_path(name);

        let bucket = self.bucket_resource_name();
        let client = self.get_data_client().await?;
        let storage_class = self.inner.config.storage_class.clone();
        let predefined_acl = self.inner.config.predefined_acl.clone();
        Box::pin(put_with_client(
            &client,
            bucket,
            full_name,
            reader,
            content_length,
            storage_class,
            predefined_acl,
        ))
        .await
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
    use std::future::{self, Future};

    use google_cloud_storage::{
        model::Object, model_ext::WriteObjectRequest, request_options::RequestOptions,
    };

    use super::*;

    fn histogram_sample_count(cloud: &str, req: &str) -> u64 {
        metrics::CLOUD_REQUEST_HISTOGRAM_VEC
            .get_metric_with_label_values(&[cloud, req])
            .unwrap()
            .get_sample_count()
    }

    fn make_test_storage(storage_class: &str, predefined_acl: &str) -> io::Result<GcsStorage> {
        let mut input = InputConfig::default();
        input.bucket = "test-bucket".to_string();
        input.prefix = "pfx".to_string();
        input.storage_class = storage_class.to_string();
        input.predefined_acl = predefined_acl.to_string();
        GcsStorage::from_input(input)
    }

    #[derive(Debug)]
    struct SuccessfulWriteStorageStub;

    impl google_cloud_storage::stub::Storage for SuccessfulWriteStorageStub {
        fn write_object_buffered<P>(
            &self,
            _payload: P,
            _req: WriteObjectRequest,
            _options: RequestOptions,
        ) -> impl Future<Output = google_cloud_storage::Result<Object>> + Send
        where
            P: google_cloud_storage::streaming_source::StreamingSource + Send + Sync + 'static,
        {
            future::ready(Ok(Object::new()))
        }
    }

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
    fn custom_endpoint_without_credentials_uses_default_credentials_mode() -> io::Result<()> {
        let mut input = InputConfig::default();
        input.bucket = "b".to_string();
        input.endpoint = "http://127.0.0.1:1".to_string();
        let s = GcsStorage::from_input(input)?;
        assert!(matches!(
            s.inner.data_credentials_mode,
            CredentialsMode::Default
        ));
        assert!(matches!(
            s.inner.control_credentials_mode,
            CredentialsMode::Default
        ));
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

    #[tokio::test]
    async fn put_emits_insert_multipart_metrics_without_network() -> io::Result<()> {
        let storage = make_test_storage("COLDLINE", "projectPrivate")?;
        let client = google_cloud_storage::client::Storage::from_stub(SuccessfulWriteStorageStub);
        let before_insert = histogram_sample_count("gcp", "insert_multipart");

        Box::pin(put_with_client(
            &client,
            storage.bucket_resource_name(),
            storage.full_path("a"),
            PutResource(Box::new(futures::io::Cursor::new(b"alpha".to_vec()))),
            5,
            storage.inner.config.storage_class.clone(),
            storage.inner.config.predefined_acl.clone(),
        ))
        .await?;

        let after_insert = histogram_sample_count("gcp", "insert_multipart");
        assert!(after_insert > before_insert);
        Ok(())
    }

    #[tokio::test]
    async fn zero_length_put_uses_insert_multipart_metrics_without_network() -> io::Result<()> {
        let storage = make_test_storage("COLDLINE", "projectPrivate")?;
        let client = google_cloud_storage::client::Storage::from_stub(SuccessfulWriteStorageStub);
        let before_multipart = histogram_sample_count("gcp", "insert_multipart");
        let before_simple = histogram_sample_count("gcp", "insert_simple");

        Box::pin(put_with_client(
            &client,
            storage.bucket_resource_name(),
            storage.full_path("zero"),
            PutResource(Box::new(futures::io::Cursor::new(Vec::<u8>::new()))),
            0,
            storage.inner.config.storage_class.clone(),
            storage.inner.config.predefined_acl.clone(),
        ))
        .await?;

        let after_multipart = histogram_sample_count("gcp", "insert_multipart");
        let after_simple = histogram_sample_count("gcp", "insert_simple");
        assert!(after_multipart > before_multipart);
        assert_eq!(after_simple, before_simple);
        Ok(())
    }
}
