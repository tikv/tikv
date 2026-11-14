// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{env, io, num::NonZero, str::FromStr, sync::Arc};

use async_trait::async_trait;
use azure_core::{
    credentials::{Secret, TokenCredential},
    http::{ClientOptions, Transport},
};
use azure_identity::ClientSecretCredential;
use azure_storage_blob::{
    BlobContainerClient, BlobServiceClient, BlobServiceClientOptions,
    models::{
        AccessTier, BlobClientDownloadOptions, BlockBlobClientUploadOptions,
        EncryptionAlgorithmType,
    },
};
use cloud::{
    blob::{
        BlobConfig, BlobObject, BlobStorage, BucketConf, DeletableStorage, IterableStorage,
        PutResource, StringNonEmpty, none_to_empty, read_to_end, unimplemented,
    },
    metrics::AZBLOB_UPLOAD_DURATION,
};
use futures::TryFutureExt;
use futures_util::{TryStreamExt, future::FutureExt, io::AsyncRead, stream, stream::StreamExt};
pub use kvproto::brpb::{AzureBlobStorage as InputConfig, AzureCustomerKey};
use tikv_util::{
    debug, defer,
    stream::{RetryError, retry},
    time::Instant,
};
use tokio::time::{Duration, timeout};

use crate::token_credentials::{
    default_credentials::DefaultAzureCredential, shared_key::SharedKeyPolicy,
};

const ENV_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const ENV_TENANT_ID: &str = "AZURE_TENANT_ID";
const ENV_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
const ENV_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT";
const ENV_SHARED_KEY: &str = "AZURE_STORAGE_KEY";

/// CredentialInfo saves the credential variables from the environment:
/// client_id:      $AZURE_CLIENT_ID,
/// tenant_id:      $AZURE_TENANT_ID,
/// client_secret:  $AZURE_CLIENT_SECRET,
#[derive(Clone, Debug)]
struct CredentialInfo {
    client_id: String,
    tenant_id: String,
    client_secret: Secret,
}

#[derive(Clone, Debug)]
struct EncryptionCustomer {
    encryption_key: String,
    encryption_key_sha256: String,
}

impl From<AzureCustomerKey> for EncryptionCustomer {
    fn from(value: AzureCustomerKey) -> Self {
        EncryptionCustomer {
            encryption_key: value.encryption_key,
            encryption_key_sha256: value.encryption_key_sha256,
        }
    }
}

impl From<EncryptionCustomer> for (String, String) {
    fn from(value: EncryptionCustomer) -> (String, String) {
        (value.encryption_key, value.encryption_key_sha256)
    }
}

#[derive(Clone)]
pub struct Config {
    bucket: BucketConf,

    account_name: Option<StringNonEmpty>,
    shared_key: Option<StringNonEmpty>,
    sas_token: Option<StringNonEmpty>,
    credential_info: Option<CredentialInfo>,
    env_account_name: Option<StringNonEmpty>,
    env_shared_key: Option<StringNonEmpty>,
    encryption_scope: Option<StringNonEmpty>,
    encryption_customer: Option<EncryptionCustomer>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("bucket", &self.bucket)
            .field("account_name", &self.account_name)
            .field("shared_key", &"?")
            .field("sas_token", &"?")
            .field("credential_info", &self.credential_info)
            .field("env_account_name", &self.env_account_name)
            .field("env_shared_key", &"?")
            .field("encryption_scope", &self.encryption_scope)
            .field("encryption_customer_key", &"?")
            .finish()
    }
}

impl Config {
    #[cfg(test)]
    pub fn default(bucket: BucketConf) -> Self {
        Self {
            bucket,
            account_name: None,
            shared_key: None,
            sas_token: None,
            credential_info: Self::load_credential_info(),
            env_account_name: Self::load_env_account_name(),
            env_shared_key: Self::load_env_shared_key(),
            encryption_scope: None,
            encryption_customer: None,
        }
    }

    fn load_credential_info() -> Option<CredentialInfo> {
        if let (Some(client_id), Some(tenant_id), Some(client_secret)) = (
            env::var(ENV_CLIENT_ID).ok(),
            env::var(ENV_TENANT_ID).ok(),
            env::var(ENV_CLIENT_SECRET).ok(),
        ) {
            if !(client_id.is_empty() || tenant_id.is_empty() || client_secret.is_empty()) {
                let client_secret = Secret::new(client_secret);
                return Some(CredentialInfo {
                    client_id,
                    tenant_id,
                    client_secret,
                });
            }
        }
        None
    }

    fn load_env_account_name() -> Option<StringNonEmpty> {
        env::var(ENV_ACCOUNT_NAME)
            .ok()
            .and_then(StringNonEmpty::opt)
    }

    fn load_env_shared_key() -> Option<StringNonEmpty> {
        env::var(ENV_SHARED_KEY).ok().and_then(StringNonEmpty::opt)
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let bucket = BucketConf {
            endpoint: StringNonEmpty::opt(input.endpoint),
            bucket: StringNonEmpty::required_field(input.bucket, "bucket")?,
            prefix: StringNonEmpty::opt(input.prefix),
            storage_class: StringNonEmpty::opt(input.storage_class),
            region: None,
        };

        let encryption_customer = input
            .encryption_key
            .into_option()
            .map(EncryptionCustomer::from);

        Ok(Config {
            bucket,
            account_name: StringNonEmpty::opt(input.account_name),
            shared_key: StringNonEmpty::opt(input.shared_key),
            sas_token: StringNonEmpty::opt(input.access_sig),
            credential_info: Self::load_credential_info(),
            env_account_name: Self::load_env_account_name(),
            env_shared_key: Self::load_env_shared_key(),
            encryption_scope: StringNonEmpty::opt(input.encryption_scope),
            encryption_customer,
        })
    }

    pub fn get_account_name(&self) -> io::Result<String> {
        if let Some(account_name) = self.account_name.as_ref() {
            Ok(account_name.to_string())
        } else if let Some(account_name) = self.env_account_name.as_ref() {
            Ok(account_name.to_string())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "account name cannot be empty to access azure blob storage",
            ))
        }
    }
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        self.bucket
            .url("azure")
            .map_err(|s| io::Error::other(format!("error creating bucket url: {}", s)))
    }
}

enum RequestError {
    InvalidInput(Box<dyn std::error::Error + Send + Sync>, String),
    TimeOut(String),
}

impl From<RequestError> for io::Error {
    fn from(err: RequestError) -> Self {
        match err {
            RequestError::InvalidInput(e, tag) => {
                // The SDK's Display omits the transport error source, whose
                // Debug representation can include a URL containing a SAS token.
                Self::new(io::ErrorKind::InvalidInput, format!("{}: {}", tag, &e))
            }
            RequestError::TimeOut(msg) => Self::new(io::ErrorKind::TimedOut, msg),
        }
    }
}

impl RetryError for RequestError {
    fn is_retryable(&self) -> bool {
        matches!(self, Self::TimeOut(_))
    }
}

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(900);

fn blob_client_options() -> io::Result<ClientOptions> {
    // Preserve the outer operation deadlines. The SDK's default read timeout
    // also covers sending the body and aborts slow Put Blob requests after 60s.
    let client = reqwest::Client::builder()
        .tls_backend_native()
        .connect_timeout(Duration::from_secs(20))
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(io::Error::other)?;
    Ok(ClientOptions {
        transport: Some(Transport::new(Arc::new(client))),
        ..Default::default()
    })
}

/// A helper for uploading a large file to Azure storage.
struct AzureUploader {
    container_client: Arc<BlobContainerClient>,
    name: String,

    storage_class: Option<AccessTier>,
    encryption_scope: Option<StringNonEmpty>,
    encryption_customer: Option<EncryptionCustomer>,
}

impl AzureUploader {
    /// Creates a new uploader with a given target location and upload
    /// configuration.
    fn new(container_client: Arc<BlobContainerClient>, config: &Config, name: String) -> Self {
        AzureUploader {
            container_client,
            name,

            storage_class: Self::parse_storage_class(none_to_empty(
                config.bucket.storage_class.clone(),
            )),
            encryption_scope: config.encryption_scope.clone(),
            encryption_customer: config.encryption_customer.clone(),
        }
    }

    fn parse_storage_class(storage_class: String) -> Option<AccessTier> {
        // The supported SDK accepts unknown values, including the empty string.
        // Preserve the legacy behavior of omitting absent or unrecognized tiers.
        match AccessTier::from_str(storage_class.as_str()).ok()? {
            AccessTier::UnknownValue(_) => None,
            tier => Some(tier),
        }
    }

    /// Executes the upload process.
    async fn run(
        self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> io::Result<()> {
        // upload the entire data.
        let begin = Instant::now_coarse();
        defer! {
            AZBLOB_UPLOAD_DURATION.observe(begin.saturating_elapsed().as_secs_f64())
        }
        let mut data = Vec::with_capacity(est_len as usize);
        read_to_end(reader, &mut data).await?;
        retry(|| self.upload(&data)).await?;
        Ok(())
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus
    /// relatively cheap to retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RequestError> {
        let res = timeout(Self::get_timeout(), async {
            let client = self
                .container_client
                .blob_client(&self.name)
                .block_blob_client();
            client
                .upload(
                    azure_core::Bytes::copy_from_slice(data).into(),
                    Some(self.upload_options(data.len())),
                )
                .await?;
            Ok(())
        })
        .await;
        match res {
            Ok(res) => match res {
                Ok(_) => Ok(()),
                Err(err) => Err(RequestError::InvalidInput(
                    err,
                    "upload block failed".to_owned(),
                )),
            },
            Err(_) => Err(RequestError::TimeOut(
                "timeout after 15mins for complete in azure storage".to_owned(),
            )),
        }
    }

    fn upload_options(&self, content_length: usize) -> BlockBlobClientUploadOptions<'static> {
        let mut options = BlockBlobClientUploadOptions {
            // Preserve a single Put Blob request, including for empty blobs. Use the
            // buffered body's actual length rather than the caller's estimate.
            partition_size: NonZero::new((content_length as u64).max(1)),
            parallel: NonZero::new(1),
            ..Default::default()
        };
        if let Some(scope) = &self.encryption_scope {
            options.encryption_scope = Some(scope.to_string());
        } else if let Some(key) = &self.encryption_customer {
            options.encryption_key = Some(key.encryption_key.clone());
            options.encryption_key_sha256 = Some(key.encryption_key_sha256.clone());
            options.encryption_algorithm = Some(EncryptionAlgorithmType::Aes256);
        } else {
            options.tier = self.storage_class.clone();
        }
        options
    }

    fn get_timeout() -> Duration {
        CONNECTION_TIMEOUT
    }
}

const STORAGE_NAME: &str = "azure";

#[derive(Clone)]
pub struct AzureStorage {
    config: Config,
    container_client: Arc<BlobContainerClient>,
}

impl AzureStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn new(config: Config) -> io::Result<AzureStorage> {
        Self::new_with_options(config, blob_client_options()?)
    }

    fn new_with_options(
        config: Config,
        mut client_options: ClientOptions,
    ) -> io::Result<AzureStorage> {
        Self::check_config(&config)?;
        let account_name = config.get_account_name()?;
        let endpoint = config
            .bucket
            .endpoint
            .as_ref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| format!("https://{}.blob.core.windows.net", account_name));
        let mut service_url = url::Url::parse(&endpoint).map_err(io::Error::other)?;
        let credential: Option<Arc<dyn TokenCredential>>;
        // Keep the existing precedence: SAS, explicit shared key, environment
        // client secret, environment shared key, then the default identity chain.
        if let Some(sas_token) = &config.sas_token {
            service_url
                .query_pairs_mut()
                .extend_pairs(url::form_urlencoded::parse(
                    sas_token.trim_start_matches('?').as_bytes(),
                ));
            credential = None;
        } else if let (Some(_), Some(shared_key)) = (&config.account_name, &config.shared_key) {
            client_options.per_try_policies.push(Arc::new(
                SharedKeyPolicy::new(account_name.clone(), shared_key.to_string())
                    .map_err(io::Error::other)?,
            ));
            credential = None;
        } else if let Some(info) = &config.credential_info {
            credential = Some(
                ClientSecretCredential::new(
                    &info.tenant_id,
                    info.client_id.clone(),
                    info.client_secret.clone(),
                    None,
                )
                .map_err(io::Error::other)?,
            );
        } else if let (Some(env_account), Some(shared_key)) =
            (&config.env_account_name, &config.env_shared_key)
        {
            client_options.per_try_policies.push(Arc::new(
                SharedKeyPolicy::new(env_account.to_string(), shared_key.to_string())
                    .map_err(io::Error::other)?,
            ));
            credential = None;
        } else {
            credential = Some(DefaultAzureCredential::new().map_err(io::Error::other)?);
        }
        let client = BlobServiceClient::new(
            service_url,
            credential,
            Some(BlobServiceClientOptions {
                client_options,
                ..Default::default()
            }),
        )
        .map_err(io::Error::other)?;
        let container_client = Arc::new(client.blob_container_client(&config.bucket.bucket));
        Ok(Self {
            config,
            container_client,
        })
    }

    fn check_config(config: &Config) -> io::Result<()> {
        if config.bucket.storage_class.is_some() {
            if config.encryption_scope.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    concat!(
                        "Set Blob Tier cannot be used with customer-provided scope. ",
                        "Please don't supply the access-tier when use encryption-scope."
                    ),
                ));
            }
            if config.encryption_customer.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    concat!(
                        "Set Blob Tier cannot be used with customer-provided key. ",
                        "Please don't supply the access-tier when use encryption-key."
                    ),
                ));
            }
        } else if config.encryption_scope.is_some() && config.encryption_customer.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                concat!(
                    "Undefined input: There are both encryption-scope and customer provided key. ",
                    "Please select only one to encrypt blobs."
                ),
            ));
        }
        Ok(())
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", prefix.trim_end_matches('/'), key);
        }
        key.to_owned()
    }

    fn get_range(
        &self,
        name: &str,
        range: Option<std::ops::Range<u64>>,
    ) -> cloud::blob::BlobStream<'_> {
        let name = self.maybe_prefix_key(name);
        debug!("read file from Azure storage"; "key" => %name);
        let t = async move {
            let blob_client = self.container_client.blob_client(&name);
            let mut options = BlobClientDownloadOptions {
                range: range.map(Into::into),
                // Preserve the old SDK's sequential 16 MiB range reads. File-level
                // concurrency is already managed by the backup/restore callers.
                parallel: NonZero::new(1),
                partition_size: NonZero::new(16 * 1024 * 1024),
                ..Default::default()
            };
            if let Some(key) = &self.config.encryption_customer {
                options.encryption_key = Some(key.encryption_key.clone());
                options.encryption_key_sha256 = Some(key.encryption_key_sha256.clone());
                options.encryption_algorithm = Some(EncryptionAlgorithmType::Aes256);
            }
            let response = blob_client.download(Some(options)).await?;
            response.body.collect().await
        };
        let stream = stream::once(
            t.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}", e))),
        )
        .boxed()
        .into_async_read();
        Box::new(stream)
    }
}

#[async_trait]
impl BlobStorage for AzureStorage {
    fn config(&self) -> Box<dyn BlobConfig> {
        Box::new(self.config.clone()) as Box<dyn BlobConfig>
    }

    async fn put(
        &self,
        name: &str,
        mut reader: PutResource<'_>,
        content_length: u64,
    ) -> io::Result<()> {
        let name = self.maybe_prefix_key(name);
        debug!("save file to Azure storage"; "key" => %name);

        let uploader = AzureUploader::new(self.container_client.clone(), &self.config, name);

        uploader.run(&mut reader, content_length).await
    }

    fn get(&self, name: &str) -> cloud::blob::BlobStream<'_> {
        self.get_range(name, None)
    }

    fn get_part(&self, name: &str, off: u64, len: u64) -> cloud::blob::BlobStream<'_> {
        self.get_range(name, Some(off..off + len))
    }
}

impl IterableStorage for AzureStorage {
    fn iter_prefix(
        &self,
        _prefix: &str,
    ) -> std::pin::Pin<
        Box<dyn futures::stream::Stream<Item = std::result::Result<BlobObject, io::Error>> + '_>,
    > {
        Box::pin(futures::future::err(unimplemented()).into_stream())
    }
}

impl DeletableStorage for AzureStorage {
    fn delete(&self, _name: &str) -> futures::prelude::future::LocalBoxFuture<'_, io::Result<()>> {
        Box::pin(futures::future::err(unimplemented()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use azure_core::http::{
        AsyncRawResponse, Body, HttpClient, Method, Request, Transport, headers::Headers,
    };
    use futures::AsyncReadExt;

    use super::*;

    #[derive(Debug, Default)]
    struct MockTransport {
        requests: Mutex<Vec<Request>>,
    }

    #[async_trait]
    impl HttpClient for MockTransport {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            self.requests.lock().unwrap().push(request.clone());
            let mut headers = Headers::new();
            if request.method() == Method::Put {
                Ok(AsyncRawResponse::from_bytes(201.into(), headers, ""))
            } else {
                headers.insert("content-length", "4");
                headers.insert("content-range", "bytes 20-23/100");
                headers.insert("etag", "\"test-version\"");
                Ok(AsyncRawResponse::from_bytes(206.into(), headers, "data"))
            }
        }
    }

    #[derive(Debug)]
    struct RangeDownloadTransport {
        data: azure_core::Bytes,
        requests: Mutex<Vec<Request>>,
        active_requests: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl HttpClient for RangeDownloadTransport {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            assert_eq!(request.method(), Method::Get);
            assert_eq!(
                self.active_requests.fetch_add(1, Ordering::SeqCst),
                0,
                "a range request started before the previous response body was consumed"
            );
            self.requests.lock().unwrap().push(request.clone());
            let (start, end) = request
                .headers()
                .get_optional_str(&"range".into())
                .unwrap()
                .strip_prefix("bytes=")
                .unwrap()
                .split_once('-')
                .unwrap();
            let start: usize = start.parse().unwrap();
            let end = (end.parse::<usize>().unwrap() + 1).min(self.data.len());
            let body = self.data.slice(start..end);
            let mut headers = Headers::new();
            headers.insert("content-length", body.len().to_string());
            headers.insert(
                "content-range",
                format!("bytes {}-{}/{}", start, end - 1, self.data.len()),
            );
            headers.insert("etag", "\"test-version\"");
            let active_requests = self.active_requests.clone();
            Ok(AsyncRawResponse::new(
                206.into(),
                headers,
                Box::pin(stream::once(async move {
                    // Keep the response in flight while other SDK tasks can run.
                    tokio::task::yield_now().await;
                    active_requests.fetch_sub(1, Ordering::SeqCst);
                    Ok(body)
                })),
            ))
        }
    }

    fn mock_config() -> Config {
        let mut bucket = BucketConf::default(StringNonEmpty::static_str("container"));
        bucket.endpoint = Some(StringNonEmpty::static_str("https://storage.example"));
        bucket.prefix = Some(StringNonEmpty::static_str("backup/prefix"));
        Config {
            bucket,
            account_name: Some(StringNonEmpty::static_str("account")),
            shared_key: Some(StringNonEmpty::static_str("c2VjcmV0")),
            sas_token: None,
            credential_info: None,
            env_account_name: None,
            env_shared_key: None,
            encryption_scope: None,
            encryption_customer: None,
        }
    }

    #[tokio::test]
    async fn test_upload_survives_sdk_read_timeout_and_obeys_operation_deadline() {
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
            sync::oneshot,
        };

        for elapsed in [
            Duration::from_secs(61),
            CONNECTION_TIMEOUT + Duration::from_secs(1),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (received_tx, received_rx) = oneshot::channel();
            let (respond_tx, respond_rx) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                loop {
                    let mut buffer = [0; 1024];
                    let count = socket.read(&mut buffer).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&buffer[..count]);
                    if request.ends_with(b"\r\n\r\ndata") {
                        break;
                    }
                }
                received_tx.send(()).unwrap();
                if respond_rx.await.unwrap() {
                    socket.write_all(b"HTTP/1.1 201 Created\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                }
            });
            let mut config = mock_config();
            config.bucket.endpoint = StringNonEmpty::opt(format!("http://{address}"));
            let storage = AzureStorage::new(config).unwrap();
            let uploader =
                AzureUploader::new(storage.container_client, &storage.config, "file".to_owned());
            let upload =
                tokio::spawn(
                    async move { uploader.upload(b"data").await.map_err(io::Error::from) },
                );

            // Wait for a real HTTP request before advancing only Tokio's clock.
            timeout(Duration::from_secs(5), received_rx)
                .await
                .unwrap()
                .unwrap();
            tokio::time::pause();
            tokio::time::advance(elapsed).await;
            tokio::task::yield_now().await;
            tokio::time::resume();
            let should_succeed = elapsed < CONNECTION_TIMEOUT;
            respond_tx.send(should_succeed).unwrap();
            let result = timeout(Duration::from_secs(5), upload)
                .await
                .unwrap()
                .unwrap();
            if should_succeed {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
            }
            server.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_upload_transport_error_omits_sas_signature() {
        use azure_core::http::RetryOptions;
        use tokio::{
            io::{AsyncReadExt, AsyncWriteExt},
            net::TcpListener,
        };

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut buffer = [0; 1024];
            assert_ne!(socket.read(&mut buffer).await.unwrap(), 0);
            socket
                .write_all(b"invalid HTTP response\r\n\r\n")
                .await
                .unwrap();
        });
        let mut config = mock_config();
        config.bucket.endpoint = StringNonEmpty::opt(format!("http://{address}"));
        config.sas_token = StringNonEmpty::opt("sig=secret%2Bsignature&sp=w".to_owned());
        let storage = AzureStorage::new_with_options(
            config,
            ClientOptions {
                retry: RetryOptions::none(),
                ..blob_client_options().unwrap()
            },
        )
        .unwrap();
        let error = timeout(
            Duration::from_secs(5),
            storage.put("file", PutResource(Box::new(&b"data"[..])), 4),
        )
        .await
        .unwrap()
        .unwrap_err();
        for message in [error.to_string(), format!("{error:?}")] {
            assert!(message.contains("upload block failed"));
            assert!(!message.contains("sig="), "{message}");
            assert!(!message.contains("signature"), "{message}");
            assert!(!message.contains(&address.to_string()), "{message}");
        }
        server.await.unwrap();
    }

    #[tokio::test]
    async fn test_blob_upload_uses_single_put_for_empty_and_large_bodies() {
        for size in [0, 4 * 1024 * 1024, 4 * 1024 * 1024 + 1, 17 * 1024 * 1024] {
            let data = vec![42; size];
            let transport = Arc::new(MockTransport::default());
            let storage = AzureStorage::new_with_options(
                mock_config(),
                ClientOptions {
                    transport: Some(Transport::new(transport.clone())),
                    ..Default::default()
                },
            )
            .unwrap();
            // Deliberately underestimate the length: only the actual body size
            // should determine whether the SDK uses a single Put Blob request.
            storage
                .put("file", PutResource(Box::new(data.as_slice())), 0)
                .await
                .unwrap();
            let requests = transport.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                1,
                "unexpected request count for {size} bytes"
            );
            let request = &requests[0];
            assert_eq!(request.method(), Method::Put);
            assert!(request.url().query_pairs().all(|(name, _)| name != "comp"));
            assert_eq!(
                request.headers().get_optional_str(&"x-ms-blob-type".into()),
                Some("BlockBlob")
            );
            match request.body() {
                Body::Bytes(bytes) => assert_eq!(bytes.as_ref(), data.as_slice()),
                Body::SeekableStream(_) => panic!("expected the buffered upload body"),
            }
        }
    }

    #[tokio::test]
    async fn test_blob_download_reads_sequential_16_mib_ranges() {
        let chunk_size = 16 * 1024 * 1024;
        let data = azure_core::Bytes::from(
            (0..2 * chunk_size + 37)
                .map(|i| (i % 251) as u8)
                .collect::<Vec<_>>(),
        );
        for range in [None, Some((chunk_size - 7, chunk_size + 19))] {
            let transport = Arc::new(RangeDownloadTransport {
                data: data.clone(),
                requests: Mutex::new(Vec::new()),
                active_requests: Arc::new(AtomicUsize::new(0)),
            });
            let storage = AzureStorage::new_with_options(
                mock_config(),
                ClientOptions {
                    transport: Some(Transport::new(transport.clone())),
                    ..Default::default()
                },
            )
            .unwrap();
            let (mut reader, expected, expected_ranges) = match range {
                None => (
                    storage.get("file"),
                    data.as_ref(),
                    vec![
                        format!("bytes=0-{}", chunk_size - 1),
                        format!("bytes={}-{}", chunk_size, 2 * chunk_size - 1),
                        format!("bytes={}-{}", 2 * chunk_size, data.len() - 1),
                    ],
                ),
                Some((offset, len)) => (
                    storage.get_part("file", offset as u64, len as u64),
                    &data[offset..offset + len],
                    vec![
                        format!("bytes={}-{}", offset, offset + chunk_size - 1),
                        format!("bytes={}-{}", offset + chunk_size, offset + len - 1),
                    ],
                ),
            };
            let mut downloaded = Vec::new();
            reader.read_to_end(&mut downloaded).await.unwrap();
            assert_eq!(downloaded, expected);
            assert_eq!(transport.active_requests.load(Ordering::SeqCst), 0);
            let requests = transport.requests.lock().unwrap();
            let ranges: Vec<_> = requests
                .iter()
                .map(|request| request.headers().get_optional_str(&"range".into()).unwrap())
                .collect();
            assert_eq!(ranges, expected_ranges);
        }
    }

    #[tokio::test]
    async fn test_blob_upload_omits_absent_or_unknown_access_tier() {
        for tier in [None, Some(StringNonEmpty::static_str("unknown"))] {
            let mut config = mock_config();
            config.bucket.storage_class = tier;
            let transport = Arc::new(MockTransport::default());
            let storage = AzureStorage::new_with_options(
                config,
                ClientOptions {
                    transport: Some(Transport::new(transport.clone())),
                    ..Default::default()
                },
            )
            .unwrap();
            storage
                .put("file", PutResource(Box::new(&b"data"[..])), 4)
                .await
                .unwrap();
            let requests = transport.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert!(
                requests[0]
                    .headers()
                    .get_optional_str(&"x-ms-access-tier".into())
                    .is_none()
            );
        }
    }

    #[tokio::test]
    async fn test_blob_upload_and_range_read_preserve_customer_encryption() {
        let mut config = mock_config();
        config.encryption_customer = Some(EncryptionCustomer {
            encryption_key: base64::encode([7; 32]),
            encryption_key_sha256: base64::encode([8; 32]),
        });
        let transport = Arc::new(MockTransport::default());
        let storage = AzureStorage::new_with_options(
            config.clone(),
            ClientOptions {
                transport: Some(Transport::new(transport.clone())),
                ..Default::default()
            },
        )
        .unwrap();
        storage
            .put("file", PutResource(Box::new(&b"data"[..])), 4)
            .await
            .unwrap();
        let mut bytes = Vec::new();
        storage
            .get_part("file", 20, 4)
            .read_to_end(&mut bytes)
            .await
            .unwrap();
        assert_eq!(bytes, b"data");
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 2);
        for request in requests.iter() {
            assert_eq!(request.url().host_str(), Some("storage.example"));
            // The supported SDK escapes separators inside the blob name.
            assert_eq!(request.url().path(), "/container/backup%2Fprefix%2Ffile");
            let headers = request.headers();
            assert_eq!(
                headers.get_optional_str(&"x-ms-encryption-key".into()),
                Some(
                    config
                        .encryption_customer
                        .as_ref()
                        .unwrap()
                        .encryption_key
                        .as_str()
                )
            );
            assert_eq!(
                headers.get_optional_str(&"x-ms-encryption-algorithm".into()),
                Some("AES256")
            );
            assert!(
                headers
                    .get_optional_str(&"authorization".into())
                    .unwrap()
                    .starts_with("SharedKey account:")
            );
            assert!(
                headers
                    .get_optional_str(&"x-ms-access-tier".into())
                    .is_none()
            );
        }
        assert_eq!(
            requests[1].headers().get_optional_str(&"range".into()),
            Some("bytes=20-23")
        );
    }

    #[tokio::test]
    async fn test_blob_sas_precedes_shared_key_and_preserves_access_tier() {
        let mut config = mock_config();
        config.sas_token = Some(StringNonEmpty::static_str("?sig=test%2Bsignature&sp=rw"));
        config.shared_key = Some(StringNonEmpty::static_str("invalid-base64-key"));
        config.bucket.storage_class = Some(StringNonEmpty::static_str("Cool"));
        let transport = Arc::new(MockTransport::default());
        let storage = AzureStorage::new_with_options(
            config,
            ClientOptions {
                transport: Some(Transport::new(transport.clone())),
                ..Default::default()
            },
        )
        .unwrap();
        storage
            .put("file", PutResource(Box::new(&b"data"[..])), 4)
            .await
            .unwrap();
        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        let request = &requests[0];
        assert_eq!(
            request
                .url()
                .query_pairs()
                .find(|(name, _)| name == "sig")
                .unwrap()
                .1,
            "test+signature"
        );
        assert!(
            request
                .headers()
                .get_optional_str(&"authorization".into())
                .is_none()
        );
        assert_eq!(
            request
                .headers()
                .get_optional_str(&"x-ms-access-tier".into()),
            Some("Cool")
        );
    }

    #[test]
    fn test_url_of_backend() {
        let container_name = StringNonEmpty::static_str("container");
        let mut bucket = BucketConf::default(container_name);
        bucket.prefix = Some(StringNonEmpty::static_str("backup 01/prefix/"));
        let config = Config::default(bucket.clone());

        assert_eq!(
            config.url().unwrap().to_string(),
            "azure://container/backup%2001/prefix/"
        );
        bucket.endpoint = Some(StringNonEmpty::static_str("http://endpoint.com"));
        let config = Config::default(bucket);
        assert_eq!(
            config.url().unwrap().to_string(),
            "http://endpoint.com/container/backup%2001/prefix/"
        );
    }

    #[test]
    fn test_azblob_config() {
        let container_name = StringNonEmpty::static_str("container");
        let mut bucket = BucketConf::default(container_name);
        bucket.endpoint = Some(StringNonEmpty::static_str("http://127.0.0.1:10000/user"));
        bucket.prefix = Some(StringNonEmpty::static_str("backup 02/prefix"));
        env::remove_var(ENV_ACCOUNT_NAME);
        env::remove_var(ENV_SHARED_KEY);
        env::remove_var(ENV_CLIENT_ID);
        env::remove_var(ENV_TENANT_ID);
        env::remove_var(ENV_CLIENT_SECRET);
        let config = Config::default(bucket.clone());

        assert_eq!(config.account_name.is_none(), true);
        assert_eq!(config.shared_key.is_none(), true);
        assert_eq!(config.credential_info.is_none(), true);
        assert_eq!(config.env_account_name.is_none(), true);
        assert_eq!(config.env_shared_key.is_none(), true);

        env::set_var(ENV_ACCOUNT_NAME, "user1");
        env::set_var(ENV_SHARED_KEY, "cGFzc3dk");
        env::set_var(ENV_CLIENT_ID, "<client_id>");
        env::set_var(ENV_TENANT_ID, "<tenant_id>");
        env::set_var(ENV_CLIENT_SECRET, "<client_secret>");

        let config = Config::default(bucket);

        assert_eq!(
            config.env_account_name.as_ref().unwrap().to_string(),
            "user1"
        );
        assert_eq!(
            config.env_shared_key.as_ref().unwrap().to_string(),
            "cGFzc3dk"
        );

        let cred = config.credential_info.as_ref().unwrap();
        let cred_str = format!(
            "{}, {}, {}",
            cred.client_id.as_str(),
            &cred.tenant_id,
            cred.client_secret.secret()
        );
        assert_eq!(cred_str, "<client_id>, <tenant_id>, <client_secret>");

        let debug_str = format!("{:?}", config);
        assert_eq!(debug_str.contains("<client_secret>"), false);
        assert_eq!(debug_str.contains("cGFzc3dk"), false);

        env::remove_var(ENV_ACCOUNT_NAME);
        env::remove_var(ENV_SHARED_KEY);
        env::remove_var(ENV_CLIENT_ID);
        env::remove_var(ENV_TENANT_ID);
        env::remove_var(ENV_CLIENT_SECRET);
    }

    #[ignore = "no available azure cloud service for the test env."]
    #[tokio::test]
    // test in Azurite emulator
    async fn test_azblob_storage() {
        let mut input = InputConfig::default();
        input.set_account_name("devstoreaccount1".to_owned());
        input.set_bucket("test".to_owned());
        input.set_shared_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned());
        input.set_endpoint("http://127.0.0.1:10000/devstoreaccount1".to_owned());
        input.set_prefix("backup 01/prefix/".to_owned());

        let storage = AzureStorage::from_input(input).unwrap();
        assert_eq!(storage.maybe_prefix_key("t"), "backup 01/prefix/t");
        // Reuse the blob name to check replacement by smaller and empty bodies,
        // as well as SharedKey signing above both SDK transfer thresholds.
        let chunk_size = 16 * 1024 * 1024;
        for size in [2 * chunk_size + 37, 4 * 1024 * 1024 + 1, 0] {
            let data: Vec<_> = (0..size).map(|i| (i % 251) as u8).collect();
            storage
                .put("t", PutResource(Box::new(data.as_slice())), size as u64)
                .await
                .unwrap();

            let mut buf = Vec::new();
            storage.get("t").read_to_end(&mut buf).await.unwrap();
            assert_eq!(buf, data);

            if size > 0 {
                let mut part = Vec::new();
                storage
                    .get_part("t", 123, 321)
                    .read_to_end(&mut part)
                    .await
                    .unwrap();
                assert_eq!(part, data[123..444]);
            }
            if size > 2 * chunk_size {
                let offset = chunk_size - 7;
                let len = chunk_size + 19;
                let mut part = Vec::new();
                storage
                    .get_part("t", offset as u64, len as u64)
                    .read_to_end(&mut part)
                    .await
                    .unwrap();
                assert_eq!(part, data[offset..offset + len]);
            }
        }
    }

    #[test]
    fn test_config_check() {
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            let config = Config::from_input(input).unwrap();
            AzureStorage::check_config(&config).unwrap();
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            input.set_storage_class("Hot".to_owned());
            let config = Config::from_input(input).unwrap();
            AzureStorage::check_config(&config).unwrap();
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            input.set_storage_class("Hot".to_owned());
            let mut encryption_key = AzureCustomerKey::default();
            encryption_key.set_encryption_key("test".to_owned());
            encryption_key.set_encryption_key_sha256("test".to_owned());
            input.set_encryption_key(encryption_key);
            let config = Config::from_input(input).unwrap();
            assert!(AzureStorage::check_config(&config).is_err());
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            input.set_storage_class("Hot".to_owned());
            input.set_encryption_scope("test".to_owned());
            let config = Config::from_input(input).unwrap();
            assert!(AzureStorage::check_config(&config).is_err());
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            input.set_storage_class("Hot".to_owned());
            let mut encryption_key = AzureCustomerKey::default();
            encryption_key.set_encryption_key("test".to_owned());
            encryption_key.set_encryption_key_sha256("test".to_owned());
            input.set_encryption_key(encryption_key);
            input.set_encryption_scope("test".to_owned());
            let config = Config::from_input(input).unwrap();
            assert!(AzureStorage::check_config(&config).is_err());
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            let mut encryption_key = AzureCustomerKey::default();
            encryption_key.set_encryption_key("test".to_owned());
            encryption_key.set_encryption_key_sha256("test".to_owned());
            input.set_encryption_key(encryption_key);
            let config = Config::from_input(input).unwrap();
            AzureStorage::check_config(&config).unwrap();
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            input.set_encryption_scope("test".to_owned());
            let config = Config::from_input(input).unwrap();
            AzureStorage::check_config(&config).unwrap();
        }
        {
            let mut input = InputConfig::default();
            input.set_bucket("test".to_owned());
            let mut encryption_key = AzureCustomerKey::default();
            input.set_encryption_scope("test".to_owned());
            encryption_key.set_encryption_key("test".to_owned());
            encryption_key.set_encryption_key_sha256("test".to_owned());
            input.set_encryption_key(encryption_key);
            let config = Config::from_input(input).unwrap();
            assert!(AzureStorage::check_config(&config).is_err());
        }
    }
}
