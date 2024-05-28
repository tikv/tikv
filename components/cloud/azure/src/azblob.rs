// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use std::{
    env, io,
    ops::Deref,
    str::FromStr,
    sync::{Arc, RwLock},
};

use async_trait::async_trait;
use azure_core::{
    auth::{TokenCredential, TokenResponse},
    new_http_client,
};
use azure_identity::{ClientSecretCredential, TokenCredentialOptions};
use azure_storage::{prelude::*, ConnectionString, ConnectionStringBuilder};
use azure_storage_blobs::{blob::operations::PutBlockBlobBuilder, prelude::*};
use cloud::blob::{
    none_to_empty, BlobConfig, BlobStorage, BucketConf, PutResource, StringNonEmpty,
};
use futures::TryFutureExt;
use futures_util::{
    io::{AsyncRead, AsyncReadExt},
    stream,
    stream::StreamExt,
    TryStreamExt,
};
pub use kvproto::backup::{AzureBlobStorage as InputConfig, AzureCustomerKey};
use oauth2::{ClientId, ClientSecret};
use tikv_util::{
    debug,
    stream::{retry, RetryError},
};
use time::OffsetDateTime;
use tokio::{
    sync::Mutex,
    time::{timeout, Duration},
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
    client_id: ClientId,
    tenant_id: String,
    client_secret: ClientSecret,
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
                let client_id = ClientId::new(client_id);
                let client_secret = ClientSecret::new(client_secret);
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

        let encryption_customer = input.encryption_key.clone().map(EncryptionCustomer::from);

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

    pub fn parse_plaintext_account_url(&self) -> Option<String> {
        if let (Some(account_name), Some(shared_key)) =
            (self.account_name.as_ref(), self.shared_key.as_ref())
        {
            let blob_endpoint = match self.bucket.endpoint.as_ref() {
                Some(s) => s.to_string(),
                None => format!("https://{}.blob.core.windows.net", account_name),
            };
            Some(
                ConnectionStringBuilder::new()
                    .account_name(account_name)
                    .account_key(shared_key)
                    .blob_endpoint(&blob_endpoint)
                    .build(),
            )
        } else {
            None
        }
    }

    pub fn parse_env_plaintext_account_url(&self) -> Option<String> {
        if let (Some(account_name), Some(shared_key)) =
            (self.env_account_name.as_ref(), self.env_shared_key.as_ref())
        {
            let blob_endpoint = match self.bucket.endpoint.as_ref() {
                Some(s) => s.to_string(),
                None => format!("https://{}.blob.core.windows.net", account_name),
            };
            Some(
                ConnectionStringBuilder::new()
                    .account_name(account_name)
                    .account_key(shared_key)
                    .blob_endpoint(&blob_endpoint)
                    .build(),
            )
        } else {
            None
        }
    }
}

impl BlobConfig for Config {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        self.bucket.url("azure").map_err(|s| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("error creating bucket url: {}", s),
            )
        })
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
                Self::new(io::ErrorKind::InvalidInput, format!("{}: {:?}", tag, &e))
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

/// A helper for uploading a large file to Azure storage.
struct AzureUploader {
    client_builder: Arc<dyn ContainerBuilder>,
    name: String,

    storage_class: Option<AccessTier>,
    encryption_scope: Option<StringNonEmpty>,
    encryption_customer: Option<EncryptionCustomer>,
}

impl AzureUploader {
    /// Creates a new uploader with a given target location and upload
    /// configuration.
    fn new(client_builder: Arc<dyn ContainerBuilder>, config: &Config, name: String) -> Self {
        AzureUploader {
            client_builder,
            name,

            storage_class: Self::parse_storage_class(none_to_empty(
                config.bucket.storage_class.clone(),
            )),
            encryption_scope: config.encryption_scope.clone(),
            encryption_customer: config.encryption_customer.clone(),
        }
    }

    fn parse_storage_class(storage_class: String) -> Option<AccessTier> {
        AccessTier::from_str(storage_class.as_str()).ok()
    }

    /// Executes the upload process.
    async fn run(
        self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> io::Result<()> {
        // upload the entire data.
        let mut data = Vec::with_capacity(est_len as usize);
        reader.read_to_end(&mut data).await?;
        retry(|| self.upload(&data)).await?;
        Ok(())
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus
    /// relatively cheap to retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RequestError> {
        let res = timeout(Self::get_timeout(), async {
            let builder = self
                .client_builder
                .get_client()
                .await
                .map_err(|e| e.to_string())?
                .blob_client(&self.name)
                .put_block_blob(data.to_vec());

            let builder = self.adjust_put_builder(builder);

            builder.await?;
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

    #[inline]
    fn adjust_put_builder(&self, builder: PutBlockBlobBuilder) -> PutBlockBlobBuilder {
        // the encryption scope and the access tier can not be both in the HTTP headers
        if let Some(scope) = &self.encryption_scope {
            return builder.encryption_scope(scope.deref().clone());
        }

        // the encryption customer provided key and the access tier can not be both in
        // the HTTP headers
        if let Some(key) = &self.encryption_customer {
            return builder.encryption_key::<(String, String)>(key.clone().into());
        }

        if let Some(tier) = self.storage_class {
            return builder.access_tier(tier);
        }

        builder
    }

    fn get_timeout() -> Duration {
        CONNECTION_TIMEOUT
    }
}

// if use azure ad to access the azure blob,
// it need to update the token at regular intervals,
// so wrap the client builder
#[async_trait]
trait ContainerBuilder: 'static + Send + Sync {
    async fn get_client(&self) -> io::Result<Arc<ContainerClient>>;
}

struct SharedKeyContainerBuilder {
    container_client: Arc<ContainerClient>,
}

#[async_trait]
impl ContainerBuilder for SharedKeyContainerBuilder {
    async fn get_client(&self) -> io::Result<Arc<ContainerClient>> {
        Ok(self.container_client.clone())
    }
}

type TokenCacheType = Arc<RwLock<Option<(TokenResponse, Arc<ContainerClient>)>>>;
struct TokenCredContainerBuilder {
    account_name: String,
    container_name: String,
    token_resource: String,
    token_cred: Arc<ClientSecretCredential>,
    token_cache: TokenCacheType,

    modify_place: Arc<Mutex<bool>>,
}

impl TokenCredContainerBuilder {
    fn new(
        account_name: String,
        container_name: String,
        token_resource: String,
        token_cred: Arc<ClientSecretCredential>,
    ) -> Self {
        Self {
            account_name,
            container_name,
            token_resource,
            token_cred,
            token_cache: Arc::new(RwLock::new(None)),

            modify_place: Arc::new(Mutex::new(true)),
        }
    }
}

// if the token only has 5 minutes left.
// Threads will try to modify it without blocked: try_lock
// The thread doesn't get the lock will continue to use the token.
const TOKEN_UPDATE_LEFT_TIME_MINS: i64 = 5;
// if the token only has 2 minutes left.
// Threads will try to modify it with blocked: lock
const TOKEN_EXPIRE_LEFT_TIME_MINS: i64 = 2;

#[async_trait]
impl ContainerBuilder for TokenCredContainerBuilder {
    async fn get_client(&self) -> io::Result<Arc<ContainerClient>> {
        // only the thread get the modify_lock can update the token,
        // so that this thread can get the token before lock wirte-lock
        // avoid to block other threads too much time.
        let mut modify_lock = None;
        {
            let token_response = self.token_cache.read().unwrap();
            if let Some(ref t) = *token_response {
                let interval = (t.0.expires_on - OffsetDateTime::now_utc()).whole_minutes();
                // keep token updated 5 minutes before it expires
                if interval > TOKEN_UPDATE_LEFT_TIME_MINS {
                    return Ok(t.1.clone());
                }

                if interval > TOKEN_EXPIRE_LEFT_TIME_MINS {
                    // there still have time to use the token,
                    // and only need one thread to update token.
                    if let Ok(l) = self.modify_place.try_lock() {
                        modify_lock = Some(l);
                    } else {
                        // otherwise, continue to use the current token
                        return Ok(t.1.clone());
                    }
                }
            }
        } // release the read lock

        // give up getting the client from cache, try update it
        if modify_lock.is_none() {
            modify_lock = Some(self.modify_place.lock().await);
        }

        if let Some(_lock) = modify_lock {
            // check whether there is another thread already updates the token.
            {
                let token_response = self.token_cache.read().unwrap();
                if let Some(ref t) = *token_response {
                    let interval = (t.0.expires_on - OffsetDateTime::now_utc()).whole_minutes();
                    // token is already updated
                    if interval > TOKEN_UPDATE_LEFT_TIME_MINS {
                        return Ok(t.1.clone());
                    }
                }
            }
            // release read lock, the thread still have modify lock,
            // so no other threads can write the token_cache, so read lock is not blocked.
            let token = self
                .token_cred
                .get_token(&self.token_resource)
                .await
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{:?}", &e)))?;
            let blob_service = BlobServiceClient::new(
                self.account_name.clone(),
                StorageCredentials::BearerToken(token.token.secret().into()),
            );
            let storage_client =
                Arc::new(blob_service.container_client(self.container_name.clone()));

            {
                let mut token_response = self.token_cache.write().unwrap();
                *token_response = Some((token, storage_client.clone()));
            }
            Ok(storage_client)
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "failed to get either modify_lock or client",
            ))
        }
    }
}

const STORAGE_NAME: &str = "azure";

#[derive(Clone)]
pub struct AzureStorage {
    config: Config,
    client_builder: Arc<dyn ContainerBuilder>,
}

impl AzureStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    /// Mock a dummpy AzureStorage with a shared key Config for
    /// testing by Azurite tool.
    ///
    /// This function should only be used for testing Blob with a
    /// local Azurite server.
    #[cfg(test)]
    #[allow(dead_code)]
    fn from_dummy_input(input: InputConfig) -> io::Result<Self> {
        let config = Config::from_input(input)?;
        let bucket = (*config.bucket.bucket).to_owned();
        Ok(AzureStorage {
            config,
            client_builder: Arc::new(SharedKeyContainerBuilder {
                container_client: Arc::new(
                    ClientBuilder::emulator()
                        .blob_service_client()
                        .container_client(bucket),
                ),
            }),
        })
    }

    pub fn new(config: Config) -> io::Result<AzureStorage> {
        Self::check_config(&config)?;

        let account_name = config.get_account_name()?;
        let bucket = (*config.bucket.bucket).to_owned();
        // priority:
        //   explicit sas token > explicit shared key > env Azure AD > env shared key
        if let Some(sas_token) = config.sas_token.as_ref() {
            let token = sas_token.deref();
            let storage_credentials = StorageCredentials::sas_token(token).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid configurations for SAS token, err: {}", e),
                )
            })?;
            let container_client = Arc::new(
                BlobServiceClient::new(account_name, storage_credentials).container_client(bucket),
            );

            let client_builder = Arc::new(SharedKeyContainerBuilder { container_client });
            Ok(AzureStorage {
                config,
                client_builder,
            })
        } else if let Some(connection_string) = config.parse_plaintext_account_url() {
            let storage_credentials = ConnectionString::new(&connection_string)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid configurations for SharedKey, err: {}", e),
                    )
                })?
                .storage_credentials()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid credentials for blob, err: {}", e),
                    )
                })?;
            let container_client = Arc::new(
                BlobServiceClient::new(account_name, storage_credentials).container_client(bucket),
            );

            let client_builder = Arc::new(SharedKeyContainerBuilder { container_client });
            Ok(AzureStorage {
                config,
                client_builder,
            })
        } else if let Some(credential_info) = config.credential_info.as_ref() {
            let token_resource = format!("https://{}.blob.core.windows.net", &account_name);
            let cred = ClientSecretCredential::new(
                new_http_client(),
                credential_info.tenant_id.clone(),
                credential_info.client_id.to_string(),
                credential_info.client_secret.secret().clone(),
                TokenCredentialOptions::default(),
            );

            let client_builder = Arc::new(TokenCredContainerBuilder::new(
                account_name,
                bucket,
                token_resource,
                Arc::new(cred),
            ));
            // get token later
            Ok(AzureStorage {
                config,
                client_builder,
            })
        } else if let Some(connection_string) = config.parse_env_plaintext_account_url() {
            let storage_credentials = ConnectionString::new(&connection_string)
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invald configurations for SharedKey from ENV, err: {}", e),
                    )
                })?
                .storage_credentials()
                .map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        format!("invalid credentials for blob, err: {}", e),
                    )
                })?;
            let container_client = Arc::new(
                BlobServiceClient::new(account_name, storage_credentials).container_client(bucket),
            );

            let client_builder = Arc::new(SharedKeyContainerBuilder { container_client });
            Ok(AzureStorage {
                config,
                client_builder,
            })
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "credential info not found".to_owned(),
            ))
        }
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
            let blob_client = self.client_builder.get_client().await?.blob_client(name);

            let builder = if let Some(r) = range {
                blob_client.get().range(r)
            } else {
                blob_client.get()
            };

            let builder = if let Some(key) = &self.config.encryption_customer {
                builder.encryption_key::<(String, String)>(key.clone().into())
            } else {
                builder
            };

            let mut chunk: Vec<u8> = vec![];
            let mut stream = builder.into_stream();
            while let Some(value) = stream.next().await {
                let value = value?.data.collect().await?;
                chunk.extend(&value);
            }
            azure_core::Result::Ok(chunk)
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
        mut reader: PutResource,
        content_length: u64,
    ) -> io::Result<()> {
        let name = self.maybe_prefix_key(name);
        debug!("save file to Azure storage"; "key" => %name);

        let uploader = AzureUploader::new(self.client_builder.clone(), &self.config, name);

        uploader.run(&mut reader, content_length).await
    }

    fn get(&self, name: &str) -> cloud::blob::BlobStream<'_> {
        self.get_range(name, None)
    }

    fn get_part(&self, name: &str, off: u64, len: u64) -> cloud::blob::BlobStream<'_> {
        self.get_range(name, Some(off..off + len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[tokio::test]
    #[cfg(feature = "azurite")]
    // test in Azurite emulator
    async fn test_azblob_storage() {
        use futures_util::stream;
        let mut input = InputConfig::default();
        input.set_account_name("devstoreaccount1".to_owned());
        input.set_bucket("test".to_owned());
        input.set_shared_key("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_owned());
        input.set_endpoint("http://127.0.0.1:10000/devstoreaccount1".to_owned());
        input.set_prefix("backup 01/prefix/".to_owned());

        let storage = AzureStorage::from_dummy_input(input).unwrap();
        assert_eq!(storage.maybe_prefix_key("t"), "backup 01/prefix/t");
        let mut magic_contents = String::new();
        for _ in 0..4096 {
            // 4 KiB
            magic_contents
                .push_str("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890\n");
        }
        let size = magic_contents.len() as u64;
        let stream = stream::once(async move { Ok(magic_contents) })
            .boxed()
            .into_async_read();
        storage
            .put("t", PutResource(Box::new(stream)), size)
            .await
            .unwrap();

        let mut reader = storage.get("t");
        let mut buf = Vec::new();
        let get_size = reader.read_to_end(&mut buf).await.unwrap() as u64;
        assert_eq!(get_size, size);
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
