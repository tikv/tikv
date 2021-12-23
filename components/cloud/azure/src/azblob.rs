use async_trait::async_trait;
use azure_core::prelude::*;
use azure_identity::client_credentials_flow;
use azure_storage::blob::prelude::*;
use azure_storage::core::{prelude::*, ConnectionStringBuilder};
use cloud::blob::{
    none_to_empty, BlobConfig, BlobStorage, BucketConf, PutResource, StringNonEmpty,
};
use futures::executor::block_on;
use futures_util::{
    stream::StreamExt,
    TryStreamExt,
    io::{AsyncRead, AsyncReadExt},
};
pub use kvproto::brpb::{Bucket as InputBucket, CloudDynamic, AzureBlobStorage as InputConfig};
use oauth2::{ClientId, ClientSecret};
use tikv_util::debug;
use tikv_util::stream::{retry, RetryError};
use tokio::time::{timeout, Duration};
use uuid::Uuid;

use std::str::FromStr;
use std::{env, io, sync::Arc};

const ENV_CLIENT_ID: &str = "AZURE_CLIENT_ID";
const ENV_TENANT_ID: &str = "AZURE_TENANT_ID";
const ENV_CLIENT_SECRET: &str = "AZURE_CLIENT_SECRET";
const ENV_ACCOUNT_NAME: &str = "AZURE_STORAGE_ACCOUNT";
const ENV_SHARED_KEY: &str = "AZURE_STORAGE_KEY";

/// CredentialInfo saves the credential variables from the enviroment:
/// client_id:      $AZURE_CLIENT_ID, 
/// tenant_id:      $AZURE_TENANT_ID, 
/// client_secret:  $AZURE_CLIENT_SECRET,
#[derive(Clone)]
struct CredentialInfo {
    client_id: ClientId, 
    tenant_id: String,
    client_secret: ClientSecret,
}

impl std::fmt::Debug for CredentialInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CredentialInfo")
            .field("client_id", &self.client_id)
            .field("tenant_id", &self.tenant_id)
            .field("client_secret", &"?")
            .finish()
    }
}

#[derive(Clone)]
pub struct Config {
    bucket: BucketConf,

    account_name: Option<StringNonEmpty>,
    shared_key: Option<StringNonEmpty>,
    credential_info: Option<CredentialInfo>,
    env_account_name: Option<StringNonEmpty>,
    env_shared_key: Option<StringNonEmpty>,
}

impl std::fmt::Debug for Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Config")
            .field("bucket", &self.bucket)
            .field("account_name", &self.account_name)
            .field("shared_key", &"?")
            .field("credential_info", &self.credential_info)
            .field("env_account_name", &self.env_account_name)
            .field("env_shared_key", &"?")
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
            credential_info: Self::load_credential_info(),
            env_account_name: Self::load_env_account_name(),
            env_shared_key: Self::load_env_shared_key(),
        }
    }

    fn load_credential_info() -> Option<CredentialInfo> {
        if let (Some(client_id), Some(tenant_id), Some(client_secret)) = (
            env::var(ENV_CLIENT_ID).ok(),
            env::var(ENV_TENANT_ID).ok(),
            env::var(ENV_CLIENT_SECRET).ok()
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
        match env::var(ENV_ACCOUNT_NAME).ok() {
            Some(name) => StringNonEmpty::opt(name),
            None => None,
        }
    }

    fn load_env_shared_key() -> Option<StringNonEmpty> {
        match env::var(ENV_SHARED_KEY).ok() {
            Some(key) => StringNonEmpty::opt(key),
            None => None,
        }
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Config> {
        let bucket = BucketConf::from_cloud_dynamic(cloud_dynamic)?;
        let attrs = &cloud_dynamic.attrs;
        let def = &String::new();

        Ok(Config {
            bucket,
            account_name: StringNonEmpty::opt(attrs.get("account_name").unwrap_or(def).clone()),
            shared_key: StringNonEmpty::opt(attrs.get("shared_key").unwrap_or(def).clone()),
            credential_info: Self::load_credential_info(),
            env_account_name: Self::load_env_account_name(),
            env_shared_key: Self::load_env_shared_key(),
        })
    }

    pub fn from_input(input: InputConfig) -> io::Result<Config> {
        let bucket = BucketConf {
            endpoint: StringNonEmpty::opt(input.endpoint),
            bucket: StringNonEmpty::required_field(input.bucket, "bucket")?,
            prefix: StringNonEmpty::opt(input.prefix),
            storage_class: StringNonEmpty::opt(input.storage_class),
            region: None,
        };
        
        Ok(Config {
            bucket,
            account_name: StringNonEmpty::opt(input.account_name),
            shared_key: StringNonEmpty::opt(input.shared_key),
            credential_info: Self::load_credential_info(),
            env_account_name: Self::load_env_account_name(),
            env_shared_key: Self::load_env_shared_key(),
        })
    }


    pub fn get_account_name(&self) -> io::Result<String> {
        if let Some(account_name) = self.account_name.as_ref() {
            Ok(account_name.to_string())
        } else if let Some(account_name) = self.env_account_name.as_ref() {
            Ok(account_name.to_string())
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "account name cannot be empty to access azure blob storage"))
        }
    }

    pub fn parse_plaintext_account_url(&self) -> Option<String> {
        if let (Some(account_name), Some(shared_key)) = (self.account_name.as_ref(), self.shared_key.as_ref()) {
            let blob_endpoint = match self.bucket.endpoint.as_ref() {
                Some(s) => s.to_string(),
                None => format!("https://{}.blob.core.windows.net", account_name),
            };
            Some(ConnectionStringBuilder::new()
                    .account_name(account_name)
                    .account_key(shared_key)
                    .blob_endpoint(&blob_endpoint)
                    .build())
        } else {
            None
        }
    }

    pub fn parse_env_plaintext_account_url(&self) -> Option<String> {
        if let (Some(account_name), Some(shared_key)) = (self.env_account_name.as_ref(), self.env_shared_key.as_ref()) {
            let blob_endpoint = match self.bucket.endpoint.as_ref() {
                Some(s) => s.to_string(),
                None => format!("https://{}.blob.core.windows.net", account_name),
            };
            Some(ConnectionStringBuilder::new()
                    .account_name(account_name)
                    .account_key(shared_key)
                    .blob_endpoint(&blob_endpoint)
                    .build())
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
            RequestError::InvalidInput(e, tag) => Self::new(io::ErrorKind::InvalidInput, format!("{}: {}", tag, &e)),
            RequestError::TimeOut(msg) => Self::new(io::ErrorKind::TimedOut, msg),
        }
    }
}


impl RetryError for RequestError {
    fn is_retryable(&self) -> bool {
        match self {
            Self::TimeOut(_) => true,
            _ => false,
        }
    }
}


/// Specifies the minimum size to use multi-block upload.
/// Azure requires each part to be at least 4 MiB compatible with 2016 version.
/// https://docs.microsoft.com/en-us/rest/api/storageservices/understanding-block-blobs--append-blobs--and-page-blobs
/// while azure-sdk-for-go use 256 MiB
const MINIMUM_BLOCK_SIZE: usize = 128 * 1024 * 1024; // 128MB

/// Specifies the size of data read each time.
const STREAM_READ_SIZE: u64 = 96 * 1024 * 1024; // 96MB

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(900);

/// A helper for uploading a large file to Azure storage.
///
/// 
struct AzureUploader {
    client: Arc<BlobClient>,
    block_list: BlockList,

    storage_class: AccessTier,
}

impl AzureUploader {
    /// Creates a new uploader with a given target location and upload configuration.
    fn new(client: Arc<ContainerClient>, config: &Config, key: &str) -> Self {

        AzureUploader {
            client: client.as_blob_client(key),
            block_list: BlockList::default(),

            storage_class: Self::parse_storage_class(none_to_empty(config.bucket.storage_class.clone())),
        }
    }

    fn parse_storage_class(storage_class: String) -> AccessTier {
        AccessTier::from_str(storage_class.as_str()).unwrap_or(AccessTier::Hot)
    }

    /// Executes the upload process.
    async fn run(
        mut self,
        reader: &mut (dyn AsyncRead + Unpin + Send),
        est_len: u64,
    ) -> io::Result<()> {
        if est_len <= MINIMUM_BLOCK_SIZE as u64 {
            // For short files, execute one put_object to upload the entire thing.
            let mut data = Vec::with_capacity(est_len as usize);
            reader.read_to_end(&mut data).await?;
            retry(|| self.upload(&data)).await?;
            Ok(())
        } else {
            let upload_res = async {
                let mut buf = vec![0; MINIMUM_BLOCK_SIZE];
                loop {
                    let data_size = reader.read(&mut buf).await?;
                    if data_size == 0 {
                        break;
                    }
                    // keep the length of the block_id the same
                    
                    let block_id = BlockId::new(base64::encode(Uuid::new_v4().to_hyphenated().to_string()));
                    let _ = retry(|| self.upload_block(block_id.clone(), &buf[..data_size])).await?;
                    self.block_list.blocks.push(BlobBlockType::new_uncommitted(block_id));
                }
                Ok(())
            }
            .await;

            // The uncommitted blocks will be later deleted during garbage collection.
            if upload_res.is_ok() {
                retry(|| self.complete()).await?;
            }
            upload_res
        }

    }

    /// Completes a multipart upload process, asking Azure to commit all the previously uploaded blocks.
    async fn complete(&self) -> Result<(), RequestError> {
        match timeout(
            Self::get_timeout(),
            self.client
                .put_block_list(&self.block_list)
                .access_tier(self.storage_class)
                .execute()
        )
        .await
        {
            Ok(res) => match res {
                Ok(_) => Ok(()),
                Err(err) => Err(RequestError::InvalidInput(err, "upload block_list failed".to_owned())),
            },
            Err(_) => Err(
                RequestError::TimeOut(
                    "timeout after 15mins for complete in azure storage".to_owned(),
                ),
            ),
        }
    }


    /// Uploads a part of the file.
    ///
    /// The `block_number` must be between 1 to 50000.
    async fn upload_block(
        &self,
        block_number: BlockId,
        data: &[u8],
    ) -> Result<(), RequestError> {
        match timeout(
            Self::get_timeout(),
            self.client
                .put_block(block_number, data.to_vec())
                .execute(),
        )
        .await
        {
            Ok(res) => match res {
                Ok(_) => Ok(()),
                Err(err) => Err(RequestError::InvalidInput(err, "upload block failed".to_owned())),
            },
            Err(_) => Err(
                RequestError::TimeOut(
                    "timeout after 15mins for complete in azure storage".to_owned(),
                ),
            ),
        }
    }

    /// Uploads a file atomically.
    ///
    /// This should be used only when the data is known to be short, and thus relatively cheap to
    /// retry the entire upload.
    async fn upload(&self, data: &[u8]) -> Result<(), RequestError> {
        match timeout(Self::get_timeout(), async {
            self.client
                .put_block_blob(data.to_vec())
                .access_tier(self.storage_class.clone())
                .execute()
                .await?;
            Ok(())
        })
        .await
        {
            Ok(res) => match res {
                Ok(_) => Ok(()),
                Err(err) => Err(RequestError::InvalidInput(err, "upload block failed".to_owned())),
            },
            Err(_) => Err(
                RequestError::TimeOut(
                    "timeout after 15mins for complete in azure storage".to_owned(),
                ),
            ),
        }   
    }

    fn get_timeout() -> Duration {
        CONNECTION_TIMEOUT
    }
}



const STORAGE_NAME: &str = "azure";

#[derive(Clone)]
pub struct AzureStorage {
    config: Config,
    client: Arc<ContainerClient>,
    
}

impl AzureStorage {
    pub fn from_input(input: InputConfig) -> io::Result<Self> {
        Self::new(Config::from_input(input)?)
    }

    pub fn from_cloud_dynamic(cloud_dynamic: &CloudDynamic) -> io::Result<Self> {
        Self::new(Config::from_cloud_dynamic(cloud_dynamic)?)
    }

    pub fn new(config: Config) -> io::Result<AzureStorage> {
        // priority: explicit shared key > env Azure AD > env shared key
        if let Some(connection_string) = config.parse_plaintext_account_url() {
            let bucket = (*config.bucket.bucket).to_owned();
            let http_client = new_http_client();
            let storage_client = 
                StorageAccountClient::new_connection_string(http_client.clone(), connection_string.as_str())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}", &e)))?
                .as_storage_client()
                .as_container_client(bucket);
            
            Ok(AzureStorage {
                config,
                client: storage_client,
            })
        } else if let Some(credential_info) = config.credential_info.as_ref() {
            let account_name = config.get_account_name()?;
            let token_scope = format!("https://{}.blob.core.windows.net/.default", &account_name);
            let client = reqwest::Client::new();
            let token = block_on(client_credentials_flow::perform(
                client,
                &credential_info.client_id, 
                &credential_info.client_secret, 
                &[&token_scope],
                &credential_info.tenant_id
            ))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}", &e)))?;
            let bucket = (*config.bucket.bucket).to_owned();
            let http_client = new_http_client();
            let storage_client = 
                StorageAccountClient::new_bearer_token(http_client, account_name, token.access_token().secret())
                .as_storage_client()
                .as_container_client(bucket);
            
            Ok(AzureStorage {
                config,
                client: storage_client,
            })
        } else if let Some(connection_string) = config.parse_env_plaintext_account_url() {
            let bucket = (*config.bucket.bucket).to_owned();
            let http_client = new_http_client();
            let storage_client = 
                StorageAccountClient::new_connection_string(http_client.clone(), connection_string.as_str())
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, format!("{}", &e)))?
                .as_storage_client()
                .as_container_client(bucket);
            
            Ok(AzureStorage {
                config,
                client: storage_client,
            })
        } else {
            Err(io::Error::new(io::ErrorKind::InvalidInput, "credential info not found".to_owned()))
        }   
    }

    fn maybe_prefix_key(&self, key: &str) -> String {
        if let Some(prefix) = &self.config.bucket.prefix {
            return format!("{}/{}", prefix.trim_end_matches('/'), key);
        }
        key.to_owned()
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

        let uploader = AzureUploader::new(
            self.client.clone(), 
            &self.config, 
            &name
        );
        
        uploader.run(
            &mut reader, 
            content_length,
        ).await
    }

    fn get(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        let name = self.maybe_prefix_key(name);
        debug!("read file from Azure storage"; "key" => %name);
        let t = Box::new(AzblobReader::new(
            self.client.as_blob_client(name), 
            |client| Box::new(
                client.get()
                    .stream(STREAM_READ_SIZE)
                    .map(|res| match res {
                        Ok(response) => Ok(response.data),
                        Err(e) => Err(io::Error::new(io::ErrorKind::InvalidInput, format!("{}", e))),
                    })
                    .boxed().into_async_read()),
        ));
        t
    }
}

use std::{pin::Pin, task::{Context, Poll}};
use ouroboros::self_referencing;
#[self_referencing]
struct AzblobReader {
    client: Arc<BlobClient>,
    #[borrows(client)]
    #[covariant]
    stream: Box<dyn AsyncRead + Unpin + 'this>,
}

impl AsyncRead for AzblobReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        self.as_mut().with_stream_mut(|stream| Pin::new(stream).poll_read(cx, buf))
    }

    fn poll_read_vectored(mut self: Pin<&mut Self>, cx: &mut Context<'_>, bufs: &mut [io::IoSliceMut<'_>]) -> Poll<io::Result<usize>> {
        self.as_mut().with_stream_mut(|stream| Pin::new(stream).poll_read_vectored(cx, bufs))
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
        let config = Config::default(bucket.clone());
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
        
        assert_eq!(config.env_account_name.as_ref().unwrap().to_string(), "user1");
        assert_eq!(config.env_shared_key.as_ref().unwrap().to_string(), "cGFzc3dk");

        let cred = config.credential_info.as_ref().unwrap();
        let cred_str = format!("{}, {}, {}", cred.client_id.as_str(), &cred.tenant_id, cred.client_secret.secret());
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

        let storage = AzureStorage::from_input(input).unwrap();
        assert_eq!(storage.maybe_prefix_key("t"), "backup 01/prefix/t");
        let mut magic_contents = String::new();
        for _ in 0..4096 { // 4 KiB
            magic_contents.push_str("qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM1234567890\n"); 
        }
        let size = magic_contents.len() as u64;
        let stream = stream::once(async move {Ok(magic_contents)}).boxed().into_async_read();
        storage.put(
            "t", 
            PutResource(Box::new(stream)), 
            size,
        )
        .await
        .unwrap();

        let mut reader = storage.get("t");
        let mut buf = Vec::new();
        let get_size = reader.read_to_end(&mut buf).await.unwrap() as u64;
        assert_eq!(get_size, size);
    }

    #[test]
    fn test_config_round_trip() {
        let mut input = InputConfig::default();
        input.set_bucket("bucket".to_owned());
        input.set_prefix("backup 02/prefix/".to_owned());
        input.set_account_name("user".to_owned());
        let c1 = Config::from_input(input.clone()).unwrap();
        let c2 = Config::from_cloud_dynamic(&cloud_dynamic_from_input(input)).unwrap();
        assert_eq!(c1.bucket.bucket, c2.bucket.bucket);
        assert_eq!(c1.bucket.prefix, c2.bucket.prefix);
        assert_eq!(c1.account_name, c2.account_name);
    }

    fn cloud_dynamic_from_input(mut azure: InputConfig) -> CloudDynamic {
        let mut bucket = InputBucket::default();
        if !azure.endpoint.is_empty() {
            bucket.endpoint = azure.take_endpoint();
        }
        if !azure.prefix.is_empty() {
            bucket.prefix = azure.take_prefix();
        }
        if !azure.storage_class.is_empty() {
            bucket.storage_class = azure.take_storage_class();
        }
        if !azure.bucket.is_empty() {
            bucket.bucket = azure.take_bucket();
        }
        let mut attrs = std::collections::HashMap::new();
        if !azure.account_name.is_empty() {
            attrs.insert("account_name".to_owned(), azure.take_account_name());
        }
        if !azure.shared_key.is_empty() {
            attrs.insert("shared_key".to_owned(), azure.take_shared_key());
        }
        let mut cd = CloudDynamic::default();
        cd.set_provider_name("azure".to_owned());
        cd.set_attrs(attrs);
        cd.set_bucket(bucket);
        cd
    }

}