// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::{self, TryFutureExt};
use kvproto::encryptionpb::EncryptedContent;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_core::RusotoError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_kms::DecryptError;
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use tokio::runtime::{Builder, Runtime};

use super::{metadata::MetadataKey, Backend, MemAesGcmBackend};
use crate::config::KmsConfig;
use crate::crypter::{Iv, PlainKey};
use crate::{Error, Result};

const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";
const AWS_KMS_VENDOR_NAME: &[u8] = b"AWS";

#[derive(PartialEq, Debug)]
pub struct KeyId(String);

// KeyID is a newtype to mark a String as an ID of a key
// This ID exists in a foreign system such as AWS
// The key id must be non-empty
impl KeyId {
    fn new(id: String) -> Result<KeyId> {
        if id.is_empty() {
            Err(box_err!("KMS key id can not be empty"))
        } else {
            Ok(KeyId(id))
        }
    }
}

pub struct AwsKms {
    client: KmsClient,
    current_key_id: KeyId,
    region: String,
    endpoint: String,
}

struct KmsClientDebug {
    region: String,
    endpoint: String,
}

impl std::fmt::Debug for KmsClientDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KmsClient")
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .finish()
    }
}

impl std::fmt::Debug for AwsKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kms_client = KmsClientDebug {
            region: self.region.clone(),
            endpoint: self.endpoint.clone(),
        };
        f.debug_struct("AwsKms")
            .field("client", &kms_client)
            .field("current_key_id", &self.current_key_id)
            .finish()
    }
}

impl AwsKms {
    fn new_creds_dispatcher<Creds, Dispatcher>(
        config: KmsConfig,
        dispatcher: Dispatcher,
        credentials_provider: Creds,
    ) -> Result<AwsKms>
    where
        Creds: ProvideAwsCredentials + Send + Sync + 'static,
        Dispatcher: DispatchSignedRequest + Send + Sync + 'static,
    {
        let region = rusoto_util::get_region(config.region.as_ref(), config.endpoint.as_ref())?;
        let client = KmsClient::new_with(dispatcher, credentials_provider, region);
        Ok(AwsKms {
            client,
            current_key_id: KeyId::new(config.key_id)?,
            region: config.region,
            endpoint: config.endpoint,
        })
    }

    fn new_with_dispatcher<D>(config: KmsConfig, dispatcher: D) -> Result<AwsKms>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        let credentials_provider = rusoto_util::CredentialsProvider::new()?;
        Self::new_creds_dispatcher(config, dispatcher, credentials_provider)
    }

    pub fn new(config: KmsConfig) -> Result<AwsKms> {
        // We must create our own dispatcher
        // See https://github.com/tikv/tikv/issues/7236
        let dispatcher = HttpClient::new()?;
        Self::new_with_dispatcher(config, dispatcher)
    }
}

impl std::convert::From<rusoto_core::request::TlsError> for Error {
    fn from(err: rusoto_core::request::TlsError) -> Error {
        box_err!(format!("{}", err))
    }
}

impl KmsProvider for AwsKms {
    fn name(&self) -> &[u8] {
        AWS_KMS_VENDOR_NAME
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    fn decrypt_data_key(&self, runtime: &mut Runtime, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let decrypt_request = DecryptRequest {
            ciphertext_blob: bytes::Bytes::copy_from_slice(&*data_key),
            // Use default algorithm SYMMETRIC_DEFAULT.
            encryption_algorithm: None,
            // Use key_id encoded in ciphertext.
            key_id: Some(self.current_key_id.0.clone()),
            // Encryption context and grant tokens are not used.
            encryption_context: None,
            grant_tokens: None,
        };
        let client = &self.client;
        let decrypt_response = retry(runtime, || {
            client.decrypt(decrypt_request.clone()).map_err(|e| {
                if let RusotoError::Service(DecryptError::IncorrectKey(e)) = e {
                    Error::WrongMasterKey(e.into())
                } else {
                    // To keep it simple, retry all errors, even though only
                    // some of them are retriable.
                    Error::Other(e.into())
                }
            })
        })?;
        Ok(decrypt_response.plaintext.unwrap().as_ref().to_vec())
    }

    fn generate_data_key(&mut self, runtime: &mut Runtime) -> Result<DataKeyPair> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.0.clone(),
            key_spec: Some(AWS_KMS_DATA_KEY_SPEC.to_owned()),
            number_of_bytes: None,
        };
        let client = &self.client;
        let generate_response = retry(runtime, || {
            client
                .generate_data_key(generate_request.clone())
                .map_err(|e| Error::Other(e.into()))
        })
        .unwrap();
        let ciphertext_key = generate_response.ciphertext_blob.unwrap().as_ref().to_vec();
        let plaintext_key = generate_response.plaintext.unwrap().as_ref().to_vec();
        Ok(DataKeyPair {
            encrypted: EncryptedKey::new(ciphertext_key)?,
            plaintext: PlainKey::new(plaintext_key)?,
        })
    }
}

fn retry<T, U, F>(runtime: &mut Runtime, mut func: F) -> Result<T>
where
    F: FnMut() -> U,
    U: Future<Output = Result<T>> + std::marker::Unpin,
{
    let retry_limit = 6;
    let timeout_duration = Duration::from_secs(10);
    let mut last_err = None;
    for _ in 0..retry_limit {
        let fut = func();

        match runtime.block_on(async move {
            let timeout = tokio::time::delay_for(timeout_duration);
            future::select(fut, timeout).await
        }) {
            future::Either::Left((Ok(resp), _)) => return Ok(resp),
            future::Either::Left((Err(e), _)) => {
                error!("kms request failed"; "error"=>?e);
                if let Error::WrongMasterKey(e) = e {
                    return Err(Error::WrongMasterKey(e));
                }
                last_err = Some(e);
            }
            future::Either::Right((_, _)) => {
                error!("kms request timeout"; "timeout" => ?timeout_duration);
            }
        }
    }
    Err(Error::Other(box_err!("{:?}", last_err)))
}

#[derive(Debug)]
struct State {
    encryption_backend: MemAesGcmBackend,
    cached_ciphertext_key: EncryptedKey,
}

impl State {
    fn new_from_datakey(datakey: DataKeyPair) -> Result<State> {
        Ok(State {
            cached_ciphertext_key: datakey.encrypted,
            encryption_backend: MemAesGcmBackend {
                key: datakey.plaintext,
            },
        })
    }

    fn cached(&self, ciphertext_key: &EncryptedKey) -> bool {
        *ciphertext_key == self.cached_ciphertext_key
    }
}

// EncryptedKey is a newtype used to mark data as an encrypted key
// It requires the vec to be non-empty
#[derive(PartialEq, Clone, Debug)]
pub struct EncryptedKey(Vec<u8>);

impl EncryptedKey {
    fn new(key: Vec<u8>) -> Result<EncryptedKey> {
        if key.is_empty() {
            error!("Encrypted content is empty");
        }
        Ok(EncryptedKey(key))
    }
}

impl std::ops::Deref for EncryptedKey {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct DataKeyPair {
    pub encrypted: EncryptedKey,
    pub plaintext: PlainKey,
}

pub trait KmsProvider: Sync + Send + 'static + std::fmt::Debug {
    fn generate_data_key(&self, runtime: &mut Runtime) -> Result<DataKeyPair>;
    fn decrypt_data_key(&self, runtime: &mut Runtime, data_key: &EncryptedKey) -> Result<Vec<u8>>;
    fn name(&self) -> &[u8];
}

#[derive(Debug)]
pub struct KmsBackend {
    state: Mutex<Option<State>>,
    kms_provider: Box<dyn KmsProvider>,
    // This mutex allows the decrypt_content API to be reference based
    runtime: Mutex<Runtime>,
}

impl KmsBackend {
    pub fn new(kms_provider: Box<dyn KmsProvider>) -> Result<KmsBackend> {
        // Basic scheduler executes futures in the current thread.
        let runtime = Mutex::new(
            Builder::new()
                .basic_scheduler()
                .thread_name("kms-runtime")
                .core_threads(1)
                .enable_all()
                .build()?,
        );

        Ok(KmsBackend {
            state: Mutex::new(None),
            runtime,
            kms_provider,
        })
    }

    fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut opt_state = self.state.lock().unwrap();
        if opt_state.is_none() {
            let mut runtime = self.runtime.lock().unwrap();
            let data_key = self.kms_provider.
          (&mut runtime)?;
            *opt_state = Some(State::new_from_datakey(data_key)?);
        }
        let state = opt_state.as_ref().unwrap();

        let mut content = state.encryption_backend.encrypt_content(plaintext, iv)?;

        // Set extra metadata for KmsBackend.
        // For now, we only support AWS.
        content.metadata.insert(
            MetadataKey::KmsVendor.as_str().to_owned(),
            self.kms_provider.name().to_vec(),
        );
        content.metadata.insert(
            MetadataKey::KmsCiphertextKey.as_str().to_owned(),
            state.cached_ciphertext_key.to_vec(),
        );

        Ok(content)
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let vendor_name = self.kms_provider.name();
        match content.metadata.get(MetadataKey::KmsVendor.as_str()) {
            Some(val) if val.as_slice() == vendor_name => (),
            None => {
                return Err(
                    // If vender is missing in metadata, it could be the encrypted content is invalid
                    // or corrupted, but it is also possible that the content is encrypted using the
                    // FileBackend. Return WrongMasterKey anyway.
                    Error::WrongMasterKey(box_err!("missing KMS vendor")),
                );
            }
            other => {
                return Err(box_err!(
                    "KMS vendor mismatch expect {:?} got {:?}",
                    vendor_name,
                    other
                ))
            }
        }

        let ciphertext_key = match content.metadata.get(MetadataKey::KmsCiphertextKey.as_str()) {
            None => return Err(box_err!("KMS ciphertext key not found")),
            Some(key) => EncryptedKey::new(key.to_vec())?,
        };

        {
            let mut opt_state = self.state.lock().unwrap();
            if let Some(state) = &*opt_state {
                if state.cached(&ciphertext_key) {
                    return state.encryption_backend.decrypt_content(content);
                }
            }
            {
                let mut runtime = self.runtime.lock().unwrap();
                let plaintext = self
                    .kms_provider
                    .decrypt_data_key(&mut runtime, &ciphertext_key)?;
                let data_key = DataKeyPair {
                    encrypted: ciphertext_key,
                    plaintext: PlainKey::new(plaintext)?,
                };
                let state = State::new_from_datakey(data_key)?;
                let content = state.encryption_backend.decrypt_content(content)?;
                *opt_state = Some(state);
                Ok(content)
            }
        }
    }
}

impl Backend for KmsBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.encrypt_content(plaintext, Iv::new_gcm())
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.decrypt_content(content)
    }

    fn is_secure(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hex::FromHex;
    use matches::assert_matches;
    use rusoto_credential::StaticProvider;
    use rusoto_kms::{DecryptResponse, GenerateDataKeyResponse};
    use rusoto_mock::MockRequestDispatcher;

    const FAKE_VENDOR_NAME: &[u8] = b"FAKE";
    const FAKE_DATA_KEY_ENCRYPTED: &[u8] = b"encrypted                       ";

    #[derive(Debug)]
    struct FakeKms {
        plaintext_key: PlainKey,
    }

    impl FakeKms {
        pub fn new(plaintext_key: PlainKey) -> Self {
            Self { plaintext_key }
        }
    }

    impl KmsProvider for FakeKms {
        fn generate_data_key(&self, _runtime: &mut Runtime) -> Result<DataKeyPair> {
            Ok(DataKeyPair {
                encrypted: EncryptedKey::new(FAKE_DATA_KEY_ENCRYPTED.to_vec())?,
                plaintext: PlainKey::new(self.plaintext_key.clone()).unwrap(),
            })
        }

        fn decrypt_data_key(
            &self,
            _runtime: &mut Runtime,
            _ciphertext: &EncryptedKey,
        ) -> Result<Vec<u8>> {
            Ok(vec![1u8, 32])
        }

        fn name(&self) -> &[u8] {
            FAKE_VENDOR_NAME
        }
    }

    fn runtime() -> Runtime {
        Builder::new()
            .basic_scheduler()
            .thread_name("kms-runtime")
            .core_threads(1)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn test_state() {
        let plaintext = PlainKey::new(vec![1u8; 32]).unwrap();
        let encrypted = EncryptedKey::new(vec![2u8; 32]).unwrap();
        let data_key = DataKeyPair {
            plaintext: PlainKey::new(plaintext.clone()).unwrap(),
            encrypted: encrypted.clone(),
        };
        let encrypted2 = EncryptedKey::new(vec![3u8; 32]).unwrap();
        let state = State::new_from_datakey(data_key).unwrap();
        // cached to the data key
        assert_eq!(state.cached(&encrypted), true);
        let state2 = State::new_from_datakey(DataKeyPair {
            plaintext,
            encrypted: encrypted2.clone(),
        })
        .unwrap();
        // updated to the new data key
        assert_eq!(state2.cached(&encrypted2), true);
    }

    #[test]
    fn test_aws_kms() {
        let magic_contents = b"5678" as &[u8];
        let key_contents = vec![1u8; 32];
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            endpoint: String::new(),
        };
        let mut runtime = runtime();

        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(magic_contents.as_ref().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(key_contents.clone().into()),
            });
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let aws_kms =
            AwsKms::new_creds_dispatcher(config.clone(), dispatcher, credentials_provider.clone())
                .unwrap();
        let data_key = aws_kms.generate_data_key(&mut runtime).unwrap();
        assert_eq!(
            data_key.encrypted,
            EncryptedKey::new(magic_contents.to_vec()).unwrap()
        );
        assert_eq!(*data_key.plaintext, key_contents);

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(key_contents.clone().into()),
            key_id: Some("test_key_id".to_string()),
            encryption_algorithm: None,
        });
        let aws_kms =
            AwsKms::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        let plaintext = aws_kms
            .decrypt_data_key(&mut runtime, &data_key.encrypted)
            .unwrap();
        assert_eq!(plaintext, key_contents);
    }

    #[test]
    fn test_kms_backend() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let key = PlainKey::new(
            Vec::from_hex("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139")
                .unwrap(),
        )
        .unwrap();
        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let backend = KmsBackend::new(Box::new(FakeKms::new(key))).unwrap();
        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        let mut vendor_not_found = encrypted_content.clone();
        vendor_not_found
            .metadata
            .remove(MetadataKey::KmsVendor.as_str());
        assert_matches!(
            backend.decrypt_content(&vendor_not_found).unwrap_err(),
            Error::WrongMasterKey(_)
        );

        let mut invalid_vendor = encrypted_content.clone();
        let mut invalid_suffix = b"_invalid".to_vec();
        invalid_vendor
            .metadata
            .get_mut(MetadataKey::KmsVendor.as_str())
            .unwrap()
            .append(&mut invalid_suffix);
        assert_matches!(
            backend.decrypt_content(&invalid_vendor).unwrap_err(),
            Error::Other(_)
        );

        let mut ciphertext_key_not_found = encrypted_content;
        ciphertext_key_not_found
            .metadata
            .remove(MetadataKey::KmsCiphertextKey.as_str());
        assert_matches!(
            backend
                .decrypt_content(&ciphertext_key_not_found)
                .unwrap_err(),
            Error::Other(_)
        );
    }

    #[test]
    fn test_kms_wrong_key_id() {
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            endpoint: String::new(),
        };

        // IncorrectKeyException
        //
        // HTTP Status Code: 400
        // Json, see:
        // https://github.com/rusoto/rusoto/blob/mock-v0.43.0/rusoto/services/kms/src/generated.rs#L1970
        // https://github.com/rusoto/rusoto/blob/mock-v0.43.0/rusoto/core/src/proto/json/error.rs#L7
        // https://docs.aws.amazon.com/kms/latest/APIReference/API_Decrypt.html#API_Decrypt_Errors
        let dispatcher = MockRequestDispatcher::with_status(400).with_body(
            r#"{
                "__type": "IncorrectKeyException",
                "Message": "mock"
            }"#,
        );
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let aws_kms =
            AwsKms::new_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        match aws_kms.decrypt_data_key(
            &mut runtime(),
            &EncryptedKey::new(b"invalid".to_vec()).unwrap(),
        ) {
            Err(Error::WrongMasterKey(_)) => (),
            other => panic!("{:?}", other),
        }
    }
}
