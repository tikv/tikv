use std::future::Future;
use std::ops::Deref;
use std::time::Duration;

use futures::future::{self};
use futures_util::TryFutureExt;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_core::RusotoError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_kms::DecryptError;
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use tikv_util::box_err;
use tokio::runtime::Runtime;

use crate::util;
use cloud::error::{Error, Result};
use cloud::kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey};

const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";
const AWS_KMS_VENDOR_NAME: &[u8] = b"AWS";

pub struct AwsKms {
    client: KmsClient,
    current_key_id: KeyId,
    region: String,
    endpoint: String,
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
        config: Config,
        dispatcher: Dispatcher,
        credentials_provider: Creds,
    ) -> Result<AwsKms>
    where
        Creds: ProvideAwsCredentials + Send + Sync + 'static,
        Dispatcher: DispatchSignedRequest + Send + Sync + 'static,
    {
        let region = util::get_region(
            config.location.region.as_ref(),
            config.location.endpoint.as_ref(),
        )?;
        let client = KmsClient::new_with(dispatcher, credentials_provider, region);
        Ok(AwsKms {
            client,
            current_key_id: config.key_id,
            region: config.location.region,
            endpoint: config.location.endpoint,
        })
    }

    fn new_with_dispatcher<D>(config: Config, dispatcher: D) -> Result<AwsKms>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        let credentials_provider = util::CredentialsProvider::new()?;
        Self::new_creds_dispatcher(config, dispatcher, credentials_provider)
    }

    pub fn new(config: Config) -> Result<AwsKms> {
        // We must create our own dispatcher
        // See https://github.com/tikv/tikv/issues/7236
        let dispatcher = HttpClient::new()?;
        Self::new_with_dispatcher(config, dispatcher)
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
            key_id: Some(self.current_key_id.deref().clone()),
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

    fn generate_data_key(&self, runtime: &mut Runtime) -> Result<DataKeyPair> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.deref().clone(),
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

pub fn retry<T, U, F>(runtime: &mut Runtime, mut func: F) -> Result<T>
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_credential::StaticProvider;
    use rusoto_kms::{DecryptResponse, GenerateDataKeyResponse};
    // use rusoto_mock::MockRequestDispatcher;
    use cloud::kms::Location;
    use rusoto_mock::MockRequestDispatcher;
    use tokio::runtime::{Builder, Runtime};

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
    fn test_aws_kms() {
        let magic_contents = b"5678" as &[u8];
        let key_contents = vec![1u8; 32];
        let config = Config {
            key_id: KeyId::new("test_key_id".to_string()).unwrap(),
            vendor: String::new(),
            location: Location {
                region: "ap-southeast-2".to_string(),
                endpoint: String::new(),
            },
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
    fn test_kms_wrong_key_id() {
        let config = Config {
            key_id: KeyId::new("test_key_id".to_string()).unwrap(),
            vendor: String::new(),
            location: Location {
                region: "ap-southeast-2".to_string(),
                endpoint: String::new(),
            },
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
