use async_trait::async_trait;
use std::ops::Deref;

use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::RusotoError;
use rusoto_credential::ProvideAwsCredentials;
use rusoto_kms::{DecryptError, GenerateDataKeyError};
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use tikv_util::stream::RetryError;

use crate::util;
use cloud::error::{Error, KmsError, Result};
use cloud::kms::{Config, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey};

const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";
const AWS_KMS_VENDOR_NAME: &[u8] = b"AWS";
pub const AWS_VENDOR_NAME: &str = "aws";

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
    fn new_with_creds_dispatcher<Creds, Dispatcher>(
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
        Self::new_with_creds_dispatcher(config, dispatcher, credentials_provider)
    }

    pub fn new(config: Config) -> Result<AwsKms> {
        let dispatcher = util::new_http_client()?;
        Self::new_with_dispatcher(config, dispatcher)
    }
}

#[async_trait]
impl KmsProvider for AwsKms {
    fn name(&self) -> &[u8] {
        AWS_KMS_VENDOR_NAME
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
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
        self.client
            .decrypt(decrypt_request.clone())
            .await
            .map_err(classify_decrypt_error)
            .map(|response| response.plaintext.unwrap().as_ref().to_vec())
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.deref().clone(),
            key_spec: Some(AWS_KMS_DATA_KEY_SPEC.to_owned()),
            number_of_bytes: None,
        };
        self.client
            .generate_data_key(generate_request)
            .await
            .map_err(classify_generate_data_key_error)
            .and_then(|response| {
                let ciphertext_key = response.ciphertext_blob.unwrap().as_ref().to_vec();
                let plaintext_key = response.plaintext.unwrap().as_ref().to_vec();
                Ok(DataKeyPair {
                    encrypted: EncryptedKey::new(ciphertext_key)?,
                    plaintext: PlainKey::new(plaintext_key)?,
                })
            })
    }
}

fn classify_generate_data_key_error(err: RusotoError<GenerateDataKeyError>) -> Error {
    if let RusotoError::Service(e) = &err {
        match &e {
            GenerateDataKeyError::NotFound(_) => Error::ApiNotFound(err.into()),
            GenerateDataKeyError::InvalidKeyUsage(_) => {
                Error::KmsError(KmsError::Other(err.into()))
            }
            GenerateDataKeyError::DependencyTimeout(_) => Error::ApiTimeout(err.into()),
            GenerateDataKeyError::KMSInternal(_) => Error::ApiInternal(err.into()),
            _ => Error::KmsError(KmsError::Other(err.into())),
        }
    } else {
        classify_error(err)
    }
}

fn classify_decrypt_error(err: RusotoError<DecryptError>) -> Error {
    if let RusotoError::Service(e) = &err {
        match &e {
            DecryptError::IncorrectKey(_) | DecryptError::NotFound(_) => {
                Error::KmsError(KmsError::WrongMasterKey(err.into()))
            }
            DecryptError::DependencyTimeout(_) => Error::ApiTimeout(err.into()),
            DecryptError::KMSInternal(_) => Error::ApiInternal(err.into()),
            _ => Error::KmsError(KmsError::Other(err.into())),
        }
    } else {
        classify_error(err)
    }
}

fn classify_error<E: std::error::Error + Send + Sync + 'static>(err: RusotoError<E>) -> Error {
    match &err {
        RusotoError::HttpDispatch(_) => Error::ApiTimeout(err.into()),
        RusotoError::Credentials(_) => Error::ApiAuthentication(err.into()),
        e if e.is_retryable() => Error::ApiInternal(err.into()),
        _ => Error::Other(err.into()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_credential::StaticProvider;
    use rusoto_kms::{DecryptResponse, GenerateDataKeyResponse};
    // use rusoto_mock::MockRequestDispatcher;
    use cloud::kms::Location;
    use rusoto_mock::MockRequestDispatcher;

    #[tokio::test]
    async fn test_aws_kms() {
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

        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(magic_contents.as_ref().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(key_contents.clone().into()),
            });
        let credentials_provider =
            StaticProvider::new_minimal("abc".to_string(), "xyz".to_string());
        let aws_kms = AwsKms::new_with_creds_dispatcher(
            config.clone(),
            dispatcher,
            credentials_provider.clone(),
        )
        .unwrap();
        let data_key = aws_kms.generate_data_key().await.unwrap();
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
            AwsKms::new_with_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        let plaintext = aws_kms.decrypt_data_key(&data_key.encrypted).await.unwrap();
        assert_eq!(plaintext, key_contents);
    }

    #[tokio::test]
    async fn test_kms_wrong_key_id() {
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
            AwsKms::new_with_creds_dispatcher(config, dispatcher, credentials_provider).unwrap();
        let enc_key = EncryptedKey::new(b"invalid".to_vec()).unwrap();
        let fut = aws_kms.decrypt_data_key(&enc_key);
        match fut.await {
            Err(Error::KmsError(KmsError::WrongMasterKey(_))) => (),
            other => panic!("{:?}", other),
        }
    }
}
