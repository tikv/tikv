use std::fs::create_dir_all;
use std::path::Path;
use std::time::Duration;

use kvproto::encryptionpb::{EncryptedContent, EncryptionMethod};
use rusoto_core::region;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_credential::{DefaultCredentialsProvider, StaticProvider};
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};

use super::{Backend, FileBackend, PlainTextBackend};
use crate::config::KmsConfig;
use crate::encrypted_file::EncryptedFile;
use crate::{Error, Result};

const KMS_ENCRYPTION_KEY_NAME: &str = "kms_encryption.key";

// Always use AES 256 for encrypting master key.
const KMS_DATA_KEY_METHOD: EncryptionMethod = EncryptionMethod::Aes256Ctr;
const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";

struct AwsKms {
    client: KmsClient,
    current_key_id: String,
}

impl AwsKms {
    fn new(config: KmsConfig) -> Result<AwsKms> {
        let http_dispatcher = HttpClient::new().unwrap();

        AwsKms::with_request_dispatcher(config, http_dispatcher)
    }

    // TODO following code is almost the same as external_storage s3 client,
    //      should be wrapped together.
    fn with_request_dispatcher<D>(config: KmsConfig, dispatcher: D) -> Result<AwsKms>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
        D::Future: Send,
    {
        if config.key_id.is_empty() {
            return Err(Error::Other(
                "KMS key id can not be empty".to_owned().into(),
            ));
        }

        let region = if config.endpoint.is_empty() {
            config.region.parse::<region::Region>().map_err(|e| {
                Error::Other(format!("invalid region format {}: {}", config.region, e).into())
            })?
        } else {
            region::Region::Custom {
                name: config.region,
                endpoint: config.endpoint,
            }
        };

        let client = if config.access_key.is_empty() || config.secret_access_key.is_empty() {
            let cred_provider = DefaultCredentialsProvider::new()
                .map_err(|e| Error::Other(format!("unable to get credentials: {}", e).into()))?;
            KmsClient::new_with(dispatcher, cred_provider, region)
        } else {
            let cred_provider = StaticProvider::new(
                config.access_key,
                config.secret_access_key,
                None, /* token */
                None, /* valid_for */
            );
            KmsClient::new_with(dispatcher, cred_provider, region)
        };

        Ok(AwsKms {
            client,
            current_key_id: config.key_id,
        })
    }

    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let decrypt_request = DecryptRequest {
            ciphertext_blob: ciphertext.into(),
            encryption_context: None,
            grant_tokens: None,
        };
        let decrypt_response = retry(|timeout| {
            let mut req = self.client.decrypt(decrypt_request.clone());
            req.set_timeout(timeout);
            req.sync().map_err(|e| Error::Other(e.into()))
        });
        let plaintext = decrypt_response.plaintext.unwrap().as_ref().to_vec();
        Ok(plaintext)
    }

    fn generate_data_key(&self) -> Result<(Vec<u8>, Vec<u8>)> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.clone(),
            key_spec: Some(AWS_KMS_DATA_KEY_SPEC.to_owned()),
            number_of_bytes: None,
        };
        let generate_response = retry(|timeout| {
            let mut req = self.client.generate_data_key(generate_request.clone());
            req.set_timeout(timeout);
            req.sync().map_err(|e| Error::Other(e.into()))
        });
        let ciphertext_key = generate_response.ciphertext_blob.unwrap().as_ref().to_vec();
        let plaintext_key = generate_response.plaintext.unwrap().as_ref().to_vec();
        Ok((ciphertext_key, plaintext_key))
    }
}

fn retry<T, F>(mut func: F) -> T
where
    F: FnMut(Duration) -> Result<T>,
{
    let retry_limit = 6;
    let timeout = Duration::from_secs(10);
    for _ in 0..retry_limit {
        match func(timeout) {
            Ok(t) => return t,
            Err(e) => {
                error!("kms request failed"; "error"=>?e);
            }
        }
    }
    panic!("kms request failed in {} times", retry_limit)
}

pub struct KmsBackend {
    file_backend: FileBackend,
}

impl KmsBackend {
    pub fn new(config: KmsConfig, base: &str) -> Result<KmsBackend> {
        let kms = AwsKms::new(config)?;
        KmsBackend::with_kms(kms, base)
    }

    fn with_kms(kms: AwsKms, base: &str) -> Result<KmsBackend> {
        // Create base dir if it is missing.
        create_dir_all(base)?;

        // Read the master key or generate a new master key.
        let key_path = Path::new(base).join(KMS_ENCRYPTION_KEY_NAME);
        let key = if !key_path.exists() {
            let (ciphertext_key, plaintext_key) = kms.generate_data_key()?;
            let f = EncryptedFile::new(Path::new(base), KMS_ENCRYPTION_KEY_NAME);
            // ciphertext_key has already be ecrypted by KMS.
            f.write(&ciphertext_key, &PlainTextBackend::default())?;
            plaintext_key
        } else {
            let f = EncryptedFile::new(Path::new(base), KMS_ENCRYPTION_KEY_NAME);
            let ciphertext_key = f.read(&PlainTextBackend::default())?;
            kms.decrypt(&ciphertext_key)?
        };

        // Always use AES 256 for encrypting master key.
        let method = KMS_DATA_KEY_METHOD;
        let file_backend = FileBackend::new(method, key)?;

        Ok(KmsBackend { file_backend })
    }
}

impl Backend for KmsBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.file_backend.encrypt(plaintext)
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.file_backend.decrypt(content)
    }

    fn is_secure(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusoto_kms::{DecryptResponse, GenerateDataKeyResponse};
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_aws_kms() {
        let magic_contents = b"5678";
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            endpoint: String::new(),
        };

        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(magic_contents.as_ref().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(magic_contents.as_ref().into()),
            });
        let aws_kms = AwsKms::with_request_dispatcher(config.clone(), dispatcher).unwrap();
        let (ciphertext, plaintext) = aws_kms.generate_data_key().unwrap();
        assert_eq!(ciphertext, magic_contents);
        assert_eq!(plaintext, magic_contents);

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(magic_contents.as_ref().into()),
            key_id: Some("test_key_id".to_string()),
        });
        let aws_kms = AwsKms::with_request_dispatcher(config, dispatcher).unwrap();
        let plaintext = aws_kms.decrypt(ciphertext.as_slice()).unwrap();
        assert_eq!(plaintext, magic_contents);
    }

    #[test]
    fn test_kms_backend() {
        let tmp = tempfile::TempDir::new().unwrap();
        let base_dir = tmp.path().as_os_str().to_str().unwrap();

        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            endpoint: String::new(),
        };

        let plaintext_key = vec![5u8; 32]; // 32 * 8 = 256 bits
        let magic_contents = b"5678";

        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(plaintext_key.as_slice().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.as_slice().into()),
            });
        let aws_kms = AwsKms::with_request_dispatcher(config.clone(), dispatcher).unwrap();
        let backend = KmsBackend::with_kms(aws_kms, base_dir).unwrap();
        let ciphertext = backend.encrypt(magic_contents).unwrap();
        assert_ne!(ciphertext.get_content(), magic_contents);

        let key_meta = tmp.path().join(KMS_ENCRYPTION_KEY_NAME).metadata().unwrap();

        // Reopen kms backup.
        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(plaintext_key.as_slice().into()),
            key_id: Some("test_key_id".to_string()),
        });
        let aws_kms = AwsKms::with_request_dispatcher(config, dispatcher).unwrap();
        let backend = KmsBackend::with_kms(aws_kms, base_dir).unwrap();
        let plaintext = backend.decrypt(&ciphertext).unwrap();
        assert_eq!(plaintext, magic_contents);

        // Make sure key file does not change on reopen.
        let key_meta1 = tmp.path().join(KMS_ENCRYPTION_KEY_NAME).metadata().unwrap();
        if let Ok(created) = key_meta.created() {
            assert_eq!(created, key_meta1.created().unwrap());
        }
        if let Ok(modified) = key_meta.modified() {
            assert_eq!(modified, key_meta1.modified().unwrap());
        }
    }
}
