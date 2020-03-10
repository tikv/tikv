use std::time::Duration;

use kvproto::encryptionpb::EncryptedContent;
use rusoto_core::region;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_credential::{DefaultCredentialsProvider, StaticProvider};
use rusoto_kms::{DecryptRequest, EncryptRequest, Kms, KmsClient};

use super::{metadata::*, Backend};
use crate::config::KmsConfig;
use crate::{Error, Result};

pub struct KmsBackend {
    client: KmsClient,
    current_key_id: String,
}

impl KmsBackend {
    pub fn new(config: KmsConfig) -> Result<KmsBackend> {
        let http_dispatcher = HttpClient::new().unwrap();

        KmsBackend::with_request_dispatcher(config, http_dispatcher)
    }

    // TODO following code is almost the same as external_storage s3 client,
    //      should be wrapped together.
    fn with_request_dispatcher<D>(config: KmsConfig, dispatcher: D) -> Result<KmsBackend>
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

        Ok(KmsBackend {
            client,
            current_key_id: config.key_id,
        })
    }

    fn encrypt_content(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        let checksum = sha256(plaintext)?;
        content
            .mut_metadata()
            .insert(MetadataKey::PlaintextSha256.as_str().to_owned(), checksum);

        let encrypt_request = EncryptRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.clone(),
            plaintext: plaintext.into(),
        };
        let encrypt_response = retry(|timeout| {
            let mut req = self.client.encrypt(encrypt_request.clone());
            req.set_timeout(timeout);
            req.sync().map_err(|e| Error::Other(e.into()))
        });
        let ciphertext = encrypt_response.ciphertext_blob.unwrap().as_ref().to_vec();
        content.set_content(ciphertext);
        Ok(content)
    }

    fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let checksum = content
            .get_metadata()
            .get(MetadataKey::PlaintextSha256.as_str())
            .ok_or_else(|| Error::Other("sha256 checksum not found".to_owned().into()))?;

        let decrypt_request = DecryptRequest {
            ciphertext_blob: content.get_content().into(),
            encryption_context: None,
            grant_tokens: None,
        };
        let decrypt_response = retry(|timeout| {
            let mut req = self.client.decrypt(decrypt_request.clone());
            req.set_timeout(timeout);
            req.sync().map_err(|e| Error::Other(e.into()))
        });
        let plaintext = decrypt_response.plaintext.unwrap().as_ref().to_vec();

        if *checksum != sha256(&plaintext)? {
            return Err(Error::Other("sha256 checksum mismatch".to_owned().into()));
        }
        Ok(plaintext)
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

impl Backend for KmsBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.encrypt_content(plaintext)
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
    use rusoto_kms::{DecryptResponse, EncryptResponse};
    use rusoto_mock::MockRequestDispatcher;

    #[test]
    fn test_kms_backend() {
        let magic_contents = b"5678";
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            endpoint: String::new(),
        };

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(EncryptResponse {
            ciphertext_blob: Some(magic_contents.as_ref().into()),
            key_id: Some("test_key_id".to_string()),
        });
        let backend = KmsBackend::with_request_dispatcher(config.clone(), dispatcher).unwrap();
        let encrypted_content = backend.encrypt(magic_contents).unwrap();
        assert_eq!(encrypted_content.get_content(), magic_contents);

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(magic_contents.as_ref().into()),
            key_id: Some("test_key_id".to_string()),
        });
        let backend = KmsBackend::with_request_dispatcher(config, dispatcher).unwrap();
        let plaintext = backend.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, magic_contents);

        // Must checksum mismatch
        let mut encrypted_content1 = encrypted_content.clone();
        encrypted_content1
            .mut_metadata()
            .get_mut(METADATA_PLAINTEXT_SHA256)
            .unwrap()[0] += 1;
        backend.decrypt_content(&encrypted_content1).unwrap_err();

        // Must checksum not found
        let mut encrypted_content2 = encrypted_content;
        encrypted_content2
            .mut_metadata()
            .remove(METADATA_PLAINTEXT_SHA256);
        backend.decrypt_content(&encrypted_content2).unwrap_err();
    }
}
