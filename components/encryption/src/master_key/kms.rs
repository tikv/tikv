// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::future::Future;
use std::marker::PhantomData;
use std::sync::Mutex;
use std::time::Duration;

use futures::future::{self, TryFutureExt};
use kvproto::encryptionpb::EncryptedContent;
use rusoto_core::request::DispatchSignedRequest;
use rusoto_core::request::HttpClient;
use rusoto_kms::{DecryptRequest, GenerateDataKeyRequest, Kms, KmsClient};
use tokio::runtime::{Builder, Runtime};

use super::{metadata::MetadataKey, Backend, MemAesGcmBackend};
use crate::config::KmsConfig;
use crate::crypter::Iv;
use crate::{Error, Result};
use rusoto_util::new_client;

const AWS_KMS_DATA_KEY_SPEC: &str = "AES_256";
const AWS_KMS_VENDOR_NAME: &[u8] = b"AWS";

struct AwsKms {
    client: KmsClient,
    current_key_id: String,
    runtime: Runtime,
    // The current implementation (rosoto 0.43.0 + hyper 0.13.3) is not `Send`
    // in practical. See more https://github.com/tikv/tikv/issues/7236.
    // FIXME: remove it.
    _not_send: PhantomData<*const ()>,
}

impl AwsKms {
    fn with_request_dispatcher<D>(config: &KmsConfig, dispatcher: D) -> Result<AwsKms>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        Self::check_config(config)?;

        // Create and run the client in the same thread.
        let client = new_client!(KmsClient, config, dispatcher);
        // Basic scheduler executes futures in the current thread.
        let runtime = Builder::new()
            .basic_scheduler()
            .thread_name("kms-runtime")
            .core_threads(1)
            .enable_all()
            .build()?;

        Ok(AwsKms {
            client,
            current_key_id: config.key_id.clone(),
            runtime,
            _not_send: PhantomData::default(),
        })
    }

    fn check_config(config: &KmsConfig) -> Result<()> {
        if config.key_id.is_empty() {
            return Err(box_err!("KMS key id can not be empty"));
        }
        Ok(())
    }

    fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        let decrypt_request = DecryptRequest {
            ciphertext_blob: ciphertext.to_vec().into(),
            // Use default algorithm SYMMETRIC_DEFAULT.
            encryption_algorithm: None,
            // Use key_id encoded in ciphertext.
            key_id: None,
            // Encryption context and grant tokens are not used.
            encryption_context: None,
            grant_tokens: None,
        };
        let runtime = &mut self.runtime;
        let client = &self.client;
        let decrypt_response = retry(runtime, || {
            client
                .decrypt(decrypt_request.clone())
                .map_err(|e| Error::Other(e.into()))
        });
        let plaintext = decrypt_response.plaintext.unwrap().as_ref().to_vec();
        Ok(plaintext)
    }

    fn generate_data_key(&mut self) -> Result<(Vec<u8>, Vec<u8>)> {
        let generate_request = GenerateDataKeyRequest {
            encryption_context: None,
            grant_tokens: None,
            key_id: self.current_key_id.clone(),
            key_spec: Some(AWS_KMS_DATA_KEY_SPEC.to_owned()),
            number_of_bytes: None,
        };
        let runtime = &mut self.runtime;
        let client = &self.client;
        let generate_response = retry(runtime, || {
            client
                .generate_data_key(generate_request.clone())
                .map_err(|e| Error::Other(e.into()))
        });
        let ciphertext_key = generate_response.ciphertext_blob.unwrap().as_ref().to_vec();
        let plaintext_key = generate_response.plaintext.unwrap().as_ref().to_vec();
        Ok((ciphertext_key, plaintext_key))
    }
}

fn retry<T, U, F>(runtime: &mut Runtime, mut func: F) -> T
where
    F: FnMut() -> U,
    U: Future<Output = Result<T>> + std::marker::Unpin,
{
    let retry_limit = 6;
    let timeout_duration = Duration::from_secs(10);
    for _ in 0..retry_limit {
        let fut = func();

        match runtime.block_on(async move {
            let timeout = tokio::time::delay_for(timeout_duration);
            future::select(fut, timeout).await
        }) {
            future::Either::Left((Ok(resp), _)) => return resp,
            future::Either::Left((Err(e), _)) => {
                error!("kms request failed"; "error"=>?e);
            }
            future::Either::Right((_, _)) => {
                error!("kms request timeout"; "timeout" => ?timeout_duration);
            }
        }
    }
    panic!("kms request failed in {} times", retry_limit)
}

struct Inner {
    config: KmsConfig,
    backend: Option<MemAesGcmBackend>,
    cached_ciphertext_key: Vec<u8>,
}

impl Inner {
    fn maybe_update_backend(&mut self, ciphertext_key: Option<&Vec<u8>>) -> Result<()> {
        let http_dispatcher = HttpClient::new().unwrap();
        self.maybe_update_backend_with(ciphertext_key, http_dispatcher)
    }

    fn maybe_update_backend_with<D>(
        &mut self,
        ciphertext_key: Option<&Vec<u8>>,
        dispatcher: D,
    ) -> Result<()>
    where
        D: DispatchSignedRequest + Send + Sync + 'static,
    {
        if self.backend.is_some()
            && ciphertext_key.map_or(true, |key| *key == self.cached_ciphertext_key)
        {
            return Ok(());
        }

        let mut kms = AwsKms::with_request_dispatcher(&self.config, dispatcher)?;
        let key = if let Some(ciphertext_key) = ciphertext_key {
            self.cached_ciphertext_key = ciphertext_key.to_owned();
            kms.decrypt(ciphertext_key)?
        } else {
            let (ciphertext_key, plaintext_key) = kms.generate_data_key()?;
            self.cached_ciphertext_key = ciphertext_key;
            plaintext_key
        };
        if self.cached_ciphertext_key == key {
            panic!(
                "ciphertext key should not be the same as master key, \
                otherwise it leaks master key!"
            );
        }

        // Always use AES 256 for encrypting master key.
        self.backend = Some(MemAesGcmBackend::new(key)?);
        Ok(())
    }
}

pub struct KmsBackend {
    inner: Mutex<Inner>,
}

impl KmsBackend {
    pub fn new(config: KmsConfig) -> Result<KmsBackend> {
        let inner = Inner {
            backend: None,
            config,
            cached_ciphertext_key: Vec::new(),
        };

        Ok(KmsBackend {
            inner: Mutex::new(inner),
        })
    }

    fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut inner = self.inner.lock().unwrap();
        inner.maybe_update_backend(None)?;
        let mut content = inner
            .backend
            .as_ref()
            .unwrap()
            .encrypt_content(plaintext, iv)?;

        // Set extra metadata for KmsBackend.
        // For now, we only support AWS.
        content.metadata.insert(
            MetadataKey::KmsVendor.as_str().to_owned(),
            AWS_KMS_VENDOR_NAME.to_vec(),
        );
        if inner.cached_ciphertext_key.is_empty() {
            return Err(box_err!("KMS ciphertext key not found"));
        }
        content.metadata.insert(
            MetadataKey::KmsCiphertextKey.as_str().to_owned(),
            inner.cached_ciphertext_key.clone(),
        );

        Ok(content)
    }

    fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        match content.metadata.get(MetadataKey::KmsVendor.as_str()) {
            // For now, we only support AWS.
            Some(val) if val.as_slice() == AWS_KMS_VENDOR_NAME => (),
            other => {
                return Err(box_err!(
                    "KMS vendor mismatch expect {:?} got {:?}",
                    AWS_KMS_VENDOR_NAME,
                    other
                ))
            }
        }

        let mut inner = self.inner.lock().unwrap();
        let ciphertext_key = content.metadata.get(MetadataKey::KmsCiphertextKey.as_str());
        if ciphertext_key.is_none() {
            return Err(box_err!("KMS ciphertext key not found"));
        }
        inner.maybe_update_backend(ciphertext_key)?;
        inner.backend.as_ref().unwrap().decrypt_content(content)
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
        let mut aws_kms = AwsKms::with_request_dispatcher(&config.clone(), dispatcher).unwrap();
        let (ciphertext, plaintext) = aws_kms.generate_data_key().unwrap();
        assert_eq!(ciphertext, magic_contents);
        assert_eq!(plaintext, magic_contents);

        let dispatcher = MockRequestDispatcher::with_status(200).with_json_body(DecryptResponse {
            plaintext: Some(magic_contents.as_ref().into()),
            key_id: Some("test_key_id".to_string()),
            encryption_algorithm: None,
        });
        let mut aws_kms = AwsKms::with_request_dispatcher(&config, dispatcher).unwrap();
        let plaintext = aws_kms.decrypt(ciphertext.as_slice()).unwrap();
        assert_eq!(plaintext, magic_contents);
    }

    #[test]
    fn test_update_backend() {
        let config = KmsConfig {
            key_id: "test_key_id".to_string(),
            region: "ap-southeast-2".to_string(),
            access_key: "abc".to_string(),
            secret_access_key: "xyz".to_string(),
            endpoint: String::new(),
        };

        let plaintext_key = vec![5u8; 32]; // 32 * 8 = 256 bits
        let ciphertext_key1 = vec![7u8; 32]; // 32 * 8 = 256 bits
        let ciphertext_key2 = vec![8u8; 32]; // 32 * 8 = 256 bits

        let mut inner = Inner {
            config,
            backend: None,
            cached_ciphertext_key: vec![],
        };

        // Update mem backend
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key1.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner.maybe_update_backend_with(None, dispatcher).unwrap();
        assert!(inner.backend.is_some());
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // Do not update mem backend if ciphertext_key is None.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(plaintext_key.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner.maybe_update_backend_with(None, dispatcher).unwrap();
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // Do not update mem backend if cached_ciphertext_key equals to ciphertext_key.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key2.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner
            .maybe_update_backend_with(Some(&ciphertext_key1.to_vec()), dispatcher)
            .unwrap();
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key1.to_vec());

        // Update mem backend if cached_ciphertext_key does not equal to ciphertext_key.
        let dispatcher =
            MockRequestDispatcher::with_status(200).with_json_body(GenerateDataKeyResponse {
                ciphertext_blob: Some(ciphertext_key2.to_vec().into()),
                key_id: Some("test_key_id".to_string()),
                plaintext: Some(plaintext_key.to_vec().into()),
            });
        inner
            .maybe_update_backend_with(Some(&ciphertext_key2.to_vec()), dispatcher)
            .unwrap();
        assert!(inner.backend.is_some());
        assert_eq!(inner.cached_ciphertext_key, ciphertext_key2.to_vec());
    }

    #[test]
    fn test_kms_backend() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let key = Vec::from_hex("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139")
            .unwrap();
        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let backend = MemAesGcmBackend::new(key.clone()).unwrap();

        let inner = Inner {
            config: KmsConfig::default(),
            backend: Some(backend),
            cached_ciphertext_key: key,
        };
        let backend = KmsBackend {
            inner: Mutex::new(inner),
        };
        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        let mut vendor_not_found = encrypted_content.clone();
        vendor_not_found
            .metadata
            .remove(MetadataKey::KmsVendor.as_str());
        backend.decrypt_content(&vendor_not_found).unwrap_err();

        let mut ciphertext_key_not_found = encrypted_content;
        ciphertext_key_not_found
            .metadata
            .remove(MetadataKey::KmsCiphertextKey.as_str());
        backend
            .decrypt_content(&ciphertext_key_not_found)
            .unwrap_err();
    }
}
