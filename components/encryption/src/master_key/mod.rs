// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use kvproto::encryptionpb::{EncryptedContent, EncryptionMethod, MasterKey};
use tikv_util::{box_err, error};
use tokio::sync::RwLock;

use crate::{manager::generate_data_key, Error, MasterKeyConfig, Result};

/// Provide API to encrypt/decrypt key dictionary content.
///
/// Can be back by KMS, or a key read from a file. If file is used, it will
/// prefix the result with the IV (nonce + initial counter) on encrypt,
/// and decode the IV on decrypt.
pub trait Backend: Sync + Send + std::fmt::Debug + 'static {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent>;
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>>;

    /// Tests whether this backend is secure.
    fn is_secure(&self) -> bool;
}

#[async_trait]
pub trait AsyncBackend: Sync + Send + std::fmt::Debug + 'static {
    async fn encrypt_async(&self, plaintext: &[u8]) -> Result<EncryptedContent>;
    async fn decrypt_async(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>>;
}

mod mem;
use self::mem::MemAesGcmBackend;

mod file;
pub use self::file::FileBackend;

mod metadata;
use self::metadata::*;

mod kms;
#[cfg(any(test, feature = "testexport"))]
pub use self::kms::fake;
pub use self::kms::KmsBackend;

#[derive(Default, Debug, Clone)]
pub struct PlaintextBackend {}

impl Backend for PlaintextBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        content.mut_metadata().insert(
            MetadataKey::Method.as_str().to_owned(),
            MetadataMethod::Plaintext.as_slice().to_vec(),
        );
        content.set_content(plaintext.to_owned());
        Ok(content)
    }
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
        let method = ciphertext
            .get_metadata()
            .get(MetadataKey::Method.as_str())
            .ok_or_else(|| {
                Error::Other(box_err!(
                    "metadata {} not found",
                    MetadataKey::Method.as_str()
                ))
            })?;
        if method.as_slice() != MetadataMethod::Plaintext.as_slice() {
            return Err(Error::WrongMasterKey(box_err!(
                "encryption method mismatch, expected {:?} vs actual {:?}",
                MetadataMethod::Plaintext.as_slice(),
                method
            )));
        }
        Ok(ciphertext.get_content().to_owned())
    }
    fn is_secure(&self) -> bool {
        // plain text backend is insecure.
        false
    }
}

#[async_trait]
impl AsyncBackend for PlaintextBackend {
    async fn encrypt_async(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.encrypt(plaintext)
    }

    async fn decrypt_async(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
        self.decrypt(ciphertext)
    }
}

/// Used for restore where multiple master keys are provided.
/// It will iterate the master key list and do the encryption/decryption.
/// If any master key succeeds, the request succeeds, if none succeeds, the
/// request will fail with a combined error of each master key error.
#[derive(Default, Debug, Clone)]
pub struct MultiMasterKeyBackend {
    inner: Arc<RwLock<MultiMasterKeyBackendInner>>,
}

#[derive(Default, Debug)]
struct MultiMasterKeyBackendInner {
    backends: Option<Vec<Box<dyn AsyncBackend>>>,
    pub(crate) configs: Option<Vec<MasterKeyConfig>>,
}
impl MultiMasterKeyBackend {
    pub fn new() -> Self {
        MultiMasterKeyBackend {
            inner: Arc::new(RwLock::new(MultiMasterKeyBackendInner {
                backends: None,
                configs: None,
            })),
        }
    }

    pub async fn update_from_proto_if_needed<F>(
        &self,
        master_keys_proto: Vec<MasterKey>,
        create_backend_fn: F,
    ) -> Result<()>
    where
        F: Fn(&MasterKeyConfig) -> Result<Box<dyn AsyncBackend>>,
    {
        if master_keys_proto.is_empty() {
            return Ok(());
        }
        let mut master_keys_config = Vec::new();
        for proto in master_keys_proto {
            let opt_master_key_config = MasterKeyConfig::from_proto(&proto);
            // sanity check
            if opt_master_key_config.is_none() {
                return Err(log_and_error(
                    "internal error: master key config should not be empty",
                ));
            }
            master_keys_config.push(opt_master_key_config.unwrap());
        }

        self.update_from_config_if_needed(master_keys_config, create_backend_fn)
            .await
    }

    pub async fn update_from_config_if_needed<F>(
        &self,
        master_keys_configs: Vec<MasterKeyConfig>,
        create_backend_fn: F,
    ) -> Result<()>
    where
        F: Fn(&MasterKeyConfig) -> Result<Box<dyn AsyncBackend>>,
    {
        if master_keys_configs.is_empty() {
            return Ok(());
        }

        let mut write_guard = self.inner.write().await;
        if write_guard.configs.as_ref() != Some(&master_keys_configs) {
            write_guard.backends = Some(create_master_key_backends(
                &master_keys_configs,
                create_backend_fn,
            )?);
            write_guard.configs = Some(master_keys_configs);
        }
        Ok(())
    }

    pub async fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let read_guard = self.inner.read().await;
        if read_guard.backends.is_none() {
            return Err(log_and_error(
                "internal error: multi master key backend not initialized when encrypting",
            ));
        }
        let mut errors = Vec::new();

        for master_key_backend in read_guard.backends.as_ref().unwrap() {
            match master_key_backend.encrypt_async(plaintext).await {
                Ok(res) => return Ok(res),
                Err(e) => errors.push(format!("Backend failed to encrypt with error: {}", e)),
            }
        }

        let combined_error = format!("failed to encrypt content: {}", errors.join("; "));
        Err(log_and_error(&combined_error))
    }
    pub async fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
        let read_guard = self.inner.read().await;
        if read_guard.backends.is_none() {
            return Err(log_and_error(
                "internal error: multi master key backend not initialized when decrypting",
            ));
        }
        let mut errors = Vec::new();

        for master_key_backend in read_guard.backends.as_ref().unwrap() {
            match master_key_backend.decrypt_async(ciphertext).await {
                Ok(res) => return Ok(res),
                Err(e) => errors.push(format!("Backend failed to decrypt with error: {}", e)),
            }
        }

        let combined_error = format!("failed to decrypt content: {}", errors.join("; "));
        Err(log_and_error(&combined_error))
    }

    pub fn generate_data_key(&self, method: EncryptionMethod) -> Result<Vec<u8>> {
        let (_id, key) = generate_data_key(method)?;
        Ok(key)
    }

    pub async fn is_initialized(&self) -> bool {
        let read_guard = self.inner.read().await;
        // configs and backends are updated together, should always be both empty or not
        // empty.
        read_guard.configs.is_some() && read_guard.backends.is_some()
    }
}

fn create_master_key_backends<F>(
    master_keys_config: &Vec<MasterKeyConfig>,
    create_backend_fn: F,
) -> Result<Vec<Box<dyn AsyncBackend>>>
where
    F: Fn(&MasterKeyConfig) -> Result<Box<dyn AsyncBackend>>,
{
    let mut backends = Vec::new();
    for master_key_config in master_keys_config {
        backends.push(create_backend_fn(master_key_config)?);
    }
    Ok(backends)
}

fn log_and_error(err_msg: &str) -> Error {
    error!("{}", err_msg);
    Error::Other(box_err!(err_msg))
}

#[cfg(test)]
pub mod tests {
    use std::{collections::HashMap, sync::Mutex};

    use lazy_static::lazy_static;

    use super::{Backend, *};
    use crate::*;

    #[derive(Debug)]
    pub struct MockBackend {
        pub is_wrong_master_key: bool,
        pub encrypt_fail: bool,
        pub track: String,
    }

    // Use a technique to use mutable state for a testing mock
    // without having it infect the rest of the program.
    lazy_static! {
        pub static ref ENCRYPT_CALLED: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());
        pub static ref DECRYPT_CALLED: Mutex<HashMap<String, usize>> = Mutex::new(HashMap::new());
    }

    pub fn decrypt_called(name: &str) -> usize {
        let track = make_track(name);
        *DECRYPT_CALLED.lock().unwrap().get(&track).unwrap()
    }

    pub fn encrypt_called(name: &str) -> usize {
        let track = make_track(name);
        *ENCRYPT_CALLED.lock().unwrap().get(&track).unwrap()
    }

    fn make_track(name: &str) -> String {
        format!("{} {:?}", name, std::thread::current().id())
    }

    impl MockBackend {
        // Callers are responsible for enabling tracking on the MockBackend by calling
        // this function This names the backend instance, allowing later fine-grained
        // recall
        pub fn track(&mut self, name: String) {
            let track = make_track(&name);
            self.track = track.clone();
            ENCRYPT_CALLED.lock().unwrap().insert(track.clone(), 0);
            DECRYPT_CALLED.lock().unwrap().insert(track, 0);
        }
    }

    impl Default for MockBackend {
        fn default() -> MockBackend {
            MockBackend {
                is_wrong_master_key: false,
                encrypt_fail: false,
                track: "Not tracked".to_string(),
            }
        }
    }

    impl Backend for MockBackend {
        fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
            let mut map = ENCRYPT_CALLED.lock().unwrap();
            if let Some(count) = map.get_mut(&self.track) {
                *count += 1
            }
            if self.encrypt_fail {
                return Err(box_err!("mock error"));
            }
            (PlaintextBackend {}).encrypt(plaintext)
        }

        fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
            let mut map = DECRYPT_CALLED.lock().unwrap();
            if let Some(count) = map.get_mut(&self.track) {
                *count += 1
            }
            if self.is_wrong_master_key {
                return Err(Error::WrongMasterKey("".to_owned().into()));
            }
            (PlaintextBackend {}).decrypt(ciphertext)
        }

        fn is_secure(&self) -> bool {
            true
        }
    }

    #[derive(Clone)]
    struct MockAsyncBackend {
        encrypt_result: Arc<dyn Fn() -> Result<EncryptedContent> + Send + Sync>,
        decrypt_result: Arc<dyn Fn() -> Result<Vec<u8>> + Send + Sync>,
    }

    #[async_trait]
    impl AsyncBackend for MockAsyncBackend {
        async fn encrypt_async(&self, _plaintext: &[u8]) -> Result<EncryptedContent> {
            (self.encrypt_result)()
        }

        async fn decrypt_async(&self, _ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
            (self.decrypt_result)()
        }
    }

    impl std::fmt::Debug for MockAsyncBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("MockAsyncBackend").finish()
        }
    }

    fn create_mock_backend<E, D>(encrypt_result: E, decrypt_result: D) -> Box<dyn AsyncBackend>
    where
        E: Fn() -> Result<EncryptedContent> + Send + Sync + 'static,
        D: Fn() -> Result<Vec<u8>> + Send + Sync + 'static,
    {
        Box::new(MockAsyncBackend {
            encrypt_result: Arc::new(encrypt_result),
            decrypt_result: Arc::new(decrypt_result),
        })
    }

    // In your tests:
    #[tokio::test]
    async fn test_multi_master_key_backend_encrypt_decrypt_failure() {
        let backend = MultiMasterKeyBackend::new();

        let configs = vec![MasterKeyConfig::File {
            config: FileConfig {
                path: "test".to_string(),
            },
        }];
        backend
            .update_from_config_if_needed(configs, |_| {
                Ok(create_mock_backend(
                    || {
                        Err(Error::Other(Box::new(std::io::Error::new(
                            ErrorKind::Other,
                            "Encrypt error",
                        ))))
                    },
                    || {
                        Err(Error::Other(Box::new(std::io::Error::new(
                            ErrorKind::Other,
                            "Decrypt error",
                        ))))
                    },
                ))
            })
            .await
            .unwrap();

        let encrypt_result = backend.encrypt(&[4, 5, 6]).await;
        assert!(encrypt_result.is_err(), "Encryption should have failed");
        if let Err(e) = encrypt_result {
            assert!(
                e.to_string().contains("Encrypt error"),
                "Unexpected error message: {}",
                e
            );
        }

        let decrypt_result = backend.decrypt(&EncryptedContent::default()).await;
        assert!(decrypt_result.is_err(), "Decryption should have failed");
        if let Err(e) = decrypt_result {
            assert!(
                e.to_string().contains("Decrypt error"),
                "Unexpected error message: {}",
                e
            );
        }
    }

    #[tokio::test]
    async fn test_multi_master_key_backend_inner_with_multiple_backends() {
        let mut inner = MultiMasterKeyBackendInner::default();

        let configs = vec![
            MasterKeyConfig::File {
                config: FileConfig {
                    path: "test1".to_string(),
                },
            },
            MasterKeyConfig::File {
                config: FileConfig {
                    path: "test2".to_string(),
                },
            },
            MasterKeyConfig::File {
                config: FileConfig {
                    path: "test3".to_string(),
                },
            },
        ];
        let backends: Vec<Box<dyn AsyncBackend>> = vec![
            create_mock_backend(
                || {
                    Err(Error::Other(Box::new(std::io::Error::new(
                        ErrorKind::Other,
                        "Encrypt error 1",
                    ))))
                },
                || Ok(vec![1, 2, 3]),
            ),
            create_mock_backend(
                || Ok(EncryptedContent::default()),
                || {
                    Err(Error::Other(Box::new(std::io::Error::new(
                        ErrorKind::Other,
                        "Decrypt error 2",
                    ))))
                },
            ),
            create_mock_backend(|| Ok(EncryptedContent::default()), || Ok(vec![7, 8, 9])),
        ];

        inner.configs = Some(configs);
        inner.backends = Some(backends);

        let backend = MultiMasterKeyBackend {
            inner: Arc::new(RwLock::new(inner)),
        };

        backend.encrypt(&[10, 11, 12]).await.unwrap();

        let decrypt_result = backend.decrypt(&EncryptedContent::default()).await.unwrap();
        assert_eq!(decrypt_result, vec![1, 2, 3]);
    }
}
