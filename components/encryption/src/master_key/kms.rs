// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use derive_more::Deref;
use kvproto::encryptionpb::EncryptedContent;
use tokio::runtime::{Builder, Runtime};

use super::{metadata::MetadataKey, Backend, MemAesGcmBackend};
use crate::crypter::{Iv, PlainKey};
use crate::{Error, Result};
use tikv_util::stream::{retry, with_timeout};

#[async_trait]
pub trait KmsProvider: Sync + Send + 'static + std::fmt::Debug {
    async fn generate_data_key(&self) -> Result<DataKeyPair>;
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>>;
    fn name(&self) -> &[u8];
}

// EncryptedKey is a newtype used to mark data as an encrypted key
// It requires the vec to be non-empty
#[derive(PartialEq, Clone, Debug, Deref)]
pub struct EncryptedKey(Vec<u8>);

impl EncryptedKey {
    pub fn new(key: Vec<u8>) -> Result<Self> {
        if key.is_empty() {
            error!("Encrypted content is empty");
        }
        Ok(Self(key))
    }
}

#[derive(Debug)]
pub struct DataKeyPair {
    pub encrypted: EncryptedKey,
    pub plaintext: PlainKey,
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

#[derive(Debug)]
pub struct KmsBackend {
    timeout_duration: Duration,
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
            timeout_duration: Duration::from_secs(10),
            state: Mutex::new(None),
            runtime,
            kms_provider,
        })
    }

    fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut opt_state = self.state.lock().unwrap();
        if opt_state.is_none() {
            let mut runtime = self.runtime.lock().unwrap();
            let data_key = runtime.block_on(retry(|| {
                with_timeout(self.timeout_duration, self.kms_provider.generate_data_key())
            }))?;
            *opt_state = Some(State::new_from_datakey(DataKeyPair {
                plaintext: PlainKey::new(data_key.plaintext.clone())?,
                encrypted: EncryptedKey::new((*data_key.encrypted).clone())?,
            })?);
        }
        let state = opt_state.as_ref().unwrap();

        let mut content = state.encryption_backend.encrypt_content(plaintext, iv)?;

        // Set extra metadata for KmsBackend.
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
                let plaintext = runtime.block_on(retry(|| {
                    with_timeout(
                        self.timeout_duration,
                        self.kms_provider.decrypt_data_key(&ciphertext_key),
                    )
                }))?;
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
mod fake {
    use super::*;

    const FAKE_VENDOR_NAME: &[u8] = b"FAKE";
    const FAKE_DATA_KEY_ENCRYPTED: &[u8] = b"encrypted                       ";

    #[derive(Debug)]
    pub struct FakeKms {
        plaintext_key: PlainKey,
    }

    impl FakeKms {
        pub fn new(plaintext_key: Vec<u8>) -> Self {
            Self {
                plaintext_key: PlainKey::new(plaintext_key).unwrap(),
            }
        }
    }

    #[async_trait]
    impl KmsProvider for FakeKms {
        async fn generate_data_key(&self) -> Result<DataKeyPair> {
            Ok(DataKeyPair {
                encrypted: EncryptedKey::new(FAKE_DATA_KEY_ENCRYPTED.to_vec())?,
                plaintext: PlainKey::new(self.plaintext_key.clone()).unwrap(),
            })
        }

        async fn decrypt_data_key(&self, _ciphertext: &EncryptedKey) -> Result<Vec<u8>> {
            Ok(vec![1u8, 32])
        }

        fn name(&self) -> &[u8] {
            FAKE_VENDOR_NAME
        }
    }
}

#[cfg(test)]
mod tests {
    use super::fake::FakeKms;
    use super::*;
    use hex::FromHex;
    use matches::assert_matches;

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
    fn test_kms_backend() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let plainkey =
            Vec::from_hex("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139")
                .unwrap();

        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let backend = KmsBackend::new(Box::new(FakeKms::new(plainkey))).unwrap();
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
}
