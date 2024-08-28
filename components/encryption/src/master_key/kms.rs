// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Mutex, time::Duration};

use cloud::kms::{CryptographyType, DataKeyPair, EncryptedKey, KmsProvider, PlainKey};
use kvproto::encryptionpb::EncryptedContent;
use tikv_util::{
    box_err,
    stream::{retry, with_timeout},
    sys::thread::ThreadBuildWrapper,
};
use tokio::runtime::{Builder, Runtime};

use super::{metadata::MetadataKey, Backend, MemAesGcmBackend};
use crate::{crypter::Iv, errors::cloud_convert_error, Error, Result};

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
            Builder::new_current_thread()
                .thread_name("kms-runtime")
                .enable_all()
                .with_sys_hooks()
                .build()?,
        );

        Ok(KmsBackend {
            timeout_duration: Duration::from_secs(10),
            state: Mutex::new(None),
            runtime,
            kms_provider,
        })
    }

    pub fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut opt_state = self.state.lock().unwrap();
        if opt_state.is_none() {
            let runtime = self.runtime.lock().unwrap();
            let data_key = runtime
                .block_on(retry(|| {
                    with_timeout(self.timeout_duration, self.kms_provider.generate_data_key())
                }))
                .map_err(cloud_convert_error("get data key failed".into()))?;
            *opt_state = Some(State::new_from_datakey(DataKeyPair {
                plaintext: PlainKey::new(data_key.plaintext.clone(), CryptographyType::AesGcm256)
                    .map_err(cloud_convert_error("invalid plain key".into()))?,
                encrypted: EncryptedKey::new((*data_key.encrypted).clone())
                    .map_err(cloud_convert_error("invalid encrypted key".into()))?,
            })?);
        }
        let state = opt_state.as_ref().unwrap();

        let mut content = state.encryption_backend.encrypt_content(plaintext, iv)?;

        // Set extra metadata for KmsBackend.
        content.metadata.insert(
            MetadataKey::KmsVendor.as_str().to_owned(),
            self.kms_provider.name().as_bytes().to_vec(),
        );
        content.metadata.insert(
            MetadataKey::KmsCiphertextKey.as_str().to_owned(),
            state.cached_ciphertext_key.to_vec(),
        );

        Ok(content)
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is
    // possible that a wrong master key has been used, or other error otherwise.
    pub fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let vendor_name = self.kms_provider.name();
        match content.metadata.get(MetadataKey::KmsVendor.as_str()) {
            Some(val) if val.as_slice() == vendor_name.as_bytes() => (),
            None => {
                return Err(
                    // If vender is missing in metadata, it could be the encrypted content is
                    // invalid or corrupted, but it is also possible that the content is encrypted
                    // using the FileBackend. Return WrongMasterKey anyway.
                    Error::WrongMasterKey(box_err!("missing KMS vendor")),
                );
            }
            other => {
                return Err(box_err!(
                    "KMS vendor mismatch expect {:?} got {:?}",
                    vendor_name,
                    other
                ));
            }
        }

        let ciphertext_key = match content.metadata.get(MetadataKey::KmsCiphertextKey.as_str()) {
            None => return Err(box_err!("KMS ciphertext key not found")),
            Some(key) => EncryptedKey::new(key.to_vec())
                .map_err(cloud_convert_error("invalid encrypted key".into()))?,
        };

        {
            let mut opt_state = self.state.lock().unwrap();
            if let Some(state) = &*opt_state {
                if state.cached(&ciphertext_key) {
                    return state.encryption_backend.decrypt_content(content);
                }
            }
            {
                let runtime = self.runtime.lock().unwrap();
                let plaintext = runtime
                    .block_on(retry(|| {
                        with_timeout(
                            self.timeout_duration,
                            self.kms_provider.decrypt_data_key(&ciphertext_key),
                        )
                    }))
                    .map_err(|e| {
                        Error::WrongMasterKey(box_err!(cloud_convert_error(
                            "decrypt encrypted key failed".into(),
                        )(e)))
                    })?;
                let data_key = DataKeyPair {
                    encrypted: ciphertext_key,
                    plaintext: PlainKey::new(plaintext, CryptographyType::AesGcm256)
                        .map_err(cloud_convert_error("invalid plain key".into()))?,
                };
                let state = State::new_from_datakey(data_key)?;
                let content = state.encryption_backend.decrypt_content(content)?;
                *opt_state = Some(state);
                Ok(content)
            }
        }
    }

    #[cfg(any(test, feature = "testexport"))]
    pub fn clear_state(&mut self) {
        let mut opt_state = self.state.lock().unwrap();
        *opt_state = None;
    }
}

impl Backend for KmsBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        self.encrypt_content(plaintext, Iv::new_gcm()?)
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.decrypt_content(content)
    }

    fn is_secure(&self) -> bool {
        true
    }
}

#[cfg(any(test, feature = "testexport"))]
pub mod fake {
    use async_trait::async_trait;
    use cloud::{
        error::{Error as CloudError, KmsError, Result},
        kms::KmsProvider,
    };
    use fail::fail_point;
    use hex::FromHex;

    use super::*;

    const FAKE_VENDOR_NAME: &str = "FAKE";
    const FAKE_DATA_KEY_ENCRYPTED: &[u8] = b"encrypted                       ";

    #[derive(Debug)]
    pub struct FakeKms {
        plaintext_key: PlainKey,
        should_decrypt_data_key_fail: bool,
    }

    impl FakeKms {
        pub fn new(plaintext_key: Vec<u8>, should_decrypt_data_key_fail: bool) -> Self {
            Self {
                plaintext_key: PlainKey::new(plaintext_key, CryptographyType::AesGcm256).unwrap(),
                should_decrypt_data_key_fail,
            }
        }
    }

    fn check_fail_point(fail_point_name: &str) -> Result<()> {
        fail_point!(fail_point_name, |val| {
            val.and_then(|x| x.parse::<bool>().ok())
                .filter(|&fail| fail)
                .map(|_| Err(CloudError::ApiTimeout(box_err!("api timeout"))))
                .unwrap_or(Ok(()))
        });
        Ok(())
    }

    #[async_trait]
    impl KmsProvider for FakeKms {
        async fn generate_data_key(&self) -> Result<DataKeyPair> {
            check_fail_point("kms_api_timeout_encrypt")?;

            Ok(DataKeyPair {
                encrypted: EncryptedKey::new(FAKE_DATA_KEY_ENCRYPTED.to_vec())?,
                plaintext: PlainKey::new(self.plaintext_key.clone(), CryptographyType::AesGcm256)
                    .unwrap(),
            })
        }

        async fn decrypt_data_key(&self, _ciphertext: &EncryptedKey) -> Result<Vec<u8>> {
            check_fail_point("kms_api_timeout_decrypt")?;

            if self.should_decrypt_data_key_fail {
                Err(CloudError::KmsError(KmsError::WrongMasterKey(box_err!(
                    "wrong master key"
                ))))
            } else {
                Ok(hex::decode(PLAINKEY_HEX).unwrap())
            }
        }

        fn name(&self) -> &str {
            FAKE_VENDOR_NAME
        }
    }

    // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
    const PLAIN_TEXT_HEX: &str = "25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749";
    const CIPHER_TEXT_HEX: &str =
        "84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980";
    const PLAINKEY_HEX: &str = "c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139";
    const IV_HEX: &str = "cafabd9672ca6c79a2fbdc22";

    pub fn prepare_data_for_encrypt() -> (Iv, Vec<u8>, Vec<u8>, Vec<u8>) {
        let iv = Vec::from_hex(IV_HEX).unwrap();
        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let pt = Vec::from_hex(PLAIN_TEXT_HEX).unwrap();
        let plainkey = Vec::from_hex(PLAINKEY_HEX).unwrap();
        let ct = Vec::from_hex(CIPHER_TEXT_HEX).unwrap();
        (iv, pt, plainkey, ct)
    }

    pub fn prepare_kms_backend(
        plainkey: Vec<u8>,
        should_decrypt_data_key_fail: bool,
    ) -> KmsBackend {
        KmsBackend::new(Box::new(FakeKms::new(
            plainkey,
            should_decrypt_data_key_fail,
        )))
        .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use matches::assert_matches;

    use super::{fake::*, *};

    #[test]
    fn test_state() {
        let plaintext = PlainKey::new(vec![1u8; 32], CryptographyType::AesGcm256).unwrap();
        let encrypted = EncryptedKey::new(vec![2u8; 32]).unwrap();
        let data_key = DataKeyPair {
            plaintext: PlainKey::new(plaintext.clone(), CryptographyType::AesGcm256).unwrap(),
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
        let (iv, pt, plainkey, ct) = prepare_data_for_encrypt();
        let backend = prepare_kms_backend(plainkey, false);

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
    fn test_kms_backend_wrong_key() {
        let (iv, pt, plainkey, ..) = prepare_data_for_encrypt();
        let mut backend = prepare_kms_backend(plainkey, true);

        let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
        // Clear the cached state to ensure that the subsequent
        // backend.decrypt_content() invocation bypasses the cache and triggers the
        // mocked FakeKMS::decrypt_data_key() function.
        backend.clear_state();

        let err = backend.decrypt_content(&encrypted_content).unwrap_err();
        assert_matches!(err, Error::WrongMasterKey(_));
    }
}
