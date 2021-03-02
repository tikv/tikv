// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;

use crate::{Error, Result};

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

mod mem;
use self::mem::MemAesGcmBackend;

mod file;
pub use self::file::FileBackend;

mod metadata;
use self::metadata::*;

mod kms;
pub use self::kms::{DataKeyPair, EncryptedKey, KmsBackend, KmsProvider};

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

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::*;
    use lazy_static::lazy_static;
    use std::collections::HashMap;
    use std::sync::Mutex;

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
        // Callers are responsible for enabling tracking on the MockBackend by calling this function
        // This names the backend instance, allowiing later fine-grained recall
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
}
