// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;

#[cfg(test)]
use crate::config::Mock;
use crate::{MasterKeyConfig, Result};

use std::path::Path;
use std::sync::Arc;

/// Provide API to encrypt/decrypt key dictionary content.
///
/// Can be back by KMS, or a key read from a file. If file is used, it will
/// prefix the result with the IV (nonce + initial counter) on encrypt,
/// and decode the IV on decrypt.
pub trait Backend: Sync + Send + 'static {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent>;
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>>;

    /// Tests whether this backend is secure.
    fn is_secure(&self) -> bool;
}

mod mem;
use self::mem::MemBackend;

mod file;
pub use self::file::FileBackend;

mod kms;
pub use self::kms::KmsBackend;

mod metadata;

#[derive(Default)]
pub(crate) struct PlainTextBackend {}

impl Backend for PlainTextBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        content.set_content(plaintext.to_owned());
        Ok(content)
    }
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
        Ok(ciphertext.get_content().to_owned())
    }
    fn is_secure(&self) -> bool {
        // plain text backend is insecure.
        false
    }
}

pub(crate) fn create_backend(config: &MasterKeyConfig) -> Result<Arc<dyn Backend>> {
    Ok(match config {
        MasterKeyConfig::Plaintext => Arc::new(PlainTextBackend {}) as _,
        MasterKeyConfig::File { method, path } => {
            Arc::new(FileBackend::new(*method, Path::new(path))?) as _
        }
        MasterKeyConfig::Kms { config } => Arc::new(KmsBackend::new(config.clone())?) as _,
        #[cfg(test)]
        MasterKeyConfig::Mock(Mock(mock)) => mock.clone() as _,
    })
}

// To make MasterKeyConfig able to compile.
#[cfg(test)]
impl std::fmt::Debug for dyn Backend {
    fn fmt(&self, _f: &mut std::fmt::Formatter) -> std::fmt::Result {
        Ok(())
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use crate::*;

    use std::sync::Mutex;

    pub(crate) struct MockBackend {
        pub inner: Box<dyn Backend>,
        pub is_wrong_master_key: bool,
        pub encrypt_fail: bool,
        pub encrypt_called: usize,
        pub decrypt_called: usize,
    }

    impl Default for MockBackend {
        fn default() -> MockBackend {
            MockBackend {
                inner: Box::new(PlainTextBackend {}),
                is_wrong_master_key: false,
                encrypt_fail: false,
                encrypt_called: 0,
                decrypt_called: 0,
            }
        }
    }

    impl Backend for Mutex<MockBackend> {
        fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
            let mut mock = self.lock().unwrap();
            mock.encrypt_called += 1;
            if mock.encrypt_fail {
                return Err(Error::Other("".to_owned().into()));
            }
            mock.inner.encrypt(plaintext)
        }
        fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
            let mut mock = self.lock().unwrap();
            mock.decrypt_called += 1;
            if mock.is_wrong_master_key {
                return Err(Error::WrongMasterKey("".to_owned().into()));
            }
            mock.inner.decrypt(ciphertext)
        }
        fn is_secure(&self) -> bool {
            true
        }
    }
}
