// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;

use crate::{Error, Result};

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

mod file;
pub use self::file::FileBackend;
mod kms;
mod metadata;
pub use self::kms::KmsBackend;

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

pub(crate) struct WithMetadata<T: Backend> {
    pub metadata: Vec<(metadata::MetadataKey, Vec<u8>)>,
    pub backend: T,
}

impl<T: Backend> Backend for WithMetadata<T> {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let mut content = self.backend.encrypt(plaintext)?;
        for (key, value) in &self.metadata {
            content
                .mut_metadata()
                .insert(key.as_str().to_owned(), value.clone());
        }
        Ok(content)
    }
    fn decrypt(&self, ciphertext: &EncryptedContent) -> Result<Vec<u8>> {
        for (key, value) in &self.metadata {
            match ciphertext.get_metadata().get(key.as_str()) {
                Some(val) if val == value => (),
                other => {
                    return Err(Error::Other(
                        format!(
                            "metadata {:?} mismatch expect {:?} got {:?}",
                            key, value, other
                        )
                        .into(),
                    ))
                }
            }
        }
        self.backend.decrypt(ciphertext)
    }
    fn is_secure(&self) -> bool {
        self.backend.is_secure()
    }
}
