// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;

use crate::Result;

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
