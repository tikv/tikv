use kvproto::encryptionpb::{EncryptedContent, EncryptionMethod};

use super::Backend;
use crate::crypter::*;
use crate::metadata::*;
use crate::{AesCtrCrypter, Error, Iv, Result};

pub struct FileBackend {
    method: EncryptionMethod,
    key: Vec<u8>,
}

impl FileBackend {
    pub fn new(method: EncryptionMethod, key: Vec<u8>) -> Result<FileBackend> {
        if key.len() != get_method_key_length(method) {
            return Err(Error::Other(
                format!(
                    "encryption method and key length mismatch, expect {} get {}",
                    get_method_key_length(method),
                    key.len()
                )
                .into(),
            ));
        }
        Ok(FileBackend { key, method })
    }
}

impl Backend for FileBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        let iv = Iv::new();
        let iv_value = iv.as_slice().to_vec();
        content
            .mut_metadata()
            .insert(METADATA_KEY_IV.to_owned(), iv_value);
        let method_value = encode_ecryption_method(self.method)?;
        content
            .mut_metadata()
            .insert(METADATA_KEY_ENCRYPTION_METHOD.to_owned(), method_value);
        let key = &self.key;
        let ciphertext = AesCtrCrypter::new(self.method, key, iv).encrypt(plaintext)?;
        content.set_content(ciphertext);
        Ok(content)
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let key = &self.key;
        let iv_value = content.get_metadata().get(METADATA_KEY_IV).ok_or_else(|| {
            Error::Other(format!("metadata {} not found", METADATA_KEY_IV).into())
        })?;
        let method_value = content
            .get_metadata()
            .get(METADATA_KEY_ENCRYPTION_METHOD)
            .ok_or_else(|| {
                Error::Other(
                    format!("metadata {} not found", METADATA_KEY_ENCRYPTION_METHOD).into(),
                )
            })?;
        let iv = Iv::from(iv_value.as_slice());
        let method = decode_ecryption_method(method_value)?;
        let ciphertext = content.get_content();
        AesCtrCrypter::new(method, key, iv).decrypt(ciphertext)
    }
}
