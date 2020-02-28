use kvproto::encryptionpb::{
    DataKey, EncryptedContent, EncryptionMethod, FileDictionary, FileInfo,
};

use super::Backend;
use crate::crypter::*;
use crate::metadata::*;
use crate::{AesCtrCtypter, Error, File, Iv, Result};

pub struct FileBased {
    method: EncryptionMethod,
    key: Vec<u8>,
}

impl FileBased {
    pub fn new(method: EncryptionMethod, key: Vec<u8>) -> Result<FileBased> {
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
        Ok(FileBased { key, method })
    }

    // TODO support online master key rotation
    pub fn rotate(&mut self) -> Result<()> {
        unimplemented!()
    }
}

impl Backend for FileBased {
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
        let ciphertext = AesCtrCtypter::new(self.method, key, iv).encrypt(plaintext)?;
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
        AesCtrCtypter::new(method, key, iv).decrypt(ciphertext)
    }
}
