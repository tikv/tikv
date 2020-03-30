// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::{EncryptedContent, EncryptionMethod};

use super::metadata::*;
use super::Backend;
use crate::crypter::*;
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

    fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        let iv_value = iv.as_slice().to_vec();
        content
            .mut_metadata()
            .insert(MetadataKey::Iv.as_str().to_owned(), iv_value);
        let method_value = encode_ecryption_method(self.method)?;
        content.mut_metadata().insert(
            MetadataKey::EncryptionMethod.as_str().to_owned(),
            method_value,
        );
        let checksum = sha256(plaintext)?;
        content
            .mut_metadata()
            .insert(MetadataKey::PlaintextSha256.as_str().to_owned(), checksum);
        let key = &self.key;
        let ciphertext = AesCtrCrypter::new(self.method, key, iv).encrypt(plaintext)?;
        content.set_content(ciphertext);
        Ok(content)
    }

    fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let key = &self.key;
        let iv_value = content
            .get_metadata()
            .get(MetadataKey::Iv.as_str())
            .ok_or_else(|| {
                Error::Other(format!("metadata {} not found", MetadataKey::Iv.as_str()).into())
            })?;
        let method_value = content
            .get_metadata()
            .get(MetadataKey::EncryptionMethod.as_str())
            .ok_or_else(|| {
                Error::Other(
                    format!(
                        "metadata {} not found",
                        MetadataKey::EncryptionMethod.as_str()
                    )
                    .into(),
                )
            })?;
        let iv = Iv::from(iv_value.as_slice());
        let method = decode_ecryption_method(method_value)?;
        let checksum = content
            .get_metadata()
            .get(MetadataKey::PlaintextSha256.as_str())
            .ok_or_else(|| Error::Other("sha256 checksum not found".to_owned().into()))?;
        let ciphertext = content.get_content();
        let plaintext = AesCtrCrypter::new(method, key, iv).decrypt(ciphertext)?;
        if *checksum != sha256(&plaintext)? {
            return Err(Error::Other("sha256 checksum mismatch".to_owned().into()));
        }
        Ok(plaintext)
    }
}

impl Backend for FileBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let iv = Iv::new();
        self.encrypt_content(plaintext, iv)
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
    use hex::FromHex;

    use super::*;

    #[test]
    fn test_file_backend_ase_256_ctr() {
        // See more https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
        let pt = Vec::from_hex(
            "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710",
        )
        .unwrap();
        let ct = Vec::from_hex(
            "601ec313775789a5b7a7f504bbf3d228f443e3ca4d62b59aca84e990cacaf5c52b0930daa23de94c\
                  e87017ba2d84988ddfc9c58db67aada613c2dd08457941a6",
        )
        .unwrap();
        let key = Vec::from_hex("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4")
            .unwrap();
        let iv = Vec::from_hex("f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff").unwrap();

        let backend = FileBackend::new(EncryptionMethod::Aes256Ctr, key).unwrap();
        let iv = Iv::from(iv.as_slice());
        let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);
    }

    #[test]
    fn test_file_backend_sha256() {
        let pt = vec![1u8, 2, 3];
        let key = Vec::from_hex("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4")
            .unwrap();

        let backend = FileBackend::new(EncryptionMethod::Aes256Ctr, key).unwrap();
        let encrypted_content = backend.encrypt(&pt).unwrap();
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        // Must checksum mismatch
        let mut encrypted_content1 = encrypted_content.clone();
        encrypted_content1
            .mut_metadata()
            .get_mut(MetadataKey::PlaintextSha256.as_str())
            .unwrap()[0] += 1;
        backend.decrypt_content(&encrypted_content1).unwrap_err();

        // Must checksum not found
        let mut encrypted_content2 = encrypted_content;
        encrypted_content2
            .mut_metadata()
            .remove(MetadataKey::PlaintextSha256.as_str());
        backend.decrypt_content(&encrypted_content2).unwrap_err();
    }
}
