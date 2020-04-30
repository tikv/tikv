// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;

use super::metadata::*;
use crate::crypter::*;
use crate::{AesGcmCrypter, Error, Iv, Result};

/// An in-memory backend, it saves master key in memory.
pub(crate) struct MemAesGcmBackend {
    pub key: Vec<u8>,
}

impl MemAesGcmBackend {
    pub fn new(key: Vec<u8>) -> Result<MemAesGcmBackend> {
        if key.len() != AesGcmCrypter::KEY_LEN {
            return Err(box_err!(
                "encryption method and key length mismatch, expect {} get {}",
                AesGcmCrypter::KEY_LEN,
                key.len()
            ));
        }
        Ok(MemAesGcmBackend { key })
    }

    pub fn encrypt_content(&self, plaintext: &[u8], iv: Iv) -> Result<EncryptedContent> {
        let mut content = EncryptedContent::default();
        content.mut_metadata().insert(
            MetadataKey::Method.as_str().to_owned(),
            MetadataMethod::Aes256Gcm.as_slice().to_vec(),
        );
        let iv_value = iv.as_slice().to_vec();
        content
            .mut_metadata()
            .insert(MetadataKey::Iv.as_str().to_owned(), iv_value);
        let (ciphertext, gcm_tag) = AesGcmCrypter::new(&self.key, iv).encrypt(plaintext)?;
        content.set_content(ciphertext);
        content.mut_metadata().insert(
            MetadataKey::AesGcmTag.as_str().to_owned(),
            gcm_tag.as_slice().to_owned(),
        );
        Ok(content)
    }

    pub fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let method = content
            .get_metadata()
            .get(MetadataKey::Method.as_str())
            .ok_or_else(|| {
                Error::Other(box_err!(
                    "metadata {} not found",
                    MetadataKey::Method.as_str()
                ))
            })?;
        if method.as_slice() != MetadataMethod::Aes256Gcm.as_slice() {
            return Err(Error::WrongMasterKey(box_err!(
                "encryption method mismatch, expected {:?} vs actual {:?}",
                MetadataMethod::Aes256Gcm.as_slice(),
                method
            )));
        }
        let key = &self.key;
        let iv_value = content
            .get_metadata()
            .get(MetadataKey::Iv.as_str())
            .ok_or_else(|| {
                Error::Other(box_err!("metadata {} not found", MetadataKey::Iv.as_str()))
            })?;
        let iv = Iv::from_slice(iv_value.as_slice())?;
        let tag = content
            .get_metadata()
            .get(MetadataKey::AesGcmTag.as_str())
            .ok_or_else(|| Error::WrongMasterKey(box_err!("gcm tag not found")))?;
        let gcm_tag = AesGcmTag::from(tag.as_slice());
        let ciphertext = content.get_content();
        let plaintext = AesGcmCrypter::new(key, iv).decrypt(ciphertext, gcm_tag)?;
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use hex::FromHex;

    use super::*;

    #[test]
    fn test_mem_backend_ase_256_gcm() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        let pt = Vec::from_hex("25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749")
            .unwrap();
        let ct = Vec::from_hex("84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980")
            .unwrap();
        let key = Vec::from_hex("c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139")
            .unwrap();
        let iv = Vec::from_hex("cafabd9672ca6c79a2fbdc22").unwrap();

        let backend = MemAesGcmBackend::new(key).unwrap();
        let iv = Iv::from_slice(iv.as_slice()).unwrap();
        let encrypted_content = backend.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);
    }

    #[test]
    fn test_mem_backend_authenticate() {
        let pt = vec![1u8, 2, 3];
        let key = Vec::from_hex("603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4")
            .unwrap();

        let backend = MemAesGcmBackend::new(key).unwrap();
        let encrypted_content = backend.encrypt_content(&pt, Iv::new_gcm()).unwrap();
        let plaintext = backend.decrypt_content(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);

        // Must fail to decrypt due to invalid tag.
        let mut encrypted_content1 = encrypted_content.clone();
        encrypted_content1
            .mut_metadata()
            .get_mut(MetadataKey::AesGcmTag.as_str())
            .unwrap()[0] += 1;
        backend.decrypt_content(&encrypted_content1).unwrap_err();

        // Must tag not found.
        let mut encrypted_content2 = encrypted_content;
        encrypted_content2
            .mut_metadata()
            .remove(MetadataKey::AesGcmTag.as_str());
        backend.decrypt_content(&encrypted_content2).unwrap_err();
    }
}
