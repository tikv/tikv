// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptedContent;
use tikv_util::box_err;

use super::metadata::*;
use crate::{crypter::*, AesGcmCrypter, Error, Iv, Result};

/// An in-memory backend, it saves master key in memory.
#[derive(Debug)]
pub(crate) struct MemAesGcmBackend {
    pub key: PlainKey,
}

impl MemAesGcmBackend {
    pub fn new(key: Vec<u8>) -> Result<MemAesGcmBackend> {
        Ok(MemAesGcmBackend {
            key: PlainKey::new(key)?,
        })
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

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is possible that
    // a wrong master key has been used, or other error otherwise.
    pub fn decrypt_content(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        let method = content
            .get_metadata()
            .get(MetadataKey::Method.as_str())
            .ok_or_else(|| {
                // Missing method in metadata. The metadata of the encrypted content is invalid or
                // corrupted.
                Error::Other(box_err!(
                    "metadata {} not found",
                    MetadataKey::Method.as_str()
                ))
            })?;
        if method.as_slice() != MetadataMethod::Aes256Gcm.as_slice() {
            // Currently we only support aes256-gcm. A different method could mean the encrypted
            // content is written by a future version of TiKV, and we don't know how to handle it.
            // Fail immediately instead of fallback to previous key.
            return Err(Error::Other(box_err!(
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
                // IV is missing. The metadata of the encrypted content is invalid or corrupted.
                Error::Other(box_err!("metadata {} not found", MetadataKey::Iv.as_str()))
            })?;
        let iv = Iv::from_slice(iv_value.as_slice())?;
        let tag = content
            .get_metadata()
            .get(MetadataKey::AesGcmTag.as_str())
            .ok_or_else(|| {
                // Tag is missing. The metadata of the encrypted content is invalid or corrupted.
                Error::Other(box_err!("gcm tag not found"))
            })?;
        let gcm_tag = AesGcmTag::from(tag.as_slice());
        let ciphertext = content.get_content();
        let plaintext = AesGcmCrypter::new(key, iv)
            .decrypt(ciphertext, gcm_tag)
            .map_err(|e|
                // Decryption error, likely caused by mismatched tag. It could be the tag is
                // corrupted, or the encrypted content is fake by an attacker, but more likely
                // it is caused by a wrong master key being used.
                Error::WrongMasterKey(box_err!("decrypt in GCM mode failed: {}", e)))?;
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests {
    use hex::FromHex;
    use matches::assert_matches;

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

        // Must fail is method not found.
        let mut encrypted_content_missing_method = encrypted_content.clone();
        encrypted_content_missing_method
            .mut_metadata()
            .remove(MetadataKey::Method.as_str());
        assert_matches!(
            backend
                .decrypt_content(&encrypted_content_missing_method)
                .unwrap_err(),
            Error::Other(_)
        );

        // Must fail if method is not aes256-gcm.
        let mut encrypted_content_invalid_method = encrypted_content.clone();
        let mut invalid_suffix = b"_invalid".to_vec();
        encrypted_content_invalid_method
            .mut_metadata()
            .get_mut(MetadataKey::Method.as_str())
            .unwrap()
            .append(&mut invalid_suffix);
        assert_matches!(
            backend
                .decrypt_content(&encrypted_content_invalid_method)
                .unwrap_err(),
            Error::Other(_)
        );

        // Must fail if tag not found.
        let mut encrypted_content_missing_tag = encrypted_content.clone();
        encrypted_content_missing_tag
            .mut_metadata()
            .remove(MetadataKey::AesGcmTag.as_str());
        assert_matches!(
            backend
                .decrypt_content(&encrypted_content_missing_tag)
                .unwrap_err(),
            Error::Other(_)
        );

        // Must fail with WrongMasterKey error due to mismatched tag.
        let mut encrypted_content_mismatch_tag = encrypted_content;
        encrypted_content_mismatch_tag
            .mut_metadata()
            .get_mut(MetadataKey::AesGcmTag.as_str())
            .unwrap()[0] ^= 0b11111111u8;
        assert_matches!(
            backend
                .decrypt_content(&encrypted_content_mismatch_tag)
                .unwrap_err(),
            Error::WrongMasterKey(_)
        );
    }
}
