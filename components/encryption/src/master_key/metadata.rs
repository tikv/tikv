// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum MetadataKey {
    Iv,
    AesGcmTag,
    KmsVendor,
    KmsCiphertextKey,
}

const METADATA_KEY_IV: &str = "IV";
const METADATA_KEY_AES_GCM_TAG: &str = "aes_gcm_tag";
const METADATA_KEY_KMS_VENDOR: &str = "kms_vendor";
const METADATA_KEY_KMS_ENCRYPTED_KEY: &str = "kms_ciphertext_key";

impl MetadataKey {
    pub fn as_str(self) -> &'static str {
        match self {
            MetadataKey::Iv => METADATA_KEY_IV,
            MetadataKey::AesGcmTag => METADATA_KEY_AES_GCM_TAG,
            MetadataKey::KmsVendor => METADATA_KEY_KMS_VENDOR,
            MetadataKey::KmsCiphertextKey => METADATA_KEY_KMS_ENCRYPTED_KEY,
        }
    }
}
