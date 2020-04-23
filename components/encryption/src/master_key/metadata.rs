// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum MetadataKey {
    Method,
    Iv,
    AesGcmTag,
    KmsVendor,
    KmsCiphertextKey,
}

const METADATA_KEY_METHOD: &str = "method";
const METADATA_KEY_IV: &str = "iv";
const METADATA_KEY_AES_GCM_TAG: &str = "aes_gcm_tag";
const METADATA_KEY_KMS_VENDOR: &str = "kms_vendor";
const METADATA_KEY_KMS_ENCRYPTED_KEY: &str = "kms_ciphertext_key";

impl MetadataKey {
    pub fn as_str(self) -> &'static str {
        match self {
            MetadataKey::Method => METADATA_KEY_METHOD,
            MetadataKey::Iv => METADATA_KEY_IV,
            MetadataKey::AesGcmTag => METADATA_KEY_AES_GCM_TAG,
            MetadataKey::KmsVendor => METADATA_KEY_KMS_VENDOR,
            MetadataKey::KmsCiphertextKey => METADATA_KEY_KMS_ENCRYPTED_KEY,
        }
    }
}

#[derive(Copy, Clone, Debug, Hash, PartialEq, Eq)]
pub enum MetadataMethod {
    Plaintext,
    Aes256Gcm,
}

const METADATA_METHOD_PLAINTEXT: &[u8] = b"plaintext";
const METADATA_METHOD_AES256_GCM: &[u8] = b"aes256-gcm";

impl MetadataMethod {
    pub fn as_slice(self) -> &'static [u8] {
        match self {
            MetadataMethod::Plaintext => METADATA_METHOD_PLAINTEXT,
            MetadataMethod::Aes256Gcm => METADATA_METHOD_AES256_GCM,
        }
    }
}
