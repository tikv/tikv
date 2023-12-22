// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::EncryptionMethod;
use proxy_ffi::interfaces_ffi::EncryptionMethod as FFIEncryptionMethod;

use crate::utils::v1::*;

#[test]
fn test_encryption_match() {
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Unknown),
        FFIEncryptionMethod::Unknown
    );
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Plaintext),
        FFIEncryptionMethod::Plaintext
    );
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Aes128Ctr),
        FFIEncryptionMethod::Aes128Ctr
    );
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Aes192Ctr),
        FFIEncryptionMethod::Aes192Ctr
    );
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Aes256Ctr),
        FFIEncryptionMethod::Aes256Ctr
    );
    assert_eq!(
        FFIEncryptionMethod::from(EncryptionMethod::Sm4Ctr),
        FFIEncryptionMethod::SM4Ctr
    );
}
