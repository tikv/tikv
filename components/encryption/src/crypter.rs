// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::fmt::{self, Debug, Formatter};

use byteorder::{BigEndian, ByteOrder};
use cloud::kms::PlainKey;
use kvproto::encryptionpb::EncryptionMethod;
use openssl::{
    rand,
    symm::{self, Cipher as OCipher},
};
use tikv_util::box_err;

use crate::{Error, Result};

pub fn get_method_key_length(method: EncryptionMethod) -> usize {
    match method {
        EncryptionMethod::Plaintext => 0,
        EncryptionMethod::Aes128Ctr => 16,
        EncryptionMethod::Aes192Ctr => 24,
        EncryptionMethod::Aes256Ctr => 32,
        EncryptionMethod::Sm4Ctr => 16,
        unknown => panic!("bad EncryptionMethod {:?}", unknown),
    }
}

#[derive(Clone, PartialEq)]
pub struct FileEncryptionInfo {
    pub method: EncryptionMethod,
    pub key: Vec<u8>,
    pub iv: Vec<u8>,
}
impl Default for FileEncryptionInfo {
    fn default() -> Self {
        FileEncryptionInfo {
            method: EncryptionMethod::Unknown,
            key: vec![],
            iv: vec![],
        }
    }
}

impl Debug for FileEncryptionInfo {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "FileEncryptionInfo [method={:?}, key=...<{} bytes>, iv=...<{} bytes>]",
            self.method,
            self.key.len(),
            self.iv.len()
        )
    }
}

impl FileEncryptionInfo {
    pub fn is_empty(&self) -> bool {
        self.key.is_empty() && self.iv.is_empty()
    }
}

// IV's the length should be 12 btyes for GCM mode.
const GCM_IV_12: usize = 12;
// IV's the length should be 16 btyes for CTR mode.
const CTR_IV_16: usize = 16;

#[derive(Debug, Clone, Copy)]
pub enum Iv {
    Gcm([u8; GCM_IV_12]),
    Ctr([u8; CTR_IV_16]),
    Empty,
}

impl Iv {
    /// Generate a random IV for AES-GCM.
    pub fn new_gcm() -> Result<Iv> {
        let mut iv = [0u8; GCM_IV_12];
        rand::rand_bytes(&mut iv)?;
        Ok(Iv::Gcm(iv))
    }

    /// Generate a random IV for AES-CTR.
    pub fn new_ctr() -> Result<Iv> {
        let mut iv = [0u8; CTR_IV_16];
        rand::rand_bytes(&mut iv)?;
        Ok(Iv::Ctr(iv))
    }

    pub fn from_slice(src: &[u8]) -> Result<Iv> {
        if src.len() == CTR_IV_16 {
            let mut iv = [0; CTR_IV_16];
            iv.copy_from_slice(src);
            Ok(Iv::Ctr(iv))
        } else if src.len() == GCM_IV_12 {
            let mut iv = [0; GCM_IV_12];
            iv.copy_from_slice(src);
            Ok(Iv::Gcm(iv))
        } else {
            Err(box_err!(
                "Nonce + Counter must be 12/16 bytes, {}",
                src.len()
            ))
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Iv::Ctr(iv) => iv,
            Iv::Gcm(iv) => iv,
            Iv::Empty => &[],
        }
    }

    pub fn add_offset(&mut self, offset: u64) -> Result<()> {
        match self {
            Iv::Ctr(iv) => {
                let v = BigEndian::read_u128(iv);
                BigEndian::write_u128(iv, v.wrapping_add(offset as u128));
                Ok(())
            }
            Iv::Gcm(_) => Err(box_err!("offset addition is not supported for GCM mode")),
            Iv::Empty => Err(box_err!("empty Iv")),
        }
    }
}

// The length GCM tag must be 16 bytes.
const GCM_TAG_LEN: usize = 16;

pub struct AesGcmTag([u8; GCM_TAG_LEN]);

impl<'a> From<&'a [u8]> for AesGcmTag {
    fn from(src: &'a [u8]) -> AesGcmTag {
        assert!(src.len() >= GCM_TAG_LEN, "AES GCM tag must be 16 bytes");
        let mut tag = [0; GCM_TAG_LEN];
        tag.copy_from_slice(src);
        AesGcmTag(tag)
    }
}

impl AesGcmTag {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
}

/// An Aes256-GCM crypter.
pub struct AesGcmCrypter<'k> {
    iv: Iv,
    key: &'k PlainKey,
}

impl<'k> AesGcmCrypter<'k> {
    /// The key length of `AesGcmCrypter` is 32 bytes.
    pub const KEY_LEN: usize = 32;

    pub fn new(key: &'k PlainKey, iv: Iv) -> AesGcmCrypter<'k> {
        AesGcmCrypter { iv, key }
    }

    pub fn encrypt(&self, pt: &[u8]) -> Result<(Vec<u8>, AesGcmTag)> {
        let cipher = OCipher::aes_256_gcm();
        let mut tag = AesGcmTag([0u8; GCM_TAG_LEN]);
        let ciphertext = symm::encrypt_aead(
            cipher,
            self.key.as_slice(),
            Some(self.iv.as_slice()),
            &[], // AAD
            pt,
            &mut tag.0,
        )?;
        Ok((ciphertext, tag))
    }

    pub fn decrypt(&self, ct: &[u8], tag: AesGcmTag) -> Result<Vec<u8>> {
        let cipher = OCipher::aes_256_gcm();
        let plaintext = symm::decrypt_aead(
            cipher,
            self.key.as_slice(),
            Some(self.iv.as_slice()),
            &[], // AAD
            ct,
            &tag.0,
        )?;
        Ok(plaintext)
    }
}

pub fn verify_encryption_config(method: EncryptionMethod, key: &[u8]) -> Result<()> {
    if method == EncryptionMethod::Unknown {
        return Err(Error::UnknownEncryption);
    }
    if method != EncryptionMethod::Plaintext {
        let key_len = get_method_key_length(method);
        if key.len() != key_len {
            return Err(box_err!(
                "unexpected key length, expected {} vs actual {}",
                key_len,
                key.len()
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use cloud::kms::CryptographyType;
    use hex::FromHex;

    use super::*;

    #[test]
    fn test_iv() {
        let mut ivs = Vec::with_capacity(100);
        for c in 0..100 {
            if c % 2 == 0 {
                ivs.push(Iv::new_ctr().unwrap());
            } else {
                ivs.push(Iv::new_gcm().unwrap());
            }
        }
        ivs.dedup_by(|a, b| a.as_slice() == b.as_slice());
        assert_eq!(ivs.len(), 100);

        for iv in ivs {
            let iv1 = Iv::from_slice(iv.as_slice()).unwrap();
            assert_eq!(iv.as_slice(), iv1.as_slice());
        }
    }

    #[test]
    fn test_ase_256_gcm() {
        // See more http://csrc.nist.gov/groups/STM/cavp/documents/mac/gcmtestvectors.zip
        //
        // [Keylen = 256]
        // [IVlen = 96]
        // [PTlen = 256]
        // [AADlen = 0]
        // [Taglen = 128]
        //
        // Count = 0
        // Key = c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139
        // IV = cafabd9672ca6c79a2fbdc22
        // CT = 84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980
        // AAD =
        // Tag = 79651c875f7941793d42bbd0af1cce7c
        // PT = 25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749

        let pt = "25431587e9ecffc7c37f8d6d52a9bc3310651d46fb0e3bad2726c8f2db653749";
        let ct = "84e5f23f95648fa247cb28eef53abec947dbf05ac953734618111583840bd980";
        let key = "c3d99825f2181f4808acd2068eac7441a65bd428f14d2aab43fefc0129091139";
        let iv = "cafabd9672ca6c79a2fbdc22";
        let tag = "79651c875f7941793d42bbd0af1cce7c";

        let pt = Vec::from_hex(pt).unwrap();
        let ct = Vec::from_hex(ct).unwrap();
        let key = PlainKey::new(Vec::from_hex(key).unwrap(), CryptographyType::AesGcm256).unwrap();
        let iv = Iv::from_slice(Vec::from_hex(iv).unwrap().as_slice()).unwrap();
        let tag = Vec::from_hex(tag).unwrap();

        let crypter = AesGcmCrypter::new(&key, iv);
        let (ciphertext, gcm_tag) = crypter.encrypt(&pt).unwrap();
        assert_eq!(ciphertext, ct, "{}", hex::encode(&ciphertext));
        assert_eq!(gcm_tag.0.to_vec(), tag, "{}", hex::encode(gcm_tag.0));
        let plaintext = crypter.decrypt(&ct, gcm_tag).unwrap();
        assert_eq!(plaintext, pt, "{}", hex::encode(&plaintext));

        // Fail to decrypt with a wrong tag.
        crypter
            .decrypt(&ct, AesGcmTag([0u8; GCM_TAG_LEN]))
            .unwrap_err();
    }
}
