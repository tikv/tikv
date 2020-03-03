use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};
use openssl::symm::{self, Cipher as OCipher};
use rocksdb::DBEncryptionMethod;

use crate::Result;

pub fn encryption_method_to_db_encryption_method(method: EncryptionMethod) -> DBEncryptionMethod {
    match method {
        EncryptionMethod::Plaintext => DBEncryptionMethod::Plaintext,
        EncryptionMethod::Aes128Ctr => DBEncryptionMethod::Aes128Ctr,
        EncryptionMethod::Aes192Ctr => DBEncryptionMethod::Aes192Ctr,
        EncryptionMethod::Aes256Ctr => DBEncryptionMethod::Aes256Ctr,
        EncryptionMethod::Unknown => DBEncryptionMethod::Unknown,
    }
}

pub fn get_method_key_length(method: EncryptionMethod) -> usize {
    match method {
        EncryptionMethod::Plaintext => 0,
        EncryptionMethod::Aes128Ctr => 16,
        EncryptionMethod::Aes192Ctr => 24,
        EncryptionMethod::Aes256Ctr => 32,
        unknown => panic!("bad EncryptionMethod {:?}", unknown),
    }
}

pub enum Cipher {
    Plaintext,
    AesCtr(OCipher),
}

impl From<EncryptionMethod> for Cipher {
    fn from(method: EncryptionMethod) -> Cipher {
        match method {
            EncryptionMethod::Plaintext => Cipher::Plaintext,
            EncryptionMethod::Aes128Ctr => Cipher::AesCtr(OCipher::aes_128_ctr()),
            EncryptionMethod::Aes192Ctr => Cipher::AesCtr(OCipher::aes_192_ctr()),
            EncryptionMethod::Aes256Ctr => Cipher::AesCtr(OCipher::aes_256_ctr()),
            unknown => panic!("bad EncryptionMethod {:?}", unknown),
        }
    }
}

// IV as an AES input, the length must be 16 btyes.
const IV_LEN: usize = 16;

#[derive(Debug, Clone, Copy)]
pub struct Iv {
    iv: [u8; IV_LEN],
}

impl Iv {
    pub fn as_slice(&self) -> &[u8; IV_LEN] {
        &self.iv
    }
}

impl<'a> From<&'a [u8]> for Iv {
    fn from(src: &'a [u8]) -> Iv {
        assert_eq!(src.len(), IV_LEN, "Nonce + Counter must be 16 bytes");
        let mut iv = [0; IV_LEN];
        iv.copy_from_slice(src);
        Iv { iv }
    }
}

impl Iv {
    /// Generate a nonce and a counter randomly.
    pub fn new() -> Iv {
        use rand::{rngs::OsRng, RngCore};

        let mut iv = [0u8; IV_LEN];
        OsRng.fill_bytes(&mut iv);

        Iv { iv }
    }
}

pub struct AesCtrCrypter<'k> {
    method: EncryptionMethod,
    iv: Iv,
    key: &'k [u8],
}

impl<'k> AesCtrCrypter<'k> {
    pub fn new(method: EncryptionMethod, key: &'k [u8], iv: Iv) -> AesCtrCrypter<'k> {
        AesCtrCrypter { method, iv, key }
    }

    pub fn encrypt(&self, pt: &[u8]) -> Result<Vec<u8>> {
        let cipher = Cipher::from(self.method);
        match cipher {
            Cipher::Plaintext => Ok(pt.to_owned()),
            Cipher::AesCtr(c) => {
                let ciphertext = symm::encrypt(c, self.key, Some(self.iv.as_slice()), &pt)?;
                Ok(ciphertext)
            }
        }
    }

    pub fn decrypt(&self, ct: &[u8]) -> Result<Vec<u8>> {
        let cipher = Cipher::from(self.method);
        match cipher {
            Cipher::Plaintext => Ok(ct.to_owned()),
            Cipher::AesCtr(c) => {
                let plaintext = symm::decrypt(c, self.key, Some(self.iv.as_slice()), &ct)?;
                Ok(plaintext)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use hex::FromHex;

    use super::*;

    #[test]
    fn test_iv() {
        let mut ivs = Vec::with_capacity(100);
        for _ in 0..100 {
            ivs.push(Iv::new());
        }
        ivs.dedup_by(|a, b| a.as_slice() == b.as_slice());
        assert_eq!(ivs.len(), 100);

        for iv in ivs {
            let iv1 = Iv::from(&iv.as_slice()[..]);
            assert_eq!(iv.as_slice(), iv1.as_slice());
        }
    }

    fn aes_ctr_test(method: EncryptionMethod, pt: &str, ct: &str, key: &str, iv: &str) {
        let pt = Vec::from_hex(pt).unwrap();
        let ct = Vec::from_hex(ct).unwrap();
        let key = Vec::from_hex(key).unwrap();
        let iv = Vec::from_hex(iv).unwrap().as_slice().into();

        let crypter = AesCtrCrypter::new(method, &key, iv);
        let ciphertext = crypter.encrypt(&pt).unwrap();
        assert_eq!(ciphertext, ct, "{}", hex::encode(&ciphertext));
        let plaintext = crypter.decrypt(&ct).unwrap();
        assert_eq!(plaintext, pt, "{}", hex::encode(&plaintext));
    }

    #[test]
    fn test_plaintext() {
        // See more https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
        let pt = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
        let ct = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
        let key = "";
        let iv = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

        aes_ctr_test(EncryptionMethod::Plaintext, pt, ct, key, iv);
    }

    #[test]
    fn test_ase_128_ctr() {
        // See more https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
        let pt = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
        let ct = "874d6191b620e3261bef6864990db6ce9806f66b7970fdff8617187bb9fffdff5ae4df3edbd5d35e\
                  5b4f09020db03eab1e031dda2fbe03d1792170a0f3009cee";
        let key = "2b7e151628aed2a6abf7158809cf4f3c";
        let iv = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

        aes_ctr_test(EncryptionMethod::Aes128Ctr, pt, ct, key, iv);
    }

    #[test]
    fn test_ase_192_ctr() {
        // See more https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
        let pt = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
        let ct = "1abc932417521ca24f2b0459fe7e6e0b090339ec0aa6faefd5ccc2c6f4ce8e941e36b26bd1ebc670\
                  d1bd1d665620abf74f78a7f6d29809585a97daec58c6b050";
        let key = "8e73b0f7da0e6452c810f32b809079e562f8ead2522c6b7b";
        let iv = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

        aes_ctr_test(EncryptionMethod::Aes192Ctr, pt, ct, key, iv);
    }

    #[test]
    fn test_ase_256_ctr() {
        // See more https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38a.pdf
        let pt = "6bc1bee22e409f96e93d7e117393172aae2d8a571e03ac9c9eb76fac45af8e5130c81c46a35ce411\
                  e5fbc1191a0a52eff69f2445df4f9b17ad2b417be66c3710";
        let ct = "601ec313775789a5b7a7f504bbf3d228f443e3ca4d62b59aca84e990cacaf5c52b0930daa23de94c\
                  e87017ba2d84988ddfc9c58db67aada613c2dd08457941a6";
        let key = "603deb1015ca71be2b73aef0857d77811f352c073b6108d72d9810a30914dff4";
        let iv = "f0f1f2f3f4f5f6f7f8f9fafbfcfdfeff";

        aes_ctr_test(EncryptionMethod::Aes256Ctr, pt, ct, key, iv);
    }
}
