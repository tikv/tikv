// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::Path;

use kvproto::encryptionpb::{EncryptedContent, EncryptionMethod};

use super::{Backend, MemBackend};
use crate::{Iv, Result};

pub struct FileBackend {
    mem_backend: MemBackend,
}

impl FileBackend {
    pub fn new(method: EncryptionMethod, key_path: &Path) -> Result<FileBackend> {
        let key = std::fs::read(key_path)?;
        let mem_backend = MemBackend::new(method, key)?;
        Ok(FileBackend { mem_backend })
    }
}

impl Backend for FileBackend {
    fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedContent> {
        let iv = Iv::new();
        self.mem_backend.encrypt_content(plaintext, iv)
    }

    fn decrypt(&self, content: &EncryptedContent) -> Result<Vec<u8>> {
        self.mem_backend.decrypt_content(content)
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

        let tmp = tempfile::TempDir::new().unwrap();
        let key_path = tmp.path().join("key");
        std::fs::write(&key_path, &key).unwrap();
        let backend = FileBackend::new(EncryptionMethod::Aes256Ctr, &key_path).unwrap();
        let iv = Iv::from(iv.as_slice());
        let encrypted_content = backend.mem_backend.encrypt_content(&pt, iv).unwrap();
        assert_eq!(encrypted_content.get_content(), ct.as_slice());
        let plaintext = backend.decrypt(&encrypted_content).unwrap();
        assert_eq!(plaintext, pt);
    }
}
