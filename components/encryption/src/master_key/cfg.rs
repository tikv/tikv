use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

use crate::{Error, Result};

use super::Backend;

pub struct CfgBased {
    master_key_backend: Box<dyn Backend>,
}

impl Backend for CfgBased {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        unimplemented!()
    }
}
