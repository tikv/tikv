use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

use crate::{Error, Result};

use super::Backend;

pub struct CfgBased {
    master_key: Vec<u8>,
}

impl CfgBased {
    pub fn new(master_key: Vec<u8>) -> CfgBased {
        CfgBased { master_key }
    }

    // TODO support online master key rotation
    pub fn rotate(&mut self, new_master_key: Vec<u8>) -> Result<()> {
        self.master_key = new_master_key;
        unimplemented!()
    }
}

impl Backend for CfgBased {
    fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        unimplemented!()
    }
    fn decrypt(&self, ciphertext: &[u8]) -> Result<Vec<u8>> {
        unimplemented!()
    }
}
