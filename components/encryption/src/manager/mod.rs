use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

use crate::master_key::Backend;
use crate::{Error, Result};

pub struct KeyManagerImpl {
    master_key_backend: Box<dyn Backend>,
}

impl rockmock::KeyManager for KeyManagerImpl {
    fn get_current_data_key() -> Result<DataKey> {
        unimplemented!()
    }

    // Get key to open existing file.
    fn get_encryption_info_for_file(file_path: &str) -> Result<FileInfo> {
        unimplemented!()
    }

    fn add_file(file_path: &str, file_info: FileInfo) -> Result<()> {
        unimplemented!()
    }

    fn delete_file(file_path: &str) -> Result<()> {
        unimplemented!()
    }
}

// Will be moved into rust-rocksdb
mod rockmock {
    use crate::{Error, Result};
    use kvproto::encryptionpb::{DataKey, EncryptionMethod, FileDictionary, FileInfo};

    pub trait KeyManager {
        // Get current key to encrypt new file.
        fn get_current_data_key() -> Result<DataKey>;

        // Get key to open existing file.
        fn get_encryption_info_for_file(file_path: &str) -> Result<FileInfo>;

        fn add_file(file_path: &str, file_info: FileInfo) -> Result<()>;
        fn delete_file(file_path: &str) -> Result<()>;
    }
}
