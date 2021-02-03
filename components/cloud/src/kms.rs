use crate::error::{empty_key_contents, Result};
use kvproto::encryptionpb::MasterKeyKms;
use tokio::runtime::Runtime;

#[derive(Debug, Clone)]
pub struct Location {
    pub region: String,
    pub endpoint: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub key_id: KeyId,
    pub location: Location,
    pub vendor: String,
}

impl Config {
    pub fn from_proto(mk: MasterKeyKms) -> Result<Self> {
        Ok(Config {
            key_id: KeyId::new(mk.key_id)?,
            location: Location {
                region: mk.region,
                endpoint: mk.endpoint,
            },
            vendor: mk.vendor,
        })
    }
}

#[derive(PartialEq, Debug, Clone)]
pub struct KeyId(String);

// KeyID is a newtype to mark a String as an ID of a key
// This ID exists in a foreign system such as AWS
// The key id must be non-empty
impl KeyId {
    pub fn new(id: String) -> Result<KeyId> {
        if id.is_empty() {
            Err(empty_key_contents("KMS key id can not be empty"))
        } else {
            Ok(KeyId(id))
        }
    }
}

impl std::ops::Deref for KeyId {
    type Target = String;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// EncryptedKey is a newtype used to mark data as an encrypted key
// It requires the vec to be non-empty
#[derive(PartialEq, Clone, Debug)]
pub struct EncryptedKey(Vec<u8>);

impl EncryptedKey {
    pub fn new(key: Vec<u8>) -> Result<Self> {
        if key.is_empty() {
            Err(empty_key_contents("Encrypted Key"))
        } else {
            Ok(Self(key))
        }
    }
}

impl std::ops::Deref for EncryptedKey {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct DataKeyPair {
    pub encrypted: EncryptedKey,
    pub plaintext: Vec<u8>,
}

pub trait KmsProvider: Sync + Send + 'static + std::fmt::Debug {
    fn generate_data_key(&self, runtime: &mut Runtime) -> Result<DataKeyPair>;
    fn decrypt_data_key(&self, runtime: &mut Runtime, data_key: &EncryptedKey) -> Result<Vec<u8>>;
    fn name(&self) -> &[u8];
}

// fake is used for testing
pub mod fake {
    use super::*;

    const FAKE_VENDOR_NAME: &[u8] = b"FAKE";
    const FAKE_DATA_KEY_ENCRYPTED: &[u8] = b"encrypted                       ";

    #[derive(Debug)]
    pub struct FakeKms {
        plaintext_key: Vec<u8>,
    }

    impl FakeKms {
        pub fn new(plaintext_key: Vec<u8>) -> Self {
            Self { plaintext_key }
        }
    }

    impl KmsProvider for FakeKms {
        fn generate_data_key(&self, _runtime: &mut Runtime) -> Result<DataKeyPair> {
            Ok(DataKeyPair {
                encrypted: EncryptedKey::new(FAKE_DATA_KEY_ENCRYPTED.to_vec())?,
                plaintext: self.plaintext_key.clone(),
            })
        }

        fn decrypt_data_key(
            &self,
            _runtime: &mut Runtime,
            _ciphertext: &EncryptedKey,
        ) -> Result<Vec<u8>> {
            Ok(vec![1u8, 32])
        }

        fn name(&self) -> &[u8] {
            FAKE_VENDOR_NAME
        }
    }
}
