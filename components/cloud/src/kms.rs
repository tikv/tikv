use crate::error::{Error, KmsError, Result};
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
            let msg = "KMS key id can not be empty";
            Err(Error::KmsError(KmsError::EmptyKey(msg.to_owned())))
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
            Err(Error::KmsError(KmsError::EmptyKey(
                "Encrypted Key".to_owned(),
            )))
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

// PlainKey is a newtype used to mark a vector a plaintext key.
// It requires the vec to be a valid AesGcmCrypter key.
pub struct PlainKey(Vec<u8>);

impl PlainKey {
    pub fn new(key: Vec<u8>) -> Result<Self> {
        // TODO: crypter.rs in encryption performs additional validation
        Ok(Self(key))
    }
}

impl std::ops::Deref for PlainKey {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

// Don't expose the key in a debug print
impl std::fmt::Debug for PlainKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("PlainKey")
            .field(&"REDACTED".to_string())
            .finish()
    }
}

#[derive(Debug)]
pub struct DataKeyPair {
    pub encrypted: EncryptedKey,
    pub plaintext: PlainKey,
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
        plaintext_key: PlainKey,
    }

    impl FakeKms {
        pub fn new(plaintext_key: Vec<u8>) -> Self {
            Self {
                plaintext_key: PlainKey::new(plaintext_key).unwrap(),
            }
        }
    }

    impl KmsProvider for FakeKms {
        fn generate_data_key(&self, _runtime: &mut Runtime) -> Result<DataKeyPair> {
            Ok(DataKeyPair {
                encrypted: EncryptedKey::new(FAKE_DATA_KEY_ENCRYPTED.to_vec())?,
                plaintext: PlainKey::new(self.plaintext_key.clone()).unwrap(),
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
