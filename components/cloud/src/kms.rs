use async_trait::async_trait;

use crate::error::{Error, KmsError, Result};
use derive_more::Deref;
use kvproto::encryptionpb::MasterKeyKms;

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

#[derive(PartialEq, Debug, Clone, Deref)]
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

// EncryptedKey is a newtype used to mark data as an encrypted key
// It requires the vec to be non-empty
#[derive(PartialEq, Clone, Debug, Deref)]
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

// PlainKey is a newtype used to mark a vector a plaintext key.
// It requires the vec to be a valid AesGcmCrypter key.
#[derive(Deref)]
pub struct PlainKey(Vec<u8>);

impl PlainKey {
    pub fn new(key: Vec<u8>) -> Result<Self> {
        // TODO: crypter.rs in encryption performs additional validation
        Ok(Self(key))
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

#[async_trait]
pub trait KmsProvider: Sync + Send + 'static + std::fmt::Debug {
    async fn generate_data_key(&self) -> Result<DataKeyPair>;
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>>;
    fn name(&self) -> &[u8];
}
