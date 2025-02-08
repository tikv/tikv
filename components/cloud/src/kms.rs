// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use async_trait::async_trait;
use derive_more::Deref;
use kvproto::encryptionpb::MasterKeyKms;
use tikv_util::box_err;

use crate::error::{Error, KmsError, OtherError, Result};

#[derive(Debug, Clone)]
pub struct Location {
    pub region: String,
    pub endpoint: String,
}

/// Configurations for Azure KMS.
#[derive(Debug, Default, Clone)]
pub struct SubConfigAzure {
    pub tenant_id: String,
    pub client_id: String,

    /// Url to access KeyVault
    pub keyvault_url: String,
    /// Key name in the HSM
    pub hsm_name: String,
    /// Url to access HSM
    pub hsm_url: String,
    /// Authorized certificate
    pub client_certificate: Option<String>,
    /// Path of local authorized certificate
    pub client_certificate_path: Option<String>,
    /// Password for the certificate
    pub client_certificate_password: String,
    /// Secret of the client.
    pub client_secret: Option<String>,
}

/// Configurations for GCP KMS.
#[derive(Debug, Default, Clone)]
pub struct SubConfigGcp {
    pub credential_file_path: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct SubConfigAws {
    pub access_key: Option<String>,
    pub secret_access_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub key_id: KeyId,
    pub location: Location,
    pub vendor: String,
    pub azure: Option<SubConfigAzure>,
    pub gcp: Option<SubConfigGcp>,
    pub aws: Option<SubConfigAws>,
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
            azure: None,
            gcp: None,
            aws: None,
        })
    }

    pub fn from_azure_kms_config(mk: MasterKeyKms, azure_kms_cfg: SubConfigAzure) -> Result<Self> {
        let mut cfg = Config::from_proto(mk)?;
        cfg.azure = Some(azure_kms_cfg);
        Ok(cfg)
    }

    pub fn from_gcp_kms_config(mk: MasterKeyKms, gcp_kms_cfg: SubConfigGcp) -> Result<Self> {
        let mut cfg = Config::from_proto(mk)?;
        cfg.gcp = Some(gcp_kms_cfg);
        Ok(cfg)
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

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for KeyId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
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

    pub fn into_inner(self) -> Vec<u8> {
        self.0
    }

    pub fn as_raw(&self) -> &[u8] {
        &self.0
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CryptographyType {
    Plain = 0,
    AesGcm256,
    // ..
}

impl CryptographyType {
    #[inline]
    pub fn target_key_size(&self) -> usize {
        match self {
            CryptographyType::Plain => 0, // Plain text has no limitation
            CryptographyType::AesGcm256 => 32,
        }
    }
}

// PlainKey is a newtype used to mark a vector a plaintext key.
// It requires the vec to be a valid AesGcmCrypter key.
pub struct PlainKey {
    tag: CryptographyType,
    key: Vec<u8>,
}

impl PlainKey {
    pub fn new(key: Vec<u8>, t: CryptographyType) -> Result<Self> {
        let limitation = t.target_key_size();
        if limitation > 0 && key.len() != limitation {
            Err(Error::KmsError(KmsError::Other(OtherError::from_box(
                box_err!(
                    "encryption method and key length mismatch, expect {} get
                    {}",
                    limitation,
                    key.len()
                ),
            ))))
        } else {
            Ok(Self { key, tag: t })
        }
    }

    pub fn key_tag(&self) -> CryptographyType {
        self.tag
    }
}

impl core::ops::Deref for PlainKey {
    type Target = Vec<u8>;
    fn deref(&self) -> &Self::Target {
        &self.key
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

/// `Key Management Service Provider`, serving for managing master key on
/// different cloud.
#[async_trait]
pub trait KmsProvider: Sync + Send + 'static + std::fmt::Debug {
    async fn generate_data_key(&self) -> Result<DataKeyPair>;
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>>;
    fn name(&self) -> &str;
}
