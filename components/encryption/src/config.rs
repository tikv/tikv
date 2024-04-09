// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use cloud::kms::{SubConfigAzure, SubConfigGcp};
use kvproto::encryptionpb::{EncryptionMethod, MasterKeyKms};
use online_config::OnlineConfig;
use serde_derive::{Deserialize, Serialize};
use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptionConfig {
    // Encryption configs.
    #[serde(with = "encryption_method_serde")]
    #[online_config(skip)]
    pub data_encryption_method: EncryptionMethod,
    #[online_config(skip)]
    pub data_key_rotation_period: ReadableDuration,
    #[online_config(skip)]
    pub enable_file_dictionary_log: bool,
    #[online_config(skip)]
    pub file_dictionary_rewrite_threshold: u64,
    #[online_config(skip)]
    pub master_key: MasterKeyConfig,
    #[online_config(skip)]
    pub previous_master_key: MasterKeyConfig,
}

impl Default for EncryptionConfig {
    fn default() -> EncryptionConfig {
        EncryptionConfig {
            data_encryption_method: EncryptionMethod::Plaintext,
            data_key_rotation_period: ReadableDuration::days(7),
            // The option is available since TiKV 4.0.9.
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 1000000,
            master_key: MasterKeyConfig::default(),
            previous_master_key: MasterKeyConfig::default(),
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FileConfig {
    pub path: String,
}

// TODO: the representation of Azure KMS to users needs to be discussed.
#[derive(Clone, Default, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct AzureConfig {
    pub tenant_id: String,
    pub client_id: String,

    /// Url to access KeyVault
    pub keyvault_url: String,
    /// Key name in the HSM
    pub hsm_name: String,
    /// Url to access HSM
    pub hsm_url: String,
    /// Authorized certificate, the certificate is expected to be in base64
    /// encoded PKCS12 format
    pub client_certificate: Option<String>,
    /// Path of local authorized certificate
    pub client_certificate_path: Option<String>,
    /// Password for the certificate
    pub client_certificate_password: String,
    /// Secret of the client.
    pub client_secret: Option<String>,
}

impl std::fmt::Debug for AzureConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureConfig")
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .field("keyvault_url", &self.keyvault_url)
            .field("hsm_name", &self.hsm_name)
            .field("hsm_url", &self.hsm_url)
            .finish()
    }
}

// TODO: the representation of GCP KMS to users needs to be discussed.
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcpConfig {
    /// User credential file path. Currently, only service account and
    /// authorized user are supported. If set to None, will try to build the
    /// `TokenProvider` following the "Google Default Credentials" flow.
    pub credential_file_path: Option<String>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct KmsConfig {
    pub key_id: String,
    pub region: String,
    pub endpoint: String,
    pub vendor: String,
    // followings are used for Azure Kms
    #[online_config(skip)]
    pub azure: Option<AzureConfig>,
    // Gcp Kms configuration.
    #[online_config(skip)]
    pub gcp: Option<GcpConfig>,
}

impl KmsConfig {
    pub fn into_proto(self) -> MasterKeyKms {
        MasterKeyKms {
            key_id: self.key_id,
            region: self.region,
            endpoint: self.endpoint,
            vendor: self.vendor,
            ..MasterKeyKms::default()
        }
    }

    pub fn convert_to_azure_kms_config(self) -> (MasterKeyKms, SubConfigAzure) {
        let azure_kms_cfg = {
            let cfg = self.azure.unwrap();
            SubConfigAzure {
                tenant_id: cfg.tenant_id,
                client_id: cfg.client_id,
                keyvault_url: cfg.keyvault_url,
                hsm_name: cfg.hsm_name,
                hsm_url: cfg.hsm_url,
                client_certificate: cfg.client_certificate,
                client_certificate_path: cfg.client_certificate_path,
                client_certificate_password: cfg.client_certificate_password,
                client_secret: cfg.client_secret,
            }
        };
        let mk = MasterKeyKms {
            key_id: self.key_id,
            region: self.region,
            endpoint: self.endpoint,
            vendor: self.vendor,
            ..MasterKeyKms::default()
        };
        (mk, azure_kms_cfg)
    }

    pub fn convert_to_gcp_config(self) -> (MasterKeyKms, SubConfigGcp) {
        let gcp_cfg = SubConfigGcp {
            credential_file_path: self.gcp.unwrap().credential_file_path,
        };
        let mk = MasterKeyKms {
            key_id: self.key_id,
            region: self.region,
            endpoint: self.endpoint,
            vendor: self.vendor,
            ..MasterKeyKms::default()
        };
        (mk, gcp_cfg)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "kebab-case", tag = "type")]
#[derive(Default)]
pub enum MasterKeyConfig {
    // Store encryption metadata as plaintext. Data still get encrypted. Not allowed to use if
    // encryption is enabled. (i.e. when encryption_config.method != Plaintext).
    #[default]
    Plaintext,

    // Pass master key from a file, with key encoded as a readable hex string. The file should end
    // with newline.
    #[serde(rename_all = "kebab-case")]
    File {
        #[serde(flatten)]
        config: FileConfig,
    },

    #[serde(rename_all = "kebab-case")]
    Kms {
        #[serde(flatten)]
        config: KmsConfig,
    },
}

mod encryption_method_serde {
    use std::fmt;

    use serde::{
        de::{self, Unexpected, Visitor},
        Deserializer, Serializer,
    };

    use super::EncryptionMethod;

    const UNKNOWN: &str = "unknown";
    const PLAINTEXT: &str = "plaintext";
    const AES128_CTR: &str = "aes128-ctr";
    const AES192_CTR: &str = "aes192-ctr";
    const AES256_CTR: &str = "aes256-ctr";
    const SM4_CTR: &str = "sm4-ctr";

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(method: &EncryptionMethod, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match method {
            EncryptionMethod::Unknown => serializer.serialize_str(UNKNOWN),
            EncryptionMethod::Plaintext => serializer.serialize_str(PLAINTEXT),
            EncryptionMethod::Aes128Ctr => serializer.serialize_str(AES128_CTR),
            EncryptionMethod::Aes192Ctr => serializer.serialize_str(AES192_CTR),
            EncryptionMethod::Aes256Ctr => serializer.serialize_str(AES256_CTR),
            EncryptionMethod::Sm4Ctr => serializer.serialize_str(SM4_CTR),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EncryptionMethod, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EncryptionMethodVisitor;

        impl<'de> Visitor<'de> for EncryptionMethodVisitor {
            type Value = EncryptionMethod;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, "valid encryption method")
            }

            fn visit_str<E>(self, value: &str) -> Result<EncryptionMethod, E>
            where
                E: de::Error,
            {
                match value.to_lowercase().as_ref() {
                    UNKNOWN => Ok(EncryptionMethod::Unknown),
                    PLAINTEXT => Ok(EncryptionMethod::Plaintext),
                    AES128_CTR => Ok(EncryptionMethod::Aes128Ctr),
                    AES192_CTR => Ok(EncryptionMethod::Aes192Ctr),
                    AES256_CTR => Ok(EncryptionMethod::Aes256Ctr),
                    SM4_CTR => Ok(EncryptionMethod::Sm4Ctr),
                    _ => Err(E::invalid_value(Unexpected::Str(value), &self)),
                }
            }
        }

        deserializer.deserialize_str(EncryptionMethodVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kms_config() {
        let kms_config = EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                    vendor: "".to_owned(),
                    azure: None,
                    gcp: None,
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 1000000,
        };
        let kms_config_azure = EncryptionConfig {
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                    vendor: "azure".to_owned(),
                    azure: Some(AzureConfig {
                        tenant_id: "tenant_id".to_owned(),
                        client_id: "client_id".to_owned(),
                        keyvault_url: "keyvault_url".to_owned(),
                        hsm_name: "hsm_name".to_owned(),
                        hsm_url: "hsm_url".to_owned(),
                        ..AzureConfig::default()
                    }),
                    gcp: None,
                },
            },
            ..kms_config.clone()
        };

        let kms_config_gcp = EncryptionConfig {
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                    vendor: "gcp".to_owned(),
                    azure: None,
                    gcp: Some(GcpConfig {
                        credential_file_path: Some("/tmp/credential.json".into()),
                    }),
                },
            },
            ..kms_config.clone()
        };

        // KMS with default(aws).
        let kms_str = r#"
            data-encryption-method = "aes128-ctr"
            data-key-rotation-period = "14d"
            enable-file-dictionary-log = true
            file-dictionary-rewrite-threshold = 1000000
            [previous-master-key]
            type = "plaintext"
            [master-key]
            type = "kms"
            key-id = "key_id"
            region = "region"
            endpoint = "endpoint"
        "#;
        // KMS with azure
        let kms_str_azure = r#"
            data-encryption-method = 'aes128-ctr'
            data-key-rotation-period = '14d'
            enable-file-dictionary-log = true
            file-dictionary-rewrite-threshold = 1000000

            [master-key]
            type = 'kms'
            key-id = 'key_id'
            region = 'region'
            endpoint = 'endpoint'
            vendor = 'azure'

            [master-key.azure]
            tenant-id = 'tenant_id'
            client-id = 'client_id'
            keyvault-url = 'keyvault_url'
            hsm-name = 'hsm_name'
            hsm-url = 'hsm_url'

            [previous-master-key]
            type = 'plaintext'
        "#;
        // KMS with gcp
        let kms_str_gcp = r#"
            data-encryption-method = 'aes128-ctr'
            data-key-rotation-period = '14d'
            enable-file-dictionary-log = true
            file-dictionary-rewrite-threshold = 1000000

            [master-key]
            type = 'kms'
            key-id = 'key_id'
            region = 'region'
            endpoint = 'endpoint'
            vendor = 'gcp'

            [master-key.gcp]
            credential-file-path = '/tmp/credential.json'
        "#;
        for (kms_cfg, kms_str) in [
            (kms_config, kms_str),
            (kms_config_azure, kms_str_azure),
            (kms_config_gcp, kms_str_gcp),
        ] {
            let cfg: EncryptionConfig = toml::from_str(kms_str).unwrap();
            assert_eq!(
                cfg,
                kms_cfg.clone(),
                "\n{}\n",
                toml::to_string_pretty(&kms_cfg).unwrap()
            );
        }
    }
}
