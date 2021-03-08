// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use kvproto::encryptionpb::{EncryptionMethod, MasterKeyKms};
use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EncryptionConfig {
    // Encryption configs.
    #[serde(with = "encryption_method_serde")]
    #[config(skip)]
    pub data_encryption_method: EncryptionMethod,
    #[config(skip)]
    pub data_key_rotation_period: ReadableDuration,
    #[config(skip)]
    pub enable_file_dictionary_log: bool,
    #[config(skip)]
    pub file_dictionary_rewrite_threshold: u64,
    #[config(skip)]
    pub master_key: MasterKeyConfig,
    #[config(skip)]
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

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FileConfig {
    pub path: String,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Configuration)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct KmsConfig {
    pub key_id: String,
    pub region: String,
    pub endpoint: String,
    pub vendor: String,
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
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case", tag = "type")]
pub enum MasterKeyConfig {
    // Store encryption metadata as plaintext. Data still get encrypted. Not allowed to use if
    // encryption is enabled. (i.e. when encryption_config.method != Plaintext).
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

impl Default for MasterKeyConfig {
    fn default() -> Self {
        MasterKeyConfig::Plaintext
    }
}

mod encryption_method_serde {
    use super::EncryptionMethod;
    use std::fmt;

    use serde::de::{self, Unexpected, Visitor};
    use serde::{Deserializer, Serializer};

    const UNKNOWN: &str = "unknown";
    const PLAINTEXT: &str = "plaintext";
    const AES128_CTR: &str = "aes128-ctr";
    const AES192_CTR: &str = "aes192-ctr";
    const AES256_CTR: &str = "aes256-ctr";

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
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<EncryptionMethod, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EncryptionMethodVisitor;

        impl<'de> Visitor<'de> for EncryptionMethodVisitor {
            type Value = EncryptionMethod;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
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
        let kms_cfg = EncryptionConfig {
            data_encryption_method: EncryptionMethod::Aes128Ctr,
            data_key_rotation_period: ReadableDuration::days(14),
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                    vendor: "".to_owned(),
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 1000000,
        };
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
        let cfg: EncryptionConfig = toml::from_str(kms_str).unwrap();
        assert_eq!(
            cfg,
            kms_cfg,
            "\n{}\n",
            toml::to_string_pretty(&kms_cfg).unwrap()
        );
    }
}
