// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::result;

use cloud::{
    kms::{SubConfigAws, SubConfigAzure, SubConfigGcp},
    Config as CloudConfig,
};
use kvproto::encryptionpb::{EncryptionMethod, MasterKey, MasterKeyKms, MasterKey_oneof_backend};
use online_config::OnlineConfig;
use serde_derive::{Deserialize, Serialize};
use tikv_util::{box_err, config::ReadableDuration};

use crate::Error;

pub type Result<T> = result::Result<T, Error>;

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

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FileConfig {
    pub path: String,
}

// TODO: the representation of Azure KMS to users needs to be discussed.
#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Eq, Hash)]
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
#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct GcpConfig {
    /// User credential file path. Currently, only service account and
    /// authorized user are supported. If set to None, will try to build the
    /// `TokenProvider` following the "Google Default Credentials" flow.
    pub credential_file_path: Option<String>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct AwsConfig {
    /// optional since can read from env if not directly passed from user.
    pub access_key: Option<String>,
    /// optional since can read from env if not directly passed from user.
    pub secret_access_key: Option<String>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq, OnlineConfig, Eq, Hash)]
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
    // optional aws Kms configuration.
    #[online_config(skip)]
    pub aws: Option<AwsConfig>,
}

// Note: could merge SubConfigAzure and SubConfigGcp into KmsConfig
impl KmsConfig {
    pub fn from_proto(proto: &MasterKeyKms) -> Self {
        let mut config = KmsConfig {
            key_id: proto.key_id.clone(),
            region: proto.region.clone(),
            endpoint: proto.endpoint.clone(),
            vendor: proto.vendor.clone(),
            azure: None,
            gcp: None,
            aws: None,
        };
        if proto.has_azure_kms() {
            let azure_config_proto = proto.azure_kms.as_ref().unwrap();
            let azure_config = AzureConfig {
                tenant_id: azure_config_proto.tenant_id.clone(),
                client_id: azure_config_proto.client_id.clone(),
                keyvault_url: azure_config_proto.key_vault_url.clone(),
                hsm_name: azure_config_proto.hsm_name.clone(),
                hsm_url: azure_config_proto.hsm_url.clone(),
                client_certificate: string_to_option(azure_config_proto.client_certificate.clone()),
                client_certificate_path: string_to_option(
                    azure_config_proto.client_certificate_path.clone(),
                ),
                client_certificate_password: azure_config_proto.client_certificate_password.clone(),
                client_secret: string_to_option(azure_config_proto.client_secret.clone()),
            };
            config.azure = Some(azure_config);
        }
        if proto.has_gcp_kms() {
            let gcp_config_proto = proto.gcp_kms.as_ref().unwrap();
            let gcp_config = GcpConfig {
                credential_file_path: string_to_option(gcp_config_proto.credential.clone()),
            };
            config.gcp = Some(gcp_config);
        }
        if proto.has_aws_kms() {
            let aws_config_proto = proto.aws_kms.as_ref().unwrap();
            let aws_config = AwsConfig {
                access_key: string_to_option(aws_config_proto.access_key.clone()),
                secret_access_key: string_to_option(aws_config_proto.secret_access_key.clone()),
            };
            config.aws = Some(aws_config)
        }
        config
    }
    pub fn to_cloud_config(&self) -> Result<CloudConfig> {
        Ok(CloudConfig {
            key_id: cloud::kms::KeyId::new(self.key_id.clone())
                .map_err(|_| Error::Other(box_err!("key id should not be empty")))?,
            location: cloud::kms::Location {
                region: self.region.clone(),
                endpoint: self.endpoint.clone(),
            },
            vendor: self.vendor.clone(),
            azure: self.azure.as_ref().map(|azure| SubConfigAzure {
                tenant_id: azure.tenant_id.clone(),
                client_id: azure.client_id.clone(),
                keyvault_url: azure.keyvault_url.clone(),
                hsm_name: azure.hsm_name.clone(),
                hsm_url: azure.hsm_url.clone(),
                client_certificate: azure.client_certificate.clone(),
                client_certificate_path: azure.client_certificate_path.clone(),
                client_certificate_password: azure.client_certificate_password.clone(),
                client_secret: azure.client_secret.clone(),
            }),
            gcp: self.gcp.as_ref().map(|gcp| SubConfigGcp {
                credential_file_path: gcp.credential_file_path.clone(),
            }),
            aws: self.aws.as_ref().map(|aws| SubConfigAws {
                access_key: aws.access_key.clone(),
                secret_access_key: aws.secret_access_key.clone(),
            }),
        })
    }
}

fn string_to_option(string: String) -> Option<String> {
    if string.is_empty() {
        None
    } else {
        Some(string)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
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

impl MasterKeyConfig {
    pub fn from_proto(proto: &MasterKey) -> Option<Self> {
        if let Some(backend) = &proto.backend {
            match backend {
                MasterKey_oneof_backend::Plaintext(_) => Some(MasterKeyConfig::Plaintext),
                MasterKey_oneof_backend::File(key_file) => Some(MasterKeyConfig::File {
                    config: FileConfig {
                        path: key_file.path.clone(),
                    },
                }),
                MasterKey_oneof_backend::Kms(kms) => Some(MasterKeyConfig::Kms {
                    config: KmsConfig::from_proto(kms),
                }),
            }
        } else {
            None
        }
    }
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
    use kvproto::encryptionpb;

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
                    aws: None,
                },
            },
            previous_master_key: MasterKeyConfig::Plaintext,
            enable_file_dictionary_log: true,
            file_dictionary_rewrite_threshold: 1000000,
        };

        let kms_config_aws = EncryptionConfig {
            master_key: MasterKeyConfig::Kms {
                config: KmsConfig {
                    key_id: "key_id".to_owned(),
                    region: "region".to_owned(),
                    endpoint: "endpoint".to_owned(),
                    vendor: "aws".to_owned(),
                    azure: None,
                    gcp: None,
                    aws: Some(AwsConfig {
                        access_key: Some("foo".into()),
                        secret_access_key: Some("bar".into()),
                    }),
                },
            },
            ..kms_config.clone()
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
                    aws: None,
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
                    aws: None,
                },
            },
            ..kms_config.clone()
        };

        // KMS with default(aws).
        let kms_str_aws = r#"
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
            vendor = "aws"

            [master-key.aws]
            access-key = "foo"
            secret-access-key = "bar"
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
            (kms_config_aws, kms_str_aws),
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

    #[test]
    fn test_from_proto() {
        // Test case 1: Basic KMS config without vendor-specific details
        let proto = MasterKeyKms {
            key_id: "test_key".to_string(),
            region: "test_region".to_string(),
            endpoint: "test_endpoint".to_string(),
            vendor: "test_vendor".to_string(),
            ..Default::default()
        };
        let config = KmsConfig::from_proto(&proto);
        assert_eq!(config.key_id, "test_key");
        assert_eq!(config.region, "test_region");
        assert_eq!(config.endpoint, "test_endpoint");
        assert_eq!(config.vendor, "test_vendor");
        assert!(config.azure.is_none());
        assert!(config.gcp.is_none());
        assert!(config.aws.is_none());
        // Test case 2: KMS config with Azure details
        let mut proto_azure = proto.clone();
        proto_azure.azure_kms = Some(encryptionpb::AzureKms {
            tenant_id: "azure_tenant".to_string(),
            client_id: "azure_client".to_string(),
            key_vault_url: "azure_vault".to_string(),
            hsm_name: "azure_hsm".to_string(),
            hsm_url: "azure_hsm_url".to_string(),
            client_certificate: "azure_cert".to_string(),
            client_certificate_path: "azure_cert_path".to_string(),
            client_certificate_password: "azure_password".to_string(),
            client_secret: "azure_secret".to_string(),
            ..Default::default()
        })
        .into();
        let config_azure = KmsConfig::from_proto(&proto_azure);
        assert!(config_azure.azure.is_some());
        let azure_config = config_azure.azure.unwrap();
        assert_eq!(azure_config.tenant_id, "azure_tenant");
        assert_eq!(azure_config.client_id, "azure_client");
        assert_eq!(azure_config.keyvault_url, "azure_vault");
        assert_eq!(azure_config.hsm_name, "azure_hsm");
        assert_eq!(azure_config.hsm_url, "azure_hsm_url");
        assert_eq!(
            azure_config.client_certificate,
            Some("azure_cert".to_string())
        );
        assert_eq!(
            azure_config.client_certificate_path,
            Some("azure_cert_path".to_string())
        );
        assert_eq!(azure_config.client_certificate_password, "azure_password");
        assert_eq!(azure_config.client_secret, Some("azure_secret".to_string()));

        // Test case 3: KMS config with GCP details
        let mut proto_gcp = proto.clone();
        proto_gcp.gcp_kms = Some(encryptionpb::GcpKms {
            credential: "gcp_credential".to_string(),
            ..Default::default()
        })
        .into();
        let config_gcp = KmsConfig::from_proto(&proto_gcp);
        assert!(config_gcp.gcp.is_some());
        let gcp_config = config_gcp.gcp.unwrap();
        assert_eq!(
            gcp_config.credential_file_path,
            Some("gcp_credential".to_string())
        );

        // Test case 4: KMS config with AWS details
        let mut proto_aws = proto.clone();
        proto_aws.aws_kms = Some(encryptionpb::AwsKms {
            access_key: "aws_access".to_string(),
            secret_access_key: "aws_secret".to_string(),
            ..Default::default()
        })
        .into();
        let config_aws = KmsConfig::from_proto(&proto_aws);
        assert!(config_aws.aws.is_some());
        let aws_config = config_aws.aws.unwrap();
        assert_eq!(aws_config.access_key, Some("aws_access".to_string()));
        assert_eq!(aws_config.secret_access_key, Some("aws_secret".to_string()));
    }

    #[test]
    fn test_to_cloud_config() {
        // Test case 1: Basic KMS config without vendor-specific details
        let kms_config = KmsConfig {
            key_id: "test_key".to_string(),
            region: "test_region".to_string(),
            endpoint: "test_endpoint".to_string(),
            vendor: "test_vendor".to_string(),
            azure: None,
            gcp: None,
            aws: None,
        };
        let cloud_config = kms_config.to_cloud_config().unwrap();
        assert_eq!(cloud_config.key_id.as_str(), "test_key");
        assert_eq!(cloud_config.location.region, "test_region");
        assert_eq!(cloud_config.location.endpoint, "test_endpoint");
        assert_eq!(cloud_config.vendor, "test_vendor");
        assert!(cloud_config.azure.is_none());
        assert!(cloud_config.gcp.is_none());
        assert!(cloud_config.aws.is_none());

        // Test case 2: KMS config with Azure details
        let kms_config_azure = KmsConfig {
            azure: Some(AzureConfig {
                tenant_id: "azure_tenant".to_string(),
                client_id: "azure_client".to_string(),
                keyvault_url: "azure_vault".to_string(),
                hsm_name: "azure_hsm".to_string(),
                hsm_url: "azure_hsm_url".to_string(),
                client_certificate: Some("azure_cert".to_string()),
                client_certificate_path: Some("azure_cert_path".to_string()),
                client_certificate_password: "azure_password".to_string(),
                client_secret: Some("azure_secret".to_string()),
            }),
            ..kms_config.clone()
        };
        let cloud_config_azure = kms_config_azure.to_cloud_config().unwrap();
        assert!(cloud_config_azure.azure.is_some());
        let azure_config = cloud_config_azure.azure.unwrap();
        assert_eq!(azure_config.tenant_id, "azure_tenant");
        assert_eq!(azure_config.client_id, "azure_client");
        assert_eq!(azure_config.keyvault_url, "azure_vault");
        assert_eq!(azure_config.hsm_name, "azure_hsm");
        assert_eq!(azure_config.hsm_url, "azure_hsm_url");
        assert_eq!(
            azure_config.client_certificate,
            Some("azure_cert".to_string())
        );
        assert_eq!(
            azure_config.client_certificate_path,
            Some("azure_cert_path".to_string())
        );
        assert_eq!(azure_config.client_certificate_password, "azure_password");
        assert_eq!(azure_config.client_secret, Some("azure_secret".to_string()));

        // Test case 3: KMS config with GCP details
        let kms_config_gcp = KmsConfig {
            gcp: Some(GcpConfig {
                credential_file_path: Some("gcp_credential".to_string()),
            }),
            ..kms_config.clone()
        };
        let cloud_config_gcp = kms_config_gcp.to_cloud_config().unwrap();
        assert!(cloud_config_gcp.gcp.is_some());
        let gcp_config = cloud_config_gcp.gcp.unwrap();
        assert_eq!(
            gcp_config.credential_file_path,
            Some("gcp_credential".to_string())
        );

        // Test case 4: KMS config with AWS details
        let kms_config_aws = KmsConfig {
            aws: Some(AwsConfig {
                access_key: Some("aws_access".to_string()),
                secret_access_key: Some("aws_secret".to_string()),
            }),
            ..kms_config.clone()
        };
        let cloud_config_aws = kms_config_aws.to_cloud_config().unwrap();
        assert!(cloud_config_aws.aws.is_some());
        let aws_config = cloud_config_aws.aws.unwrap();
        assert_eq!(aws_config.access_key, Some("aws_access".to_string()));
        assert_eq!(aws_config.secret_access_key, Some("aws_secret".to_string()));

        // Test case 5: KMS config with empty key_id (should return an error)
        let kms_config_empty_key = KmsConfig {
            key_id: "".to_string(),
            ..kms_config.clone()
        };
        kms_config_empty_key.to_cloud_config().unwrap_err();
    }
}
