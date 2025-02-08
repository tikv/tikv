// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use azure_core::{auth::TokenCredential, new_http_client, Error as AzureError};
use azure_identity::{
    AutoRefreshingTokenCredential, ClientSecretCredential, TokenCredentialOptions,
};
use azure_security_keyvault::{prelude::*, KeyClient};
use cloud::{
    error::{Error as CloudError, KmsError, OtherError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey},
};
use tikv_util::box_err;

use crate::{ClientCertificateCredentialExt, STORAGE_VENDOR_NAME_AZURE};

/// Use 256 bits for data key as default.
const DEFAULT_DATAKEY_SIZE: u8 = 32;
const ENCRYPTION_VENDOR_NAME_AZURE_KMS: &str = STORAGE_VENDOR_NAME_AZURE;

pub struct AzureKms {
    tenant_id: String,
    client_id: String,
    /// Keyvault client to encrypt/decrypt data key.
    client: KeyClient,
    current_key_id: KeyId,
    keyvault_url: String,
    /// Hsm client to get random bytes for generating data key.
    hsm_client: KeyClient,
    hsm_name: String,
    hsm_url: String,
}

impl std::fmt::Debug for AzureKms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keyvault_client = AzureKeyVaultClientDebug {
            tenant_id: self.tenant_id.clone(),
            client_id: self.client_id.clone(),
            keyvault_url: self.keyvault_url.clone(),
        };
        let hsm_client = AzureHsmClientDebug {
            hsm_name: self.hsm_name.clone(),
            hsm_url: self.hsm_url.clone(),
        };
        f.debug_struct("AzureKms")
            .field("keyvault_client", &keyvault_client)
            .field("current_key_id", &self.current_key_id)
            .field("hsm_client", &hsm_client)
            .finish()
    }
}

impl AzureKms {
    #[inline]
    fn new_with_credentials<Creds>(
        config: Config,
        keyvault_credentials: Creds,
        hsm_credentials: Creds,
    ) -> Result<Self>
    where
        Creds: TokenCredential + Send + Sync + 'static,
    {
        assert!(config.azure.is_some());
        let azure_cfg = config.azure.unwrap();
        let keyvault_client = new_key_client(&azure_cfg.keyvault_url, keyvault_credentials)?;
        let hsm_client = new_key_client(&azure_cfg.hsm_url, hsm_credentials)?;
        Ok(Self {
            client: keyvault_client,
            current_key_id: config.key_id,
            tenant_id: azure_cfg.tenant_id,
            client_id: azure_cfg.client_id,
            keyvault_url: azure_cfg.keyvault_url,
            hsm_client,
            hsm_name: azure_cfg.hsm_name,
            hsm_url: azure_cfg.hsm_url,
        })
    }

    pub fn new(config: Config) -> Result<Self> {
        assert!(config.azure.is_some());
        let azure_cfg = config.azure.clone().unwrap();
        // Priority: explicit certificate > path of local certificate > client secret.
        // And credentials for accessing KeyVault and Hsm should be different.
        if let Some(certificate) = azure_cfg.client_certificate.clone() {
            // Certificate to accessing KeyVault.
            let (keyvault_credential, hsm_credential) = (
                ClientCertificateCredentialExt::new(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    certificate.clone(),
                    azure_cfg.client_certificate_password.clone(),
                ),
                ClientCertificateCredentialExt::new(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id,
                    certificate,
                    azure_cfg.client_certificate_password,
                ),
            );
            Self::new_with_credentials(config, keyvault_credential, hsm_credential)
        } else if let Some(certificate_path) = azure_cfg.client_certificate_path.clone() {
            // Certificate recorded in a file to accessing KeyVault.
            let (keyvault_credential, hsm_credential) = (
                ClientCertificateCredentialExt::build(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    certificate_path.clone(),
                    azure_cfg.client_certificate_password.clone(),
                )
                .map_err(|e| CloudError::Other(e))?,
                ClientCertificateCredentialExt::build(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id,
                    certificate_path,
                    azure_cfg.client_certificate_password,
                )
                .map_err(|e| CloudError::Other(e))?,
            );
            Self::new_with_credentials(config, keyvault_credential, hsm_credential)
        } else if let Some(client_secret) = azure_cfg.client_secret.clone() {
            // Client secret to access KeyVault.
            let (keyvault_credential, hsm_credential) = (
                ClientSecretCredential::new(
                    new_http_client(),
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    client_secret.clone(),
                    TokenCredentialOptions::default(),
                ),
                ClientSecretCredential::new(
                    new_http_client(),
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id,
                    client_secret,
                    TokenCredentialOptions::default(),
                ),
            );
            Self::new_with_credentials(config, keyvault_credential, hsm_credential)
        } else {
            Err(CloudError::KmsError(KmsError::Other(OtherError::from_box(
                box_err!("invalid configurations for Azure KMS"),
            ))))
        }
    }
}

#[async_trait]
impl KmsProvider for AzureKms {
    fn name(&self) -> &str {
        ENCRYPTION_VENDOR_NAME_AZURE_KMS
    }

    // On decrypt failure, the rule is to return WrongMasterKey error in case it is
    // possible that a wrong master key has been used, or other error
    // otherwise.
    async fn decrypt_data_key(&self, data_key: &EncryptedKey) -> Result<Vec<u8>> {
        let decrypt_params = DecryptParameters {
            ciphertext: data_key.clone().into_inner(),
            // TODO: the final choice of encryption algorithm for Azure waited
            // to be discussed. And as the AesGcm only valid for HSM,
            // encrypt/decrypt just uses the Rsa256 as default currently.
            decrypt_parameters_encryption: CryptographParamtersEncryption::Rsa(
                RsaEncryptionParameters::new(EncryptionAlgorithm::RsaOaep256).unwrap(),
            ),
        };
        self.client
            .decrypt(self.current_key_id.deref().clone(), decrypt_params)
            .await
            .map_err(convert_azure_error)
            .map(|response| response.result.to_vec())
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        // Firstly, it should use `GetRandomBytes` API to get random bytes,
        // generated by remote Azure, as the plaintext of a new data key.
        let random_bytes = {
            let resp = self
                .hsm_client
                .get_random_bytes(&self.hsm_name, DEFAULT_DATAKEY_SIZE)
                .await
                .map_err(convert_azure_error)?;
            resp.result
        };
        let encrypt_params = EncryptParameters {
            plaintext: random_bytes.clone(),
            encrypt_parameters_encryption: CryptographParamtersEncryption::Rsa(
                RsaEncryptionParameters::new(EncryptionAlgorithm::RsaOaep256).unwrap(),
            ),
        };
        self.client
            .encrypt(&self.current_key_id.clone().into_inner(), encrypt_params)
            .await
            .map_err(convert_azure_error)
            .and_then(|response| {
                let ciphertext = response.result;
                Ok(DataKeyPair {
                    encrypted: EncryptedKey::new(ciphertext)?,
                    plaintext: PlainKey::new(random_bytes, CryptographyType::AesGcm256)?,
                })
            })
    }
}

struct AzureKeyVaultClientDebug {
    tenant_id: String,
    client_id: String,
    keyvault_url: String,
}

impl std::fmt::Debug for AzureKeyVaultClientDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureKeyVaultClientDebug")
            .field("tenant_id", &self.tenant_id)
            .field("client_id", &self.client_id)
            .field("keyvault_url", &self.keyvault_url)
            .finish()
    }
}

struct AzureHsmClientDebug {
    hsm_name: String,
    hsm_url: String,
}

impl std::fmt::Debug for AzureHsmClientDebug {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AzureHsmClientDebug")
            .field("hsm_name", &self.hsm_name)
            .field("hsm_url", &self.hsm_url)
            .finish()
    }
}

fn convert_azure_error(err: AzureError) -> CloudError {
    let err_msg = if let Ok(e) = err.into_inner() {
        e
    } else {
        Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "unknown error",
        ))
    };
    CloudError::KmsError(KmsError::Other(OtherError::from_box(err_msg)))
}

#[inline]
fn new_key_client<Creds>(url: &str, credentials: Creds) -> Result<KeyClient>
where
    Creds: TokenCredential + Send + Sync + 'static,
{
    KeyClient::new(
        url,
        Arc::new(AutoRefreshingTokenCredential::new(Arc::new(credentials))),
    )
    .map_err(|e| CloudError::Other(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use cloud::kms::{Location, SubConfigAzure};

    use super::*;

    #[test]
    fn test_init_azure_kms() {
        let err_azure_cfg = SubConfigAzure {
            tenant_id: "tenant_id".to_owned(),
            client_id: "client_id".to_owned(),
            keyvault_url: "https://keyvault_url.vault.azure.net".to_owned(),
            hsm_name: "hsm_name".to_owned(),
            hsm_url: "https://hsm_url.managedhsm.azure.net/".to_owned(),
            ..SubConfigAzure::default()
        };
        let err_config = Config {
            key_id: KeyId::new("test_key_id".to_string()).unwrap(),
            vendor: STORAGE_VENDOR_NAME_AZURE.to_owned(),
            location: Location {
                region: "southeast".to_string(),
                endpoint: String::new(),
            },
            azure: Some(err_azure_cfg.clone()),
            gcp: None,
            aws: None,
        };
        AzureKms::new(err_config.clone()).unwrap_err();
        let azure_cfg = SubConfigAzure {
            client_secret: Some("client_secret".to_owned()),
            ..err_azure_cfg
        };
        let config = Config {
            azure: Some(azure_cfg),
            ..err_config
        };
        let azure_kms = AzureKms::new(config).unwrap();
        assert_eq!(
            azure_kms.name(),
            STORAGE_VENDOR_NAME_AZURE,
            "{:?}",
            azure_kms
        );
    }

    #[tokio::test]
    async fn test_azure_kms() {
        // TODO: this is End2End test for testing the API connectivity
        // and validity of AzureKms. And if you wanna to use
        // this case, you should set a valid configuration for it.
        let azure_cfg = SubConfigAzure {
            tenant_id: "tenant_id".to_owned(),
            client_id: "client_id".to_owned(),
            keyvault_url: "https://keyvault_url.vault.azure.net".to_owned(),
            hsm_name: "hsm_name".to_owned(),
            hsm_url: "https://hsm_url.managedhsm.azure.net/".to_owned(),
            client_certificate: Some("client_certificate".to_owned()),
            client_certificate_path: Some("client_certificate_path".to_owned()),
            client_secret: Some("client_secret".to_owned()),
            ..SubConfigAzure::default()
        };
        let config = Config {
            key_id: KeyId::new("ExampleKey".to_string()).unwrap(),
            vendor: "STORAGE_VENDOR_NAME_AZURE".to_owned(),
            location: Location {
                region: "us-west".to_string(),
                endpoint: String::new(),
            },
            azure: Some(azure_cfg),
            gcp: None,
            aws: None,
        };
        if config.vendor != STORAGE_VENDOR_NAME_AZURE {
            AzureKms::new(config).unwrap();
        } else {
            // Unless the configurations of Azure KMS is valid, following
            // codes could be executed.
            let azure_kms = AzureKms::new(config).unwrap();
            let data_key = azure_kms.generate_data_key().await.unwrap();
            let encrypted_data_key = EncryptedKey::new(data_key.encrypted.to_vec()).unwrap();
            let decrypt_data_key = azure_kms
                .decrypt_data_key(&encrypted_data_key)
                .await
                .unwrap();
            assert_eq!(*data_key.plaintext, decrypt_data_key);
        }
    }
}
