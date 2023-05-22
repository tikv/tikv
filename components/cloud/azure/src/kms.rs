// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use async_trait::async_trait;
use azure_core::{auth::TokenCredential, new_http_client, Error as AzureError};
use azure_identity::{
    AutoRefreshingTokenCredential, ClientSecretCredential, TokenCredentialOptions,
};
use azure_security_keyvault::{prelude::*, KeyClient};
use cloud::{
    error::{Error as CloudError, KmsError, Result},
    kms::{Config, CryptographyType, DataKeyPair, EncryptedKey, KeyId, KmsProvider, PlainKey},
};
use tikv_util::box_err;

use crate::ClientCertificateCredentialExt;

/// Use 256 bits for data key as default.
const DEFAULT_DATAKEY_SIZE: u8 = 32;
const ENCRYPTION_VENDOR_NAME_AZURE_KMS: &str = "Azure";

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
        let keyvault_client = KeyClient::new(
            &azure_cfg.keyvault_url,
            Arc::new(AutoRefreshingTokenCredential::new(Arc::new(
                keyvault_credentials,
            ))),
        )
        .map_err(|e| CloudError::Other(Box::new(e)))?;
        let hsm_client = KeyClient::new(
            &azure_cfg.hsm_url,
            Arc::new(AutoRefreshingTokenCredential::new(Arc::new(
                hsm_credentials,
            ))),
        )
        .map_err(|e| CloudError::Other(Box::new(e)))?;
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
                    "".to_owned(),
                ),
                ClientCertificateCredentialExt::new(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    certificate,
                    "".to_owned(),
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
                )
                .map_err(|e| CloudError::Other(e))?,
                ClientCertificateCredentialExt::build(
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    certificate_path,
                )
                .map_err(|e| CloudError::Other(e))?,
            );
            Self::new_with_credentials(config, keyvault_credential, hsm_credential)
        } else if let Some(_) = azure_cfg.client_secret.clone() {
            // Client secret to access KeyVault.
            let (keyvault_credential, hsm_credential) = (
                ClientSecretCredential::new(
                    new_http_client(),
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    azure_cfg.client_secret.clone().unwrap(),
                    TokenCredentialOptions::default(),
                ),
                ClientSecretCredential::new(
                    new_http_client(),
                    azure_cfg.tenant_id.clone(),
                    azure_cfg.client_id.clone(),
                    azure_cfg.client_secret.clone().unwrap(),
                    TokenCredentialOptions::default(),
                ),
            );
            Self::new_with_credentials(config, keyvault_credential, hsm_credential)
        } else {
            Err(CloudError::KmsError(KmsError::Other(box_err!(
                "invalid configurations for Azure KMS"
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
                let plaintext_key = response.result;
                Ok(DataKeyPair {
                    encrypted: EncryptedKey::new(random_bytes)?,
                    plaintext: PlainKey::new(plaintext_key, CryptographyType::AesGcm256)?,
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
    CloudError::KmsError(KmsError::Other(err_msg))
}
