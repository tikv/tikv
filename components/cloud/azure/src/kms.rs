// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use async_trait::async_trait;
use azure_core::{Error as AzureError, credentials::TokenCredential};
use azure_identity::ClientSecretCredential;
use azure_security_keyvault_keys::{
    KeyClient, KeyClientOptions,
    models::{EncryptionAlgorithm, GetRandomBytesParameters, KeyOperationParameters},
};
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
    fn new_with_credentials(
        config: Config,
        keyvault_credentials: Arc<dyn TokenCredential>,
        hsm_credentials: Arc<dyn TokenCredential>,
    ) -> Result<Self> {
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
            Self::new_with_credentials(
                config,
                Arc::new(keyvault_credential),
                Arc::new(hsm_credential),
            )
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
            Self::new_with_credentials(
                config,
                Arc::new(keyvault_credential),
                Arc::new(hsm_credential),
            )
        } else if let Some(client_secret) = azure_cfg.client_secret.clone() {
            let keyvault_credential = ClientSecretCredential::new(
                &azure_cfg.tenant_id,
                azure_cfg.client_id.clone(),
                client_secret.clone().into(),
                None,
            )
            .map_err(convert_azure_error)?;
            let hsm_credential = ClientSecretCredential::new(
                &azure_cfg.tenant_id,
                azure_cfg.client_id,
                client_secret.into(),
                None,
            )
            .map_err(convert_azure_error)?;
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
        let parameters = KeyOperationParameters {
            value: Some(data_key.clone().into_inner()),
            algorithm: Some(EncryptionAlgorithm::RsaOaep256),
            ..Default::default()
        };
        let response = self
            .client
            .decrypt(
                &self.current_key_id,
                "",
                parameters.try_into().map_err(convert_azure_error)?,
                None,
            )
            .await
            .map_err(convert_azure_error)?;
        required_value(response.into_model().map_err(convert_azure_error)?.result)
    }

    async fn generate_data_key(&self) -> Result<DataKeyPair> {
        let parameters = GetRandomBytesParameters {
            count: Some(DEFAULT_DATAKEY_SIZE.into()),
        };
        let response = self
            .hsm_client
            .get_random_bytes(parameters.try_into().map_err(convert_azure_error)?, None)
            .await
            .map_err(convert_azure_error)?;
        let random_bytes =
            required_value(response.into_model().map_err(convert_azure_error)?.value)?;
        let parameters = KeyOperationParameters {
            value: Some(random_bytes.clone()),
            algorithm: Some(EncryptionAlgorithm::RsaOaep256),
            ..Default::default()
        };
        let response = self
            .client
            .encrypt(
                &self.current_key_id,
                parameters.try_into().map_err(convert_azure_error)?,
                None,
            )
            .await
            .map_err(convert_azure_error)?;
        let ciphertext =
            required_value(response.into_model().map_err(convert_azure_error)?.result)?;
        Ok(DataKeyPair {
            encrypted: EncryptedKey::new(ciphertext)?,
            plaintext: PlainKey::new(random_bytes, CryptographyType::AesGcm256)?,
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
    CloudError::KmsError(KmsError::Other(OtherError::from_box(Box::new(err))))
}

fn required_value(value: Option<Vec<u8>>) -> Result<Vec<u8>> {
    value.ok_or_else(|| {
        convert_azure_error(AzureError::with_message(
            azure_core::error::ErrorKind::DataConversion,
            "Azure KMS response is missing key material",
        ))
    })
}

fn new_key_client(url: &str, credentials: Arc<dyn TokenCredential>) -> Result<KeyClient> {
    KeyClient::new(
        url,
        credentials,
        Some(KeyClientOptions {
            // The legacy SDK sent 7.0. Keep the new SDK's requests on an explicit
            // 7.2 contract; validate Key Vault and Managed HSM when backporting.
            api_version: "7.2".to_owned(),
            ..Default::default()
        }),
    )
    .map_err(convert_azure_error)
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use azure_core::{
        credentials::{AccessToken, TokenRequestOptions},
        http::{
            AsyncRawResponse, Body, ClientOptions, HttpClient, Method, Request, Transport,
            headers::Headers,
        },
    };
    use cloud::kms::{Location, SubConfigAzure};
    use serde_json::json;
    use time::OffsetDateTime;

    use super::*;

    #[derive(Debug, Default)]
    struct MockCredential {
        scopes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl TokenCredential for MockCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            self.scopes
                .lock()
                .unwrap()
                .extend(scopes.iter().map(|s| s.to_string()));
            Ok(AccessToken::new(
                "test-token",
                OffsetDateTime::now_utc() + time::Duration::hours(1),
            ))
        }
    }

    #[derive(Debug, Default)]
    struct MockTransport {
        requests: Mutex<Vec<Request>>,
    }

    #[async_trait]
    impl HttpClient for MockTransport {
        async fn execute_request(&self, request: &Request) -> azure_core::Result<AsyncRawResponse> {
            let mut headers = Headers::new();
            if request
                .headers()
                .get_optional_str(&"authorization".into())
                .is_none()
            {
                // The supported SDK obtains a challenge without sending key material.
                assert_eq!(request.body().is_empty(), Some(true));
                let resource = if request
                    .url()
                    .host_str()
                    .unwrap()
                    .ends_with("managedhsm.azure.net")
                {
                    "https://managedhsm.azure.net"
                } else {
                    "https://vault.azure.net"
                };
                headers.insert("www-authenticate", format!(r#"Bearer authorization="https://login.microsoftonline.com/tenant", resource="{resource}""#));
                return Ok(AsyncRawResponse::from_bytes(401.into(), headers, ""));
            }
            assert_eq!(
                request.headers().get_optional_str(&"authorization".into()),
                Some("Bearer test-token")
            );
            self.requests.lock().unwrap().push(request.clone());
            let value = if request.url().path().ends_with("/encrypt") {
                vec![9; 48]
            } else {
                vec![7; 32]
            };
            headers.insert("content-type", "application/json");
            Ok(AsyncRawResponse::from_bytes(
                200.into(),
                headers,
                json!({"value": azure_core::base64::encode_url_safe(value)}).to_string(),
            ))
        }
    }

    #[tokio::test]
    async fn test_kms_preserves_hsm_randomness_and_rsa_oaep_round_trip() {
        let transport = Arc::new(MockTransport::default());
        let credential = Arc::new(MockCredential::default());
        let make_client = |url| {
            KeyClient::new(
                url,
                credential.clone(),
                Some(KeyClientOptions {
                    api_version: "7.2".to_owned(),
                    client_options: ClientOptions {
                        transport: Some(Transport::new(transport.clone())),
                        ..Default::default()
                    },
                    ..Default::default()
                }),
            )
            .unwrap()
        };
        let kms = AzureKms {
            tenant_id: "tenant".into(),
            client_id: "client".into(),
            client: make_client("https://test.vault.azure.net"),
            current_key_id: KeyId::new("ExampleKey".to_owned()).unwrap(),
            keyvault_url: "https://test.vault.azure.net".into(),
            hsm_client: make_client("https://test.managedhsm.azure.net"),
            hsm_name: "test".into(),
            hsm_url: "https://test.managedhsm.azure.net".into(),
        };
        let data_key = kms.generate_data_key().await.unwrap();
        assert_eq!(&*data_key.plaintext, &[7; 32]);
        assert_eq!(&*data_key.encrypted, &[9; 48]);
        assert_eq!(
            kms.decrypt_data_key(&data_key.encrypted).await.unwrap(),
            vec![7; 32]
        );

        let requests = transport.requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        for request in requests.iter() {
            assert_eq!(request.method(), Method::Post);
            assert_eq!(request.url().query(), Some("api-version=7.2"));
        }
        let body = |request: &Request| match request.body() {
            Body::Bytes(bytes) => serde_json::from_slice::<serde_json::Value>(bytes).unwrap(),
            Body::SeekableStream(_) => panic!("KMS requests should have buffered JSON bodies"),
        };
        assert_eq!(
            requests[0].url().host_str(),
            Some("test.managedhsm.azure.net")
        );
        assert_eq!(requests[0].url().path(), "/rng");
        assert_eq!(body(&requests[0]), json!({"count": 32}));
        for (request, operation, input) in [
            (&requests[1], "encrypt", vec![7; 32]),
            (&requests[2], "decrypt", vec![9; 48]),
        ] {
            assert_eq!(request.url().host_str(), Some("test.vault.azure.net"));
            assert_eq!(
                request.url().path(),
                format!("/keys/ExampleKey//{operation}")
            );
            assert_eq!(
                body(request),
                json!({"alg": "RSA-OAEP-256", "value": azure_core::base64::encode_url_safe(input)})
            );
        }
        assert_eq!(
            *credential.scopes.lock().unwrap(),
            vec![
                "https://managedhsm.azure.net/.default",
                "https://vault.azure.net/.default",
            ]
        );
    }

    #[test]
    fn test_init_azure_kms() {
        let err_azure_cfg = SubConfigAzure {
            tenant_id: "test-tenant".to_owned(),
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
            tenant_id: "test-tenant".to_owned(),
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
