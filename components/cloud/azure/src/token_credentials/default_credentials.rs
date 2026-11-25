// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.
// Copyright 2026 PingCAP, Inc.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use azure_core::{
    credentials::{AccessToken, TokenCredential, TokenRequestOptions},
    error::ErrorKind,
    http::ClientMethodOptions,
};
use azure_identity::{
    AzureCliCredential, ClientAssertion, ClientAssertionCredential, ManagedIdentityCredential,
    WorkloadIdentityCredential,
};

use super::credential_error_summary;

/// Preserves the legacy SDK's workload identity, managed identity and CLI
/// fallback. The SDK credentials and authentication policy cache tokens.
#[derive(Debug)]
pub struct DefaultAzureCredential {
    workload: Option<Arc<dyn TokenCredential>>,
    managed: Option<Arc<dyn TokenCredential>>,
    cli: Option<Arc<dyn TokenCredential>>,
    initialization_errors: Vec<String>,
}

impl DefaultAzureCredential {
    /// Build the supported identity sources in their fallback order.
    pub fn new() -> azure_core::Result<Arc<Self>> {
        // Preserve the legacy environment token's precedence over a token file.
        // Client-secret credentials are handled before this chain in azblob.
        let workload = if let (Ok(tenant), Ok(client), Ok(_)) = (
            std::env::var("AZURE_TENANT_ID"),
            std::env::var("AZURE_CLIENT_ID"),
            std::env::var("AZURE_FEDERATED_TOKEN"),
        ) {
            ClientAssertionCredential::new(tenant, client, EnvironmentAssertion, None)
                .map(|c| c as Arc<dyn TokenCredential>)
        } else {
            WorkloadIdentityCredential::new(None).map(|c| c as Arc<dyn TokenCredential>)
        };
        let mut initialization_errors = Vec::new();
        let workload =
            initialize_credential("workload identity", workload, &mut initialization_errors);
        let managed = initialize_credential(
            "managed identity",
            ManagedIdentityCredential::new(None).map(|c| c as Arc<dyn TokenCredential>),
            &mut initialization_errors,
        );
        let cli = initialize_credential(
            "Azure CLI",
            AzureCliCredential::new(None).map(|c| c as Arc<dyn TokenCredential>),
            &mut initialization_errors,
        );
        Ok(Arc::new(Self {
            workload,
            managed,
            cli,
            initialization_errors,
        }))
    }
}

fn initialize_credential(
    name: &str,
    credential: azure_core::Result<Arc<dyn TokenCredential>>,
    errors: &mut Vec<String>,
) -> Option<Arc<dyn TokenCredential>> {
    match credential {
        Ok(credential) => Some(credential),
        Err(error) => {
            errors.push(format!(
                "{name} initialization: {}",
                credential_error_summary(&error)
            ));
            None
        }
    }
}

#[async_trait]
impl TokenCredential for DefaultAzureCredential {
    async fn get_token(
        &self,
        scopes: &[&str],
        options: Option<TokenRequestOptions<'_>>,
    ) -> azure_core::Result<AccessToken> {
        let mut errors = self.initialization_errors.clone();
        for (name, credential, timeout) in [
            ("workload identity", &self.workload, None),
            (
                "managed identity",
                &self.managed,
                Some(Duration::from_secs(1)),
            ),
            ("Azure CLI", &self.cli, None),
        ] {
            let Some(credential) = credential else {
                continue;
            };
            let request = credential.get_token(scopes, options.clone());
            // Bound the IMDS probe so local CLI users do not wait through the
            // SDK's network retry budget, and retain that timeout as a cause.
            let result = match timeout {
                Some(timeout) => match tokio::time::timeout(timeout, request).await {
                    Ok(result) => result,
                    Err(_) => {
                        errors.push(format!(
                            "{name}: timed out after {} second(s)",
                            timeout.as_secs()
                        ));
                        continue;
                    }
                },
                None => request.await,
            };
            match result {
                Ok(token) => return Ok(token),
                Err(error) => errors.push(format!("{name}: {}", credential_error_summary(&error))),
            }
        }
        Err(azure_core::Error::with_message(
            ErrorKind::Credential,
            format!("Azure credential sources failed: {}", errors.join("; ")),
        ))
    }
}

#[derive(Debug)]
struct EnvironmentAssertion;

#[async_trait]
impl ClientAssertion for EnvironmentAssertion {
    async fn secret(&self, _: Option<ClientMethodOptions<'_>>) -> azure_core::Result<String> {
        std::env::var("AZURE_FEDERATED_TOKEN").map_err(|err| {
            azure_core::Error::with_error(ErrorKind::Credential, err, "missing federated token")
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct UnavailableIdentity;

    #[async_trait]
    impl TokenCredential for UnavailableIdentity {
        async fn get_token(
            &self,
            _: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            std::future::pending().await
        }
    }

    #[derive(Debug)]
    struct LocalCredential;

    #[async_trait]
    impl TokenCredential for LocalCredential {
        async fn get_token(
            &self,
            scopes: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            assert_eq!(scopes, &["https://storage.azure.com/.default"]);
            Ok(AccessToken::new(
                "local-token",
                time::OffsetDateTime::now_utc() + time::Duration::hours(1),
            ))
        }
    }

    #[tokio::test]
    async fn test_unavailable_managed_identity_falls_back_to_cli() {
        let credential = DefaultAzureCredential {
            workload: None,
            managed: Some(Arc::new(UnavailableIdentity)),
            cli: Some(Arc::new(LocalCredential)),
            initialization_errors: vec![],
        };
        let token = tokio::time::timeout(
            Duration::from_secs(5),
            credential.get_token(&["https://storage.azure.com/.default"], None),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(token.token.secret(), "local-token");
    }

    #[derive(Debug)]
    struct FailingCredential;

    #[async_trait]
    impl TokenCredential for FailingCredential {
        async fn get_token(
            &self,
            _: &[&str],
            _: Option<TokenRequestOptions<'_>>,
        ) -> azure_core::Result<AccessToken> {
            Err(azure_core::Error::with_message(
                ErrorKind::Credential,
                "secret-token",
            ))
        }
    }

    #[tokio::test]
    async fn test_failed_credentials_report_each_source_and_redact_errors() {
        let credential = DefaultAzureCredential {
            workload: Some(Arc::new(FailingCredential)),
            managed: Some(Arc::new(FailingCredential)),
            cli: Some(Arc::new(FailingCredential)),
            initialization_errors: vec![],
        };
        let error = credential.get_token(&["scope"], None).await.unwrap_err();
        let message = format!("{error:?}");
        for source in ["workload identity", "managed identity", "Azure CLI"] {
            assert!(message.contains(source));
        }
        assert!(!message.contains("secret-token"));
    }

    #[tokio::test]
    async fn test_failed_credentials_keep_initialization_and_imds_timeout_causes() {
        let mut initialization_errors = vec![];
        let workload = initialize_credential(
            "workload identity",
            Err(azure_core::Error::new(
                ErrorKind::Credential,
                std::env::VarError::NotPresent,
            )),
            &mut initialization_errors,
        );
        let credential = DefaultAzureCredential {
            workload,
            managed: Some(Arc::new(UnavailableIdentity)),
            cli: Some(Arc::new(FailingCredential)),
            initialization_errors,
        };
        let message = credential
            .get_token(&["scope"], None)
            .await
            .unwrap_err()
            .to_string();
        assert!(message.contains(
            "workload identity initialization: required environment variable is missing"
        ));
        assert!(message.contains("managed identity: timed out after 1 second(s)"));
        assert!(message.contains("Azure CLI"));
    }
}
