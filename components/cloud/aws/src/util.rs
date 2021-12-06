// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self, Error, ErrorKind};

use rusoto_core::{
    region::Region,
    request::{HttpClient, HttpConfig},
};
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProvideAwsCredentials,
};
use rusoto_sts::WebIdentityProvider;

use async_trait::async_trait;

#[allow(dead_code)] // This will be used soon, please remove the allow.
const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

pub fn new_http_client() -> io::Result<HttpClient> {
    let mut http_config = HttpConfig::new();
    // This can greatly improve performance dealing with payloads greater
    // than 100MB. See https://github.com/rusoto/rusoto/pull/1227
    // for more information.
    http_config.read_buf_size(READ_BUF_SIZE);
    // It is important to explicitly create the client and not use a global
    // See https://github.com/tikv/tikv/issues/7236.
    HttpClient::new_with_config(http_config).map_err(|e| {
        Error::new(
            ErrorKind::Other,
            format!("create aws http client error: {}", e),
        )
    })
}

pub fn get_region(region: &str, endpoint: &str) -> io::Result<Region> {
    if !endpoint.is_empty() {
        Ok(Region::Custom {
            name: region.to_owned(),
            endpoint: endpoint.to_owned(),
        })
    } else if !region.is_empty() {
        region.parse::<Region>().map_err(|e| {
            Error::new(
                ErrorKind::InvalidInput,
                format!("invalid aws region format {}: {}", region, e),
            )
        })
    } else {
        Ok(Region::default())
    }
}

pub struct CredentialsProvider(AutoRefreshingProvider<DefaultCredentialsProvider>);

impl CredentialsProvider {
    pub fn new() -> io::Result<CredentialsProvider> {
        Ok(CredentialsProvider(
            AutoRefreshingProvider::new(DefaultCredentialsProvider::default()).map_err(|e| {
                Error::new(
                    ErrorKind::Other,
                    format!("create aws credentials provider error: {}", e),
                )
            })?,
        ))
    }
}

#[async_trait]
impl ProvideAwsCredentials for CredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        self.0.credentials().await
    }
}

// Same as rusoto_credentials::DefaultCredentialsProvider with extra
// rusoto_sts::WebIdentityProvider support.
pub struct DefaultCredentialsProvider {
    // Underlying implementation of rusoto_credentials::DefaultCredentialsProvider.
    default_provider: ChainProvider,
    // Provider IAM support in Kubernetes.
    web_identity_provider: WebIdentityProvider,
}

impl Default for DefaultCredentialsProvider {
    fn default() -> DefaultCredentialsProvider {
        DefaultCredentialsProvider {
            default_provider: ChainProvider::new(),
            web_identity_provider: WebIdentityProvider::from_k8s_env(),
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for DefaultCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        // Prefer the web identity provider first for the kubernetes environment.
        // Search for both in parallel.
        let web_creds = self.web_identity_provider.credentials();
        let def_creds = self.default_provider.credentials();
        let k8s_error = match web_creds.await {
            res @ Ok(_) => return res,
            Err(e) => e,
        };
        let def_error = match def_creds.await {
            res @ Ok(_) => return res,
            Err(e) => e,
        };
        Err(CredentialsError::new(format_args!(
            "Couldn't find AWS credentials in default sources ({}) or k8s environment ({}).",
            def_error.message, k8s_error.message,
        )))
    }
}
