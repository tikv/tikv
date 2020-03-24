// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::env::{var, VarError};
use std::io::{self, Error, ErrorKind};

use rusoto_core::{
    region::Region,
    request::{HttpClient, HttpConfig},
};
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProvideAwsCredentials,
    StaticProvider, Variable,
};
use rusoto_sts::WebIdentityProvider;

use async_trait::async_trait;

const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

#[macro_export]
macro_rules! new_client {
    ($client: ty, $config: ident) => {{
        let http_client = $crate::new_http_client()?;
        new_client!($client, $config, http_client)
    }};
    ($client: ty, $config: ident, $dispatcher: ident) => {{
        let region = $crate::get_region($config.region.as_ref(), $config.endpoint.as_ref())?;
        let cred_provider = $crate::CredentialsProvider::new(
            $config.access_key.as_ref(),
            $config.secret_access_key.as_ref(),
        )?;
        <$client>::new_with($dispatcher, cred_provider, region)
    }};
}

pub fn new_http_client() -> io::Result<HttpClient> {
    let mut http_config = HttpConfig::new();
    // This can greatly improve performance dealing with payloads greater
    // than 100MB. See https://github.com/rusoto/rusoto/pull/1227
    // for more information.
    http_config.read_buf_size(READ_BUF_SIZE);
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

pub enum CredentialsProvider {
    Default(Box<AutoRefreshingProvider<DefaultCredentialsProvider>>),
    Static(StaticProvider),
}

impl CredentialsProvider {
    pub fn new(access_key: &str, secret_access_key: &str) -> io::Result<CredentialsProvider> {
        let cred_provider = if !access_key.is_empty() && !secret_access_key.is_empty() {
            CredentialsProvider::Static(StaticProvider::new(
                access_key.to_owned(),
                secret_access_key.to_owned(),
                None, /* token */
                None, /* valid_for*/
            ))
        } else {
            CredentialsProvider::Default(Box::new(
                AutoRefreshingProvider::new(DefaultCredentialsProvider::default()).map_err(
                    |e| {
                        Error::new(
                            ErrorKind::Other,
                            format!("create aws credentials provider error: {}", e),
                        )
                    },
                )?,
            ))
        };
        Ok(cred_provider)
    }
}

#[async_trait]
impl ProvideAwsCredentials for CredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        match self {
            CredentialsProvider::Default(default_provider) => default_provider.credentials().await,
            CredentialsProvider::Static(static_provider) => static_provider.credentials().await,
        }
    }
}

const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
const AWS_ROLE_ARN: &str = "AWS_ROLE_ARN";
const AWS_ROLE_SESSION_NAME: &str = "AWS_ROLE_SESSION_NAME";
const DEFAULT_SESSION_NAME: &str = "WebIdentitySession";

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
            // We should be using WebIdentityProvider::from_k8s_env(), after the issue have been
            // fixed: https://github.com/rusoto/rusoto/pull/1724
            web_identity_provider: WebIdentityProvider::new(
                // Get token file name from env var, then read from the file.
                Variable::dynamic(|| {
                    Variable::from_text_file(
                        Variable::<String, CredentialsError>::from_env_var(
                            AWS_WEB_IDENTITY_TOKEN_FILE,
                        )
                        .resolve()?,
                    )
                    .resolve()
                }),
                Variable::from_env_var(AWS_ROLE_ARN),
                // As AWS_ROLE_SESSION_NAME is optional, we cannot use Variable::from_env_var.
                Some(Variable::dynamic(|| {
                    match var(AWS_ROLE_SESSION_NAME).map(|v| v.trim().to_owned()) {
                        Ok(v) if !v.is_empty() => Ok(v),
                        Ok(_) | Err(VarError::NotPresent) => Ok(DEFAULT_SESSION_NAME.to_owned()),
                        Err(e) => Err(CredentialsError::from(e)),
                    }
                })),
            ),
        }
    }
}

#[async_trait]
impl ProvideAwsCredentials for DefaultCredentialsProvider {
    async fn credentials(&self) -> Result<AwsCredentials, CredentialsError> {
        // Need to use web identity provider first to prevent default provider takes precedence in
        // kubernetes environment.
        if let Ok(creds) = self.web_identity_provider.credentials().await {
            return Ok(creds);
        }
        if let Ok(creds) = self.default_provider.credentials().await {
            return Ok(creds);
        }
        Err(CredentialsError::new(
            "Couldn't find AWS credentials in default sources or k8s environment.",
        ))
    }
}
