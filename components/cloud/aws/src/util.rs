// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{self, Error, ErrorKind};

use async_trait::async_trait;
use cloud::metrics;
use futures::{future::TryFutureExt, Future};
use rusoto_core::{
    region::Region,
    request::{HttpClient, HttpConfig},
};
use rusoto_credential::{
    AutoRefreshingProvider, AwsCredentials, ChainProvider, CredentialsError, ProvideAwsCredentials,
};
use rusoto_sts::WebIdentityProvider;
use tikv_util::{
    stream::{retry_ext, RetryError, RetryExt},
    warn,
};

#[allow(dead_code)] // This will be used soon, please remove the allow.
const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";
struct CredentialsErrorWrapper(CredentialsError);

impl From<CredentialsErrorWrapper> for CredentialsError {
    fn from(c: CredentialsErrorWrapper) -> CredentialsError {
        c.0
    }
}

impl std::fmt::Display for CredentialsErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.message)?;
        Ok(())
    }
}

impl RetryError for CredentialsErrorWrapper {
    fn is_retryable(&self) -> bool {
        true
    }
}

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
    if !region.is_empty() || !endpoint.is_empty() {
        Ok(Region::Custom {
            name: region.to_owned(),
            endpoint: endpoint.to_owned(),
        })
    } else {
        Ok(Region::default())
    }
}

pub async fn retry_and_count<G, T, F, E>(action: G, name: &'static str) -> Result<T, E>
where
    G: FnMut() -> F,
    F: Future<Output = Result<T, E>>,
    E: RetryError + std::fmt::Display,
{
    let id = uuid::Uuid::new_v4();
    retry_ext(
        action,
        RetryExt::default().with_fail_hook(move |err: &E| {
            warn!("aws request fails"; "err" => %err, "retry?" => %err.is_retryable(), "context" => %name, "uuid" => %id);
            metrics::CLOUD_ERROR_VEC.with_label_values(&["aws", name]).inc();
        }),
    ).await
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
        // use web identity provider first for the kubernetes environment.
        let cred = if std::env::var(AWS_WEB_IDENTITY_TOKEN_FILE).is_ok() {
            // we need invoke assume_role in web identity provider
            // this API may failed sometimes.
            // according to AWS experience, it's better to retry it with 10 times
            // exponential backoff for every error, because we cannot
            // distinguish the error type.
            retry_and_count(
                || {
                    #[cfg(test)]
                    fail::fail_point!("cred_err", |_| {
                        Box::pin(futures::future::err(CredentialsErrorWrapper(
                            CredentialsError::new("injected error"),
                        )))
                            as std::pin::Pin<Box<dyn futures::Future<Output = _> + Send>>
                    });
                    let res = self
                        .web_identity_provider
                        .credentials()
                        .map_err(|e| CredentialsErrorWrapper(e));
                    #[cfg(test)]
                    return Box::pin(res);
                    #[cfg(not(test))]
                    res
                },
                "get_cred_over_the_cloud",
            )
            .await
            .map_err(|e| e.0)
        } else {
            // Add exponential backoff for every error, because we cannot
            // distinguish the error type.
            retry_and_count(
                || {
                    self.default_provider
                        .credentials()
                        .map_err(|e| CredentialsErrorWrapper(e))
                },
                "get_cred_on_premise",
            )
            .await
            .map_err(|e| e.0)
        };

        cred.map_err(|e| {
            CredentialsError::new(format_args!(
                "Couldn't find AWS credentials in sources ({}).",
                e.message
            ))
        })
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[cfg(feature = "failpoints")]
    #[tokio::test]
    async fn test_default_provider() {
        let default_provider = DefaultCredentialsProvider::default();
        std::env::set_var(AWS_WEB_IDENTITY_TOKEN_FILE, "tmp");
        // mock k8s env with web_identitiy_provider
        fail::cfg("cred_err", "return").unwrap();
        fail::cfg("retry_count", "return(1)").unwrap();
        let res = default_provider.credentials().await;
        assert_eq!(res.is_err(), true);
        assert_eq!(
            res.err().unwrap().message,
            "Couldn't find AWS credentials in sources (injected error)."
        );
        fail::remove("cred_err");
        fail::remove("retry_count");

        std::env::remove_var(AWS_WEB_IDENTITY_TOKEN_FILE);
    }
}
