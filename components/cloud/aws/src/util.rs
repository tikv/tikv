// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.
use std::{error::Error as StdError, io};

use ::aws_smithy_runtime_api::client::orchestrator::HttpResponse;
use aws_config::{
    default_provider::credentials::DefaultCredentialsChain,
    environment::EnvironmentVariableRegionProvider,
    meta::region::{self, ProvideRegion, RegionProviderChain},
    profile::ProfileFileRegionProvider,
    provider_config::ProviderConfig,
    ConfigLoader, Region,
};
use aws_credential_types::provider::{error::CredentialsError, ProvideCredentials};
use aws_sdk_kms::config::SharedHttpClient;
use aws_sdk_s3::config::HttpClient;
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use cloud::metrics;
use futures::{Future, TryFutureExt};
use hyper::Client;
use hyper_tls::HttpsConnector;
use tikv_util::{
    stream::{block_on_external_io, retry_ext, RetryError, RetryExt},
    warn,
};

const READ_BUF_SIZE: usize = 1024 * 1024 * 2;

const DEFAULT_REGION: &str = "us-east-1";

pub(crate) type SdkError<E, R = HttpResponse> =
    ::aws_smithy_runtime_api::client::result::SdkError<E, R>;

struct CredentialsErrorWrapper(CredentialsError);

impl From<CredentialsErrorWrapper> for CredentialsError {
    fn from(c: CredentialsErrorWrapper) -> CredentialsError {
        c.0
    }
}

impl std::fmt::Display for CredentialsErrorWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.0)?;
        Ok(())
    }
}

impl RetryError for CredentialsErrorWrapper {
    fn is_retryable(&self) -> bool {
        true
    }
}

pub fn new_http_client() -> SharedHttpClient {
    let mut hyper_builder = Client::builder();
    hyper_builder.http1_read_buf_exact_size(READ_BUF_SIZE);

    HyperClientBuilder::new()
        .hyper_builder(hyper_builder)
        .build(HttpsConnector::new())
}

pub fn new_credentials_provider(http: impl HttpClient + 'static) -> DefaultCredentialsProvider {
    let fut = DefaultCredentialsProvider::new(http);
    if let Ok(hnd) = tokio::runtime::Handle::try_current() {
        tokio::task::block_in_place(move || hnd.block_on(fut))
    } else {
        block_on_external_io(fut)
    }
}

pub fn is_retryable<T>(error: &SdkError<T>) -> bool {
    match error {
        SdkError::TimeoutError(_) => true,
        SdkError::DispatchFailure(_) => true,
        SdkError::ResponseError(resp_err) => {
            let code = resp_err.raw().status();
            code.is_server_error() || code.as_u16() == http::StatusCode::REQUEST_TIMEOUT.as_u16()
        }
        _ => false,
    }
}

pub fn configure_endpoint(loader: ConfigLoader, endpoint: &str) -> ConfigLoader {
    if !endpoint.is_empty() {
        loader.endpoint_url(endpoint)
    } else {
        loader
    }
}

pub fn configure_region(
    loader: ConfigLoader,
    region: &str,
    custom: bool,
) -> io::Result<ConfigLoader> {
    if !region.is_empty() {
        validate_region(region, custom)?;
        Ok(loader.region(Region::new(region.to_owned())))
    } else {
        Ok(loader.region(DefaultRegionProvider::new()))
    }
}

fn validate_region(region: &str, custom: bool) -> io::Result<()> {
    if custom {
        return Ok(());
    }
    let v: &str = &region.to_lowercase();

    match v {
        "ap-east-1" | "apeast1" | "ap-northeast-1" | "apnortheast1" | "ap-northeast-2"
        | "apnortheast2" | "ap-northeast-3" | "apnortheast3" | "ap-south-1" | "apsouth1"
        | "ap-southeast-1" | "apsoutheast1" | "ap-southeast-2" | "apsoutheast2"
        | "ca-central-1" | "cacentral1" | "eu-central-1" | "eucentral1" | "eu-west-1"
        | "euwest1" | "eu-west-2" | "euwest2" | "eu-west-3" | "euwest3" | "eu-north-1"
        | "eunorth1" | "eu-south-1" | "eusouth1" | "me-south-1" | "mesouth1" | "us-east-1"
        | "useast1" | "sa-east-1" | "saeast1" | "us-east-2" | "useast2" | "us-west-1"
        | "uswest1" | "us-west-2" | "uswest2" | "us-gov-east-1" | "usgoveast1"
        | "us-gov-west-1" | "usgovwest1" | "cn-north-1" | "cnnorth1" | "cn-northwest-1"
        | "cnnorthwest1" | "af-south-1" | "afsouth1" => Ok(()),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("invalid aws region format {}", region),
        )),
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

#[derive(Debug)]
struct DefaultRegionProvider(RegionProviderChain);

impl DefaultRegionProvider {
    fn new() -> Self {
        let env_provider = EnvironmentVariableRegionProvider::new();
        let profile_provider = ProfileFileRegionProvider::builder().build();

        // same as default region resolving in rusoto
        let chain = RegionProviderChain::first_try(env_provider)
            .or_else(profile_provider)
            .or_else(Region::new(DEFAULT_REGION));

        Self(chain)
    }
}

impl ProvideRegion for DefaultRegionProvider {
    fn region(&self) -> region::future::ProvideRegion<'_> {
        ProvideRegion::region(&self.0)
    }
}

#[derive(Debug)]
pub struct DefaultCredentialsProvider {
    default_provider: DefaultCredentialsChain,
}

impl DefaultCredentialsProvider {
    async fn new(cli: impl HttpClient + 'static) -> Self {
        let cfg = ProviderConfig::default().with_http_client(cli);
        let default_provider = DefaultCredentialsChain::builder()
            .configure(cfg)
            .build()
            .await;
        Self { default_provider }
    }
}

impl ProvideCredentials for DefaultCredentialsProvider {
    fn provide_credentials<'a>(
        &'a self,
    ) -> aws_credential_types::provider::future::ProvideCredentials<'a>
    where
        Self: 'a,
    {
        aws_credential_types::provider::future::ProvideCredentials::new(async move {
            // Add exponential backoff for every error, because we cannot
            // distinguish the error type.
            let cred = retry_and_count(
                || {
                    #[cfg(test)]
                    fail::fail_point!("cred_err", |_| {
                        let cause: Box<dyn StdError + Send + Sync + 'static> =
                            String::from("injected error").into();
                        Box::pin(futures::future::err(CredentialsErrorWrapper(
                            CredentialsError::provider_error(cause),
                        )))
                            as std::pin::Pin<Box<dyn futures::Future<Output = _> + Send>>
                    });

                    Box::pin(
                        self.default_provider
                            .provide_credentials()
                            .map_err(|e| CredentialsErrorWrapper(e)),
                    )
                },
                "get_cred_on_premise",
            )
            .await
            .map_err(|e| e.0);

            cred.map_err(|e| {
                let msg = e
                    .source()
                    .map(|src_err| src_err.to_string())
                    .unwrap_or_else(|| e.to_string());
                let cause: Box<dyn StdError + Send + Sync + 'static> =
                    format_args!("Couldn't find AWS credentials in sources ({}).", msg)
                        .to_string()
                        .into();
                CredentialsError::provider_error(cause)
            })
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
        const AWS_WEB_IDENTITY_TOKEN_FILE: &str = "AWS_WEB_IDENTITY_TOKEN_FILE";

        let default_provider = DefaultCredentialsProvider::new(new_http_client()).await;
        std::env::set_var(AWS_WEB_IDENTITY_TOKEN_FILE, "tmp");
        // mock k8s env with web_identitiy_provider
        fail::cfg("cred_err", "return").unwrap();
        fail::cfg("retry_count", "return(1)").unwrap();
        let res = default_provider.provide_credentials().await;
        assert_eq!(res.is_err(), true);

        let err = res.unwrap_err();

        match err {
            CredentialsError::ProviderError(_) => {
                assert_eq!(
                    err.source().unwrap().to_string(),
                    "Couldn't find AWS credentials in sources (injected error)."
                )
            }
            err => panic!("unexpected error type: {}", err),
        }

        fail::remove("cred_err");
        fail::remove("retry_count");
        std::env::remove_var(AWS_WEB_IDENTITY_TOKEN_FILE);
    }
}
