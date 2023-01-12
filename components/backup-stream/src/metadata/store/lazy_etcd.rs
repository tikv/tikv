// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use etcd_client::{ConnectOptions, Error as EtcdError, OpenSslClientConfig};
use futures::Future;
use openssl::x509::verify::X509VerifyFlags;
use tikv_util::{
    info,
    stream::{RetryError, RetryExt},
};
use tokio::sync::OnceCell;

use super::{etcd::EtcdSnapshot, EtcdStore, MetaStore};
use crate::errors::{ContextualResultExt, Result};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct LazyEtcdClient(Arc<LazyEtcdClientInner>);

#[derive(Debug)]
pub struct ConnectionConfig {
    pub tls: Option<security::ClientSuite>,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
}

impl ConnectionConfig {
    /// Convert the config to the connection option.
    fn to_connection_options(&self) -> ConnectOptions {
        let mut opts = ConnectOptions::new();
        if let Some(tls) = &self.tls {
            opts = opts.with_openssl_tls(
                OpenSslClientConfig::default()
                    .ca_cert_pem(&tls.ca)
                    // Some of users may prefer using multi-level self-signed certs.
                    // In this scenario, we must set this flag or openssl would probably complain it cannot found the root CA.
                    // (Because the flags we provide allows users providing exactly one CA cert.)
                    // We haven't make it configurable because it is enabled in gRPC by default too.
                    // TODO: Perhaps implement grpc-io based etcd client, fully remove the difference between gRPC TLS and our custom TLS?
                    .manually(|c| c.cert_store_mut().set_flags(X509VerifyFlags::PARTIAL_CHAIN))
                    .client_cert_pem_and_key(&tls.client_cert, &tls.client_key.0),
            )
        }
        opts = opts
            .with_keep_alive(self.keep_alive_interval, self.keep_alive_timeout)
            .with_keep_alive_while_idle(false)
            .with_timeout(RPC_TIMEOUT);

        opts
    }
}

impl LazyEtcdClient {
    pub fn new(endpoints: &[String], conf: ConnectionConfig) -> Self {
        Self(Arc::new(LazyEtcdClientInner {
            opt: conf.to_connection_options(),
            endpoints: endpoints.iter().map(ToString::to_string).collect(),
            cli: OnceCell::new(),
        }))
    }
}

impl std::ops::Deref for LazyEtcdClient {
    type Target = LazyEtcdClientInner;

    fn deref(&self) -> &Self::Target {
        Arc::deref(&self.0)
    }
}

#[derive(Clone)]
pub struct LazyEtcdClientInner {
    opt: ConnectOptions,
    endpoints: Vec<String>,

    cli: OnceCell<EtcdStore>,
}

fn etcd_error_is_retryable(etcd_err: &EtcdError) -> bool {
    match etcd_err {
        EtcdError::InvalidArgs(_)
        | EtcdError::InvalidUri(_)
        | EtcdError::Utf8Error(_)
        | EtcdError::InvalidHeaderValue(_)
        | EtcdError::EndpointError(_)
        | EtcdError::OpenSsl(_) => false,
        EtcdError::TransportError(_)
        | EtcdError::IoError(_)
        | EtcdError::WatchError(_)
        | EtcdError::LeaseKeepAliveError(_)
        | EtcdError::ElectError(_) => true,
        EtcdError::GRpcStatus(grpc) => matches!(
            grpc.code(),
            tonic::Code::Unavailable
                | tonic::Code::Aborted
                | tonic::Code::Internal
                | tonic::Code::ResourceExhausted
        ),
    }
}

#[derive(Debug)]
struct RetryableEtcdError(EtcdError);

impl RetryError for RetryableEtcdError {
    fn is_retryable(&self) -> bool {
        etcd_error_is_retryable(&self.0)
    }
}

impl From<EtcdError> for RetryableEtcdError {
    fn from(e: EtcdError) -> Self {
        Self(e)
    }
}

pub async fn retry<T, F>(mut action: impl FnMut() -> F) -> Result<T>
where
    F: Future<Output = std::result::Result<T, EtcdError>>,
{
    use futures::TryFutureExt;
    let r = tikv_util::stream::retry_ext(
        move || action().err_into::<RetryableEtcdError>(),
        RetryExt::default().with_fail_hook(|err| info!("retry it"; "err" => ?err)),
    )
    .await;
    r.map_err(|err| err.0.into())
}

impl LazyEtcdClientInner {
    async fn connect(&self) -> Result<EtcdStore> {
        let store = retry(|| {
            // For now, the interface of the `etcd_client` doesn't us to control
            // how to create channels when connecting, hence we cannot update the tls config
            // at runtime.
            // TODO: maybe add some method like `with_channel` for `etcd_client`, and adapt
            // the `SecurityManager` API, instead of doing everything by own.
            etcd_client::Client::connect(self.endpoints.clone(), Some(self.opt.clone()))
        })
        .await
        .context("during connecting to the etcd")?;
        Ok(EtcdStore::from(store))
    }

    pub async fn get_cli(&self) -> Result<&EtcdStore> {
        let store = self.cli.get_or_try_init(|| self.connect()).await?;
        Ok(store)
    }
}

#[async_trait::async_trait]
impl MetaStore for LazyEtcdClient {
    type Snap = EtcdSnapshot;

    async fn snapshot(&self) -> Result<Self::Snap> {
        self.0.get_cli().await?.snapshot().await
    }

    async fn watch(
        &self,
        keys: super::Keys,
        start_rev: i64,
    ) -> Result<super::KvChangeSubscription> {
        self.0.get_cli().await?.watch(keys, start_rev).await
    }

    async fn txn(&self, txn: super::Transaction) -> Result<()> {
        self.0.get_cli().await?.txn(txn).await
    }

    async fn txn_cond(&self, txn: super::CondTransaction) -> Result<()> {
        self.0.get_cli().await?.txn_cond(txn).await
    }
}
