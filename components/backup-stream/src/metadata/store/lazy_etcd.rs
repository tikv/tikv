// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{sync::Arc, time::Duration};

use etcd_client::{ConnectOptions, Error as EtcdError, TlsOptions};
use futures::Future;
use tikv_util::stream::RetryError;
use tokio::sync::OnceCell;

use super::{etcd::EtcdSnapshot, EtcdStore, MetaStore};
use crate::errors::{ContextualResultExt, Result};

#[derive(Clone)]
pub struct LazyEtcdClient(Arc<LazyEtcdClientInner>);

pub struct ConnectionConfig {
    pub tls: Option<TlsOptions>,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
}

impl ConnectionConfig {
    /// Convert the config to the connection option.
    fn to_connection_options(&self) -> ConnectOptions {
        let mut opts = ConnectOptions::new();
        if let Some(tls) = &self.tls {
            opts = opts.with_tls(tls.clone())
        }
        opts = opts.with_keep_alive(self.keep_alive_interval, self.keep_alive_timeout);
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
        | EtcdError::InvalidHeaderValue(_) => false,
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
    let r = tikv_util::stream::retry(move || action().err_into::<RetryableEtcdError>()).await;
    r.map_err(|err| err.0.into())
}

impl LazyEtcdClientInner {
    async fn connect(&self) -> Result<EtcdStore> {
        let store = retry(|| {
            // For now, the interface of the `etcd_client` doesn't us to control
            // how to create channels when connecting, hence we cannot update the tls config at runtime.
            // TODO: maybe add some method like `with_channel` for `etcd_client`, and adapt the `SecurityManager` API,
            //       instead of doing everything by own.
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
}
