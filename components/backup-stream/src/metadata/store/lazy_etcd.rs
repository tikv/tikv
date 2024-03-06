// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use etcd_client::{ConnectOptions, Error as EtcdError, OpenSslClientConfig};
use futures::Future;
use openssl::{
    pkey::PKey,
    x509::{verify::X509VerifyFlags, X509},
};
use security::SecurityManager;
use tikv_util::{
    info,
    stream::{RetryError, RetryExt},
    warn,
};
use tokio::sync::Mutex as AsyncMutex;

use super::{
    etcd::{EtcdSnapshot, TopologyUpdater},
    EtcdStore, MetaStore,
};
use crate::errors::{ContextualResultExt, Result};

const RPC_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Clone)]
pub struct LazyEtcdClient(Arc<AsyncMutex<LazyEtcdClientInner>>);

#[derive(Clone)]
pub struct ConnectionConfig {
    pub tls: Arc<SecurityManager>,
    pub keep_alive_interval: Duration,
    pub keep_alive_timeout: Duration,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            tls: Default::default(),
            keep_alive_interval: Duration::from_secs(10),
            keep_alive_timeout: Duration::from_secs(3),
        }
    }
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("keep_alive_interval", &self.keep_alive_interval)
            .field("keep_alive_timeout", &self.keep_alive_timeout)
            .finish()
    }
}

impl ConnectionConfig {
    /// Convert the config to the connection option.
    fn to_connection_options(&self) -> ConnectOptions {
        let mut opts = ConnectOptions::new();
        if let Some(tls) = &self
            .tls
            .client_suite()
            .map_err(|err| warn!("failed to load client suite!"; "err" => %err))
            .ok()
        {
            opts = opts.with_openssl_tls(
                OpenSslClientConfig::default()
                    .ca_cert_pem(&tls.ca)
                    // Some of users may prefer using multi-level self-signed certs.
                    // In this scenario, we must set this flag or openssl would probably complain it cannot found the root CA.
                    // (Because the flags we provide allows users providing exactly one CA cert.)
                    // We haven't make it configurable because it is enabled in gRPC by default too.
                    // TODO: Perhaps implement grpc-io based etcd client, fully remove the difference between gRPC TLS and our custom TLS?
                    .manually(|c| c.cert_store_mut().set_flags(X509VerifyFlags::PARTIAL_CHAIN))
                    .manually(|c| {
                        let mut client_certs= X509::stack_from_pem(&tls.client_cert)?;
                        let client_key = PKey::private_key_from_pem(&tls.client_key.0)?;
                        if !client_certs.is_empty() {
                            c.set_certificate(&client_certs[0])?;
                        }
                        if client_certs.len() > 1 {
                            for i in client_certs.drain(1..) {
                                c.add_extra_chain_cert(i)?;
                            }
                        }
                        c.set_private_key(&client_key)?;
                        Ok(())
                    }),
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
        let mut inner = LazyEtcdClientInner::new(endpoints, conf);
        inner.normalize_urls();
        Self(Arc::new(AsyncMutex::new(inner)))
    }

    // For testing -- check whether the endpoints are properly normalized.
    #[cfg(test)]
    pub(super) fn endpoints(&self) -> Vec<String> {
        self.0.blocking_lock().endpoints.clone()
    }

    async fn get_cli(&self) -> Result<EtcdStore> {
        let mut l = self.0.lock().await;
        l.get_cli().await.cloned()
    }
}

#[derive(Clone)]
pub struct LazyEtcdClientInner {
    conf: ConnectionConfig,
    endpoints: Vec<String>,

    last_modified: Option<SystemTime>,
    cli: Option<EtcdStore>,
}

impl LazyEtcdClientInner {
    fn new(endpoints: &[String], conf: ConnectionConfig) -> Self {
        LazyEtcdClientInner {
            conf,
            endpoints: endpoints.iter().map(ToString::to_string).collect(),
            last_modified: None,
            cli: None,
        }
    }
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
    fn normalize_urls(&mut self) {
        let enabled_tls = self.conf.tls.client_suite().is_ok();
        for endpoint in self.endpoints.iter_mut() {
            // Don't touch them when the schemes already provided.
            // Given etcd is based on gRPC (which relies on HTTP/2),
            // there shouldn't be other schemes available (Hopefully...)
            if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
                continue;
            }
            let expected_scheme = if enabled_tls { "https" } else { "http" };
            *endpoint = format!("{}://{}", expected_scheme, endpoint)
        }
        info!("log backup normalized etcd endpoints"; "endpoints" => ?self.endpoints);
    }

    async fn connect(&mut self) -> Result<&EtcdStore> {
        let store = retry(|| {
            // For now, the interface of the `etcd_client` doesn't us to control
            // how to create channels when connecting, hence we cannot update the tls config
            // at runtime, now what we did is manually check that each time we are getting
            // the clients.
            etcd_client::Client::connect(
                self.endpoints.clone(),
                Some(self.conf.to_connection_options()),
            )
        })
        .await
        .context("during connecting to the etcd")?;
        let store = EtcdStore::from(store);
        let mut updater = TopologyUpdater::new(Arc::downgrade(store.inner()));
        self.cli = Some(store);
        updater.init(self.endpoints.iter().cloned());
        tokio::task::spawn(updater.main_loop());
        Ok(self.cli.as_ref().unwrap())
    }

    pub async fn get_cli(&mut self) -> Result<&EtcdStore> {
        let modified = self.conf.tls.get_config().is_modified(&mut self.last_modified)
            // Don't reload once we cannot check whether it is modified.
            // Because when TLS disabled, this would always fail.
            .unwrap_or(false);
        if !modified && self.cli.is_some() {
            return Ok(self.cli.as_ref().unwrap());
        }
        info!("log backup reconnecting to the etcd service."; "tls_modified" => %modified, "connected_before" => %self.cli.is_some());
        self.connect().await
    }
}

#[async_trait::async_trait]
impl MetaStore for LazyEtcdClient {
    type Snap = EtcdSnapshot;

    async fn snapshot(&self) -> Result<Self::Snap> {
        self.get_cli().await?.snapshot().await
    }

    async fn watch(
        &self,
        keys: super::Keys,
        start_rev: i64,
    ) -> Result<super::KvChangeSubscription> {
        self.get_cli().await?.watch(keys, start_rev).await
    }

    async fn txn(&self, txn: super::Transaction) -> Result<()> {
        self.get_cli().await?.txn(txn).await
    }

    async fn txn_cond(&self, txn: super::CondTransaction) -> Result<()> {
        self.get_cli().await?.txn_cond(txn).await
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, io::Write, path::PathBuf, sync::Arc};

    use security::{SecurityConfig, SecurityManager};
    use tempfile::TempDir;

    use super::LazyEtcdClient;
    use crate::{errors::Result, metadata::ConnectionConfig};

    #[test]
    fn test_normalize_url() -> Result<()> {
        let endpoints = ["http://pd-1".to_owned(), "pd-2".to_owned()];
        let le = LazyEtcdClient::new(&endpoints, Default::default());
        assert_eq!(le.endpoints(), &["http://pd-1", "http://pd-2"]);

        let tempdir = TempDir::new()?;
        let write_all = |path: &PathBuf, content| {
            let mut f = File::create(path)?;
            f.write_all(content)?;
            Result::Ok(())
        };
        let ca = tempdir.path().join("ca");
        let cert = tempdir.path().join("cert");
        let key = tempdir.path().join("key");
        write_all(&ca, b"CA :3")?;
        write_all(&cert, b"Cert :D")?;
        write_all(&key, b"Key X)")?;

        let cfg = SecurityConfig {
            ca_path: ca.to_string_lossy().into_owned(),
            cert_path: cert.to_string_lossy().into_owned(),
            key_path: key.to_string_lossy().into_owned(),

            ..Default::default()
        };
        let sm = SecurityManager::new(&cfg).unwrap();
        let endpoints = ["https://pd-1".to_owned(), "pd-2".to_owned()];
        let le = LazyEtcdClient::new(
            &endpoints,
            ConnectionConfig {
                tls: Arc::new(sm),
                ..Default::default()
            },
        );
        assert_eq!(le.endpoints(), &["https://pd-1", "https://pd-2"]);
        Result::Ok(())
    }
}
