// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

#[macro_use]
extern crate serde_derive;

use std::{
    error::Error,
    fs::{self, File},
    io::Read,
    sync::{Arc, Mutex},
    time::SystemTime,
};

use collections::HashSet;
use encryption::EncryptionConfig;
use tonic::transport::{Certificate, Channel, ClientTlsConfig, Endpoint, Identity};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct SecurityConfig {
    // SSL configs.
    pub ca_path: String,
    pub cert_path: String,
    pub key_path: String,
    // Test purpose only.
    #[serde(skip)]
    pub override_ssl_target: String,
    pub cert_allowed_cn: HashSet<String>,
    pub redact_info_log: Option<bool>,
    pub encryption: EncryptionConfig,
}

/// Checks and opens key file. Returns `Ok(None)` if the path is empty.
///
///  # Arguments
///
///  - `tag`: only used in the error message, like "ca key", "cert key",
///    "private key", etc.
fn check_key_file(tag: &str, path: &str) -> Result<Option<File>, Box<dyn Error>> {
    if path.is_empty() {
        return Ok(None);
    }
    match File::open(path) {
        Err(e) => Err(format!("failed to open {} to load {}: {:?}", path, tag, e).into()),
        Ok(f) => Ok(Some(f)),
    }
}

/// Loads key file content. Returns `Ok(vec![])` if the path is empty.
fn load_key(tag: &str, path: &str) -> Result<Vec<u8>, Box<dyn Error>> {
    let mut key = vec![];
    let f = check_key_file(tag, path)?;
    match f {
        None => return Ok(vec![]),
        Some(mut f) => {
            if let Err(e) = f.read_to_end(&mut key) {
                return Err(format!("failed to load {} from path {}: {:?}", tag, path, e).into());
            }
        }
    }
    Ok(key)
}

type CertResult = Result<(Vec<u8>, Vec<u8>, Vec<u8>), Box<dyn Error>>;

type Pem = Box<[u8]>;

pub struct Secret(pub Pem);

impl std::fmt::Debug for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Secret").finish()
    }
}

#[derive(Debug)]
pub struct ClientSuite {
    pub ca: Pem,
    pub client_cert: Pem,
    pub client_key: Secret,
}

impl SecurityConfig {
    /// Validates ca, cert and private key.
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        check_key_file("ca key", &self.ca_path)?;
        check_key_file("cert key", &self.cert_path)?;
        check_key_file("private key", &self.key_path)?;
        // TODO: validate whether ca, cert and private key match.
        if (!self.ca_path.is_empty() || !self.cert_path.is_empty() || !self.key_path.is_empty())
            && (self.ca_path.is_empty() || self.cert_path.is_empty() || self.key_path.is_empty())
        {
            return Err("ca, cert and private key should be all configured.".into());
        }
        Ok(())
    }

    /// Load certificates from the given file path.
    /// Return ca, cert, key after successful read.
    fn load_certs(&self) -> CertResult {
        let ca = load_key("CA", &self.ca_path)?;
        let cert = load_key("certificate", &self.cert_path)?;
        let key = load_key("private key", &self.key_path)?;
        if ca.is_empty() || cert.is_empty() || key.is_empty() {
            return Err("ca, cert and private key should be all configured.".into());
        }
        Ok((ca, cert, key))
    }

    /// Determine if the cert file has been modified.
    /// If modified, update the timestamp of this modification.
    pub fn is_modified(&self, last: &mut Option<SystemTime>) -> Result<bool, Box<dyn Error>> {
        let this = fs::metadata(&self.cert_path)?.modified()?;
        if let Some(last) = last {
            if *last == this {
                return Ok(false);
            }
        }
        *last = Some(this);
        Ok(true)
    }
}

#[derive(Default)]
pub struct SecurityManager {
    cfg: Arc<SecurityConfig>,
}

impl SecurityManager {
    pub fn new(cfg: &SecurityConfig) -> Result<SecurityManager, Box<dyn Error>> {
        Ok(SecurityManager {
            cfg: Arc::new(cfg.clone()),
        })
    }

    pub fn is_ssl_enabled(&self) -> bool {
        !self.cfg.ca_path.is_empty()
    }

    pub fn client_suite(&self) -> Result<ClientSuite, Box<dyn Error>> {
        let (ca, cert, key) = self.cfg.load_certs()?;
        Ok(ClientSuite {
            ca: ca.into_boxed_slice(),
            client_cert: cert.into_boxed_slice(),
            client_key: Secret(key.into_boxed_slice()),
        })
    }

    pub fn set_tls_config(&self, mut cb: Endpoint) -> Result<Endpoint, tonic::transport::Error> {
        if !self.cfg.ca_path.is_empty() {
            if !self.cfg.override_ssl_target.is_empty() {
                // TODO: support override_ssl_target
                // cb = cb.override_ssl_target(self.cfg.override_ssl_target.
                // clone());
            }
            // Fill in empty certificate information if read fails.
            // Returning empty certificates delays error processing until
            // actual connection in grpc.
            let (ca, cert, key) = self.cfg.load_certs().unwrap_or_default();
            let mut tls_cfg = ClientTlsConfig::new();
            if !ca.is_empty() {
                tls_cfg = tls_cfg.ca_certificate(Certificate::from_pem(ca));
            }
            if !cert.is_empty() && !key.is_empty() {
                tls_cfg = tls_cfg.identity(Identity::from_pem(cert, key));
            }
            cb = cb.tls_config(tls_cfg)?;
        }
        Ok(cb)
    }

    pub fn server_tls_config(
        &self,
        builder: tonic::transport::Server,
    ) -> Result<tonic::transport::Server, tonic::transport::Error> {
        if !self.cfg.ca_path.is_empty() {
            let fetcher = Arc::new(Fetcher {
                cfg: self.cfg.clone(),
                last_modified_time: Arc::new(Mutex::new(None)),
            });
            return builder.with_tls_provider(fetcher);
        }
        Ok(builder)
    }

    pub async fn connect(&self, cb: Endpoint) -> Result<Channel, tonic::transport::Error> {
        let cb = self.set_tls_config(cb)?;
        cb.connect().await
    }

    pub fn get_config(&self) -> &SecurityConfig {
        &self.cfg
    }
}

struct Fetcher {
    cfg: Arc<SecurityConfig>,
    last_modified_time: Arc<Mutex<Option<SystemTime>>>,
}

impl tonic::transport::server::TlsConfigProvider for Fetcher {
    fn fetch(&self) -> Option<tonic::transport::ServerTlsConfig> {
        if let Ok(mut last) = self.last_modified_time.try_lock() {
            // Reload only when cert is modified.
            if self.cfg.is_modified(&mut last).unwrap_or(false) {
                let (ca, cert, key) = self.cfg.load_certs().ok()?;
                let tls_config = tonic::transport::ServerTlsConfig::new()
                    .client_ca_root(Certificate::from_pem(ca))
                    .identity(Identity::from_pem(cert, key));
                return Some(tls_config);
            }
        }
        None
    }
}

/// Check peer CN with a set of allowed CN.
pub fn match_peer_names(allowed_cn: &HashSet<String>, name: &str) -> bool {
    for cn in allowed_cn {
        if cn == name {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use std::{fs, io::Write};

    use tempfile::Builder;

    use super::*;

    #[test]
    fn test_security() {
        let cfg = SecurityConfig::default();
        // default is disable secure connection.
        cfg.validate().unwrap();
        let mgr = SecurityManager::new(&cfg).unwrap();
        assert!(mgr.cfg.ca_path.is_empty());
        assert!(mgr.cfg.cert_path.is_empty());
        assert!(mgr.cfg.key_path.is_empty());

        let assert_cfg = |c: fn(&mut SecurityConfig), valid: bool| {
            let mut invalid_cfg = cfg.clone();
            c(&mut invalid_cfg);
            assert_eq!(invalid_cfg.validate().is_ok(), valid);
        };

        // invalid path should be rejected.
        assert_cfg(
            |c| {
                c.ca_path = "invalid ca path".to_owned();
                c.cert_path = "invalid cert path".to_owned();
                c.key_path = "invalid key path".to_owned();
            },
            false,
        );

        let temp = Builder::new().prefix("test_cred").tempdir().unwrap();
        let example_ca = temp.path().join("ca");
        let example_cert = temp.path().join("cert");
        let example_key = temp.path().join("key");
        for (id, f) in [&example_ca, &example_cert, &example_key]
            .iter()
            .enumerate()
        {
            fs::write(f, [id as u8]).unwrap();
        }

        let mut c = cfg.clone();
        c.cert_path = format!("{}", example_cert.display());
        c.key_path = format!("{}", example_key.display());
        // incomplete configuration.
        c.validate().unwrap_err();

        // data should be loaded from file after validating.
        c.ca_path = format!("{}", example_ca.display());
        c.validate().unwrap();

        let (ca, cert, key) = c.load_certs().unwrap_or_default();
        assert_eq!(ca, vec![0]);
        assert_eq!(cert, vec![1]);
        assert_eq!(key, vec![2]);
    }

    #[test]
    fn test_modify_file() {
        let mut file = Builder::new().prefix("test_modify").tempfile().unwrap();
        let cfg = SecurityConfig {
            cert_path: file.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };

        let mut last = None;
        assert!(cfg.is_modified(&mut last).unwrap());
        assert!(!cfg.is_modified(&mut last).unwrap());

        std::thread::sleep(std::time::Duration::from_millis(10));
        writeln!(file, "something").unwrap();
        assert!(cfg.is_modified(&mut last).unwrap());
        assert!(!cfg.is_modified(&mut last).unwrap());
    }
}
