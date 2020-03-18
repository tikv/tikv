// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::fs;
use std::fs::File;
use std::io::Read;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::SystemTime;

use grpcio::{
    CertificateRequestType, Channel, ChannelBuilder, ChannelCredentialsBuilder, ServerBuilder,
    ServerCredentialsBuilder, ServerCredentialsFetcher,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct SecurityConfig {
    pub ca_path: String,
    pub cert_path: String,
    pub key_path: String,
    pub reload_mode: bool,
    // Test purpose only.
    #[serde(skip)]
    pub override_ssl_target: String,
    pub cipher_file: String,
}

impl Default for SecurityConfig {
    fn default() -> SecurityConfig {
        SecurityConfig {
            ca_path: String::new(),
            cert_path: String::new(),
            key_path: String::new(),
            reload_mode: false,
            override_ssl_target: String::new(),
            cipher_file: String::new(),
        }
    }
}

/// Checks and opens key file. Returns `Ok(None)` if the path is empty.
///
///  # Arguments
///
///  - `tag`: only used in the error message, like "ca key", "cert key", "private key", etc.
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
}

struct Certs {
    pub ca: Vec<u8>,
    pub cert: Vec<u8>,
    pub key: Vec<u8>,
}

impl Default for Certs {
    fn default() -> Self {
        Certs {
            ca: vec![],
            cert: vec![],
            key: vec![],
        }
    }
}

#[derive(Default)]
pub struct SecurityManager {
    certs: Arc<RwLock<Certs>>,
    reload_cfg: Option<Arc<SecurityConfig>>,
    override_ssl_target: String,
    cipher_file: String,
}

impl Drop for SecurityManager {
    fn drop(&mut self) {
        use zeroize::Zeroize;
        let mut certs = self.certs.write().unwrap();
        certs.key.zeroize();
    }
}

impl SecurityManager {
    pub fn new(cfg: &SecurityConfig) -> Result<SecurityManager, Box<dyn Error>> {
        Ok(SecurityManager {
            certs: Arc::new(RwLock::new(Certs {
                ca: load_key("CA", &cfg.ca_path)?,
                cert: load_key("certificate", &cfg.cert_path)?,
                key: load_key("private key", &cfg.key_path)?,
            })),
            reload_cfg: if cfg.reload_mode {
                Some(Arc::new(cfg.clone()))
            } else {
                None
            },
            override_ssl_target: cfg.override_ssl_target.clone(),
            cipher_file: cfg.cipher_file.clone(),
        })
    }

    pub fn connect(&self, mut cb: ChannelBuilder, addr: &str) -> Channel {
        let certs = self.certs.read().unwrap();
        if certs.ca.is_empty() {
            cb.connect(addr)
        } else {
            if !self.override_ssl_target.is_empty() {
                cb = cb.override_ssl_target(self.override_ssl_target.clone());
            }
            let cred = ChannelCredentialsBuilder::new()
                .root_cert(certs.ca.clone())
                .cert(certs.cert.clone(), certs.key.clone())
                .build();
            cb.secure_connect(addr, cred)
        }
    }

    pub fn bind(&self, sb: ServerBuilder, addr: &str, port: u16) -> ServerBuilder {
        let certs = self.certs.read().unwrap();
        if certs.ca.is_empty() {
            sb.bind(addr, port)
        } else if self.reload_cfg.is_some() {
            let fetcher = Box::new(Fetcher {
                certs: self.certs.clone(),
                last_modified: Arc::new(RwLock::new(SystemTime::now())),
                cfg: self.reload_cfg.clone().unwrap(),
            });
            sb.bind_with_fetcher(
                addr,
                port,
                fetcher,
                CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
            )
        } else {
            let cred = ServerCredentialsBuilder::new()
                .root_cert(
                    certs.ca.clone(),
                    CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
                )
                .add_cert(certs.cert.clone(), certs.key.clone())
                .build();
            sb.bind_with_cred(addr, port, cred)
        }
    }

    pub fn cipher_file(&self) -> &str {
        &self.cipher_file
    }
}

struct Fetcher {
    certs: Arc<RwLock<Certs>>,
    last_modified: Arc<RwLock<SystemTime>>,
    cfg: Arc<SecurityConfig>,
}

impl ServerCredentialsFetcher for Fetcher {
    fn fetch(&self) -> Result<Option<ServerCredentialsBuilder>, Box<dyn Error>> {
        let cert_modified_time = fs::metadata(&self.cfg.cert_path)?.modified()?;
        let last = self.last_modified.read().unwrap();
        if *last == cert_modified_time {
            Ok(None)
        } else {
            drop(last);
            *self.last_modified.write().unwrap() = cert_modified_time;
            let ca = load_key("CA", &self.cfg.ca_path)?;
            let cert = load_key("certificate", &self.cfg.cert_path)?;
            let key = load_key("private key", &self.cfg.key_path)?;
            if ca.is_empty() || cert.is_empty() || key.is_empty() {
                return Err("ca, cert and private key should be all configured.".into());
            }
            let new_cred = ServerCredentialsBuilder::new()
                .add_cert(cert.clone(), key.clone())
                .root_cert(
                    ca.clone(),
                    CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
                );
            let mut certs = self.certs.write().unwrap();
            certs.ca = ca;
            certs.cert = cert;
            certs.key = key;
            Ok(Some(new_cred))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use tempfile::Builder;

    #[test]
    fn test_security() {
        let mut cfg = SecurityConfig::default();
        // default is disable secure connection.
        cfg.validate().unwrap();
        let mut mgr = SecurityManager::new(&cfg).unwrap();
        assert!(mgr.ca.is_empty());
        assert!(mgr.cert.is_empty());
        assert!(mgr.key.is_empty());

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
        for (id, f) in (&[&example_ca, &example_cert, &example_key])
            .iter()
            .enumerate()
        {
            fs::write(f, &[id as u8]).unwrap();
        }
        let mut c = cfg.clone();
        c.cert_path = format!("{}", example_cert.display());
        c.key_path = format!("{}", example_key.display());
        // incomplete configuration.
        c.validate().unwrap_err();

        // data should be loaded from file after validating.
        c.ca_path = format!("{}", example_ca.display());
        c.validate().unwrap();
        mgr = SecurityManager::new(&c).unwrap();
        assert_eq!(mgr.ca, vec![0]);
        assert_eq!(mgr.cert, vec![1]);
        assert_eq!(mgr.key, vec![2]);
    }
}
