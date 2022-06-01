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
use grpcio::{
    CertificateRequestType, Channel, ChannelBuilder, ChannelCredentialsBuilder, CheckResult,
    RpcContext, RpcStatus, RpcStatusCode, ServerBuilder, ServerChecker, ServerCredentialsBuilder,
    ServerCredentialsFetcher,
};
#[cfg(feature = "tonic")]
use tonic::transport::{channel::ClientTlsConfig, Certificate, Identity};

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

type CertResult = Result<(Vec<u8>, Vec<u8>, Vec<u8>), Box<dyn Error>>;

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

    #[cfg(feature = "tonic")]
    /// Make a tonic tls config via the config.
    pub fn tonic_tls_config(&self) -> Option<ClientTlsConfig> {
        let (ca, cert, key) = self.cfg.load_certs().unwrap_or_default();
        if ca.is_empty() && cert.is_empty() && key.is_empty() {
            return None;
        }
        let mut cfg = ClientTlsConfig::new();
        if !ca.is_empty() {
            cfg = cfg.ca_certificate(Certificate::from_pem(ca));
        }
        if !cert.is_empty() && !key.is_empty() {
            cfg = cfg.identity(Identity::from_pem(cert, key));
        }
        Some(cfg)
    }

    pub fn connect(&self, mut cb: ChannelBuilder, addr: &str) -> Channel {
        if self.cfg.ca_path.is_empty() {
            cb.connect(addr)
        } else {
            if !self.cfg.override_ssl_target.is_empty() {
                cb = cb.override_ssl_target(self.cfg.override_ssl_target.clone());
            }
            // Fill in empty certificate information if read fails.
            // Returning empty certificates delays error processing until
            // actual connection in grpc.
            let (ca, cert, key) = self.cfg.load_certs().unwrap_or_default();

            let cred = ChannelCredentialsBuilder::new()
                .root_cert(ca)
                .cert(cert, key)
                .build();
            cb.secure_connect(addr, cred)
        }
    }

    pub fn bind(&self, mut sb: ServerBuilder, addr: &str, port: u16) -> ServerBuilder {
        if self.cfg.ca_path.is_empty() {
            sb.bind(addr, port)
        } else {
            if !self.cfg.cert_allowed_cn.is_empty() {
                let cn_checker = CNChecker {
                    allowed_cn: Arc::new(self.cfg.cert_allowed_cn.clone()),
                };
                sb = sb.add_checker(cn_checker);
            }
            let fetcher = Box::new(Fetcher {
                cfg: self.cfg.clone(),
                last_modified_time: Arc::new(Mutex::new(None)),
            });
            sb.bind_with_fetcher(
                addr,
                port,
                fetcher,
                CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
            )
        }
    }
}

#[derive(Clone)]
struct CNChecker {
    allowed_cn: Arc<HashSet<String>>,
}

impl ServerChecker for CNChecker {
    fn check(&mut self, ctx: &RpcContext<'_>) -> CheckResult {
        match check_common_name(&self.allowed_cn, ctx) {
            Ok(()) => CheckResult::Continue,
            Err(reason) => CheckResult::Abort(RpcStatus::with_message(
                RpcStatusCode::UNAUTHENTICATED,
                format!(
                    "Common name check fail, reason: {}, cert_allowed_cn: {:?}",
                    reason, self.allowed_cn
                ),
            )),
        }
    }

    fn box_clone(&self) -> Box<dyn ServerChecker> {
        Box::new(self.clone())
    }
}

struct Fetcher {
    cfg: Arc<SecurityConfig>,
    last_modified_time: Arc<Mutex<Option<SystemTime>>>,
}

impl ServerCredentialsFetcher for Fetcher {
    // Retrieves updated credentials. When returning `None` or
    // error, gRPC will continue to use the previous certificates
    // returned by the method.
    fn fetch(&self) -> Result<Option<ServerCredentialsBuilder>, Box<dyn Error>> {
        if let Ok(mut last) = self.last_modified_time.try_lock() {
            // Reload only when cert is modified.
            if self.cfg.is_modified(&mut last)? {
                let (ca, cert, key) = self.cfg.load_certs()?;
                let new_cred = ServerCredentialsBuilder::new()
                    .add_cert(cert, key)
                    .root_cert(
                        ca,
                        CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
                    );
                Ok(Some(new_cred))
            } else {
                Ok(None)
            }
        } else {
            Ok(None)
        }
    }
}

/// Check peer CN with cert-allowed-cn field.
/// Return true when the match is successful (support wildcard pattern).
/// Skip the check when the secure channel is not used.
fn check_common_name(
    cert_allowed_cn: &HashSet<String>,
    ctx: &RpcContext<'_>,
) -> Result<(), String> {
    if let Some(auth_ctx) = ctx.auth_context() {
        if let Some(auth_property) = auth_ctx
            .into_iter()
            .find(|x| x.name() == "x509_common_name")
        {
            let peer_cn = auth_property.value_str().unwrap();
            if match_peer_names(cert_allowed_cn, peer_cn) {
                Ok(())
            } else {
                Err(format!("x509_common_name from peer is {}", peer_cn))
            }
        } else {
            Err("x509_common_name doesn't exist".to_owned())
        }
    } else {
        Ok(())
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
