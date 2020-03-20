// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;
use std::fs::File;
use std::io::Read;

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

#[derive(Default)]
pub struct SecurityManager {
    ca_path: String,
    cert_path: String,
    key_path: String,
    override_ssl_target: String,
    cipher_file: String,
}

impl SecurityManager {
    pub fn new(cfg: &SecurityConfig) -> Result<SecurityManager, Box<dyn Error>> {
        Ok(SecurityManager {
            ca_path: cfg.ca_path.clone(),
            cert_path: cfg.cert_path.clone(),
            key_path: cfg.key_path.clone(),
            override_ssl_target: cfg.override_ssl_target.clone(),
            cipher_file: cfg.cipher_file.clone(),
        })
    }

    pub fn connect(&self, mut cb: ChannelBuilder, addr: &str) -> Channel {
        if self.ca_path.is_empty() {
            cb.connect(addr)
        } else {
            if !self.override_ssl_target.is_empty() {
                cb = cb.override_ssl_target(self.override_ssl_target.clone());
            }
            // Fill in empty certificate information if read fails.
            let (ca, cert, key) =
                load_certs(&self.ca_path, &self.cert_path, &self.key_path).unwrap_or_default();

            let cred = ChannelCredentialsBuilder::new()
                .root_cert(ca)
                .cert(cert, key)
                .build();
            cb.secure_connect(addr, cred)
        }
    }

    pub fn bind(&self, sb: ServerBuilder, addr: &str, port: u16) -> ServerBuilder {
        if self.ca_path.is_empty() {
            sb.bind(addr, port)
        } else {
            let fetcher = Box::new(Fetcher {
                ca_path: self.ca_path.clone(),
                cert_path: self.cert_path.clone(),
                key_path: self.key_path.clone(),
            });
            sb.bind_with_fetcher(
                addr,
                port,
                fetcher,
                CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
            )
        }
    }

    pub fn cipher_file(&self) -> &str {
        &self.cipher_file
    }
}

struct Fetcher {
    ca_path: String,
    cert_path: String,
    key_path: String,
}

impl ServerCredentialsFetcher for Fetcher {
    fn fetch(&self) -> Result<Option<ServerCredentialsBuilder>, Box<dyn Error>> {
        let (ca, cert, key) = load_certs(&self.ca_path, &self.cert_path, &self.key_path)?;
        let new_cred = ServerCredentialsBuilder::new()
            .add_cert(cert, key)
            .root_cert(
                ca,
                CertificateRequestType::RequestAndRequireClientCertificateAndVerify,
            );
        Ok(Some(new_cred))
    }
}

fn load_certs(
    ca_path: &str,
    cert_path: &str,
    key_path: &str,
) -> Result<(Vec<u8>, Vec<u8>, Vec<u8>), Box<dyn Error>> {
    let ca = load_key("CA", ca_path)?;
    let cert = load_key("certificate", cert_path)?;
    let key = load_key("private key", key_path)?;
    if ca.is_empty() || cert.is_empty() || key.is_empty() {
        return Err("ca, cert and private key should be all configured.".into());
    }
    Ok((ca, cert, key))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    use tempfile::Builder;

    #[test]
    fn test_security() {
        let cfg = SecurityConfig::default();
        // default is disable secure connection.
        cfg.validate().unwrap();
        let mgr = SecurityManager::new(&cfg).unwrap();
        assert!(mgr.ca_path.is_empty());
        assert!(mgr.cert_path.is_empty());
        assert!(mgr.key_path.is_empty());

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

        let (ca, cert, key) = load_certs(
            &format!("{}", example_ca.display()),
            &format!("{}", example_cert.display()),
            &format!("{}", example_key.display()),
        )
        .unwrap_or_default();

        assert_eq!(ca, vec![0]);
        assert_eq!(cert, vec![1]);
        assert_eq!(key, vec![2]);
    }
}
