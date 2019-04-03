// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::ptr;
use std::sync::Arc;

use crate::storage::engine::Env;

use crate::grpc::{
    Channel, ChannelBuilder, ChannelCredentialsBuilder, ServerBuilder, ServerCredentialsBuilder,
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
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
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
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
    override_ssl_target: String,
    cipher_file: String,
}

impl Drop for SecurityManager {
    fn drop(&mut self) {
        unsafe {
            for b in &mut self.key {
                ptr::write_volatile(b, 0);
            }
        }
    }
}

impl SecurityManager {
    pub fn new(cfg: &SecurityConfig) -> Result<SecurityManager, Box<dyn Error>> {
        Ok(SecurityManager {
            ca: load_key("CA", &cfg.ca_path)?,
            cert: load_key("certificate", &cfg.cert_path)?,
            key: load_key("private key", &cfg.key_path)?,
            override_ssl_target: cfg.override_ssl_target.clone(),
            cipher_file: cfg.cipher_file.clone(),
        })
    }

    pub fn connect(&self, mut cb: ChannelBuilder, addr: &str) -> Channel {
        if self.ca.is_empty() {
            cb.connect(addr)
        } else {
            if !self.override_ssl_target.is_empty() {
                cb = cb.override_ssl_target(self.override_ssl_target.clone());
            }
            let cred = ChannelCredentialsBuilder::new()
                .root_cert(self.ca.clone())
                .cert(self.cert.clone(), self.key.clone())
                .build();
            cb.secure_connect(addr, cred)
        }
    }

    pub fn bind(&self, sb: ServerBuilder, addr: &str, port: u16) -> ServerBuilder {
        if self.ca.is_empty() {
            sb.bind(addr, port)
        } else {
            let cred = ServerCredentialsBuilder::new()
                .root_cert(self.ca.clone(), true)
                .add_cert(self.cert.clone(), self.key.clone())
                .build();
            sb.bind_secure(addr, port, cred)
        }
    }

    pub fn cipher_file(&self) -> &str {
        &self.cipher_file
    }
}

pub fn encrypted_env_from_cipher_file<P: AsRef<Path>>(
    path: P,
    base_env: Option<Arc<Env>>,
) -> Result<Arc<Env>, String> {
    let cipher_hex = match fs::read_to_string(path) {
        Err(e) => return Err(format!("failed to load cipher file: {:?}", e)),
        Ok(s) => s.trim().as_bytes().to_vec(),
    };
    let cipher_text = match ::hex::decode(cipher_hex) {
        Err(e) => return Err(format!("cipher file should be hex type, error: {:?}", e)),
        Ok(text) => text,
    };
    let base = match base_env {
        Some(env) => env,
        None => Arc::new(Env::default()),
    };
    match Env::new_ctr_encrypted_env(base, &cipher_text) {
        Err(e) => Err(format!("failed to create encrypted env: {:?}", e)),
        Ok(env) => Ok(Arc::new(env)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::{self, File};
    use std::io::Write;

    use tempdir::TempDir;

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

        let temp = TempDir::new("test_cred").unwrap();
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

    #[test]
    fn test_encrypted_env_from_cipher_file() {
        let path = TempDir::new("/tmp/encrypted_env_from_cipher_file").unwrap();

        // Cipher file not exists.
        assert!(encrypted_env_from_cipher_file(path.path().join("file0"), None).is_err());

        // Cipher file in hex type.
        let mut file1 = File::create(path.path().join("file1")).unwrap();
        file1.write_all(b"ACFFDBCC").unwrap();
        file1.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file1"), None).is_ok());

        // Cipher file not in hex type.
        let mut file2 = File::create(path.path().join("file2")).unwrap();
        file2.write_all(b"AGGGGGGG").unwrap();
        file2.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file2"), None).is_err());

        // The length of cipher file's content is not power of 2.
        let mut file3 = File::create(path.path().join("file3")).unwrap();
        file3.write_all(b"ACFFDBCCA").unwrap();
        file3.sync_all().unwrap();
        assert!(encrypted_env_from_cipher_file(path.path().join("file3"), None).is_err());
    }
}
