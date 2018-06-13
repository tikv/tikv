// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::ptr;

use grpc::{
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
}

impl Default for SecurityConfig {
    fn default() -> SecurityConfig {
        SecurityConfig {
            ca_path: String::new(),
            cert_path: String::new(),
            key_path: String::new(),
            override_ssl_target: String::new(),
        }
    }
}

fn check_key_file(tag: &str, path: &str) -> Result<Option<File>, Box<Error>> {
    if path.is_empty() {
        return Ok(None);
    }
    match File::open(path) {
        Err(e) => Err(format!("failed to open {} to load {}: {:?}", path, tag, e).into()),
        Ok(f) => Ok(Some(f)),
    }
}

fn load_key(tag: &str, path: &str) -> Result<Vec<u8>, Box<Error>> {
    let mut key = vec![];
    let f = check_key_file(tag, path)?;
    match f {
        None => return Ok(vec![]),
        Some(mut f) => if let Err(e) = f.read_to_end(&mut key) {
            return Err(format!("failed to load {} from path {}: {:?}", tag, path, e).into());
        },
    }
    Ok(key)
}

impl SecurityConfig {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
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

pub struct SecurityManager {
    ca: Vec<u8>,
    cert: Vec<u8>,
    key: Vec<u8>,
    override_ssl_target: String,
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
    pub fn new(cfg: &SecurityConfig) -> Result<SecurityManager, Box<Error>> {
        Ok(SecurityManager {
            ca: load_key("CA", &cfg.ca_path)?,
            cert: load_key("certificate", &cfg.cert_path)?,
            key: load_key("private key", &cfg.key_path)?,
            override_ssl_target: cfg.override_ssl_target.clone(),
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
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
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
            .into_iter()
            .enumerate()
        {
            File::create(f).unwrap().write_all(&[id as u8]).unwrap();
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
