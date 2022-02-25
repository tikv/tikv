// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::fs;
use std::io::Read;
use std::path::PathBuf;

use collections::HashSet;
use encryption_export::EncryptionConfig;
use grpcio::{ChannelCredentials, ChannelCredentialsBuilder};
use security::SecurityConfig;

pub fn new_security_cfg(cn: Option<HashSet<String>>) -> SecurityConfig {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    SecurityConfig {
        ca_path: format!("{}", p.join("data/ca.pem").display()),
        cert_path: format!("{}", p.join("data/server.pem").display()),
        key_path: format!("{}", p.join("data/key.pem").display()),
        override_ssl_target: "".to_owned(),
        cert_allowed_cn: cn.unwrap_or_default(),
        encryption: EncryptionConfig::default(),
        redact_info_log: Some(true),
    }
}

pub fn new_channel_cred() -> ChannelCredentials {
    let (ca, cert, key) = load_certs();
    ChannelCredentialsBuilder::new()
        .root_cert(ca.into())
        .cert(cert.into(), key.into())
        .build()
}

fn load_certs() -> (String, String, String) {
    let mut cert = String::new();
    let mut key = String::new();
    let mut ca = String::new();
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    fs::File::open(format!("{}", p.join("data/server.pem").display()))
        .unwrap()
        .read_to_string(&mut cert)
        .unwrap();
    fs::File::open(format!("{}", p.join("data/key.pem").display()))
        .unwrap()
        .read_to_string(&mut key)
        .unwrap();
    fs::File::open(format!("{}", p.join("data/ca.pem").display()))
        .unwrap()
        .read_to_string(&mut ca)
        .unwrap();
    (ca, cert, key)
}
