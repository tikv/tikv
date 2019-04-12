// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use tikv_util::security::SecurityConfig;

pub fn new_security_cfg() -> SecurityConfig {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    SecurityConfig {
        ca_path: format!("{}", p.join("data/ca.crt").display()),
        cert_path: format!("{}", p.join("data/server.crt").display()),
        key_path: format!("{}", p.join("data/server.pem").display()),
        override_ssl_target: "example.com".to_owned(),
        cipher_file: "".to_owned(),
    }
}
