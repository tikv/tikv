// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

use tikv_util::security::SecurityConfig;

pub fn new_security_cfg() -> SecurityConfig {
    let p = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    SecurityConfig {
        ca_path: format!("{}", p.join("data/ca.pem").display()),
        cert_path: format!("{}", p.join("data/server.pem").display()),
        key_path: format!("{}", p.join("data/key.pem").display()),
        override_ssl_target: "".to_owned(),
        cipher_file: "".to_owned(),
    }
}
