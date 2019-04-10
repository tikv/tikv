// Copyright 2018 PingCAP, Inc.
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
