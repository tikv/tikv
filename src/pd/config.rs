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

use util::config;

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub endpoints: Vec<String>,
    pub ca_path: String,
    pub ca: Vec<u8>,
    // Test purpose only.
    #[serde(skip)]
    pub override_ssl_target: String,
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<Error>> {
        if self.endpoints.is_empty() {
            return Err("please specify pd.endpoints.".into());
        }
        for addr in &self.endpoints {
            config::check_addr(addr)?;
        }
        if self.ca.is_empty() && !self.ca_path.is_empty() {
            File::open(&self.ca_path)
                .and_then(|mut f| f.read_to_end(&mut self.ca))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::io::Write;

    use tempdir::TempDir;

    #[test]
    fn test_pd_cfg() {
        let mut cfg = Config::default();
        // endpoints is required.
        cfg.validate().unwrap_err();
        cfg.endpoints = vec!["127.0.0.1:2333".to_owned()];
        cfg.validate().unwrap();

        cfg.ca_path = "invalid ca path".to_owned();
        cfg.validate().unwrap_err();

        let cred_dir = TempDir::new("test_pd_cfg").unwrap();
        let ca_path = cred_dir.path().join("ca");
        File::create(&ca_path).unwrap().write_all(&[2]).unwrap();
        cfg.ca_path = format!("{}", ca_path.display());
        cfg.validate().unwrap();
        // Content should be read.
        assert_eq!(cfg.ca, vec![2]);

        // Prefer content to path.
        cfg.ca = vec![3];
        cfg.validate().unwrap();
        assert_eq!(cfg.ca, vec![3]);
    }
}
