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

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub endpoints: Vec<String>,
}

impl Config {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.endpoints.is_empty() {
            return Err("please specify pd.endpoints.".into());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pd_cfg() {
        let mut cfg = Config::default();
        // endpoints is required.
        cfg.validate().unwrap_err();
        cfg.endpoints = vec!["127.0.0.1:2333".to_owned()];
        cfg.validate().unwrap();
    }
}
