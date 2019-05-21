// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub enabled: bool,
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}
