// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::error::Error;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Default)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub prefix: String,

    pub s3_endpoint: String,

    pub s3_key_id: String,

    pub s3_secret_key: String,

    pub s3_bucket: String,

    pub s3_region: String,

    pub remote_compactor_addr: String,
}

impl Config {
    #[allow(dead_code)]
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        // TODO(x) validate dfs config
        Ok(())
    }
}
