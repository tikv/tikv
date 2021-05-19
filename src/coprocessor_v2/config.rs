// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub coprocessor_plugin_directory: Option<PathBuf>,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            coprocessor_plugin_directory: None,
        }
    }
}
