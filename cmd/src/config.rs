// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.

use tikv::config::TiKvConfig;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(deny_unknown_fields)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    #[serde(flatten)]
    pub tikv_cfg: TiKvConfig,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            tikv_cfg: TiKvConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use toml;

    #[test]
    fn test_flatten() {
        let tikv_str = toml::to_string(&TiKvConfig::default()).unwrap();
        let cfg_str = toml::to_string(&Config::default()).unwrap();
        assert_eq!(tikv_str, cfg_str);
    }
}
