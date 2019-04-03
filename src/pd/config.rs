// Copyright 2017 TiKV Project Authors. Licensed under Apache-2.0.
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
