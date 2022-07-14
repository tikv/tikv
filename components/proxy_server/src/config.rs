// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::RandomState, HashSet},
    iter::FromIterator,
    path::{Path, PathBuf},
};

use itertools::Itertools;
use online_config::OnlineConfig;
use serde_derive::{Deserialize, Serialize};
use tikv::config::TiKvConfig;
use tikv_util::crit;

use crate::fatal;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ProxyConfig {
    pub snap_handle_pool_size: usize,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            snap_handle_pool_size: 2,
        }
    }
}

impl ProxyConfig {
    pub fn from_file(
        path: &Path,
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let cfg: ProxyConfig = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <ProxyConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        Ok(cfg)
    }
}

pub fn ensure_no_common_unrecognized_keys(
    proxy_unrecognized_keys: &[String],
    unrecognized_keys: &[String],
) -> Result<(), String> {
    // We can't just compute intersection, since `rocksdb.z` equals not `rocksdb`.
    let proxy_part = HashSet::<_>::from_iter(proxy_unrecognized_keys.iter());
    let inter = unrecognized_keys
        .iter()
        .filter(|s| {
            let mut pref: String = String::from("");
            for p in s.split('.') {
                if !pref.is_empty() {
                    pref += "."
                }
                pref += p;
                if proxy_part.contains(&pref) {
                    // common unrecognized by both config.
                    return true;
                }
            }
            false
        })
        .collect::<Vec<_>>();
    if inter.len() != 0 {
        return Err(inter.iter().join(", "));
    }
    Ok(())
}

pub fn address_proxy_config(config: &mut TiKvConfig) {
    // We must add engine label to our TiFlash config
    pub const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";
    let engine_name = match option_env!("ENGINE_LABEL_VALUE") {
        None => {
            fatal!("should set engine name with env variable `ENGINE_LABEL_VALUE`");
        }
        Some(name) => name.to_owned(),
    };
    config
        .server
        .labels
        .insert(DEFAULT_ENGINE_LABEL_KEY.to_owned(), engine_name);
}
