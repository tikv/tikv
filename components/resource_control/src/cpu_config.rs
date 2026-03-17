// Copyright 2026 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use tikv_util::resource_control::DEFAULT_RESOURCE_GROUP_NAME;

/// Runtime configuration for coprocessor CPU throttling.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CpuThrottleConfig {
    pub enabled: bool,
    pub max_read_cpu_ratio: f64,
    pub estimated_cpu_per_request_us: u64,
    pub resource_group_estimated_cpu_per_request_us: String,
    pub enable_adaptive_estimated_cpu_per_request_us: bool,
    pub stats_interval_ms: u64,
    pub refill_interval_ms: u64,
    pub enable_dynamic_adjustment: bool,
    pub high_watermark: f64,
    pub low_watermark: f64,
    pub enable_fair_allocation: bool,
    pub fair_allocation_threshold: f64,
    pub enable_burst: bool,
    pub burst_threshold: f64,
    pub enable_runtime_token_management: bool,
    pub runtime_check_interval_us: u64,
    pub additional_allocation_threshold: f64,
    pub per_allocation_us: u64,
    pub throttle_default_group: bool,
    pub default_group_weight: Option<u64>,
    pub debug: bool,
}

impl Default for CpuThrottleConfig {
    fn default() -> Self {
        crate::config::Config::default().to_cpu_throttle_config()
    }
}

impl CpuThrottleConfig {
    pub fn canonicalize_group_name(name: &str) -> String {
        if name.is_empty() {
            DEFAULT_RESOURCE_GROUP_NAME.to_owned()
        } else {
            name.to_ascii_lowercase()
        }
    }

    pub fn parse_resource_group_estimated_cpu_per_request_us(
        estimated_str: &str,
    ) -> HashMap<String, u64> {
        let mut overrides = HashMap::new();
        if estimated_str.is_empty() {
            return overrides;
        }

        for entry in estimated_str.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                continue;
            }
            let Some((group_name, cpu_us)) = entry.split_once(':') else {
                tikv_util::warn!(
                    "invalid resource group estimated cpu format, expected 'name:cpu_us'";
                    "entry" => entry,
                );
                continue;
            };
            let group_name = Self::canonicalize_group_name(group_name.trim());
            match cpu_us.trim().parse::<u64>() {
                Ok(cpu_us) if cpu_us > 0 => {
                    overrides.insert(group_name, cpu_us);
                }
                _ => {
                    tikv_util::warn!(
                        "invalid estimated cpu override, expected positive integer";
                        "entry" => entry,
                    );
                }
            }
        }

        overrides
    }

    pub fn default_group_weight(&self) -> u64 {
        if self.throttle_default_group {
            self.default_group_weight.expect(
                "cpu throttle config invariant violated: default_group_weight must be set when throttle_default_group is enabled",
            )
        } else {
            self.default_group_weight.unwrap_or(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::CpuThrottleConfig;

    #[test]
    #[should_panic(
        expected = "cpu throttle config invariant violated: default_group_weight must be set when throttle_default_group is enabled"
    )]
    fn test_default_group_weight_panics_when_default_group_throttling_is_invalid() {
        let config = CpuThrottleConfig {
            throttle_default_group: true,
            default_group_weight: None,
            ..CpuThrottleConfig::default()
        };

        let _ = config.default_group_weight();
    }
}
