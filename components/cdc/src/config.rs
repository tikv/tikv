use tikv_util::config::ReadableDuration;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub cdc_scan_worker_pool_size: usize,
    pub cdc_min_ts_interval: ReadableDuration,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            cdc_scan_worker_pool_size: 4,
            cdc_min_ts_interval: ReadableDuration::secs(1),
        }
    }
}
