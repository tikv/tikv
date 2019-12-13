// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::cmp::{Ord, Ordering};
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use kvproto::configpb::{self, StatusCode};

use super::TiKvConfig;
use crate::raftstore::store::PdTask;
use configuration::{ConfigChange, ConfigValue, Configuration};
use pd_client::PdClient;
use tikv_util::config::{ReadableDuration, ReadableSize};
use tikv_util::worker::FutureScheduler;

fn to_config_entry(change: ConfigChange) -> CfgResult<Vec<configpb::ConfigEntry>> {
    // This helper function translate nested module config to a list
    // of name/value pair and seperated module by '.' in the name field,
    // by recursive call helper function with an prefix which represent
    // th prefix of current module. And also compatible the field name
    // in config struct with the name in toml file.
    fn helper(prefix: String, change: ConfigChange) -> CfgResult<Vec<configpb::ConfigEntry>> {
        let mut entries = Vec::with_capacity(change.len());
        for (mut name, value) in change {
            if name == "raft_store" {
                name = "raftstore".to_owned();
            } else {
                name = name.replace("_", "-");
            }
            if !prefix.is_empty() {
                let mut p = prefix.clone();
                p.push_str(&format!(".{}", name));
                name = p;
            }
            if let ConfigValue::Module(change) = value {
                entries.append(&mut helper(name, change)?);
            } else {
                let mut e = configpb::ConfigEntry::default();
                e.set_name(name);
                e.set_value(from_change_value(value)?);
                entries.push(e);
            }
        }
        Ok(entries)
    };
    helper("".to_owned(), change)
}

fn from_change_value(v: ConfigValue) -> CfgResult<String> {
    let s = match v {
        ConfigValue::Duration(_) => {
            let v: ReadableDuration = v.into();
            toml::to_string(&v)?
        }
        ConfigValue::Size(_) => {
            let v: ReadableSize = v.into();
            toml::to_string(&v)?
        }
        ConfigValue::U64(ref v) => toml::to_string(v)?,
        ConfigValue::F64(ref v) => toml::to_string(v)?,
        ConfigValue::Usize(ref v) => toml::to_string(v)?,
        ConfigValue::Bool(ref v) => toml::to_string(v)?,
        ConfigValue::String(ref v) => toml::to_string(v)?,
        _ => unreachable!(),
    };
    Ok(s)
}

/// Comparing two `Version` with the assumption of `global` and `local`
/// should be monotonically increased, if `global` or `local` of _current config_
/// less than _incoming config_ means there are update in _incoming config_
pub fn cmp_version(current: &configpb::Version, incoming: &configpb::Version) -> Ordering {
    match (
        Ord::cmp(&current.local, &incoming.local),
        Ord::cmp(&current.global, &incoming.global),
    ) {
        (Ordering::Equal, Ordering::Equal) => Ordering::Equal,
        (Ordering::Less, _) | (_, Ordering::Less) => Ordering::Less,
        _ => Ordering::Greater,
    }
}

type CfgResult<T> = Result<T, Box<dyn Error>>;

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default)]
pub struct ConfigController {
    current: TiKvConfig,
}

impl ConfigController {
    pub fn new(cfg: TiKvConfig) -> Self {
        ConfigController { current: cfg }
    }

    fn update_or_rollback(&mut self, mut incoming: TiKvConfig) -> Option<ConfigChange> {
        if incoming.validate().is_err() {
            return Some(incoming.diff(&self.current));
        }
        let diff = self.current.diff(&incoming);
        if !diff.is_empty() {
            // TODO: dispatch change to each module
            self.current.update(diff);
        }
        None
    }

    pub fn get_current(&self) -> &TiKvConfig {
        &self.current
    }
}

pub struct ConfigHandler {
    id: String,
    version: configpb::Version,
    config_controller: ConfigController,
}

impl ConfigHandler {
    pub fn start(
        id: String,
        controller: ConfigController,
        version: configpb::Version,
        _scheduler: FutureScheduler<PdTask>,
    ) -> CfgResult<Self> {
        // TODO: currently we can't handle RefreshConfig task, because
        // PD have not implement such service yet.

        // if let Err(e) = scheduler.schedule(PdTask::RefreshConfig) {
        //     return Err(format!("failed to schedule refresh config task: {:?}", e).into());
        // }
        Ok(ConfigHandler {
            id,
            version,
            config_controller: controller,
        })
    }

    pub fn get_refresh_interval(&self) -> Duration {
        Duration::from(self.config_controller.current.refresh_config_interval)
    }

    pub fn get_id(&self) -> String {
        self.id.clone()
    }

    pub fn get_version(&self) -> &configpb::Version {
        &self.version
    }

    pub fn get_config(&self) -> &TiKvConfig {
        self.config_controller.get_current()
    }
}

impl ConfigHandler {
    /// Register the local config to pd
    pub fn create(
        id: String,
        pd_client: Arc<impl PdClient>,
        cfg: TiKvConfig,
    ) -> CfgResult<(configpb::Version, TiKvConfig)> {
        let cfg = toml::to_string(&cfg)?;
        let version = configpb::Version::default();
        let mut resp = pd_client.register_config(id, version, cfg)?;
        match resp.get_status().get_code() {
            StatusCode::Ok => {
                let cfg: TiKvConfig = toml::from_str(resp.get_config())?;
                Ok((resp.take_version(), cfg))
            }
            code => {
                debug!("failed to register config"; "status" => ?code);
                Err(format!("{:?}", resp).into())
            }
        }
    }

    /// Update the local config if remote config had been changed,
    /// rollback the remote config if the change are invalid.
    pub fn refresh_config(&mut self, pd_client: Arc<impl PdClient>) -> CfgResult<()> {
        let mut resp = pd_client.get_config(self.get_id(), self.version.clone())?;
        let version = resp.take_version();
        match resp.get_status().get_code() {
            StatusCode::NotChange => Ok(()),
            StatusCode::WrongVersion if cmp_version(&self.version, &version) == Ordering::Less => {
                let incoming: TiKvConfig = toml::from_str(resp.get_config())?;
                if let Some(rollback_change) = self.config_controller.update_or_rollback(incoming) {
                    debug!(
                        "tried to update local config to an invalid config";
                        "version" => ?resp.version
                    );
                    let entries = to_config_entry(rollback_change)?;
                    self.update_config(version, entries, pd_client)?;
                } else {
                    info!("local config updated"; "version" => ?resp.version);
                    self.version = version;
                }
                Ok(())
            }
            code => {
                debug!(
                    "failed to get remote config";
                    "status" => ?code,
                    "version" => ?version
                );
                Err(format!("{:?}", resp).into())
            }
        }
    }

    fn update_config(
        &mut self,
        version: configpb::Version,
        entries: Vec<configpb::ConfigEntry>,
        pd_client: Arc<impl PdClient>,
    ) -> CfgResult<()> {
        let mut resp = pd_client.update_config(self.get_id(), version, entries)?;
        match resp.get_status().get_code() {
            StatusCode::Ok => {
                self.version = resp.take_version();
                Ok(())
            }
            code => {
                debug!("failed to update remote config"; "status" => ?code);
                Err(format!("{:?}", resp).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TiKvConfig;
    use crate::tikv_util::config::ReadableDuration;
    use kvproto::configpb::Version;
    use std::cmp::Ordering;

    #[test]
    fn test_config_change_to_config_entry() {
        let old = TiKvConfig::default();
        let mut incoming = TiKvConfig::default();
        incoming.refresh_config_interval = ReadableDuration::hours(10);
        let diff = to_config_entry(old.diff(&incoming)).unwrap();
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].name, "refresh-config-interval");
        assert_eq!(
            diff[0].value,
            toml::to_string(&incoming.refresh_config_interval).unwrap()
        );
    }

    #[test]
    fn test_cmp_version() {
        fn new_version((g1, l1): (u64, u64), (g2, l2): (u64, u64)) -> (Version, Version) {
            let mut v1 = Version::default();
            v1.set_global(g1);
            v1.set_local(l1);
            let mut v2 = Version::default();
            v2.set_global(g2);
            v2.set_local(l2);
            (v1, v2)
        }

        let (v1, v2) = new_version((10, 10), (10, 10));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Equal);

        // either global or local of v1 less than global or local of v2
        // Ordering::Less shuold be returned
        let (small, big) = (10, 11);
        let (v1, v2) = new_version((small, 10), (big, 10));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);
        let (v1, v2) = new_version((small, 20), (big, 10));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);
        let (v1, v2) = new_version((small, 10), (big, 20));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);

        let (v1, v2) = new_version((10, small), (10, big));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);
        let (v1, v2) = new_version((20, small), (10, big));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);
        let (v1, v2) = new_version((10, small), (20, big));
        assert_eq!(cmp_version(&v1, &v2), Ordering::Less);
    }
}
