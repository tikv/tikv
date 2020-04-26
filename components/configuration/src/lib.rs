// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;
use std::fmt::{self, Debug, Display, Formatter};

pub use configuration_derive::*;

pub type ConfigChange = HashMap<String, ConfigValue>;

#[derive(Clone, PartialEq)]
pub enum ConfigValue {
    Duration(u64),
    Size(u64),
    U64(u64),
    F64(f64),
    I32(i32),
    U32(u32),
    Usize(usize),
    Bool(bool),
    String(String),
    BlobRunMode(String),
    Module(ConfigChange),
    Skip,
}

impl Display for ConfigValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ConfigValue::Duration(v) => write!(f, "{}ms", v),
            ConfigValue::Size(v) => write!(f, "{}b", v),
            ConfigValue::U64(v) => write!(f, "{}", v),
            ConfigValue::F64(v) => write!(f, "{}", v),
            ConfigValue::I32(v) => write!(f, "{}", v),
            ConfigValue::U32(v) => write!(f, "{}", v),
            ConfigValue::Usize(v) => write!(f, "{}", v),
            ConfigValue::Bool(v) => write!(f, "{}", v),
            ConfigValue::String(v) => write!(f, "{}", v),
            ConfigValue::BlobRunMode(v) => write!(f, "{}", v),
            ConfigValue::Module(v) => write!(f, "{:?}", v),
            ConfigValue::Skip => write!(f, "ConfigValue::Skip"),
        }
    }
}

impl Debug for ConfigValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

macro_rules! impl_from {
    ($from: ty, $to: tt) => {
        impl From<$from> for ConfigValue {
            fn from(r: $from) -> ConfigValue {
                ConfigValue::$to(r)
            }
        }
    };
}
impl_from!(u64, U64);
impl_from!(f64, F64);
impl_from!(i32, I32);
impl_from!(u32, U32);
impl_from!(usize, Usize);
impl_from!(bool, Bool);
impl_from!(String, String);
impl_from!(ConfigChange, Module);

macro_rules! impl_into {
    ($into: ty, $from: tt) => {
        impl Into<$into> for ConfigValue {
            fn into(self) -> $into {
                if let ConfigValue::$from(v) = self {
                    v
                } else {
                    panic!(
                        "expect: {:?}, got: {:?}",
                        format!("ConfigValue::{}", stringify!($from)),
                        self
                    );
                }
            }
        }
    };
}
impl_into!(u64, U64);
impl_into!(f64, F64);
impl_into!(i32, I32);
impl_into!(u32, U32);
impl_into!(usize, Usize);
impl_into!(bool, Bool);
impl_into!(String, String);
impl_into!(ConfigChange, Module);

// TODO: remove
pub struct RollbackCollector<'a, 'b, T> {
    pub cfg: &'a T,
    change: &'b mut ConfigChange,
}

impl<'a, 'b, T> RollbackCollector<'a, 'b, T> {
    pub fn new(cfg: &'a T, change: &'b mut ConfigChange) -> Self {
        RollbackCollector { cfg, change }
    }

    pub fn push(&mut self, name: String, val: impl Into<ConfigValue>) {
        self.change.insert(name, val.into());
    }
}

#[macro_export]
macro_rules! rollback_or {
    ($rollback: ident, $($name: ident),+ , $err: block) => {{
        let err = $err;
        if let Some(rb_collector) = &mut $rollback {
            $(
                rb_collector.push(
                    stringify!($name).to_owned(),
                    rb_collector.cfg.$name.clone()
                );
            )*
            warn!("Invalid config"; "err" => ?err)
        } else { return err; }
    }};
    ($rollback: ident, $name: ident, $valid_or_rb: expr, $else_branch: expr) => {
        if let Some(rb_collector) = &mut $rollback {
            let mut r = std::collections::HashMap::new();
            let sub_rb_collector = RollbackCollector::new(&rb_collector.cfg.$name, &mut r);
            let _ = $valid_or_rb(Some(sub_rb_collector));
            if !r.is_empty() {
                rb_collector.push(stringify!($name).to_owned(), r);
            }
        } else {$else_branch;}
    };
}

/// The Configuration trait
///
/// There are four type of fields inside derived Configuration struct:
/// 1. `#[config(skip)]` field, these fields will not return
/// by `diff` method and have not effect of `update` method
/// 2. `#[config(hidden)]` field, these fields have the same effect of
/// `#[config(skip)]` field, in addition, these fields will not appear
/// at the output of serializing `Self::Encoder`
/// 3. `#[config(submodule)]` field, these fields represent the
/// submodule, and should also derive `Configuration`
/// 4. normal fields, the type of these fields should be implment
/// `Into` and `From` for `ConfigValue`
pub trait Configuration<'a> {
    type Encoder: serde::Serialize;
    /// Compare to other config, return the difference
    fn diff(&self, _: &Self) -> ConfigChange;
    /// Update config with difference returned by `diff`
    fn update(&mut self, _: ConfigChange);
    /// Get encoder that can be serialize with `serde::Serializer`
    /// with the disappear of `#[config(hidden)]` field
    fn get_encoder(&'a self) -> Self::Encoder;
    /// Get all fields and their type of the config
    fn typed(&self) -> ConfigChange;
}

pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub trait ConfigManager: Send + Sync {
    fn dispatch(&mut self, _: ConfigChange) -> Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate as configuration;

    #[derive(Clone, Configuration, Debug, Default, PartialEq)]
    pub struct TestConfig {
        field1: usize,
        field2: String,
        #[config(skip)]
        skip_field: u64,
        #[config(submodule)]
        submodule_field: SubConfig,
    }

    #[derive(Clone, Configuration, Debug, Default, PartialEq)]
    pub struct SubConfig {
        field1: u64,
        field2: bool,
        #[config(skip)]
        skip_field: String,
    }

    #[test]
    fn test_update_fields() {
        let mut cfg = TestConfig::default();
        let mut updated_cfg = cfg.clone();
        {
            // update fields
            updated_cfg.field1 = 100;
            updated_cfg.field2 = "1".to_owned();
            updated_cfg.submodule_field.field1 = 1000;
            updated_cfg.submodule_field.field2 = true;
        }
        let diff = cfg.diff(&updated_cfg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 3);
            assert_eq!(diff.remove("field1").map(Into::into), Some(100usize));
            assert_eq!(diff.remove("field2").map(Into::into), Some("1".to_owned()));
            // submodule should also be updated
            let sub_m = diff.remove("submodule_field").map(Into::into);
            assert!(sub_m.is_some());
            let mut sub_diff: ConfigChange = sub_m.unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(1000u64));
            assert_eq!(sub_diff.remove("field2").map(Into::into), Some(true));
        }
        cfg.update(diff);
        assert_eq!(cfg, updated_cfg, "cfg should be updated");
    }

    #[test]
    fn test_not_update() {
        let mut cfg = TestConfig::default();
        let diff = cfg.diff(&cfg.clone());
        assert!(diff.is_empty(), "diff should be empty");

        cfg.update(diff);
        assert_eq!(cfg, TestConfig::default(), "cfg should not be updated");
    }

    #[test]
    fn test_update_skip_field() {
        let mut cfg = TestConfig::default();
        let mut updated_cfg = cfg.clone();

        updated_cfg.skip_field = 100;
        assert!(cfg.diff(&updated_cfg).is_empty(), "diff should be empty");

        let mut diff = HashMap::new();
        diff.insert("skip_field".to_owned(), ConfigValue::U64(123));
        cfg.update(diff);
        assert_eq!(cfg, TestConfig::default(), "cfg should not be updated");
    }

    #[test]
    fn test_update_submodule() {
        let mut cfg = TestConfig::default();
        let mut updated_cfg = cfg.clone();

        updated_cfg.submodule_field.field1 = 12345;
        updated_cfg.submodule_field.field2 = true;

        let diff = cfg.diff(&updated_cfg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 1);
            let mut sub_diff: ConfigChange =
                diff.remove("submodule_field").map(Into::into).unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(12345u64));
            assert_eq!(sub_diff.remove("field2").map(Into::into), Some(true));
        }

        cfg.update(diff);
        assert_eq!(
            cfg.submodule_field, updated_cfg.submodule_field,
            "submodule should be updated"
        );
        assert_eq!(cfg, updated_cfg, "cfg should be updated");
    }

    #[test]
    fn test_hidden_field() {
        use serde::Serialize;

        #[derive(Configuration, Default, Serialize)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct TestConfig {
            #[config(skip)]
            skip_field: String,
            #[config(hidden)]
            hidden_field: u64,
            #[config(submodule)]
            submodule_field: SubConfig,
        }

        #[derive(Configuration, Default, Serialize)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct SubConfig {
            #[serde(rename = "rename_field")]
            bool_field: bool,
            #[config(hidden)]
            hidden_field: usize,
        }

        let cfg = SubConfig::default();
        assert_eq!(
            toml::to_string(&cfg).unwrap(),
            "rename_field = false\nhidden-field = 0\n"
        );
        assert_eq!(
            toml::to_string(&cfg.get_encoder()).unwrap(),
            "rename_field = false\n"
        );

        let cfg = TestConfig::default();
        assert_eq!(
            toml::to_string(&cfg).unwrap(),
            "skip-field = \"\"\nhidden-field = 0\n\n[submodule-field]\nrename_field = false\nhidden-field = 0\n"
        );
        assert_eq!(
            toml::to_string(&cfg.get_encoder()).unwrap(),
            "skip-field = \"\"\n\n[submodule-field]\nrename_field = false\n"
        );
    }
}
