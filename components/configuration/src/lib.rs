// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::HashMap;

pub use configuration_derive::*;

pub type ConfigChange = HashMap<String, ConfigValue>;

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValue {
    Duration(u64),
    Size(u64),
    U64(u64),
    F64(f64),
    Usize(usize),
    Bool(bool),
    String(String),
    Module(ConfigChange),
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
                        format!("ConfigValue::{:}", stringify!($from)),
                        self
                    );
                }
            }
        }
    };
}
impl_into!(u64, U64);
impl_into!(f64, F64);
impl_into!(usize, Usize);
impl_into!(bool, Bool);
impl_into!(String, String);
impl_into!(ConfigChange, Module);

/// the Configuration trait
/// There are three type of fields inside derived Configuration struct:
/// 1. `#[config(skip)]` field, these fields will not return
/// by `diff` method and have not effect of `update` method
/// 2. `#[config(submodule)]` field, these fields represent the
/// submodule, and should also derive `Configuration`
/// 3. normal fields, the type of these fields should be implment
/// `Into` and `From` for `ConfigValue`
pub trait Configuration {
    /// Compare to other config, return the difference
    fn diff(&self, _: &Self) -> ConfigChange;
    /// Update config with difference returned by `diff`
    fn update(&mut self, _: ConfigChange);
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
}
