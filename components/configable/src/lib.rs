use std::collections::HashMap;
use tikv_util::config::{ReadableDuration, ReadableSize};

pub use config_derive::*;

pub type ConfigChange = HashMap<String, ConfigValue>;

#[derive(Clone, Debug, PartialEq)]
pub enum ConfigValue {
    Duration(ReadableDuration),
    Size(ReadableSize),
    U64(u64),
    F64(f64),
    Usize(usize),
    Bool(bool),
    String(String),
    Module(ConfigChange),
}

macro_rules! impl_from {
    ($from: ty, $to: ident) => {
        impl From<$from> for ConfigValue {
            fn from(r: $from) -> ConfigValue {
                ConfigValue::$to(r)
            }
        }
    };
}
impl_from!(ReadableDuration, Duration);
impl_from!(ReadableSize, Size);
impl_from!(u64, U64);
impl_from!(f64, F64);
impl_from!(usize, Usize);
impl_from!(bool, Bool);
impl_from!(String, String);
impl_from!(ConfigChange, Module);

macro_rules! impl_into {
    ($into: ty, $from: pat, $v: ident) => {
        impl Into<$into> for ConfigValue {
            fn into(self) -> $into {
                if let $from = self {
                    $v
                } else {
                    unreachable!()
                }
            }
        }
    };
}
impl_into!(ReadableDuration, ConfigValue::Duration(v), v);
impl_into!(ReadableSize, ConfigValue::Size(v), v);
impl_into!(u64, ConfigValue::U64(v), v);
impl_into!(f64, ConfigValue::F64(v), v);
impl_into!(usize, ConfigValue::Usize(v), v);
impl_into!(bool, ConfigValue::Bool(v), v);
impl_into!(String, ConfigValue::String(v), v);
impl_into!(ConfigChange, ConfigValue::Module(v), v);

/// the Configable trait
/// There are three type of fields inside derived Configable struct:
/// 1. `#[config(not_support)]` field, these fields will not return
/// by `diff` method and have not effect of `update` method
/// 2. `#[config(submodule)]` field, these fields represent the
/// submodule, and should also derive `Configable`
/// 3. normal fields, the type of these fields should be implment
/// `Into` and `From` for `ConfigValue`
pub trait Configable {
    /// Compare to other config, return the difference
    fn diff(&self, _: &Self) -> ConfigChange;
    /// Update config with difference returned by `diff`
    fn update(&mut self, _: ConfigChange);
}

mod tests {
    use super::*;
    use crate as configable;
    use tikv_util::config::{ReadableDuration, ReadableSize};

    #[derive(Clone, Configable, Debug, PartialEq)]
    pub struct TestConfig {
        field1: usize,
        field2: String,
        field3: ReadableSize,
        #[config(not_support)]
        not_support_field: u64,
        #[config(submodule)]
        submodule_field: SubConfig,
    }

    #[derive(Clone, Configable, Debug, PartialEq)]
    pub struct SubConfig {
        field1: u64,
        field2: ReadableDuration,
        #[config(not_support)]
        not_support_field: String,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            TestConfig {
                field1: 0,
                field2: "".to_owned(),
                field3: ReadableSize::mb(0),
                not_support_field: 0,
                submodule_field: SubConfig {
                    field1: 0,
                    field2: ReadableDuration::millis(0),
                    not_support_field: "".to_owned(),
                },
            }
        }
    }

    #[test]
    fn test_update_fields() {
        let mut cfg = TestConfig::default();
        let mut updated_cfg = cfg.clone();
        {
            // update fields
            updated_cfg.field1 = 100;
            updated_cfg.field2 = "1".to_owned();
            updated_cfg.field3 = ReadableSize::gb(1);
            updated_cfg.submodule_field.field1 = 1000;
            updated_cfg.submodule_field.field2 = ReadableDuration::secs(1);
        }
        let diff = cfg.diff(&updated_cfg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 4);
            assert_eq!(diff.remove("field1").map(Into::into), Some(100usize));
            assert_eq!(diff.remove("field2").map(Into::into), Some("1".to_owned()));
            assert_eq!(
                diff.remove("field3").map(Into::into),
                Some(ReadableSize::gb(1))
            );
            // submodule should also be updated
            let sub_m = diff.remove("submodule_field").map(Into::into);
            assert!(sub_m.is_some());
            let mut sub_diff: ConfigChange = sub_m.unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(1000u64));
            assert_eq!(
                sub_diff.remove("field2").map(Into::into),
                Some(ReadableDuration::secs(1))
            );
        }
        cfg.update(diff);
        assert_eq!(cfg, updated_cfg, "cfg should be updated");
    }

    #[test]
    fn test_not_update() {
        let mut cfg = TestConfig::default();
        let diff = cfg.diff(&cfg.clone());
        assert!(diff.is_empty(), "diff should be empty");

        let cfg_clone = cfg.clone();
        cfg.update(diff);
        assert_eq!(cfg, cfg_clone, "cfg should not be updated");
    }

    #[test]
    fn test_update_not_support() {
        let mut cfg = TestConfig::default();

        let mut updated_cfg = cfg.clone();
        updated_cfg.not_support_field = 100;
        assert!(cfg.diff(&updated_cfg).is_empty(), "diff should be empty");

        let mut diff = HashMap::new();
        diff.insert("not_support_field".to_owned(), ConfigValue::U64(0));
        cfg.update(diff);
        assert_eq!(cfg, TestConfig::default(), "cfg should not be updated");
    }

    #[test]
    fn test_update_submodule() {
        let mut cfg = TestConfig::default();
        let mut updated_cfg = cfg.clone();

        updated_cfg.submodule_field.field1 = 12345;
        updated_cfg.submodule_field.field2 = ReadableDuration::secs(1);

        let diff = cfg.diff(&updated_cfg);
        {
            let mut diff = diff.clone();
            assert_eq!(diff.len(), 1);
            let mut sub_diff: ConfigChange =
                diff.remove("submodule_field").map(Into::into).unwrap();
            assert_eq!(sub_diff.len(), 2);
            assert_eq!(sub_diff.remove("field1").map(Into::into), Some(12345u64));
            assert_eq!(
                sub_diff.remove("field2").map(Into::into),
                Some(ReadableDuration::secs(1))
            );
        }

        cfg.update(diff);
        assert_eq!(
            cfg.submodule_field, updated_cfg.submodule_field,
            "submodule should be updated"
        );
        assert_eq!(cfg, updated_cfg, "cfg should be updated");
    }
}
