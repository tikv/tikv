// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub use config_info_derive::*;
use serde::Serialize;

pub trait ConfigInfo {
    type Encoder: Serialize;
    /// Get encoder that can be serialize with `serde::Serializer`
    /// with the disappear of `#[online_config(hidden)]` field
    fn get_cfg_encoder(&self, cfg: &Self) -> Self::Encoder;
}

#[derive(Serialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldInfo<T> {
    #[serde(rename = "Type")]
    field_type: FieldCfgType,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_value: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_value: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_options: Option<Vec<T>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_value: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_in_file: Option<T>,
    description: String,
}

impl<T: Serialize> FieldInfo<T> {
    pub fn new(
        field_type: FieldCfgType,
        default_value: Option<T>,
        value_in_file: Option<T>,
        description: String,
    ) -> Self {
        Self {
            field_type,
            min_value: None,
            max_value: None,
            value_options: None,
            default_value,
            value_in_file,
            description,
        }
    }

    pub fn set_min_value(mut self, v: T) -> Self {
        self.min_value = Some(v);
        self
    }

    pub fn set_max_value(mut self, v: T) -> Self {
        self.max_value = Some(v);
        self
    }

    pub fn set_value_options(mut self, ops: Vec<T>) -> Self {
        self.value_options = Some(ops);
        self
    }
}

#[derive(Serialize)]
pub enum FieldCfgType {
    /// represent `[T; N]` and `Vec<T>` types.
    Array,
    /// represent `bool` type
    Boolean,
    /// represent map types `HashMap<K, V>` or `BTreeMap<K, V>`
    Map,
    /// represent all numeric types like `i32`, `u64`, `f64`
    Number,
    /// represent all other types that input as string.
    String,
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};
    use serde_json::{from_str, to_string, Value};
    use tikv_util::config::{ReadableDuration, ReadableSize};

    use crate::{self as config_info, ConfigInfo};

    #[derive(Serialize, Deserialize, ConfigInfo)]
    struct Config {
        // this is a hidden field that should be skipped in config info.
        #[config_info(skip)]
        _hidden: i32,
        /// a is an  i64 field with three integer options.
        ///
        /// NOTE: this comment is invisible.
        #[config_info(options = r#"[0, 10, 20]"#)]
        a: i64,
        #[config_info(min = 0, max = 10)]
        /// b is an optional i32 field with not-none default value
        b: Option<i32>,
        #[config_info(options = r#"["debug", "info"]"#)]
        /// c is a string value with 2 string options
        c: String,
        #[config_info(submodule)]
        sub: SubConfig,
        #[config_info(max = "100ms")]
        /// d is an optional field with default None.
        d: Option<ReadableDuration>,
        /// e is a custom type with 2 string options.
        #[config_info(options = r#"["1MB", "10KB"]"#)]
        e: ReadableSize,
        #[config_info(type = "Number")]
        /// a custom field with manually assigned type
        f: NewType,
    }

    impl Default for Config {
        fn default() -> Self {
            Self {
                _hidden: 0,
                a: 10,
                b: Some(1),
                c: "test".into(),
                sub: SubConfig {
                    h: ReadableSize::kb(3),
                },
                d: None,
                e: ReadableSize::mb(1),
                f: NewType(1),
            }
        }
    }

    #[derive(Serialize, Deserialize, ConfigInfo)]
    struct SubConfig {
        /// test submodule field
        h: ReadableSize,
    }

    #[derive(Serialize, Deserialize, Default, Clone, PartialEq)]
    struct NewType(i32);

    #[test]
    fn test_config() {
        let cfg = Config::default();
        let mut new_cfg = Config::default();
        new_cfg.a = 20;
        new_cfg.c = "test123".into();
        new_cfg.d = Some(ReadableDuration::secs(2));
        let str_value = to_string(&cfg.get_cfg_encoder(&new_cfg)).unwrap();
        let expected = r###"
          {
            "a": {
              "Type": "Number",
              "ValueOptions": [
                0,
                10,
                20
              ],
              "DefaultValue": 10,
              "ValueInFile": 20,
              "Description": " a is an  i64 field with three integer options."
            },
            "b": {
              "Type": "Number",
              "MinValue": 0,
              "MaxValue": 10,
              "DefaultValue": 1,
              "Description": " b is an optional i32 field with not-none default value"
            },
            "c": {
              "Type": "String",
              "ValueOptions": [
                "debug",
                "info"
              ],
              "DefaultValue": "test",
              "ValueInFile": "test123",
              "Description": " c is a string value with 2 string options"
            },
            "sub": {
              "h": {
                "Type": "String",
                "DefaultValue": "3KiB",
                "Description": " test submodule field"
              }
            },
            "d": {
              "Type": "String",
              "MaxValue": "100ms",
              "ValueInFile": "2s",
              "Description": " d is an optional field with default None."
            },
            "e": {
              "Type": "String",
              "ValueOptions": [
                "1MiB",
                "10KiB"
              ],
              "DefaultValue": "1MiB",
              "Description": " e is a custom type with 2 string options."
            },
            "f": {
              "Type": "Number",
              "DefaultValue": 1,
              "Description": " a custom field with manually assigned type"
            }
          }"###;
        let source_obj = from_str::<Value>(&*str_value).unwrap();
        let expected_obj = from_str::<Value>(expected).unwrap();
        assert_eq!(source_obj, expected_obj);
    }
}
