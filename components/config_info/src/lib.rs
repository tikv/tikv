// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

pub use config_info_derive::*;
use serde::Serialize;

/// ConfigInfo is a proc-macro to auto-generate a Serialize for target config struct.
///
/// it support following field attributes with the format `#[config_info(...)]`
/// - type. Explicitly set the config type of target field.
/// - default_desc. Describe the default value with a given string.
/// - min. The minimal valid value of target field. Must be a literal value with the same type or
///        can be coverted to the type by `TryInfo::try_into`.
/// - min_desc. Describe the minimal valid value as a string.
/// - max. The maximal valid value of target field. Must be a literal value with the same type or
///        can be coverted to the type by `TryInfo::try_into`.
/// - max_desc. Describe the maximal valid value as a string.
/// - options. A list of value that define the set of all valid values.
/// - skip. Skip generate config info for this field.
/// - submodule. This field is a submodule.
///
///
/// # Field Type(#[config_info(type= "..")])
///
/// this attribue the value type in the config file, all valid options are:
/// - Number. Represent a numeric value, auto-assigned for all primitive numerics types.
/// - Array. Represent the array type. auto-assigned for `[T, N]` and `Vec<T>` types.
/// - Stirng. Represent string type. auto-assigned for `String`, `ReadableSize` and `ReadableDuration`.
/// - Boolean. Represent the bool type. auto-assigned for `bool`.
/// - Map. Represent the map type. auto-assigned for `HashMap` and `BTreeMap`.
///
/// NOTE: The `type` attribute should be explicitly set if target field type is not build-in supported。
///
///
/// # Field Value Bound (#config_info(min = .., max = ..))
///
/// The `min` and `max` attribue define the lower and upper bound of target field. The provide literal
/// value must be the same type with target field or can be converted to target type with `TryInto::try_into`.
///
/// # Field Value Bound Description(#config_info(default_desc="..", min_desc = "...", max_desc = "..."))
///
/// All the config types in TiKV has implement the `Default` trait, so the `ConfigInfo` marco by default
/// generate the `default` value by the `Default` implementation. But some config fields' default value is
/// based on some env parameters(such as the CPU or Memory quota), so output the default value is some what
/// misleading. In this can, user can explicitly set the `default_desc` attribue to privide a string to
/// describe the default value choosen rule.
///
/// This same rule also applies for the `min_desc` and `max_desc`.
///
/// # Config Value Options(#[config_info(opitons= [ .., .. ])])
/// The `options` attribue define the set of all valid valid for target field. The value of each element
/// must be either the same type with target field or can be converted to target type with `TryInto::try_into`.
///
/// Example:
/// Please refer the unit test at the bottom of the file.
///
pub trait ConfigInfo {
    type Encoder: Serialize;
    /// Get encoder that can be serialize with `serde::Serializer`
    /// with the disappear of `#[config_info(skip)]` field
    fn get_cfg_encoder(&self, cfg: &Self) -> Self::Encoder;
}

// this is the stub Serialize stuct for each config field
#[doc(hidden)]
#[derive(Serialize)]
#[serde(default)]
#[serde(rename_all = "PascalCase")]
pub struct FieldInfo<T: Serialize> {
    #[serde(rename = "Type")]
    field_type: FieldCfgType,
    #[serde(skip_serializing_if = "Option::is_none")]
    min_value: Option<ConfigValue<T>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_value: Option<ConfigValue<T>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_options: Option<Vec<T>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    default_value: Option<ConfigValue<T>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_in_file: Option<T>,
    description: String,
}

impl<T: Serialize> FieldInfo<T> {
    pub fn new(
        field_type: FieldCfgType,
        default_value: Option<ConfigValue<T>>,
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

    pub fn set_min_value(mut self, v: ConfigValue<T>) -> Self {
        self.min_value = Some(v);
        self
    }

    pub fn set_max_value(mut self, v: ConfigValue<T>) -> Self {
        self.max_value = Some(v);
        self
    }

    pub fn set_value_options(mut self, ops: Vec<T>) -> Self {
        self.value_options = Some(ops);
        self
    }
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum ConfigValue<T: Serialize> {
    // a concrete value of type T
    Concrete(T),
    // a string description.
    Desc(String),
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
    use serde_json::{from_str, to_string_pretty, Value};
    use tikv_util::config::{ReadableDuration, ReadableSize};

    use crate::{self as config_info, ConfigInfo};

    #[derive(Serialize, Deserialize, ConfigInfo)]
    struct Config {
        // this is a hidden field that should be skipped in config info.
        #[config_info(skip)]
        _hidden: i32,
        /// This is an i64 field with three integer options,
        /// the comment can across multiple lines.
        ///
        /// But not this line because of the privious empty line.
        #[config_info(options = r#"[0, 10, 20]"#)]
        a: i64,
        #[config_info(min = 0, max = 10)]
        /// b is an optional i32 field with not-none default value
        b: Option<i32>,
        #[config_info(options = r#"["debug", "info"]"#)]
        /// c is a string value with 2 string options
        c: String,
        // a sub module field
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
        /// This is a field that it's value bound depend on other field or the env variable.
        #[config_info(
            default_desc = "MAX(4, CPU * 0.8)",
            min_desc = "value of `a`",
            max_desc = "`a` * 5"
        )]
        g: u64,
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
                g: 10,
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
        let str_value = to_string_pretty(&cfg.get_cfg_encoder(&new_cfg)).unwrap();
        println!("{}", str_value);
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
                "Description": "This is an i64 field with three integer options, the comment can across multiple lines."
            },
            "b": {
                "Type": "Number",
                "MinValue": 0,
                "MaxValue": 10,
                "DefaultValue": 1,
                "Description": "b is an optional i32 field with not-none default value"
            },
            "c": {
                "Type": "String",
                "ValueOptions": [
                    "debug",
                    "info"
              ],
                "DefaultValue": "test",
                "ValueInFile": "test123",
                "Description": "c is a string value with 2 string options"
            },
            "sub": {
                "h": {
                    "Type": "String",
                    "DefaultValue": "3KiB",
                    "Description": "test submodule field"
                }
            },
            "d": {
                "Type": "String",
                "MaxValue": "100ms",
                "ValueInFile": "2s",
                "Description": "d is an optional field with default None."
            },
            "e": {
                "Type": "String",
                "ValueOptions": [
                    "1MiB",
                    "10KiB"
                ],
                "DefaultValue": "1MiB",
                "Description": "e is a custom type with 2 string options."
            },
            "f": {
                "Type": "Number",
                "DefaultValue": 1,
                "Description": "a custom field with manually assigned type"
            },
            "g": {
                "Type": "Number",
                "MinValue": "value of `a`",
                "MaxValue": "`a` * 5",
                "DefaultValue": "MAX(4, CPU * 0.8)",
                "Description": "This is a field that it's value bound depend on other field or the env variable."
            }
          }"###;
        let source_obj = from_str::<Value>(&*str_value).unwrap();
        let expected_obj = from_str::<Value>(expected).unwrap();
        assert_eq!(source_obj, expected_obj);
    }
}
