use std::collections::HashMap;
use tikv_util::config::{ReadableDuration, ReadableSize};

pub use config_derive::*;

pub type PartialChange = HashMap<String, ConfigValue>;

#[derive(Clone, PartialEq)]
pub enum ConfigValue {
    Duration(ReadableDuration),
    Size(ReadableSize),
    U64(u64),
    F64(f64),
    Usize(usize),
    Bool(bool),
    String(String),
    Module(PartialChange),
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
impl_from!(PartialChange, Module);

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
impl_into!(PartialChange, ConfigValue::Module(v), v);

pub trait Configable {
    fn diff(&self, _: Self) -> PartialChange;
    fn update(&mut self, _: PartialChange);
}
