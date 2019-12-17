// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use crate::codec::Result;

// use codec::prelude::{NumberDecoder, NumberEncoder};
use std::string::ToString;

#[derive(Debug, Clone)]
pub struct Enum {
    name: String,
    value: u64,
}

impl Enum {
    // Constructors
    pub fn new(name: impl Into<String>, value: u64) -> Self {
        Enum {
            name: name.into(),
            value,
        }
    }

    pub fn try_from_name(name: impl AsRef<str>, elems: Vec<String>) -> Result<Self> {
        let name = name.as_ref();
        let value = elems.iter().position(|x| x.as_str() == name);
        if let Some(pos) = value {
            Ok(Enum {
                name: name.to_owned(),
                value: pos as u64,
                // elems,
            })
        } else {
            unimplemented!();
        }
    }

    pub fn try_from_value(value: u64, elems: Vec<String>) -> Result<Self> {
        let idx = value as usize;
        if idx == 0 || idx > elems.len() {
            unimplemented!();
        } else {
            Ok(Enum {
                name: elems[idx - 1].to_owned(),
                value,
                // elems,
            })
        }
    }

    // Readers
    pub fn get_name(&self) -> &str {
        self.name.as_str()
    }

    pub fn get_value(&self) -> u64 {
        self.value
    }
}

impl PartialEq for Enum {
    fn eq(&self, rhs: &Self) -> bool {
        self.name == rhs.name
    }
}

impl Eq for Enum {}

impl ToString for Enum {
    fn to_string(&self) -> String {
        self.get_name().to_string()
    }
}

// pub trait EnumEncoder: NumberEncoder
