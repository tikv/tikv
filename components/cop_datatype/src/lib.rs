// Copyright 2018 TiKV Project Authors.
#![cfg_attr(test, feature(test))]

#[macro_use]
extern crate bitflags;
#[macro_use]
extern crate enum_primitive_derive;
#[macro_use]
extern crate failure;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

mod def;
mod error;

pub mod prelude {
    pub use super::def::FieldTypeAccessor;
}

pub use self::def::*;
pub use self::error::*;
