// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! This crate stores data types which used by other tidb query related crates.

#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(test)]
#![feature(str_internals)]

#[macro_use]
extern crate num_derive;
#[macro_use]
extern crate static_assertions;
#[macro_use(box_err, box_try, try_opt, error, warn)]
extern crate tikv_util;

#[macro_use]
extern crate bitflags;
#[allow(unused_extern_crates)]
extern crate tikv_alloc;

pub mod builder;
pub mod def;
pub mod error;

pub mod prelude {
    pub use super::def::FieldTypeAccessor;
}

pub use self::{def::*, error::*};

#[cfg(test)]
extern crate test;

pub mod codec;
pub mod expr;
