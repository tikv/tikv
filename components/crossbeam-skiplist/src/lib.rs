//! TODO

#![doc(test(
    no_crate_inject,
    attr(
        deny(warnings, rust_2018_idioms),
        allow(dead_code, unused_assignments, unused_variables)
    )
))]
#![warn(missing_docs, missing_debug_implementations, rust_2018_idioms)]

extern crate alloc;

use crossbeam_epoch as epoch;
use crossbeam_utils as utils;

pub mod base;
#[doc(inline)]
pub use crate::base::SkipList;

pub mod map;
#[doc(inline)]
pub use crate::map::SkipMap;

pub mod set;
#[doc(inline)]
pub use crate::set::SkipSet;
