// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod gbk;
mod latin1;
mod utf8;

pub use gbk::*;
pub use latin1::*;
pub use utf8::*;

use std::str;

use super::Encoding;
use crate::codec::data_type::{Bytes, BytesRef};
use crate::codec::Error;
use crate::codec::Result;
