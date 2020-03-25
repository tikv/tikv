// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use failure::Fail;

#[derive(Debug, Fail)]
pub enum DataTypeError {
    #[fail(display = "Unsupported type: {}", name)]
    UnsupportedType { name: String },

    #[fail(display = "Unsupported collation code: {}", code)]
    UnsupportedCollation { code: i32 },
}
