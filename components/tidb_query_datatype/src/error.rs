// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum DataTypeError {
    #[error("Unsupported type: {name}")]
    UnsupportedType { name: String },

    #[error("Unsupported collation code: {code}")]
    UnsupportedCollation { code: i32 },

    #[error("Unsupported charset : {name}")]
    UnsupportedCharset { name: String },
}
