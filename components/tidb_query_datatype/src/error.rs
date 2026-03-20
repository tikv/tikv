// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use thiserror::Error;

#[derive(Debug, Error)]
#[allow(clippy::enum_variant_names)]
pub enum DataTypeError {
    #[error("Unsupported type: {name}")]
    UnsupportedType { name: String },

    #[error("Unsupported collation code: {code}")]
    UnsupportedCollation { code: i32 },

    #[error("Unsupported charset : {name}")]
    UnsupportedCharset { name: String },
}
