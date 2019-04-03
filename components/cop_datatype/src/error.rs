// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.

#[derive(Debug, Fail)]
pub enum DataTypeError {
    #[fail(display = "Unsupported type: {}", name)]
    UnsupportedType { name: String },
}
