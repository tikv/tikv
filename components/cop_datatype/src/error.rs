// Copyright 2018 TiKV Project Authors.
#[derive(Debug, Fail)]
pub enum DataTypeError {
    #[fail(display = "Unsupported type: {}", name)]
    UnsupportedType { name: String },
}
