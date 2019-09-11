// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

//! Builder utilities for making type representations. Currently only includes
//! `FieldTypeBuilder` for building the `FieldType` protobuf message.

mod field_type;

pub use self::field_type::FieldTypeBuilder;
