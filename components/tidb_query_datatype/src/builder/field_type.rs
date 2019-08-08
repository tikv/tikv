// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use tipb::FieldType;

use crate::FieldTypeAccessor;

/// Helper to build `FieldType`.
pub struct FieldTypeBuilder(FieldType);

impl FieldTypeBuilder {
    pub fn new() -> Self {
        Self(FieldType::default())
    }

    pub fn tp(mut self, v: crate::FieldTypeTp) -> Self {
        FieldTypeAccessor::set_tp(&mut self.0, v);
        self
    }

    pub fn flag(mut self, v: crate::FieldTypeFlag) -> Self {
        FieldTypeAccessor::set_flag(&mut self.0, v);
        self
    }

    pub fn flen(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_flen(&mut self.0, v);
        self
    }

    pub fn decimal(mut self, v: isize) -> Self {
        FieldTypeAccessor::set_decimal(&mut self.0, v);
        self
    }

    pub fn collation(mut self, v: crate::Collation) -> Self {
        FieldTypeAccessor::set_collation(&mut self.0, v);
        self
    }

    pub fn build(self) -> FieldType {
        self.0
    }
}

impl From<FieldTypeBuilder> for FieldType {
    fn from(fp_builder: FieldTypeBuilder) -> FieldType {
        fp_builder.build()
    }
}
