// Copyright 2018 TiKV Project Authors. Licensed under Apache-2.0.
mod chunk;
mod column;

pub use crate::coprocessor::codec::{Error, Result};

pub use self::chunk::{Chunk, ChunkEncoder};

#[cfg(test)]
mod tests {
    use cop_datatype::FieldTypeAccessor;
    use cop_datatype::FieldTypeTp;
    use tipb::expression::FieldType;

    pub fn field_type(tp: FieldTypeTp) -> FieldType {
        let mut fp = FieldType::new();
        fp.as_mut_accessor().set_tp(tp);
        fp
    }
}
